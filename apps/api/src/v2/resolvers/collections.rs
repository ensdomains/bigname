//! Exhaustive reads of supported resolver record links and per-registration permission rows.
use std::collections::BTreeMap;

use super::{
    parse_numeric_chain_id, product_resolver_reason, require_phase_target_snapshot,
    resolver_snapshot_scope,
};
use crate::v2::list_cursor::{ListCursor, ListPosition};
use crate::{
    AppState,
    v2::{
        Envelope, Page, QueryParamAllowlist, QueryParams, SnapshotReadResource, StrictQueryParams,
        V2Error, V2Result, encode_at_token, permission_powers_value, resolve_v2_snapshot_for,
        snapshot_meta,
    },
};
use axum::{
    Json,
    extract::{Path, State},
};
use serde_json::Value;

#[path = "collections/reads.rs"]
mod reads;

pub(crate) struct ResolverCollectionParams;
impl QueryParamAllowlist for ResolverCollectionParams {
    const ALLOWED: &'static [&'static str] = &["at", "finality", "cursor", "page_size"];
}
type ResolverCollectionQuery = StrictQueryParams<ResolverCollectionParams>;

pub(crate) async fn get_resolver_links(
    Path(path): Path<(String, String)>,
    params: ResolverCollectionQuery,
    State(state): State<AppState>,
) -> V2Result<Json<Envelope<Vec<Value>>>> {
    collection(path, params.into_inner(), state, "links").await
}

pub(crate) async fn get_resolver_roles(
    Path(path): Path<(String, String)>,
    params: ResolverCollectionQuery,
    State(state): State<AppState>,
) -> V2Result<Json<Envelope<Vec<Value>>>> {
    collection(path, params.into_inner(), state, "roles").await
}

async fn collection(
    (chain, address): (String, String),
    params: QueryParams,
    state: AppState,
    section: &str,
) -> V2Result<Json<Envelope<Vec<Value>>>> {
    let (chain_id, slug) = parse_numeric_chain_id(&chain)?;
    let address = crate::v2::support::parse_evm_address(&address, "address")
        .map_err(crate::v2::api_error_to_v2)?;
    let filters = BTreeMap::from([
        ("chain_id".to_owned(), chain_id.to_string()),
        ("resolver".to_owned(), address.clone()),
        ("section".to_owned(), section.to_owned()),
    ]);
    let list = ListCursor::new("identity_asc", filters);
    list.check_shape(
        params.cursor.as_deref(),
        &["key1", "key2"],
        params.at.is_some(),
    )?;
    let publication = crate::v2::collection_snapshot::CollectionSnapshot::capture_for_namespace(
        &state,
        None,
        Some(super::resolver_namespace(slug)?),
    )
    .await?;
    let selected = resolve_v2_snapshot_for(
        &state.pool,
        &resolver_snapshot_scope(slug)?,
        params.at.as_ref(),
        params.finality,
        SnapshotReadResource::Resolver,
    )
    .await?;
    let generations =
        crate::v2::lookup::head::load_selected_project_generations(&state.pool, &selected).await?;
    let list = list.pinned_at(params.at.is_some().then(|| encode_at_token(&selected)));
    let key = list
        .read(params.cursor.as_deref(), &["key1", "key2"])?
        .map(|position| {
            Ok((
                position.get("key1")?.to_owned(),
                position.get("key2")?.to_owned(),
            ))
        })
        .transpose()?;
    let row = bigname_storage::load_phase_resolver_current(&state.pool, slug, &address)
        .await
        .map_err(crate::v2::name_rows_error(
            SnapshotReadResource::Resolver,
            |_| read_error(),
        ))?
        .ok_or_else(|| V2Error::not_found("resolver was not found"))?;
    require_phase_target_snapshot(&row.chain_positions, slug, &selected)?;
    let summary_key = match section {
        "roles" => "role_holders",
        _ => "links",
    };
    let summary = row.declared_summary.get(summary_key);
    let mut meta = snapshot_meta(&selected)?;
    let supported = summary
        .is_some_and(|summary| summary.get("status").and_then(Value::as_str) == Some("supported"));
    let (mut rows, total) = if supported {
        let height = selected
            .chain_positions
            .as_map()
            .values()
            .find(|p| p.chain_id == slug)
            .ok_or_else(read_error)?
            .block_number;
        let mut publication_block_bounds = publication.block_bounds();
        if let Some(bound) = publication_block_bounds.get_mut(slug) {
            *bound = (*bound).min(height);
        }
        reads::page(
            &state.pool,
            slug,
            &address,
            section,
            (height, &publication_block_bounds),
            key.as_ref(),
            params.page_size,
        )
        .await?
    } else {
        meta.completeness = Some(crate::v2::vocab::Completeness::Unsupported);
        meta.unsupported_fields = Some(vec![section.to_owned()]);
        meta.unsupported_reason = Some(
            summary
                .and_then(|s| s.get("unsupported_reason"))
                .and_then(Value::as_str)
                .map(product_resolver_reason)
                .transpose()?
                .unwrap_or_else(|| "resolver_overview_not_supported".to_owned()),
        );
        (Vec::new(), 0)
    };
    let has_more = rows.len() as u64 > params.page_size;
    rows.truncate(params.page_size as usize);
    let next_cursor = if has_more {
        rows.last().map(|row| {
            list.next(ListPosition::new([
                ("key1", row.0.clone()),
                ("key2", row.1.clone()),
            ]))
        })
    } else {
        None
    };
    let mut data = Vec::with_capacity(rows.len());
    for (_, _, mut item) in rows {
        match section {
            "roles" => {
                if let Some(selector) = item
                    .as_object_mut()
                    .and_then(|object| object.remove("record_resource_selector"))
                    && let Some(resource) =
                        crate::v2::record_resource_value(&selector, &item["powers"])?
                {
                    item["record_resource"] = resource;
                }
                item["powers"] = permission_powers_value(&item["powers"])?;
                data.push(item);
            }
            _ => data.push(super::link_items::compact_resolver_link_item(&item)?),
        }
    }
    super::revalidate_project_generations(
        &state.pool,
        &selected,
        &generations,
        params.at.is_some(),
        "resolver collection changed while reading; retry the request",
    )
    .await?;
    publication.finish(&state).await?;
    Ok(Json(Envelope {
        data,
        page: Some(Page {
            cursor: params.cursor,
            next_cursor,
            page_size: params.page_size,
            total_count: supported.then_some(total),
            has_more,
        }),
        meta,
    }))
}

fn read_error() -> V2Error {
    V2Error::internal_error("failed to read resolver collection")
}
