//! Exhaustive reads of supported resolver alias and per-registration permission rows.
use std::collections::BTreeMap;

use super::{
    overview_items::{projected_section_items, summary_is_supported},
    parse_numeric_chain_id, product_resolver_reason, require_phase_target_snapshot,
    resolver_snapshot_scope,
};
use crate::{
    AppState,
    v2::{
        AtSelector, CursorPayload, Envelope, Page, QueryParamAllowlist, QueryParams,
        SnapshotReadResource, StrictQueryParams, V2Error, V2Result, decode, encode,
        encode_at_token, permission_powers_value, resolve_v2_snapshot_for, snapshot_meta,
    },
};
use axum::{
    Json,
    extract::{Path, State},
};
use serde_json::{Value, json};

#[path = "collections/reads.rs"]
mod reads;

pub(crate) struct ResolverCollectionParams;
impl QueryParamAllowlist for ResolverCollectionParams {
    const ALLOWED: &'static [&'static str] = &["at", "finality", "cursor", "page_size"];
}
type ResolverCollectionQuery = StrictQueryParams<ResolverCollectionParams>;

pub(crate) async fn get_resolver_aliases(
    Path(path): Path<(String, String)>,
    params: ResolverCollectionQuery,
    State(state): State<AppState>,
) -> V2Result<Json<Envelope<Vec<Value>>>> {
    collection(path, params.into_inner(), state, "aliases").await
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
    let cursor = params.cursor.as_deref().map(decode).transpose()?;
    if let Some(cursor) = &cursor
        && (cursor.filters != filters || cursor.sort != "identity_asc")
    {
        return Err(V2Error::invalid_input(
            "cursor does not match this resolver collection",
        ));
    }
    let publication = crate::v2::collection_snapshot::CollectionSnapshot::capture_for_namespace(
        &state,
        None,
        Some(super::resolver_namespace(slug)?),
    )
    .await?;
    if let Some(cursor) = &cursor {
        validate_publication(&publication, cursor)?;
    }
    let cursor_at = cursor
        .as_ref()
        .and_then(|cursor| cursor.snapshot.clone())
        .map(AtSelector::SnapshotToken);
    let selected = resolve_v2_snapshot_for(
        &state.pool,
        &resolver_snapshot_scope(slug)?,
        params.at.as_ref().or(cursor_at.as_ref()),
        params.finality,
        SnapshotReadResource::Resolver,
    )
    .await?;
    // Current projections are not historical tables. Reject old generations rather than
    // combining old cursor positions with newly published permission/name rows.
    let generations =
        crate::v2::lookup::head::load_selected_project_generations(&state.pool, &selected).await?;
    let token = encode_at_token(&selected);
    let generation = serde_json::to_string(&generations).expect("generation map serializes");
    let key = if let Some(cursor) = &cursor {
        if cursor.snapshot.as_ref() != Some(&token) {
            return Err(V2Error::invalid_input("cursor snapshot does not match at"));
        }
        if cursor.last_item.get("generation") != Some(&generation) {
            return Err(V2Error::stale(
                "resolver collection changed; restart pagination",
            ));
        }
        if cursor.last_item.len() != 4 {
            return Err(V2Error::invalid_input("invalid resolver collection cursor"));
        }
        Some((
            cursor
                .last_item
                .get("key1")
                .cloned()
                .ok_or_else(invalid_cursor)?,
            cursor
                .last_item
                .get("key2")
                .cloned()
                .ok_or_else(invalid_cursor)?,
        ))
    } else {
        None
    };
    let row = bigname_storage::load_phase_resolver_current(&state.pool, slug, &address)
        .await
        .map_err(|_| read_error())?
        .ok_or_else(|| V2Error::not_found("resolver was not found"))?;
    require_phase_target_snapshot(&row.chain_positions, slug, &selected)?;
    let summary_key = if section == "roles" {
        "role_holders"
    } else {
        "aliases"
    };
    let summary = row.declared_summary.get(summary_key);
    let mut meta = snapshot_meta(&selected)?;
    let supported = summary.is_some_and(summary_is_supported);
    let (mut rows, total) = if supported {
        let height = selected
            .chain_positions
            .as_map()
            .values()
            .find(|p| p.chain_id == slug)
            .ok_or_else(read_error)?
            .block_number;
        reads::page(
            &state.pool,
            slug,
            &address,
            section,
            height,
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
            encode(&bind_publication(
                &publication,
                CursorPayload::new(
                    "identity_asc",
                    filters,
                    BTreeMap::from([
                        ("key1".to_owned(), row.0.clone()),
                        ("key2".to_owned(), row.1.clone()),
                        ("generation".to_owned(), generation),
                    ]),
                    Some(token),
                ),
            ))
        })
    } else {
        None
    };
    let mut data = Vec::with_capacity(rows.len());
    for (_, _, mut item) in rows {
        if section == "roles" {
            item["powers"] = permission_powers_value(&item["powers"])?;
            data.push(item);
        } else {
            let mapped =
                projected_section_items(&json!({"status":"supported", "items":[item]}), "aliases")?
                    .ok_or_else(read_error)?;
            data.extend(mapped.as_array().ok_or_else(read_error)?.iter().cloned());
        }
    }
    if crate::v2::lookup::head::load_selected_project_generations(&state.pool, &selected).await?
        != generations
    {
        return Err(V2Error::stale(
            "resolver collection changed while reading; restart pagination",
        ));
    }
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

fn invalid_cursor() -> V2Error {
    V2Error::invalid_input("invalid resolver collection cursor")
}
fn read_error() -> V2Error {
    V2Error::internal_error("failed to read resolver collection")
}

// Resolver cursors retain their ordinary `at` token; the shared collection
// fingerprint additionally binds publication/manifest revisions.
pub(super) fn bind_publication(
    snapshot: &crate::v2::collection_snapshot::CollectionSnapshot,
    mut cursor: CursorPayload,
) -> CursorPayload {
    let bound = snapshot.bind_cursor(cursor.clone());
    cursor.last_item.insert(
        "publication".to_owned(),
        bound.snapshot.expect("bound snapshot"),
    );
    cursor.evaluated_at = bound.evaluated_at;
    cursor
}
pub(super) fn validate_publication(
    snapshot: &crate::v2::collection_snapshot::CollectionSnapshot,
    cursor: &CursorPayload,
) -> V2Result<()> {
    let mut cursor = cursor.clone();
    cursor.snapshot = cursor.last_item.remove("publication");
    snapshot.validate_cursor(&cursor)
}
