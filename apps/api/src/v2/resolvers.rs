use std::collections::BTreeMap;

#[path = "resolvers/collections.rs"]
mod collections;
pub(crate) use collections::{get_resolver_aliases, get_resolver_links, get_resolver_roles};

use axum::{
    Json,
    extract::{Path, State},
};
use bigname_storage::{
    NameCurrentListCursor, NameCurrentListCursorValue, NameCurrentListRow, ResolverCurrentRow,
    SelectedSnapshot, SnapshotPositionRequirement, SnapshotSelectionScope,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tracing::error;

use super::support::parse_evm_address;
use crate::AppState;

#[path = "resolvers/bound_names_cursor.rs"]
mod bound_names_cursor;
pub(crate) use bound_names_cursor::{
    BoundNamesCursorBinding, bound_names_cursor_payload, bound_names_storage_cursor,
};

#[path = "resolvers/link_items.rs"]
mod link_items;

#[path = "resolvers/overview_items.rs"]
mod overview_items;

#[path = "resolvers/role_grants.rs"]
mod role_grants;

#[path = "resolvers/snapshot_checks.rs"]
mod snapshot_checks;
use snapshot_checks::{require_phase_name_snapshot, require_phase_target_snapshot};

use super::{
    Envelope, Finality, NameRecord, PRODUCT_PIPELINE_TERMS, Page, QueryParamAllowlist,
    SnapshotReadResource, StrictQueryParams, V2Error, V2Result, api_error_to_v2, build_name_record,
    contains_boundary_vocabulary, decode, encode, encode_at_token, name_record, numeric_to_slug,
    resolve_v2_snapshot_for, snapshot_meta, snapshot_slot_for_slug,
    vocab::{Resolver, Status},
};

const BOUND_NAMES_SORT_TOKEN: &str = "name_asc";

pub(crate) struct ResolverQueryParams;

impl QueryParamAllowlist for ResolverQueryParams {
    const ALLOWED: &'static [&'static str] = &["at", "finality", "cursor", "page_size"];
}

pub(crate) type ResolverQuery = StrictQueryParams<ResolverQueryParams>;

/// The resolver overview: its identity, its mirror declaration and the names bound to it. It
/// carries no section counts or samples; the `/aliases`, `/links` and `/roles` collections page
/// those rows with exact totals (docs/api-v1-routes.md, the resolver overview).
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub(crate) struct ResolverOverview {
    pub(crate) chain_id: u64,
    pub(crate) address: String,
    /// Present only for a declared ENSv1 mirror resolver: the ENSv1 registry it reads.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) mirror: Option<ResolverMirror>,
    pub(crate) bound_names: BoundNames,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub(crate) struct ResolverMirror {
    pub(crate) kind: String,
    pub(crate) registry: Resolver,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub(crate) struct BoundNames {
    pub(crate) data: Vec<NameRecord>,
    pub(crate) page: Page,
}

pub(crate) async fn get_resolver(
    Path((chain_id, address)): Path<(String, String)>,
    params: ResolverQuery,
    State(state): State<AppState>,
) -> V2Result<Json<Envelope<ResolverOverview>>> {
    let params = params.into_inner();
    let (numeric_chain_id, chain_id_slug) = parse_numeric_chain_id(&chain_id)?;
    let normalized_address = parse_evm_address(&address, "address").map_err(api_error_to_v2)?;
    let publication = super::collection_snapshot::CollectionSnapshot::capture_for_namespace(
        &state,
        None,
        Some(resolver_namespace(chain_id_slug)?),
    )
    .await?
    .continuing_from_request_cursor(params.cursor.is_some());

    let scope = resolver_snapshot_scope(chain_id_slug)?;
    let require_selected_head = params.at.is_none() && params.finality == Finality::Latest;
    let selected_snapshot = resolve_v2_snapshot_for(
        &state.pool,
        &scope,
        params.at.as_ref(),
        params.finality,
        SnapshotReadResource::Resolver,
    )
    .await?;
    let project_generations =
        load_resolver_project_generations(&state.pool, &selected_snapshot, require_selected_head)
            .await?;
    let row = bigname_storage::load_phase_resolver_current(
        &state.pool,
        chain_id_slug,
        &normalized_address,
    )
    .await
    .map_err(|_| {
        V2Error::internal_error(format!(
            "failed to load resolver data for chain_id {chain_id_slug} address {normalized_address}"
        ))
    })?;
    let Some(row) = row else {
        let current = load_resolver_project_generations(
            &state.pool,
            &selected_snapshot,
            require_selected_head,
        )
        .await?;
        if current != project_generations {
            return Err(V2Error::stale(
                "served resolver data changed while the request was being read",
            ));
        }
        if !require_selected_head {
            return Err(V2Error::stale(
                "resolver data is unavailable at the selected historical position",
            ));
        }
        return Err(V2Error::not_found(format!(
            "resolver {normalized_address} was not found on chain {numeric_chain_id}"
        )));
    };
    require_phase_target_snapshot(&row.chain_positions, &row.chain_id, &selected_snapshot)?;
    let snapshot_token = encode_at_token(&selected_snapshot);
    let resolver_generation =
        serde_json::to_string(&project_generations).expect("resolver generation map serializes");
    let cursor_binding = BoundNamesCursorBinding {
        chain_id: numeric_chain_id,
        resolver_address: &normalized_address,
        namespace: params.namespace.as_deref(),
        sort: BOUND_NAMES_SORT_TOKEN,
        snapshot_token: &snapshot_token,
    };
    let storage_cursor = params
        .cursor
        .as_deref()
        .map(|cursor| {
            let payload = decode(cursor)?;
            let mut storage_payload = payload.clone();
            storage_payload.last_item.remove("publication");
            storage_payload.last_item.remove("resolver_generation");
            let mut structural_payload = storage_payload.clone();
            if payload.last_item.contains_key("publication") {
                structural_payload.snapshot = Some(snapshot_token.clone());
            }
            bound_names_storage_cursor(&structural_payload, &cursor_binding)?;
            publication.validate_token(payload.last_item.get("publication").map(String::as_str))?;
            if payload.last_item.get("resolver_generation") != Some(&resolver_generation) {
                return Err(V2Error::stale(
                    "resolver publication changed; restart pagination",
                ));
            }
            bound_names_storage_cursor(&storage_payload, &cursor_binding)
        })
        .transpose()?;

    let (bound_name_rows, storage_next_cursor) = load_bound_name_rows(
        &state.pool,
        chain_id_slug,
        params.namespace.as_deref(),
        storage_cursor.as_ref(),
        params.page_size,
        numeric_chain_id,
        &normalized_address,
    )
    .await?;
    for bound_name_row in &bound_name_rows {
        require_phase_name_snapshot(bound_name_row, &selected_snapshot)?;
    }
    let current =
        load_resolver_project_generations(&state.pool, &selected_snapshot, require_selected_head)
            .await?;
    if current != project_generations {
        return Err(V2Error::stale(
            "served resolver data changed while the request was being read",
        ));
    }

    let next_cursor = storage_next_cursor.as_ref().map(|cursor| {
        let mut cursor = bound_names_cursor_payload(cursor, &cursor_binding);
        cursor.last_item.insert(
            "resolver_generation".to_owned(),
            resolver_generation.clone(),
        );
        encode(&collections::bind_publication(&publication, cursor))
    });
    let has_more = next_cursor.is_some();
    let bound_name_records = bound_name_rows
        .iter()
        .map(|row| build_bound_name_record(row, numeric_chain_id))
        .collect::<V2Result<Vec<_>>>()?;
    let bound_names = BoundNames {
        data: bound_name_records,
        page: Page {
            cursor: params.cursor.clone(),
            next_cursor,
            page_size: params.page_size,
            total_count: None,
            has_more,
        },
    };
    let meta = snapshot_meta(&selected_snapshot)?;
    let data = build_resolver_overview(row, numeric_chain_id, bound_names);
    publication.finish(&state).await?;

    Ok(Json(Envelope {
        data,
        page: None,
        meta,
    }))
}

async fn load_resolver_project_generations(
    pool: &sqlx::PgPool,
    selected: &SelectedSnapshot,
    require_selected_head: bool,
) -> V2Result<BTreeMap<String, String>> {
    if require_selected_head {
        super::lookup::head::load_selected_project_generations(pool, selected).await
    } else {
        super::lookup::head::load_project_generations(pool, selected).await
    }
}

pub(crate) fn build_resolver_overview(
    row: ResolverCurrentRow,
    chain_id: u64,
    bound_names: BoundNames,
) -> ResolverOverview {
    let mirror = resolver_mirror(&row.declared_summary, chain_id);
    ResolverOverview {
        chain_id,
        address: row.resolver_address,
        mirror,
        bound_names,
    }
}

fn resolver_mirror(declared_summary: &Value, chain_id: u64) -> Option<ResolverMirror> {
    let mirror = declared_summary.get("classification")?.get("mirror")?;
    let address = mirror.get("mirrored_registry_address")?.as_str()?;
    Some(ResolverMirror {
        kind: "ensv1_registry".to_owned(),
        registry: Resolver {
            chain_id,
            address: address.to_ascii_lowercase(),
        },
    })
}

pub(crate) fn build_bound_name_record(
    row: &NameCurrentListRow,
    chain_id: u64,
) -> V2Result<NameRecord> {
    build_name_record(&row.row, None, Some(chain_id), Status::Ok)
}

async fn load_bound_name_rows(
    pool: &sqlx::PgPool,
    chain_id_slug: &str,
    namespace: Option<&str>,
    cursor: Option<&NameCurrentListCursor>,
    page_size: u64,
    chain_id: u64,
    resolver_address: &str,
) -> V2Result<(Vec<NameCurrentListRow>, Option<NameCurrentListCursor>)> {
    let loaded = bigname_storage::load_phase_resolver_bound_name_rows(
        pool,
        chain_id_slug,
        resolver_address,
        namespace,
        cursor,
        page_size.saturating_add(1) as i64,
    )
    .await
    .map_err(|_| {
        V2Error::internal_error(format!(
            "failed to load bound names for resolver {resolver_address} on chain {chain_id}"
        ))
    })?;
    let mut rows = loaded
        .into_iter()
        .map(|row| NameCurrentListRow {
            row,
            labelhash: None,
            token_id: None,
            owner: None,
            registrant: None,
            created_at: None,
            registration_date: None,
            expiry_date: None,
            resolver_address: Some(resolver_address.to_owned()),
        })
        .filter(|row| bound_name_row_matches_chain(row, chain_id))
        .collect::<Vec<_>>();
    let target_len = page_size as usize;
    let has_more = rows.len() > target_len;
    rows.truncate(target_len);
    let next_cursor = has_more
        .then(|| rows.last().map(bound_name_cursor_from_row))
        .flatten();
    Ok((rows, next_cursor))
}

fn bound_name_cursor_from_row(row: &NameCurrentListRow) -> NameCurrentListCursor {
    NameCurrentListCursor {
        sort_value: NameCurrentListCursorValue::Name(row.row.canonical_display_name.clone()),
        namespace: row.row.namespace.clone(),
        normalized_name: row.row.normalized_name.clone(),
        namehash: row.row.namehash.clone(),
    }
}

pub(crate) fn parse_numeric_chain_id(value: &str) -> V2Result<(u64, &'static str)> {
    let value = value.trim();
    if value.is_empty() || !value.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(invalid_chain_id());
    }

    let chain_id = value.parse::<u64>().map_err(|_| invalid_chain_id())?;
    let slug = numeric_to_slug(chain_id).ok_or_else(invalid_chain_id)?;

    Ok((chain_id, slug))
}

fn resolver_namespace(chain_id_slug: &str) -> V2Result<&'static str> {
    match chain_id_slug {
        "ethereum-mainnet" | "ethereum-sepolia" => Ok("ens"),
        "base-mainnet" | "base-sepolia" => Ok("basenames"),
        _ => Err(invalid_chain_id()),
    }
}

fn invalid_chain_id() -> V2Error {
    V2Error::invalid_input("chain_id must be a supported numeric EVM chain id")
}

pub(crate) fn resolver_snapshot_scope(chain_id_slug: &str) -> V2Result<SnapshotSelectionScope> {
    let slot = snapshot_slot_for_slug(chain_id_slug).ok_or_else(|| {
        error!(
            service = "api",
            chain_id = %chain_id_slug,
            "failed to map resolver snapshot slot"
        );
        V2Error::internal_error("failed to build resolver snapshot scope")
    })?;
    SnapshotSelectionScope::new(
        vec![SnapshotPositionRequirement::new(
            slot.to_owned(),
            chain_id_slug.to_owned(),
        )],
        Some(slot.to_owned()),
    )
    .map_err(|error| {
        error!(
            service = "api",
            chain_id = %chain_id_slug,
            slot = %slot,
            message = %error.message(),
            "failed to build resolver snapshot scope"
        );
        V2Error::internal_error("failed to build resolver snapshot scope")
    })
}

fn product_resolver_reason(reason: &str) -> V2Result<String> {
    match reason {
        "resolver_binding_enumeration_not_projected" => {
            Ok("binding_enumeration_not_supported".to_owned())
        }
        _ if contains_boundary_vocabulary(reason, PRODUCT_PIPELINE_TERMS) => {
            error!(%reason, "rejected resolver reason containing pipeline vocabulary");
            Err(V2Error::internal_error(
                "failed to map resolver reason vocabulary",
            ))
        }
        _ => Ok(reason.to_owned()),
    }
}

fn bound_name_row_matches_chain(row: &NameCurrentListRow, chain_id: u64) -> bool {
    name_record::resolver(&row.row.declared_summary)
        .is_some_and(|resolver| resolver.chain_id == chain_id)
}
