use std::collections::BTreeMap;

use axum::{
    Json,
    extract::{Path, State},
};
use bigname_storage::{
    ChainPositions, NameCurrentListCursor, NameCurrentListCursorValue, NameCurrentListRow,
    ResolverCurrentRow, SelectedSnapshot, SnapshotPositionRequirement, SnapshotSelectionScope,
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

#[path = "resolvers/overview_items.rs"]
mod overview_items;
use overview_items::{projected_section_items, summary_is_supported};

use super::{
    Envelope, Finality, Meta, NameRecord, PRODUCT_PIPELINE_TERMS, Page, QueryParamAllowlist,
    SnapshotReadResource, StrictQueryParams, V2Error, V2Result, api_error_to_v2, build_name_record,
    contains_boundary_vocabulary, decode, encode, encode_at_token, name_record, numeric_to_slug,
    resolve_v2_snapshot_for, snapshot_meta, snapshot_slot_for_slug,
    vocab::{Completeness, Status},
};

const BOUND_NAMES_SORT_TOKEN: &str = "name_asc";
const RESOLVER_SECTIONS: [(&str, &str, &str); 4] = [
    ("nodes", "nodes", "bindings"),
    ("aliases", "aliases", "aliases"),
    ("roles", "role_holders", "role_holders"),
    ("events", "events", "event_summary"),
];

pub(crate) struct ResolverQueryParams;

impl QueryParamAllowlist for ResolverQueryParams {
    const ALLOWED: &'static [&'static str] = &["include", "at", "finality", "cursor", "page_size"];
}

pub(crate) type ResolverQuery = StrictQueryParams<ResolverQueryParams>;

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub(crate) struct ResolverOverview {
    pub(crate) chain_id: u64,
    pub(crate) address: String,
    pub(crate) counts: BTreeMap<String, u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) nodes: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) aliases: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) roles: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) events: Option<Value>,
    pub(crate) bound_names: BoundNames,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub(crate) struct BoundNames {
    pub(crate) data: Vec<NameRecord>,
    pub(crate) page: Page,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ResolverOverviewInclude {
    nodes: bool,
    aliases: bool,
    roles: bool,
    events: bool,
}

impl ResolverOverviewInclude {
    fn all() -> Self {
        Self {
            nodes: true,
            aliases: true,
            roles: true,
            events: true,
        }
    }

    fn empty() -> Self {
        Self {
            nodes: false,
            aliases: false,
            roles: false,
            events: false,
        }
    }

    fn requests(self, section: &str) -> bool {
        match section {
            "nodes" => self.nodes,
            "aliases" => self.aliases,
            "roles" => self.roles,
            "events" => self.events,
            _ => false,
        }
    }
}

pub(crate) async fn get_resolver(
    Path((chain_id, address)): Path<(String, String)>,
    params: ResolverQuery,
    State(state): State<AppState>,
) -> V2Result<Json<Envelope<ResolverOverview>>> {
    let params = params.into_inner();
    let (numeric_chain_id, chain_id_slug) = parse_numeric_chain_id(&chain_id)?;
    let normalized_address = parse_evm_address(&address, "address").map_err(api_error_to_v2)?;
    let include = resolver_overview_include(&params.include)?;

    let scope = resolver_snapshot_scope(chain_id_slug)?;
    let require_selected_head = params.at.is_none() && params.finality == Finality::Latest;
    let selected_snapshot = resolve_v2_snapshot_for(
        &state.lookup_pool,
        &scope,
        params.at.as_ref(),
        params.finality,
        SnapshotReadResource::Resolver,
    )
    .await?;
    let project_generations = load_resolver_project_generations(
        &state.lookup_pool,
        &selected_snapshot,
        require_selected_head,
    )
    .await?;
    let row = bigname_storage::load_phase_resolver_current(
        &state.lookup_pool,
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
            &state.lookup_pool,
            &selected_snapshot,
            require_selected_head,
        )
        .await?;
        if current != project_generations {
            return Err(V2Error::stale(
                "resolver project publication changed while the request was being served",
            ));
        }
        if !require_selected_head {
            return Err(V2Error::stale(
                "resolver projection is unavailable at the selected historical position",
            ));
        }
        return Err(V2Error::not_found(format!(
            "resolver {normalized_address} was not found on chain {numeric_chain_id}"
        )));
    };
    require_phase_target_snapshot(
        "resolver overview",
        &row.chain_positions,
        &row.chain_id,
        &selected_snapshot,
    )?;
    let snapshot_token = encode_at_token(&selected_snapshot);
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
            bound_names_storage_cursor(&payload, &cursor_binding)
        })
        .transpose()?;

    let (bound_name_rows, storage_next_cursor) = load_bound_name_rows(
        &state.lookup_pool,
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
    let current = load_resolver_project_generations(
        &state.lookup_pool,
        &selected_snapshot,
        require_selected_head,
    )
    .await?;
    if current != project_generations {
        return Err(V2Error::stale(
            "resolver project publication changed while the request was being served",
        ));
    }

    let next_cursor = storage_next_cursor
        .as_ref()
        .map(|cursor| encode(&bound_names_cursor_payload(cursor, &cursor_binding)));
    let has_more = next_cursor.is_some();
    let bound_names = BoundNames {
        data: bound_name_rows
            .iter()
            .map(|row| build_bound_name_record(row, numeric_chain_id))
            .collect(),
        page: Page {
            cursor: params.cursor.clone(),
            next_cursor,
            page_size: params.page_size,
            total_count: None,
            has_more,
        },
    };
    let mut meta = snapshot_meta(&selected_snapshot)?;
    apply_resolver_support_meta(&mut meta, &row, include)?;
    let data = build_resolver_overview(row, numeric_chain_id, include, bound_names)?;

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
    include: ResolverOverviewInclude,
    bound_names: BoundNames,
) -> V2Result<ResolverOverview> {
    let mut counts = BTreeMap::new();
    let mut nodes = None;
    let mut aliases = None;
    let mut roles = None;
    let mut events = None;

    for (field_key, count_key, summary_key) in RESOLVER_SECTIONS {
        let section_summary = resolver_overview_summary(&row, summary_key);
        if let Some(count) = section_summary.and_then(projected_section_count) {
            counts.insert(count_key.to_owned(), count);
        }

        if include.requests(field_key) {
            let items = section_summary
                .map(|summary| projected_section_items(summary, field_key))
                .transpose()?
                .flatten()
                .unwrap_or(Value::Null);
            match field_key {
                "nodes" => nodes = Some(items),
                "aliases" => aliases = Some(items),
                "roles" => roles = Some(items),
                "events" => events = Some(items),
                _ => {}
            }
        }
    }

    Ok(ResolverOverview {
        chain_id,
        address: row.resolver_address,
        counts,
        nodes,
        aliases,
        roles,
        events,
        bound_names,
    })
}

pub(crate) fn build_bound_name_record(row: &NameCurrentListRow, chain_id: u64) -> NameRecord {
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

fn apply_resolver_support_meta(
    meta: &mut Meta,
    row: &ResolverCurrentRow,
    include: ResolverOverviewInclude,
) -> V2Result<()> {
    let mut fields = Vec::new();
    let mut reason = None;
    let requested_count = RESOLVER_SECTIONS
        .iter()
        .filter(|(field_key, _, _)| include.requests(field_key))
        .count();

    for (field_key, _, summary_key) in RESOLVER_SECTIONS {
        if !include.requests(field_key) {
            continue;
        }
        let summary = resolver_overview_summary(row, summary_key);
        if summary.is_none_or(|summary| !summary_is_supported(summary)) {
            fields.push(field_key.to_owned());
            if reason.is_none() {
                reason = summary
                    .and_then(|summary| summary.get("unsupported_reason"))
                    .and_then(Value::as_str)
                    .map(product_resolver_reason)
                    .transpose()?;
            }
        }
    }

    if !fields.is_empty() {
        let completeness = if fields.len() == requested_count {
            Completeness::Unsupported
        } else {
            Completeness::Partial
        };
        if completeness == Completeness::Unsupported && reason.is_none() {
            reason = Some("resolver_overview_not_supported".to_owned());
        }
        meta.completeness = Some(completeness);
        meta.unsupported_fields = Some(fields);
        meta.unsupported_reason = reason;
    }

    Ok(())
}

fn bound_name_cursor_from_row(row: &NameCurrentListRow) -> NameCurrentListCursor {
    NameCurrentListCursor {
        sort_value: NameCurrentListCursorValue::Name(row.row.canonical_display_name.clone()),
        namespace: row.row.namespace.clone(),
        normalized_name: row.row.normalized_name.clone(),
        namehash: row.row.namehash.clone(),
    }
}

pub(crate) fn resolver_overview_include(include: &[String]) -> V2Result<ResolverOverviewInclude> {
    let mut parsed = ResolverOverviewInclude::empty();
    let mut saw_value = false;

    for value in include {
        saw_value = true;
        match value.as_str() {
            "nodes" => parsed.nodes = true,
            "aliases" => parsed.aliases = true,
            "roles" => parsed.roles = true,
            "events" => parsed.events = true,
            _ => {
                return Err(V2Error::invalid_input(
                    "include must contain only nodes, aliases, roles, or events",
                ));
            }
        }
    }

    Ok(if saw_value {
        parsed
    } else {
        ResolverOverviewInclude::all()
    })
}

fn parse_numeric_chain_id(value: &str) -> V2Result<(u64, &'static str)> {
    let value = value.trim();
    if value.is_empty() || !value.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(invalid_chain_id());
    }

    let chain_id = value.parse::<u64>().map_err(|_| invalid_chain_id())?;
    let slug = numeric_to_slug(chain_id).ok_or_else(invalid_chain_id)?;

    Ok((chain_id, slug))
}

fn invalid_chain_id() -> V2Error {
    V2Error::invalid_input("chain_id must be a supported numeric EVM chain id")
}

fn resolver_snapshot_scope(chain_id_slug: &str) -> V2Result<SnapshotSelectionScope> {
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

fn resolver_overview_summary<'a>(
    row: &'a ResolverCurrentRow,
    summary_key: &str,
) -> Option<&'a Value> {
    row.declared_summary
        .get(summary_key)
        .filter(|value| value.is_object())
}

fn projected_section_count(summary: &Value) -> Option<u64> {
    if !summary_is_supported(summary) {
        return None;
    }
    summary.get("count").and_then(Value::as_u64).or_else(|| {
        summary
            .get("items")
            .and_then(Value::as_array)
            .map(|items| items.len() as u64)
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

fn require_phase_name_snapshot(
    row: &NameCurrentListRow,
    selected_snapshot: &SelectedSnapshot,
) -> V2Result<()> {
    let projected = ChainPositions::from_value(&row.row.chain_positions)
        .map_err(|_| V2Error::stale("bound-name phase projection has unusable chain positions"))?;
    let slot = match row.row.namespace.as_str() {
        "ens" if projected.get("ethereum-sepolia").is_some() => "ethereum-sepolia",
        "ens" => "ethereum",
        "basenames" => "base",
        _ => {
            return Err(V2Error::stale(
                "bound-name phase projection has no served position",
            ));
        }
    };
    let projected = projected
        .get(slot)
        .ok_or_else(|| V2Error::stale("bound-name phase projection has no served position"))?;
    let selected = selected_snapshot
        .chain_positions
        .get(slot)
        .ok_or_else(|| V2Error::stale("bound-name phase projection is outside the snapshot"))?;
    if projected.chain_id == selected.chain_id
        && projected.block_number <= selected.block_number
        && (projected.block_number != selected.block_number
            || projected.block_hash == selected.block_hash)
    {
        Ok(())
    } else {
        Err(V2Error::stale(
            "bound-name phase projection is outside the selected project publication",
        ))
    }
}

fn require_phase_target_snapshot(
    projection_family: &str,
    chain_positions: &Value,
    chain_id: &str,
    selected_snapshot: &SelectedSnapshot,
) -> V2Result<()> {
    let number = chain_positions
        .get("target_block_number")
        .and_then(Value::as_i64)
        .ok_or_else(|| V2Error::stale(format!("{projection_family} has no target block number")))?;
    let hash = chain_positions
        .get("target_block_hash")
        .and_then(Value::as_str)
        .ok_or_else(|| V2Error::stale(format!("{projection_family} has no target block hash")))?;
    let selected = selected_snapshot
        .chain_positions
        .as_map()
        .values()
        .find(|position| position.chain_id == chain_id)
        .ok_or_else(|| V2Error::stale(format!("{projection_family} is outside the snapshot")))?;
    if number > selected.block_number
        || (number == selected.block_number && hash != selected.block_hash)
    {
        return Err(V2Error::stale(format!(
            "{projection_family} is outside the selected project publication"
        )));
    }
    Ok(())
}
