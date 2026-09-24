use std::collections::{BTreeMap, BTreeSet};

use axum::{
    Json,
    extract::{Path, State},
};
use bigname_storage::{
    HistoryBlockWindow, HistoryEvent as StorageHistoryEvent, HistoryOrder, HistoryPageOptions,
    HistorySummary, HistorySummaryMode, SnapshotAt, SnapshotSelectionScope,
};
use serde::{Deserialize, Serialize};
use sqlx::types::{
    Uuid,
    time::{OffsetDateTime, UtcOffset},
};

use crate::AppState;

use super::cursor::invalid_cursor_error;
use super::support::{
    ExactNameSnapshotSelector, exact_name_snapshot_scope, normalize_inferred_route_name,
};
use super::{
    AtSelector, Envelope, EventDetail, HistoryEventType, HistoryInclude, HistoryScope, Page,
    QueryParamAllowlist, QueryParams, SortOrder, StrictQueryParams, V2Error, V2Result,
    all_chain_slugs, api_error_to_v2, build_event_detail, decode, decode_at_token, encode,
    raw_event_kind, validate_latest_collection_selectors,
};

const HISTORY_SORT_DESC: &str = "chain_position_desc";
const HISTORY_SORT_ASC: &str = "chain_position_asc";
const NAMESPACE_FILTER_KEY: &str = "namespace";
const NAME_FILTER_KEY: &str = "name";
const SCOPE_FILTER_KEY: &str = "scope";
const TYPE_FILTER_KEY: &str = "type";
const FROM_TIMESTAMP_FILTER_KEY: &str = "from_timestamp";
const TO_TIMESTAMP_FILTER_KEY: &str = "to_timestamp";

/// Anchored history counts are exact up to this many product-visible rows;
/// larger results report `total_count=null` instead of scanning further.
pub(crate) const HISTORY_TOTAL_COUNT_CAP: u64 = 10_000;

pub(crate) struct HistoryQueryParams;

impl QueryParamAllowlist for HistoryQueryParams {
    const ALLOWED: &'static [&'static str] = &[
        "namespace",
        "at",
        "finality",
        "scope",
        "type",
        "order",
        "from_timestamp",
        "to_timestamp",
        "include",
        "cursor",
        "page_size",
    ];
}

pub(crate) type HistoryQuery = StrictQueryParams<HistoryQueryParams>;

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub(crate) struct HistoryEvent {
    pub(crate) id: String,
    #[serde(rename = "type")]
    pub(crate) event_type: HistoryEventType,
    pub(crate) name: String,
    /// Present only with `include=child_registrations`: `name` or `child`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) subject: Option<HistoryRowSubject>,
    pub(crate) namespace: String,
    pub(crate) registration_id: Option<String>,
    pub(crate) block_number: Option<i64>,
    pub(crate) timestamp: Option<String>,
    pub(crate) transaction_hash: Option<String>,
    pub(crate) log_index: Option<i64>,
    /// Present only with `include=data`: `contract_address`, `data`.
    #[serde(flatten)]
    pub(crate) detail: Option<EventDetail>,
    /// Present only with `include=raw`: the raw storage event kind.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) kind: Option<String>,
}

pub(crate) async fn get_history(
    Path(input_name): Path<String>,
    params: HistoryQuery,
    State(state): State<AppState>,
) -> V2Result<Json<Envelope<Vec<HistoryEvent>>>> {
    let params = params.into_inner();
    validate_latest_collection_selectors(params.at.as_ref(), params.finality)?;
    let (include, child_registrations) = children::name_history_include(&params.include)?;
    let normalized = normalize_inferred_route_name(&input_name)
        .map_err(|error| V2Error::invalid_input(error.message))?;
    let namespace = params
        .namespace
        .clone()
        .unwrap_or_else(|| normalized.namespace.to_owned());

    let logical_name_id =
        bigname_storage::logical_name_id_for_name(&namespace, &normalized.normalized_name);
    children::refuse_registrar_root(child_registrations, &namespace, &logical_name_id)?;
    let cursor_binding = HistoryCursorBinding {
        namespace: &namespace,
        parent_logical_name_id: &logical_name_id,
        scope: params.scope,
        order: history_storage_order(params.order),
        params: &params,
        child_registrations,
    };
    let (snapshot, request_cursor) =
        super::collection_snapshot::CollectionSnapshot::capture_history(
            &state,
            Some(&namespace),
            || {
                params
                    .cursor
                    .as_deref()
                    .map(|cursor| history_storage_cursor(&decode(cursor)?, &cursor_binding))
                    .transpose()
            },
        )
        .await?;
    let storage_cursor = match request_cursor {
        Some(cursor) => Some(super::history_keyset::resolve(&state, cursor).await?),
        None => None,
    };
    let block_window = Some(bound_history_block_window(
        resolve_history_block_window(&state.pool, &params).await?,
        &snapshot.block_bounds(),
    ));
    let interpret_redo_fence = bigname_storage::capture_interpret_redo_fence(&state.pool)
        .await
        .map_err(|error| map_history_page_error(error, "failed to load name history"))?;
    // The first page proves the name exists. A continuation does not look it up again: its cursor
    // binds the name, and its rows come only from evidence at or below the published block.
    if storage_cursor.is_none() {
        let parent = bigname_storage::load_name_current(&state.pool, &logical_name_id)
            .await
            .map_err(|error| {
                tracing::error!(error = ?error, "failed to load history parent projection");
                V2Error::internal_error(format!(
                    "failed to load history for {}/{}",
                    namespace, normalized.normalized_name
                ))
            })?;
        if parent.is_none() {
            let current_fence = bigname_storage::capture_interpret_redo_fence(&state.pool)
                .await
                .map_err(|error| map_history_page_error(error, "failed to load name history"))?;
            if current_fence != interpret_redo_fence {
                return Err(history_redo_stale_error());
            }
            return Err(V2Error::not_found(format!(
                "name {} was not found in namespace {namespace}",
                normalized.normalized_name
            )));
        }
    }

    let resource_ids = if matches!(params.scope, HistoryScope::Name) {
        Vec::new()
    } else {
        registration_resource_ids(&state.pool, &logical_name_id, &snapshot.block_bounds()).await?
    };
    let storage_scope = history_storage_scope(params.scope);
    let mut options = history_page_options(&params, block_window);
    options.publication_block_bounds = Some(snapshot.block_bounds());

    let summary_mode = if params.include.iter().any(|v| v == "total_count") {
        HistorySummaryMode::Count
    } else {
        HistorySummaryMode::CappedCount(HISTORY_TOTAL_COUNT_CAP)
    };
    let storage_page = children::load_page(
        &state,
        children::PageRequest {
            logical_name_id: &logical_name_id,
            resource_ids: &resource_ids,
            scope: storage_scope,
            cursor: storage_cursor.as_ref(),
            page_size: params.page_size,
            summary_mode,
            options: &options,
            interpret_redo_fence: &interpret_redo_fence,
            anchor_name: &normalized.normalized_name,
            include,
        },
        child_registrations,
    )
    .await?;

    let next_cursor = storage_page
        .next_cursor
        .as_ref()
        .map(|cursor| encode(&history_cursor_payload(cursor, &cursor_binding)));
    let has_more = next_cursor.is_some();
    let total_count = if params.include.iter().any(|v| v == "total_count") {
        storage_page.summary.as_ref().map(|s| s.total_count)
    } else {
        history_total_count(storage_page.summary.as_ref())
    };
    Ok(Json(Envelope {
        data: storage_page.rows,
        page: Some(Page {
            cursor: params.cursor.clone(),
            next_cursor,
            page_size: params.page_size,
            total_count,
            has_more,
        }),
        meta: snapshot.finish_history(&state).await?,
    }))
}

/// The registration resources of a name as they stood at `block_bounds`: the resources its surface
/// bindings reached at or below the published block, and the BaseRegistrar leases those bindings
/// do not reach. A wrapped name is bound to its NameWrapper resource, so its lease rows are
/// reached through the link each `NameWrapped` row recorded. A lease granted with `registerOnly`
/// while the name stayed bound to a registry-only resource (a registrar token transferred without
/// `reclaim`) has no binding or link at all, so every published registrar grant carrying the
/// name's namehash is followed too. The registration Project selected for the name is not read:
/// it is current state, and the grants above already reach it.
/// (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L118-L152 @ ens_v1@91c966f)
/// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L240-L305 @ ens_v1@91c966f)
async fn registration_resource_ids(
    pool: &sqlx::PgPool,
    logical_name_id: &str,
    block_bounds: &BTreeMap<String, i64>,
) -> V2Result<Vec<Uuid>> {
    bigname_storage::load_bounded_registration_resource_ids(pool, logical_name_id, block_bounds)
        .await
        .map_err(|error| {
            tracing::error!(logical_name_id, error = ?error, "failed to load history registrations");
            V2Error::internal_error("failed to load name history")
        })
}

/// History routes default to newest-first; `order=asc` is the exact reverse.
pub(crate) fn history_storage_order(order: Option<SortOrder>) -> HistoryOrder {
    match order {
        None | Some(SortOrder::Desc) => HistoryOrder::Desc,
        Some(SortOrder::Asc) => HistoryOrder::Asc,
    }
}

pub(crate) fn history_sort_token(order: HistoryOrder) -> &'static str {
    match order {
        HistoryOrder::Desc => HISTORY_SORT_DESC,
        HistoryOrder::Asc => HISTORY_SORT_ASC,
    }
}

/// Cursor filter entries shared by every history collection: the canonical
/// `type` set and the canonical timestamp bounds, each only when requested.
pub(crate) fn insert_history_filter_keys(
    filters: &mut BTreeMap<String, String>,
    params: &QueryParams,
) {
    if let Some(event_types) = params.event_types.as_ref() {
        filters.insert(TYPE_FILTER_KEY.to_owned(), event_types.canonical_value());
    }
    if let Some(bound) = params.from_timestamp.as_ref() {
        filters.insert(
            FROM_TIMESTAMP_FILTER_KEY.to_owned(),
            bound.canonical.clone(),
        );
    }
    if let Some(bound) = params.to_timestamp.as_ref() {
        filters.insert(TO_TIMESTAMP_FILTER_KEY.to_owned(), bound.canonical.clone());
    }
}

/// Intersect the caller's time window with the publication's readable per-chain positions.
/// Forward Interpret work beyond Project's served blocks never changes a continued history.
pub(crate) fn bound_history_block_window(
    requested: Option<HistoryBlockWindow>,
    published: &BTreeMap<String, i64>,
) -> HistoryBlockWindow {
    let ranges = match requested {
        Some(window) => window
            .ranges
            .into_iter()
            .filter_map(|mut range| {
                let published = *published.get(&range.chain_id)?;
                range.to_block = Some(range.to_block.map_or(published, |to| to.min(published)));
                (range.from_block.is_none_or(|from| from <= published)).then_some(range)
            })
            .collect(),
        None => published
            .iter()
            .map(|(chain_id, block)| bigname_storage::ChainBlockRange {
                chain_id: chain_id.clone(),
                from_block: None,
                to_block: Some(*block),
            })
            .collect(),
    };
    HistoryBlockWindow { ranges }
}

/// Map `from_timestamp`/`to_timestamp` to per-chain block ranges from readable
/// lineage rows. `None` when no timestamp bound was requested.
pub(crate) async fn resolve_history_block_window(
    pool: &sqlx::PgPool,
    params: &QueryParams,
) -> V2Result<Option<HistoryBlockWindow>> {
    if params.from_timestamp.is_none() && params.to_timestamp.is_none() {
        return Ok(None);
    }
    let ranges = bigname_storage::resolve_chain_block_ranges(
        pool,
        &all_chain_slugs(),
        params.from_timestamp.as_ref().map(|bound| bound.value),
        params.to_timestamp.as_ref().map(|bound| bound.value),
    )
    .await
    .map_err(|error| {
        tracing::error!(error = ?error, "failed to resolve history timestamp window");
        V2Error::internal_error("failed to resolve history timestamp window")
    })?;
    Ok(Some(HistoryBlockWindow { ranges }))
}

/// Product read options: an explicit `type` set narrows the stored kinds and
/// joins cursor anchor validation; otherwise every product kind is read.
pub(crate) fn history_page_options(
    params: &QueryParams,
    block_window: Option<HistoryBlockWindow>,
) -> HistoryPageOptions {
    HistoryPageOptions {
        publication_block_bounds: None,
        order: history_storage_order(params.order),
        event_kinds: params
            .event_types
            .as_ref()
            .map(|event_types| event_types.storage_event_kinds())
            .unwrap_or_else(product_history_event_kinds),
        bind_cursor_anchor_to_event_kinds: params.event_types.is_some(),
        block_window,
    }
}

/// A capped count above the cap means the exact count was not computed.
pub(crate) fn history_total_count(summary: Option<&HistorySummary>) -> Option<u64> {
    summary
        .map(|summary| summary.total_count)
        .filter(|total_count| *total_count <= HISTORY_TOTAL_COUNT_CAP)
}

pub(crate) fn build_history_event(
    row: &StorageHistoryEvent,
    anchor_name: &str,
    include: HistoryInclude,
) -> Option<HistoryEvent> {
    let event_type = history_event_type(&row.event_kind)?;

    Some(HistoryEvent {
        id: history_event_id(row),
        event_type,
        name: history_event_name(row, anchor_name),
        subject: None,
        namespace: row.namespace.clone(),
        registration_id: row
            .registration_id
            .map(|registration_id| registration_id.to_string()),
        block_number: row.block_number,
        timestamp: row.block_timestamp.map(format_timestamp),
        transaction_hash: row.transaction_hash.clone(),
        log_index: row.log_index,
        detail: include.data.then(|| build_event_detail(row, event_type)),
        kind: raw_event_kind(row, include),
    })
}

/// The opaque identity of one event row, shared by every history route: a digest of
/// the storage identity that redo re-derives byte-for-byte for the same block, so a
/// consumer can merge and de-duplicate feeds without reading the identity itself.
/// It is not stable across a re-derivation boundary.
pub(crate) fn history_event_id(row: &StorageHistoryEvent) -> String {
    use sha2::{Digest, Sha256};
    hex::encode(Sha256::digest(row.event_identity.as_bytes()))
}

pub(crate) fn history_event_type(event_kind: &str) -> Option<HistoryEventType> {
    HistoryEventType::ALL
        .iter()
        .copied()
        .find(|event_type| event_type.storage_event_kinds().contains(&event_kind))
}

pub(crate) fn product_history_event_kinds() -> Vec<String> {
    HistoryEventType::ALL
        .iter()
        .flat_map(|event_type| event_type.storage_event_kinds())
        .copied()
        .collect::<BTreeSet<_>>()
        .into_iter()
        .map(str::to_owned)
        .collect()
}

pub(crate) fn map_history_page_error(
    error: anyhow::Error,
    internal_message: &'static str,
) -> V2Error {
    if error
        .downcast_ref::<bigname_storage::InterpretRedoInProgress>()
        .is_some()
    {
        tracing::debug!(error = ?error, "history page refused during Interpret redo");
        history_redo_stale_error()
    } else if error
        .downcast_ref::<bigname_storage::InvalidHistoryCursor>()
        .is_some()
    {
        invalid_cursor_error()
    } else {
        tracing::error!(error = ?error, message = internal_message, "failed to load history page");
        V2Error::internal_error(internal_message)
    }
}

fn history_redo_stale_error() -> V2Error {
    V2Error::stale("history is temporarily unavailable while Interpret redo is in progress")
}

fn history_event_name(row: &StorageHistoryEvent, anchor_name: &str) -> String {
    let _ = row;
    anchor_name.to_owned()
}

pub(crate) fn history_storage_scope(scope: HistoryScope) -> bigname_storage::HistoryScope {
    match scope {
        HistoryScope::Name => bigname_storage::HistoryScope::Surface,
        HistoryScope::Registration => bigname_storage::HistoryScope::Resource,
        HistoryScope::Both => bigname_storage::HistoryScope::Both,
    }
}

pub(crate) async fn v2_exact_name_snapshot_scope(
    state: &AppState,
    namespace: &str,
    at: Option<&AtSelector>,
) -> V2Result<SnapshotSelectionScope> {
    v2_exact_name_snapshot_scope_with_resolution_auxiliary(state, namespace, at, false).await
}

pub(crate) async fn v2_exact_name_snapshot_scope_with_resolution_auxiliary(
    state: &AppState,
    namespace: &str,
    at: Option<&AtSelector>,
    include_resolution_auxiliary: bool,
) -> V2Result<SnapshotSelectionScope> {
    let at_positions = at.map(v2_snapshot_scope_at_selector).transpose()?.flatten();
    let selector = at_positions
        .as_deref()
        .map(ExactNameSnapshotSelector::from_at)
        .unwrap_or_default();

    exact_name_snapshot_scope(
        &state.pool,
        namespace,
        selector,
        include_resolution_auxiliary,
    )
    .await
    .map_err(api_error_to_v2)
}

fn v2_snapshot_scope_at_selector(at: &AtSelector) -> V2Result<Option<String>> {
    match at {
        AtSelector::Timestamp(_) => Ok(None),
        AtSelector::SnapshotToken(token) => {
            let SnapshotAt::ResolvedPositions(chain_positions) = decode_at_token(token)? else {
                return Ok(None);
            };
            Ok(Some(chain_positions.to_value().to_string()))
        }
    }
}

pub(crate) fn format_timestamp(value: OffsetDateTime) -> String {
    let value = value.to_offset(UtcOffset::UTC);
    format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}Z",
        value.year(),
        value.month() as u8,
        value.day(),
        value.hour(),
        value.minute(),
        value.second()
    )
}

mod children;
mod cursor;

pub(crate) use self::children::HistoryRowSubject;

pub(crate) use self::cursor::{
    HistoryCursorBinding, history_cursor_payload, history_storage_cursor,
};

#[cfg(test)]
mod tests;
