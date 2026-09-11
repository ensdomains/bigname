use std::collections::{BTreeMap, BTreeSet};

use axum::{
    Json,
    extract::{Path, State},
};
use bigname_storage::{
    HistoryBlockWindow, HistoryCursor, HistoryEvent as StorageHistoryEvent, HistoryOrder,
    HistoryPageOptions, HistorySummary, HistorySummaryMode, SnapshotAt, SnapshotSelectionScope,
};
use serde::{Deserialize, Serialize};
use sqlx::types::time::{OffsetDateTime, UtcOffset};

use crate::AppState;

use super::cursor::{cursor_value, invalid_cursor_error};
use super::support::{
    ExactNameSnapshotSelector, exact_name_snapshot_scope, normalize_inferred_route_name,
};
use super::{
    AtSelector, CursorPayload, Envelope, EventDetail, HistoryEventType, HistoryInclude,
    HistoryScope, Meta, Page, QueryParamAllowlist, QueryParams, SortOrder, StrictQueryParams,
    V2Error, V2Result, all_chain_slugs, api_error_to_v2, build_event_detail, decode,
    decode_at_token, encode, history_include, raw_event_kind, validate_latest_collection_selectors,
};

const HISTORY_SORT_DESC: &str = "chain_position_desc";
const HISTORY_SORT_ASC: &str = "chain_position_asc";
const NAMESPACE_FILTER_KEY: &str = "namespace";
const NAME_FILTER_KEY: &str = "name";
const SCOPE_FILTER_KEY: &str = "scope";
const TYPE_FILTER_KEY: &str = "type";
const FROM_TIMESTAMP_FILTER_KEY: &str = "from_timestamp";
const TO_TIMESTAMP_FILTER_KEY: &str = "to_timestamp";
const NORMALIZED_EVENT_ID_CURSOR_KEY: &str = "normalized_event_id";
const EVENT_IDENTITY_CURSOR_KEY: &str = "event_identity";

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
    #[serde(rename = "type")]
    pub(crate) event_type: HistoryEventType,
    pub(crate) name: String,
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
    let include = history_include(&params.include)?;
    let normalized = normalize_inferred_route_name(&input_name)
        .map_err(|error| V2Error::invalid_input(error.message))?;
    let namespace = params
        .namespace
        .clone()
        .unwrap_or_else(|| normalized.namespace.to_owned());

    let logical_name_id =
        bigname_storage::logical_name_id_for_name(&namespace, &normalized.normalized_name);
    let cursor_binding = HistoryCursorBinding {
        namespace: &namespace,
        parent_logical_name_id: &logical_name_id,
        scope: params.scope,
        order: history_storage_order(params.order),
        params: &params,
    };
    let storage_cursor = params
        .cursor
        .as_deref()
        .map(|cursor| {
            let payload = decode(cursor)?;
            history_storage_cursor(&payload, &cursor_binding)
        })
        .transpose()?;
    let block_window = resolve_history_block_window(&state.pool, &params).await?;
    let interpret_redo_fence = bigname_storage::capture_interpret_redo_fence(&state.pool)
        .await
        .map_err(|error| map_history_page_error(error, "failed to load name history"))?;
    let parent = bigname_storage::load_name_current(&state.pool, &logical_name_id)
        .await
        .map_err(|error| {
            tracing::error!(error = ?error, "failed to load history parent projection");
            V2Error::internal_error(format!(
                "failed to load history for {}/{}",
                namespace, normalized.normalized_name
            ))
        })?;
    let parent = match parent {
        Some(parent) => parent,
        None => {
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
    };

    let resource_ids = if matches!(params.scope, HistoryScope::Name) {
        Vec::new()
    } else {
        bigname_storage::load_surface_bindings_by_logical_name_id(
            &state.pool,
            &parent.logical_name_id,
        )
        .await
        .map_err(|error| {
            tracing::error!(
                logical_name_id = %parent.logical_name_id,
                error = ?error,
                "failed to load history registration bindings"
            );
            V2Error::internal_error(format!(
                "failed to load history for {}/{}",
                namespace, normalized.normalized_name
            ))
        })?
        .into_iter()
        .map(|binding| binding.resource_id)
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
    };
    let storage_scope = history_storage_scope(params.scope);
    let options = history_page_options(&params, block_window);

    let storage_page = bigname_storage::load_name_history_page(
        &state.pool,
        &parent.logical_name_id,
        &resource_ids,
        storage_scope,
        true,
        storage_cursor.as_ref(),
        params.page_size,
        HistorySummaryMode::CappedCount(HISTORY_TOTAL_COUNT_CAP),
        &options,
        Some(&interpret_redo_fence),
    )
    .await
    .map_err(|error| map_history_page_error(error, "failed to load name history"))?;

    let next_cursor = storage_page
        .next_cursor
        .as_ref()
        .map(|cursor| encode(&history_cursor_payload(cursor, &cursor_binding)));
    let has_more = next_cursor.is_some();
    let total_count = history_total_count(storage_page.summary.as_ref());
    let data = storage_page
        .rows
        .iter()
        .filter_map(|row| build_history_event(row, &normalized.normalized_name, include))
        .collect();
    Ok(Json(Envelope {
        data,
        page: Some(Page {
            cursor: params.cursor.clone(),
            next_cursor,
            page_size: params.page_size,
            total_count,
            has_more,
        }),
        meta: Meta::default(),
    }))
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
        event_type,
        name: history_event_name(row, anchor_name),
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

#[derive(Clone, Debug)]
pub(crate) struct HistoryCursorBinding<'a> {
    pub(crate) namespace: &'a str,
    pub(crate) parent_logical_name_id: &'a str,
    pub(crate) scope: HistoryScope,
    pub(crate) order: HistoryOrder,
    pub(crate) params: &'a QueryParams,
}

fn history_cursor_filters(binding: &HistoryCursorBinding<'_>) -> BTreeMap<String, String> {
    let mut filters = BTreeMap::from([
        (
            NAMESPACE_FILTER_KEY.to_owned(),
            binding.namespace.to_owned(),
        ),
        (
            NAME_FILTER_KEY.to_owned(),
            binding.parent_logical_name_id.to_owned(),
        ),
        (
            SCOPE_FILTER_KEY.to_owned(),
            binding.scope.as_str().to_owned(),
        ),
    ]);
    insert_history_filter_keys(&mut filters, binding.params);
    filters
}

pub(crate) fn history_cursor_payload(
    cursor: &HistoryCursor,
    binding: &HistoryCursorBinding<'_>,
) -> CursorPayload {
    CursorPayload::new(
        history_sort_token(binding.order),
        history_cursor_filters(binding),
        BTreeMap::from([
            (
                NORMALIZED_EVENT_ID_CURSOR_KEY.to_owned(),
                cursor.normalized_event_id.to_string(),
            ),
            (
                EVENT_IDENTITY_CURSOR_KEY.to_owned(),
                cursor.event_identity.clone(),
            ),
        ]),
        None,
    )
}

pub(crate) fn history_storage_cursor(
    payload: &CursorPayload,
    binding: &HistoryCursorBinding<'_>,
) -> V2Result<HistoryCursor> {
    if payload.sort != history_sort_token(binding.order) {
        return Err(invalid_cursor_error());
    }
    if payload.filters != history_cursor_filters(binding) {
        return Err(invalid_cursor_error());
    }
    if payload.last_item.len() != 2 {
        return Err(invalid_cursor_error());
    }

    let normalized_event_id = cursor_value(
        payload,
        NORMALIZED_EVENT_ID_CURSOR_KEY,
        invalid_cursor_error,
    )?
    .parse::<i64>()
    .map_err(|_| invalid_cursor_error())?;
    let event_identity = cursor_value(payload, EVENT_IDENTITY_CURSOR_KEY, invalid_cursor_error)?;

    Ok(HistoryCursor {
        normalized_event_id,
        event_identity,
    })
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::v2::RawQueryParams;

    fn sample_cursor() -> HistoryCursor {
        HistoryCursor {
            normalized_event_id: 42,
            event_identity: "event:42".to_owned(),
        }
    }

    fn params(raw: RawQueryParams) -> QueryParams {
        QueryParams::try_from(raw).expect("params must parse")
    }

    fn binding<'a>(params: &'a QueryParams, scope: HistoryScope) -> HistoryCursorBinding<'a> {
        HistoryCursorBinding {
            namespace: "ens",
            parent_logical_name_id: "ens:parent.eth",
            scope,
            order: history_storage_order(params.order),
            params,
        }
    }

    #[test]
    fn history_cursor_payload_round_trips_storage_cursor() {
        let cursor = sample_cursor();
        let params = params(RawQueryParams::default());
        let binding = binding(&params, HistoryScope::Both);
        let payload = history_cursor_payload(&cursor, &binding);

        assert_eq!(payload.sort, "chain_position_desc");
        assert_eq!(
            payload.filters,
            BTreeMap::from([
                ("namespace".to_owned(), "ens".to_owned()),
                ("name".to_owned(), "ens:parent.eth".to_owned()),
                ("scope".to_owned(), "both".to_owned()),
            ])
        );

        assert_eq!(
            history_storage_cursor(&payload, &binding).expect("cursor must decode"),
            cursor
        );
        assert!(payload.snapshot.is_none());
    }

    #[test]
    fn history_cursor_binds_order_type_set_and_timestamp_window() {
        let cursor = sample_cursor();
        let filtered = params(RawQueryParams {
            order: Some("asc".to_owned()),
            event_type: Some("renewal,registration".to_owned()),
            from_timestamp: Some("2023-11-14T23:15:04+01:00".to_owned()),
            to_timestamp: Some("2023-11-14T22:15:07Z".to_owned()),
            ..RawQueryParams::default()
        });
        let filtered_binding = binding(&filtered, HistoryScope::Both);
        let payload = history_cursor_payload(&cursor, &filtered_binding);

        assert_eq!(payload.sort, "chain_position_asc");
        assert_eq!(
            payload.filters,
            BTreeMap::from([
                ("namespace".to_owned(), "ens".to_owned()),
                ("name".to_owned(), "ens:parent.eth".to_owned()),
                ("scope".to_owned(), "both".to_owned()),
                ("type".to_owned(), "registration,renewal".to_owned()),
                (
                    "from_timestamp".to_owned(),
                    "2023-11-14T22:15:04Z".to_owned()
                ),
                ("to_timestamp".to_owned(), "2023-11-14T22:15:07Z".to_owned()),
            ])
        );
        assert_eq!(
            history_storage_cursor(&payload, &filtered_binding).expect("cursor must decode"),
            cursor
        );

        let unfiltered = params(RawQueryParams::default());
        assert!(
            history_storage_cursor(&payload, &binding(&unfiltered, HistoryScope::Both)).is_err()
        );
        let other_order = params(RawQueryParams {
            event_type: Some("renewal,registration".to_owned()),
            from_timestamp: Some("2023-11-14T22:15:04Z".to_owned()),
            to_timestamp: Some("2023-11-14T22:15:07Z".to_owned()),
            ..RawQueryParams::default()
        });
        assert!(
            history_storage_cursor(&payload, &binding(&other_order, HistoryScope::Both)).is_err()
        );
        let other_types = params(RawQueryParams {
            order: Some("asc".to_owned()),
            event_type: Some("renewal".to_owned()),
            from_timestamp: Some("2023-11-14T22:15:04Z".to_owned()),
            to_timestamp: Some("2023-11-14T22:15:07Z".to_owned()),
            ..RawQueryParams::default()
        });
        assert!(
            history_storage_cursor(&payload, &binding(&other_types, HistoryScope::Both)).is_err()
        );
    }

    #[test]
    fn history_cursor_rejects_wrong_sort_filter_or_scope() {
        let cursor = sample_cursor();
        let params = params(RawQueryParams::default());
        let both = binding(&params, HistoryScope::Both);

        let mut payload = history_cursor_payload(&cursor, &both);
        payload.sort = "wrong".to_owned();
        assert!(history_storage_cursor(&payload, &both).is_err());

        let mut payload = history_cursor_payload(&cursor, &both);
        payload
            .filters
            .insert("name".to_owned(), "ens:other.eth".to_owned());
        assert!(history_storage_cursor(&payload, &both).is_err());

        let payload = history_cursor_payload(&cursor, &binding(&params, HistoryScope::Name));
        assert!(history_storage_cursor(&payload, &both).is_err());
    }

    #[test]
    fn history_cursor_ignores_legacy_snapshot_component() {
        let cursor = sample_cursor();
        let params = params(RawQueryParams::default());
        let both = binding(&params, HistoryScope::Both);
        let mut payload = history_cursor_payload(&cursor, &both);
        payload.snapshot = Some("legacy-snapshot".to_owned());

        assert_eq!(
            history_storage_cursor(&payload, &both)
                .expect("legacy snapshot component must not bind a latest-state cursor"),
            cursor
        );
    }

    #[test]
    fn history_page_options_follow_type_set_and_order() {
        let defaulted = history_page_options(&params(RawQueryParams::default()), None);
        assert_eq!(defaulted.order, HistoryOrder::Desc);
        assert_eq!(defaulted.event_kinds, product_history_event_kinds());
        assert!(!defaulted.bind_cursor_anchor_to_event_kinds);
        assert!(defaulted.block_window.is_none());

        let filtered = history_page_options(
            &params(RawQueryParams {
                order: Some("asc".to_owned()),
                event_type: Some("renewal".to_owned()),
                ..RawQueryParams::default()
            }),
            Some(HistoryBlockWindow::default()),
        );
        assert_eq!(filtered.order, HistoryOrder::Asc);
        assert_eq!(filtered.event_kinds, vec!["RegistrationRenewed".to_owned()]);
        assert!(filtered.bind_cursor_anchor_to_event_kinds);
        assert!(filtered.block_window.is_some());
    }

    #[test]
    fn history_total_count_applies_the_cap() {
        let summary = |total_count: u64| HistorySummary {
            total_count,
            normalized_event_ids: Vec::new(),
            raw_fact_refs: Vec::new(),
            manifest_versions: Vec::new(),
            chain_position_samples: Vec::new(),
            last_updated: None,
        };
        assert_eq!(history_total_count(None), None);
        assert_eq!(history_total_count(Some(&summary(0))), Some(0));
        assert_eq!(
            history_total_count(Some(&summary(HISTORY_TOTAL_COUNT_CAP))),
            Some(HISTORY_TOTAL_COUNT_CAP)
        );
        assert_eq!(
            history_total_count(Some(&summary(HISTORY_TOTAL_COUNT_CAP + 1))),
            None
        );
    }

    #[test]
    fn history_event_type_filters_non_product_kinds() {
        assert_eq!(
            history_event_type("RegistrationRenewed"),
            Some(HistoryEventType::Renewal)
        );
        assert_eq!(
            history_event_type("RegistrationReleased"),
            Some(HistoryEventType::Release)
        );
        assert_eq!(
            history_event_type("ExpiryChanged"),
            Some(HistoryEventType::Expiry)
        );
        assert_eq!(
            history_event_type("AuthorityEpochChanged"),
            Some(HistoryEventType::Authority)
        );
        assert_eq!(history_event_type("SurfaceBound"), None);
        assert_eq!(history_event_type("PreimageObserved"), None);
        assert_eq!(history_event_type("MigrationApplied"), None);
        assert_eq!(history_event_type("ContractDiscovered"), None);
    }
}
