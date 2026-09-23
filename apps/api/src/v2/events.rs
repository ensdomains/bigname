use std::collections::{BTreeMap, BTreeSet};

use axum::{Json, extract::State};
use bigname_storage::{
    EventHistoryAddressFilter, EventHistoryFilter, EventHistoryResolverFilter, HistoryCursor,
    HistoryEvent as StorageHistoryEvent, HistoryOrder, HistorySummaryMode,
};
use serde::{Deserialize, Serialize};
use sqlx::types::Uuid;

use crate::AppState;

use super::cursor::{cursor_value, invalid_cursor_error};
use super::support::normalize_inferred_route_name;
use super::{
    CursorPayload, Envelope, EventDetail, HISTORY_TOTAL_COUNT_CAP, HistoryEventType,
    HistoryInclude, Page, QueryParamAllowlist, QueryParams, StrictQueryParams, V2Error, V2Result,
    build_event_detail, decode, encode, format_timestamp, history_event_type, history_include,
    history_sort_token, history_storage_order, history_total_count, insert_history_filter_keys,
    map_history_page_error, product_history_event_kinds, raw_event_kind,
    resolve_history_block_window, validate_latest_collection_selectors,
};

const NAMESPACE_FILTER_KEY: &str = "namespace";
const NAME_FILTER_KEY: &str = "name";
const ADDRESS_FILTER_KEY: &str = "address";
const RESOLVER_FILTER_KEY: &str = "resolver";
const CONTRACT_ADDRESS_FILTER_KEY: &str = "contract_address";
const REGISTRATION_ID_FILTER_KEY: &str = "registration_id";
const FROM_BLOCK_FILTER_KEY: &str = "from_block";
const TO_BLOCK_FILTER_KEY: &str = "to_block";
const NORMALIZED_EVENT_ID_CURSOR_KEY: &str = "normalized_event_id";
const EVENT_IDENTITY_CURSOR_KEY: &str = "event_identity";

pub(crate) struct EventsQueryParams;

impl QueryParamAllowlist for EventsQueryParams {
    const ALLOWED: &'static [&'static str] = &[
        "namespace",
        "name",
        "address",
        "resolver",
        "contract_address",
        "registration_id",
        "type",
        "from_block",
        "to_block",
        "from_timestamp",
        "to_timestamp",
        "order",
        "include",
        "at",
        "finality",
        "cursor",
        "page_size",
    ];
}

pub(crate) type EventsQuery = StrictQueryParams<EventsQueryParams>;

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub(crate) struct Event {
    pub(crate) id: String,
    #[serde(rename = "type")]
    pub(crate) event_type: HistoryEventType,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) name: Option<String>,
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

#[derive(Debug)]
pub(crate) struct ParsedEventsFilter {
    pub(crate) storage_filter: EventHistoryFilter,
    pub(crate) cursor_filters: BTreeMap<String, String>,
    /// Whether an anchor (name, registration, address, or resolver) bounds the
    /// read, which is what makes a capped `total_count` affordable.
    pub(crate) anchored: bool,
}

/// `namespace` defaults to the name's inferred namespace when `name` is provided
/// and `namespace` is omitted; otherwise defaults to `ens`, except that a
/// `resolver` filter alone reads every namespace the resolver contract serves.
pub(crate) async fn get_events(
    params: EventsQuery,
    State(state): State<AppState>,
) -> V2Result<Json<Envelope<Vec<Event>>>> {
    let params = params.into_inner();
    validate_latest_collection_selectors(params.at.as_ref(), params.finality)?;
    let include = history_include(&params.include)?;
    let namespace = resolve_events_namespace(&params)?;
    let mut parsed = parse_events_filter(&params, namespace.as_deref())?;
    if params.event_types.is_none() {
        parsed.storage_filter.event_kinds = product_history_event_kinds();
    }
    let order = parsed.storage_filter.order;

    let cursor = params.cursor.as_deref().map(decode).transpose()?;
    let storage_cursor = cursor
        .as_ref()
        .map(|payload| events_storage_cursor(payload, &parsed.cursor_filters, order))
        .transpose()?;
    let history = super::collection_binding::HistoryCollection::capture(
        &state,
        cursor.as_ref(),
        namespace.as_deref(),
    )
    .await?;
    parsed.storage_filter.block_window = Some(super::history::bound_history_block_window(
        resolve_history_block_window(&state.pool, &params).await?,
        &history.block_bounds(),
    ));
    parsed.storage_filter.publication_block_bounds = Some(history.block_bounds());
    let summary_mode = if parsed.anchored {
        if params.include.iter().any(|v| v == "total_count") {
            HistorySummaryMode::Count
        } else {
            HistorySummaryMode::CappedCount(HISTORY_TOTAL_COUNT_CAP)
        }
    } else {
        HistorySummaryMode::None
    };

    let storage_page = match bigname_storage::load_event_history_page_with_redo_policy(
        &state.pool,
        parsed.storage_filter,
        true,
        storage_cursor.as_ref(),
        params.page_size,
        summary_mode,
        false,
        true,
    )
    .await
    {
        Ok(page) => page,
        Err(error) => {
            let error = map_history_page_error(error, "failed to load events");
            return Err(history.fail(&state, error).await);
        }
    };

    #[cfg(test)]
    bigname_storage::history_anchor_read_test_hooks::run(
        &state.pool,
        bigname_storage::history_anchor_read_test_hooks::HistoryReadHookPoint::AfterPage,
    )
    .await
    .map_err(|_| V2Error::internal_error("failed to run history read test hook"))?;

    let next_cursor = storage_page.next_cursor.as_ref().map(|cursor| {
        encode(&history.bind_cursor(events_cursor_payload(cursor, &parsed.cursor_filters, order)))
    });
    let has_more = next_cursor.is_some();
    let total_count = if params.include.iter().any(|v| v == "total_count") {
        storage_page.summary.as_ref().map(|s| s.total_count)
    } else {
        history_total_count(storage_page.summary.as_ref())
    };
    let logical_name_ids = storage_page
        .rows
        .iter()
        .filter_map(|row| row.logical_name_id.clone())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    let names = bigname_storage::load_name_current_by_logical_name_ids(
        &state.pool,
        &logical_name_ids,
    )
    .await
    .map_err(|error| {
        tracing::error!(error = ?error, "failed to load event names from phase projections");
        V2Error::internal_error("failed to load events")
    })?;
    if let Some(fence) = storage_page.interpret_redo_fence.as_ref() {
        bigname_storage::revalidate_interpret_redo_fence(&state.pool, fence)
            .await
            .map_err(|error| map_history_page_error(error, "failed to load events"))?;
    }
    let data = storage_page
        .rows
        .iter()
        .filter_map(|row| {
            let name = row
                .logical_name_id
                .as_ref()
                .and_then(|logical_name_id| names.get(logical_name_id))
                .map(|row| row.normalized_name.as_str());
            build_event(row, name, include)
        })
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
        meta: history.finish(&state).await?,
    }))
}

pub(crate) fn build_event(
    row: &StorageHistoryEvent,
    name: Option<&str>,
    include: HistoryInclude,
) -> Option<Event> {
    let event_type = history_event_type(&row.event_kind)?;

    Some(Event {
        id: super::history_event_id(row),
        event_type,
        name: name.map(str::to_owned),
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

pub(crate) fn events_cursor_payload(
    cursor: &HistoryCursor,
    filters: &BTreeMap<String, String>,
    order: HistoryOrder,
) -> CursorPayload {
    CursorPayload::new(
        history_sort_token(order),
        filters.clone(),
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

pub(crate) fn events_storage_cursor(
    payload: &CursorPayload,
    expected_filters: &BTreeMap<String, String>,
    order: HistoryOrder,
) -> V2Result<HistoryCursor> {
    if payload.sort != history_sort_token(order) {
        return Err(invalid_cursor_error());
    }
    if &payload.filters != expected_filters {
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

pub(crate) fn resolve_events_namespace(params: &QueryParams) -> V2Result<Option<String>> {
    match (params.namespace.as_deref(), params.name.as_deref()) {
        (Some(namespace), _) => Ok(Some(namespace.to_owned())),
        (None, Some(name)) => normalize_inferred_route_name(name)
            .map(|normalized| Some(normalized.namespace.to_owned()))
            .map_err(|error| V2Error::invalid_input(error.message)),
        (None, None) if params.resolver.is_some() => Ok(None),
        (None, None) => Ok(Some("ens".to_owned())),
    }
}

pub(crate) fn parse_events_filter(
    params: &QueryParams,
    namespace: Option<&str>,
) -> V2Result<ParsedEventsFilter> {
    if matches!(
        (params.from_block, params.to_block),
        (Some(from_block), Some(to_block)) if from_block > to_block
    ) {
        return Err(V2Error::invalid_input(
            "from_block must be less than or equal to to_block",
        ));
    }

    let logical_name_id = params
        .name
        .as_deref()
        .map(|name| {
            let normalized = normalize_inferred_route_name(name)
                .map_err(|error| V2Error::invalid_input(error.message))?;
            let namespace = namespace.unwrap_or(normalized.namespace);
            Ok::<_, V2Error>(bigname_storage::logical_name_id_for_name(
                namespace,
                &normalized.normalized_name,
            ))
        })
        .transpose()?;
    let resource_id = params
        .registration_id
        .as_deref()
        .map(|registration_id| {
            Uuid::parse_str(registration_id)
                .map_err(|_| V2Error::invalid_input("registration_id must be a UUID"))
        })
        .transpose()?;
    let event_kinds = params
        .event_types
        .as_ref()
        .map(|event_types| event_types.storage_event_kinds())
        .unwrap_or_default();

    let anchored = logical_name_id.is_some()
        || resource_id.is_some()
        || params.address.is_some()
        || params.resolver.is_some();
    let mut cursor_filters = BTreeMap::new();
    if let Some(namespace) = namespace {
        cursor_filters.insert(NAMESPACE_FILTER_KEY.to_owned(), namespace.to_owned());
    }
    if let Some(resolver) = params.resolver.as_ref() {
        cursor_filters.insert(RESOLVER_FILTER_KEY.to_owned(), resolver.canonical());
    }
    if let Some(logical_name_id) = logical_name_id.as_ref() {
        cursor_filters.insert(NAME_FILTER_KEY.to_owned(), logical_name_id.clone());
    }
    if let Some(address) = params.address.as_ref() {
        cursor_filters.insert(ADDRESS_FILTER_KEY.to_owned(), address.clone());
    }
    if let Some(contract_address) = params.contract_address.as_ref() {
        cursor_filters.insert(
            CONTRACT_ADDRESS_FILTER_KEY.to_owned(),
            contract_address.clone(),
        );
    }
    if let Some(registration_id) = params.registration_id.as_ref() {
        cursor_filters.insert(
            REGISTRATION_ID_FILTER_KEY.to_owned(),
            registration_id.clone(),
        );
    }
    insert_history_filter_keys(&mut cursor_filters, params);
    if let Some(from_block) = params.from_block {
        cursor_filters.insert(FROM_BLOCK_FILTER_KEY.to_owned(), from_block.to_string());
    }
    if let Some(to_block) = params.to_block {
        cursor_filters.insert(TO_BLOCK_FILTER_KEY.to_owned(), to_block.to_string());
    }

    Ok(ParsedEventsFilter {
        storage_filter: EventHistoryFilter {
            publication_block_bounds: None,
            namespace: namespace.map(str::to_owned),
            logical_name_id,
            resource_id,
            address: params
                .address
                .as_ref()
                .map(|address| EventHistoryAddressFilter {
                    address: address.clone(),
                    relation: None,
                }),
            resolver: params
                .resolver
                .as_ref()
                .map(|resolver| EventHistoryResolverFilter {
                    chain_id: resolver.chain_slug.to_owned(),
                    address: resolver.address.clone(),
                }),
            contract_address: params.contract_address.clone(),
            event_kinds,
            bind_cursor_anchor_to_event_kinds: params.event_types.is_some(),
            from_block: params.from_block,
            to_block: params.to_block,
            order: history_storage_order(params.order),
            block_window: None,
        },
        cursor_filters,
        anchored,
    })
}

#[cfg(test)]
mod tests;
