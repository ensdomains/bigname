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
    HistoryInclude, Meta, Page, QueryParamAllowlist, QueryParams, StrictQueryParams, V2Error,
    V2Result, build_event_detail, decode, encode, format_timestamp, history_event_type,
    history_include, history_sort_token, history_storage_order, history_total_count,
    insert_history_filter_keys, map_history_page_error, product_history_event_kinds,
    raw_event_kind, resolve_history_block_window, validate_latest_collection_selectors,
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

    let storage_cursor = params
        .cursor
        .as_deref()
        .map(|cursor| {
            let payload = decode(cursor)?;
            events_storage_cursor(&payload, &parsed.cursor_filters, order)
        })
        .transpose()?;
    parsed.storage_filter.block_window = resolve_history_block_window(&state.pool, &params).await?;
    let summary_mode = if parsed.anchored {
        HistorySummaryMode::CappedCount(HISTORY_TOTAL_COUNT_CAP)
    } else {
        HistorySummaryMode::None
    };

    let storage_page = bigname_storage::load_event_history_page_with_redo_policy(
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
    .map_err(|error| map_history_page_error(error, "failed to load events"))?;

    #[cfg(test)]
    bigname_storage::history_anchor_read_test_hooks::run(
        &state.pool,
        bigname_storage::history_anchor_read_test_hooks::HistoryReadHookPoint::AfterPage,
    )
    .await
    .map_err(|_| V2Error::internal_error("failed to run history read test hook"))?;

    let next_cursor = storage_page.next_cursor.as_ref().map(|cursor| {
        encode(&events_cursor_payload(
            cursor,
            &parsed.cursor_filters,
            order,
        ))
    });
    let has_more = next_cursor.is_some();
    let total_count = history_total_count(storage_page.summary.as_ref());
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
        meta: Meta::default(),
    }))
}

pub(crate) fn build_event(
    row: &StorageHistoryEvent,
    name: Option<&str>,
    include: HistoryInclude,
) -> Option<Event> {
    let event_type = history_event_type(&row.event_kind)?;

    Some(Event {
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
mod tests {
    use std::collections::BTreeMap;

    use bigname_storage::CanonicalityState;
    use serde_json::{Value, json};

    use super::*;
    use crate::v2::{ErrorCode, RawQueryParams};

    const ADDRESS: &str = "0x00000000000000000000000000000000000000aa";
    const REGISTRATION_ID: &str = "550e8400-e29b-41d4-a716-446655440000";

    fn sample_cursor() -> HistoryCursor {
        HistoryCursor {
            normalized_event_id: 42,
            event_identity: "event:42".to_owned(),
        }
    }

    fn sample_filters() -> BTreeMap<String, String> {
        BTreeMap::from([
            ("namespace".to_owned(), "ens".to_owned()),
            ("type".to_owned(), "registration".to_owned()),
            ("from_block".to_owned(), "10".to_owned()),
        ])
    }

    fn storage_event(event_kind: &str, logical_name_id: Option<&str>) -> StorageHistoryEvent {
        StorageHistoryEvent {
            normalized_event_id: 1,
            event_identity: "event:1".to_owned(),
            namespace: "ens".to_owned(),
            logical_name_id: logical_name_id.map(str::to_owned),
            resource_id: Some(Uuid::parse_str(REGISTRATION_ID).expect("uuid literal must parse")),
            registration_id: Some(
                Uuid::parse_str(REGISTRATION_ID).expect("uuid literal must parse"),
            ),
            event_kind: event_kind.to_owned(),
            source_family: "ens_v1".to_owned(),
            manifest_version: 1,
            source_manifest_id: Some(1),
            chain_id: Some("eip155:1".to_owned()),
            block_number: Some(100),
            block_hash: Some("0xblock".to_owned()),
            block_timestamp: None,
            transaction_hash: Some("0xtx".to_owned()),
            log_index: Some(5),
            raw_fact_ref: json!({}),
            derivation_kind: "direct".to_owned(),
            canonicality_state: CanonicalityState::Canonical,
            before_state: json!({}),
            after_state: json!({}),
            migration_correlation_ids: Vec::new(),
            consumer_visibility: "activated".to_owned(),
            migration_associations: json!([]),
            provenance: json!({}),
            coverage: json!({}),
        }
    }

    #[test]
    fn resolve_events_namespace_uses_explicit_inferred_or_global_default() {
        let params = QueryParams::try_from(RawQueryParams {
            namespace: Some("ens".to_owned()),
            name: Some("alice.base.eth".to_owned()),
            ..RawQueryParams::default()
        })
        .expect("params must parse");
        assert_eq!(
            resolve_events_namespace(&params).expect("namespace"),
            Some("ens".to_owned())
        );

        let params = QueryParams::try_from(RawQueryParams {
            name: Some("alice.base.eth".to_owned()),
            ..RawQueryParams::default()
        })
        .expect("params must parse");
        assert_eq!(
            resolve_events_namespace(&params).expect("namespace"),
            Some("basenames".to_owned())
        );

        let params = QueryParams::try_from(RawQueryParams {
            name: Some("alice.eth".to_owned()),
            ..RawQueryParams::default()
        })
        .expect("params must parse");
        assert_eq!(
            resolve_events_namespace(&params).expect("namespace"),
            Some("ens".to_owned())
        );

        let params = QueryParams::try_from(RawQueryParams::default()).expect("params must parse");
        assert_eq!(
            resolve_events_namespace(&params).expect("namespace"),
            Some("ens".to_owned())
        );

        let params = QueryParams::try_from(RawQueryParams {
            name: Some("bad name.eth".to_owned()),
            ..RawQueryParams::default()
        })
        .expect("params must parse");
        let error = resolve_events_namespace(&params).expect_err("invalid name must fail");
        assert_eq!(error.code(), ErrorCode::InvalidInput);
    }

    #[test]
    fn events_cursor_payload_round_trips_storage_cursor() {
        let cursor = sample_cursor();
        let filters = sample_filters();
        let payload = events_cursor_payload(&cursor, &filters, HistoryOrder::Desc);

        assert_eq!(payload.sort, "chain_position_desc");
        assert_eq!(payload.filters, filters);
        assert_eq!(
            events_storage_cursor(&payload, &sample_filters(), HistoryOrder::Desc)
                .expect("cursor must decode"),
            cursor
        );
        assert!(payload.snapshot.is_none());

        let payload = events_cursor_payload(&cursor, &filters, HistoryOrder::Asc);
        assert_eq!(payload.sort, "chain_position_asc");
        assert_eq!(
            events_storage_cursor(&payload, &filters, HistoryOrder::Asc)
                .expect("asc cursor must decode"),
            cursor
        );
        assert!(events_storage_cursor(&payload, &filters, HistoryOrder::Desc).is_err());
    }

    #[test]
    fn events_cursor_rejects_wrong_sort_or_filters() {
        let cursor = sample_cursor();
        let filters = sample_filters();

        let mut payload = events_cursor_payload(&cursor, &filters, HistoryOrder::Desc);
        payload.sort = "name".to_owned();
        assert!(events_storage_cursor(&payload, &filters, HistoryOrder::Desc).is_err());

        let mut payload = events_cursor_payload(&cursor, &filters, HistoryOrder::Desc);
        payload
            .filters
            .insert("to_block".to_owned(), "20".to_owned());
        assert!(events_storage_cursor(&payload, &filters, HistoryOrder::Desc).is_err());

        let mut payload = events_cursor_payload(&cursor, &filters, HistoryOrder::Desc);
        payload.filters.remove("namespace");
        assert!(events_storage_cursor(&payload, &filters, HistoryOrder::Desc).is_err());

        let payload = events_cursor_payload(&cursor, &filters, HistoryOrder::Desc);
        assert!(
            events_storage_cursor(
                &payload,
                &BTreeMap::from([("namespace".to_owned(), "ens".to_owned())]),
                HistoryOrder::Desc,
            )
            .is_err()
        );
    }

    #[test]
    fn events_cursor_ignores_legacy_snapshot_component() {
        let cursor = sample_cursor();
        let filters = sample_filters();
        let mut payload = events_cursor_payload(&cursor, &filters, HistoryOrder::Desc);
        payload.snapshot = Some("legacy-snapshot".to_owned());

        assert_eq!(
            events_storage_cursor(&payload, &filters, HistoryOrder::Desc)
                .expect("legacy snapshot component must not bind a latest-state cursor"),
            cursor
        );
    }

    #[test]
    fn build_event_derives_name_and_drops_non_product_kinds() {
        let event = build_event(
            &storage_event("RegistrationGranted", Some("ens:alice.eth")),
            Some("alice.eth"),
            HistoryInclude::default(),
        )
        .expect("product event must build");

        assert_eq!(event.event_type, HistoryEventType::Registration);
        assert_eq!(event.name, Some("alice.eth".to_owned()));
        assert_eq!(event.namespace, "ens");
        assert_eq!(event.registration_id, Some(REGISTRATION_ID.to_owned()));
        assert_eq!(event.block_number, Some(100));
        assert_eq!(event.transaction_hash, Some("0xtx".to_owned()));
        assert_eq!(event.log_index, Some(5));

        let event = build_event(
            &storage_event("RecordChanged", None),
            None,
            HistoryInclude::default(),
        )
        .expect("product event without name must build");
        assert_eq!(event.name, None);
        assert!(event.detail.is_none());
        assert!(event.kind.is_none());
        let serialized = serde_json::to_value(&event).expect("event must serialize");
        assert!(serialized.get("kind").is_none());
        assert!(serialized.get("data").is_none());
        assert!(serialized.get("contract_address").is_none());

        let detailed = build_event(
            &storage_event("RecordChanged", None),
            None,
            HistoryInclude::DATA,
        )
        .expect("detailed product event must build");
        let serialized = serde_json::to_value(&detailed).expect("event must serialize");
        assert!(serialized.get("kind").is_none());
        assert_eq!(serialized["contract_address"], Value::Null);
        assert_eq!(serialized["data"], json!({}));

        let raw = build_event(
            &storage_event("RecordChanged", None),
            None,
            HistoryInclude::RAW,
        )
        .expect("raw product event must build");
        let serialized = serde_json::to_value(&raw).expect("event must serialize");
        assert_eq!(serialized["kind"], json!("RecordChanged"));
        assert!(serialized.get("data").is_none());
        assert!(serialized.get("contract_address").is_none());

        let both = build_event(
            &storage_event("RecordChanged", None),
            None,
            HistoryInclude {
                data: true,
                raw: true,
            },
        )
        .expect("raw detailed product event must build");
        let serialized = serde_json::to_value(&both).expect("event must serialize");
        assert_eq!(serialized["kind"], json!("RecordChanged"));
        assert_eq!(serialized["data"], json!({}));

        assert!(
            build_event(
                &storage_event("SurfaceBound", Some("ens:alice.eth")),
                Some("alice.eth"),
                HistoryInclude::default(),
            )
            .is_none()
        );
        assert!(
            build_event(
                &storage_event("MigrationApplied", Some("ens:alice.eth")),
                Some("alice.eth"),
                HistoryInclude::default(),
            )
            .is_none()
        );
        assert!(
            build_event(
                &storage_event("ContractDiscovered", None),
                None,
                HistoryInclude::default()
            )
            .is_none()
        );
    }

    #[test]
    fn events_filter_rejects_invalid_block_range() {
        let params = QueryParams::try_from(RawQueryParams {
            from_block: Some("20".to_owned()),
            to_block: Some("10".to_owned()),
            ..RawQueryParams::default()
        })
        .expect("block bounds parse globally");
        let error =
            parse_events_filter(&params, Some("ens")).expect_err("bad block range must fail");
        assert_eq!(error.code(), ErrorCode::InvalidInput);
    }

    #[test]
    fn events_filter_builds_storage_filter_and_cursor_filters() {
        let params = QueryParams::try_from(RawQueryParams {
            namespace: Some("basenames".to_owned()),
            event_type: Some("permission".to_owned()),
            name: Some(" Alice.base.eth ".to_owned()),
            registration_id: Some(REGISTRATION_ID.to_owned()),
            address: Some(ADDRESS.to_owned()),
            from_block: Some("10".to_owned()),
            to_block: Some("20".to_owned()),
            from_timestamp: Some("2023-11-14T22:15:04Z".to_owned()),
            to_timestamp: Some("2023-11-14T23:15:07+01:00".to_owned()),
            order: Some("asc".to_owned()),
            ..RawQueryParams::default()
        })
        .expect("filters must parse globally");

        let parsed = parse_events_filter(&params, Some("basenames")).expect("filter must build");

        assert!(parsed.anchored);
        assert_eq!(parsed.storage_filter.order, HistoryOrder::Asc);
        assert!(parsed.storage_filter.block_window.is_none());
        assert_eq!(
            parsed.cursor_filters,
            BTreeMap::from([
                ("address".to_owned(), ADDRESS.to_owned()),
                ("from_block".to_owned(), "10".to_owned()),
                (
                    "from_timestamp".to_owned(),
                    "2023-11-14T22:15:04Z".to_owned()
                ),
                (
                    "name".to_owned(),
                    bigname_storage::logical_name_id_for_name("basenames", "alice.base.eth"),
                ),
                ("namespace".to_owned(), "basenames".to_owned()),
                ("registration_id".to_owned(), REGISTRATION_ID.to_owned()),
                ("to_block".to_owned(), "20".to_owned()),
                ("to_timestamp".to_owned(), "2023-11-14T22:15:07Z".to_owned()),
                ("type".to_owned(), "permission".to_owned()),
            ])
        );
        assert_eq!(
            parsed.storage_filter.event_kinds,
            vec![
                "PermissionChanged".to_owned(),
                "PermissionScopeChanged".to_owned(),
                "RolesChanged".to_owned(),
                "EACRolesChanged".to_owned(),
            ]
        );

        let unanchored = parse_events_filter(
            &QueryParams::try_from(RawQueryParams {
                namespace: Some("ens".to_owned()),
                event_type: Some("renewal,registration".to_owned()),
                ..RawQueryParams::default()
            })
            .expect("filters must parse globally"),
            Some("ens"),
        )
        .expect("filter must build");
        assert!(!unanchored.anchored);
        assert_eq!(unanchored.storage_filter.order, HistoryOrder::Desc);
        assert_eq!(
            unanchored.cursor_filters,
            BTreeMap::from([
                ("namespace".to_owned(), "ens".to_owned()),
                ("type".to_owned(), "registration,renewal".to_owned()),
            ])
        );
        assert_eq!(
            unanchored.storage_filter.event_kinds,
            vec![
                "RegistrationGranted".to_owned(),
                "LabelRegistered".to_owned(),
                "RegistrationRenewed".to_owned(),
            ]
        );
        assert_eq!(
            parsed.storage_filter.logical_name_id,
            Some(bigname_storage::logical_name_id_for_name(
                "basenames",
                "alice.base.eth"
            ))
        );
        assert_eq!(parsed.storage_filter.from_block, Some(10));
        assert_eq!(parsed.storage_filter.to_block, Some(20));
        assert_eq!(
            parsed
                .storage_filter
                .address
                .as_ref()
                .expect("address filter must exist")
                .relation,
            None
        );
    }
}
