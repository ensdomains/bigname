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
        registration_id: Some(Uuid::parse_str(REGISTRATION_ID).expect("uuid literal must parse")),
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
    let error = parse_events_filter(&params, Some("ens")).expect_err("bad block range must fail");
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
