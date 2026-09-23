use super::*;
use crate::v2::RawQueryParams;
use bigname_storage::HistoryCursor;

fn sample_cursor() -> HistoryCursor {
    HistoryCursor {
        normalized_event_id: Some(42),
        event_identity: "event:42".to_owned(),
        position: None,
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
        child_registrations: false,
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
    assert!(history_storage_cursor(&payload, &binding(&unfiltered, HistoryScope::Both)).is_err());
    let other_order = params(RawQueryParams {
        event_type: Some("renewal,registration".to_owned()),
        from_timestamp: Some("2023-11-14T22:15:04Z".to_owned()),
        to_timestamp: Some("2023-11-14T22:15:07Z".to_owned()),
        ..RawQueryParams::default()
    });
    assert!(history_storage_cursor(&payload, &binding(&other_order, HistoryScope::Both)).is_err());
    let other_types = params(RawQueryParams {
        order: Some("asc".to_owned()),
        event_type: Some("renewal".to_owned()),
        from_timestamp: Some("2023-11-14T22:15:04Z".to_owned()),
        to_timestamp: Some("2023-11-14T22:15:07Z".to_owned()),
        ..RawQueryParams::default()
    });
    assert!(history_storage_cursor(&payload, &binding(&other_types, HistoryScope::Both)).is_err());
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

#[test]
fn history_publication_window_intersects_each_chain_and_excludes_future_ranges() {
    let published = BTreeMap::from([
        ("ethereum-mainnet".to_owned(), 100),
        ("ethereum-sepolia".to_owned(), 20),
    ]);
    let bounded = bound_history_block_window(None, &published);
    assert_eq!(bounded.ranges[0].to_block, Some(100));
    assert_eq!(bounded.ranges[1].to_block, Some(20));
    let requested = HistoryBlockWindow {
        ranges: vec![
            bigname_storage::ChainBlockRange {
                chain_id: "ethereum-mainnet".to_owned(),
                from_block: Some(50),
                to_block: Some(150),
            },
            bigname_storage::ChainBlockRange {
                chain_id: "ethereum-sepolia".to_owned(),
                from_block: Some(21),
                to_block: None,
            },
            bigname_storage::ChainBlockRange {
                chain_id: "unserved".to_owned(),
                from_block: None,
                to_block: None,
            },
        ],
    };
    let bounded = bound_history_block_window(Some(requested), &published);
    assert_eq!(bounded.ranges.len(), 1);
    assert_eq!(bounded.ranges[0].from_block, Some(50));
    assert_eq!(bounded.ranges[0].to_block, Some(100));
}

#[test]
fn history_cursor_binds_the_child_registrations_option_in_both_directions() {
    let cursor = sample_cursor();
    let params = params(RawQueryParams::default());
    let plain = binding(&params, HistoryScope::Both);
    let with_children = HistoryCursorBinding {
        child_registrations: true,
        ..binding(&params, HistoryScope::Both)
    };

    let plain_payload = history_cursor_payload(&cursor, &plain);
    let children_payload = history_cursor_payload(&cursor, &with_children);
    assert!(!plain_payload.filters.contains_key("children"));
    assert_eq!(
        children_payload.filters.get("children").map(String::as_str),
        Some("registrations")
    );
    assert_eq!(
        history_storage_cursor(&children_payload, &with_children).expect("cursor must decode"),
        cursor
    );
    assert!(history_storage_cursor(&children_payload, &plain).is_err());
    assert!(history_storage_cursor(&plain_payload, &with_children).is_err());
}
