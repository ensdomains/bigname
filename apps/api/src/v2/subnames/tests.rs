use super::*;

fn default_binding<'a>() -> SubnamesCursorBinding<'a> {
    SubnamesCursorBinding {
        namespace: "ens",
        parent_logical_name_id: "ens:parent.eth",
        q: None,
        include_expired: DEFAULT_INCLUDE_EXPIRED,
        sort: AddressNamesSort::Name,
        order: SortOrder::Asc,
    }
}

fn name_cursor() -> ChildrenCurrentKeysetCursor {
    ChildrenCurrentKeysetCursor {
        sort_value: ChildrenCurrentSortValue::Name,
        canonical_display_name: "alice.eth".to_owned(),
        child_logical_name_id: "ens:alice.eth".to_owned(),
    }
}

fn legacy_payload(namespace: &str, parent: &str) -> CursorPayload {
    CursorPayload::new(
        LEGACY_SUBNAMES_SORT,
        BTreeMap::from([
            ("namespace".to_owned(), namespace.to_owned()),
            ("parent".to_owned(), parent.to_owned()),
        ]),
        BTreeMap::from([
            ("display_name".to_owned(), "alice.eth".to_owned()),
            (
                "child_logical_name_id".to_owned(),
                "ens:alice.eth".to_owned(),
            ),
        ]),
        None,
    )
}

#[test]
fn subname_cursor_payload_round_trips_storage_cursor() {
    let cursor = name_cursor();
    let binding = default_binding();
    let payload = subname_cursor_payload(&cursor, &binding);

    assert_eq!(payload.sort, "name");
    assert_eq!(
        payload.filters,
        BTreeMap::from([
            ("namespace".to_owned(), "ens".to_owned()),
            ("parent".to_owned(), "ens:parent.eth".to_owned()),
            ("order".to_owned(), "asc".to_owned()),
            ("q".to_owned(), String::new()),
            ("include_expired".to_owned(), "true".to_owned()),
        ])
    );
    assert_eq!(
        subname_storage_cursor(&payload, &binding).expect("cursor must decode"),
        cursor
    );
    assert!(payload.snapshot.is_none());
}

#[test]
fn subname_cursor_round_trips_timestamp_sorts() {
    let binding = SubnamesCursorBinding {
        q: Some("al"),
        include_expired: false,
        sort: AddressNamesSort::ExpiresAt,
        order: SortOrder::Desc,
        ..default_binding()
    };
    let dated = ChildrenCurrentKeysetCursor {
        sort_value: ChildrenCurrentSortValue::Timestamp(Some(
            bigname_storage::parse_rfc3339_utc_timestamp("2027-01-02T03:04:05Z")
                .expect("timestamp must parse"),
        )),
        canonical_display_name: "alice.eth".to_owned(),
        child_logical_name_id: "ens:alice.eth".to_owned(),
    };
    let payload = subname_cursor_payload(&dated, &binding);
    assert_eq!(payload.sort, "expires_at");
    assert_eq!(payload.filters["q"], "al");
    assert_eq!(payload.filters["include_expired"], "false");
    assert_eq!(payload.filters["order"], "desc");
    assert_eq!(payload.last_item["sort_kind"], "timestamp_value");
    assert_eq!(payload.last_item["sort_value"], "2027-01-02T03:04:05Z");
    assert_eq!(payload.last_item["display_name"], "alice.eth");
    assert_eq!(
        subname_storage_cursor(&payload, &binding).expect("dated cursor must decode"),
        dated
    );

    let undated = ChildrenCurrentKeysetCursor {
        sort_value: ChildrenCurrentSortValue::Timestamp(None),
        canonical_display_name: "bob.eth".to_owned(),
        child_logical_name_id: "ens:bob.eth".to_owned(),
    };
    let payload = subname_cursor_payload(&undated, &binding);
    assert_eq!(payload.last_item["sort_kind"], "timestamp_null");
    assert_eq!(
        subname_storage_cursor(&payload, &binding).expect("undated cursor must decode"),
        undated
    );

    let other_sort = SubnamesCursorBinding {
        sort: AddressNamesSort::RegisteredAt,
        ..binding
    };
    assert!(subname_storage_cursor(&payload, &other_sort).is_err());
}

#[test]
fn subname_cursor_rejects_wrong_sort_or_filter() {
    let cursor = name_cursor();
    let binding = default_binding();
    let mut payload = subname_cursor_payload(&cursor, &binding);
    payload.sort = "wrong".to_owned();
    assert!(subname_storage_cursor(&payload, &binding).is_err());

    let payload = subname_cursor_payload(&cursor, &binding);
    for other in [
        SubnamesCursorBinding {
            namespace: "basenames",
            ..binding
        },
        SubnamesCursorBinding {
            parent_logical_name_id: "ens:parent-b.eth",
            ..binding
        },
        SubnamesCursorBinding {
            order: SortOrder::Desc,
            ..binding
        },
        SubnamesCursorBinding {
            q: Some("al"),
            ..binding
        },
        SubnamesCursorBinding {
            include_expired: false,
            ..binding
        },
        SubnamesCursorBinding {
            sort: AddressNamesSort::ExpiresAt,
            ..binding
        },
    ] {
        assert!(
            subname_storage_cursor(&payload, &other).is_err(),
            "{other:?} must reject a cursor bound to {binding:?}"
        );
    }
}

#[test]
fn legacy_subname_cursor_decodes_only_for_the_default_page() {
    let binding = default_binding();
    let payload = legacy_payload("ens", "ens:parent.eth");
    assert_eq!(
        subname_storage_cursor(&payload, &binding).expect("legacy cursor must decode"),
        name_cursor()
    );

    assert!(subname_storage_cursor(&legacy_payload("ens", "ens:parent-b.eth"), &binding).is_err());
    for other in [
        SubnamesCursorBinding {
            order: SortOrder::Desc,
            ..binding
        },
        SubnamesCursorBinding {
            q: Some("al"),
            ..binding
        },
        SubnamesCursorBinding {
            include_expired: false,
            ..binding
        },
        SubnamesCursorBinding {
            sort: AddressNamesSort::RegisteredAt,
            ..binding
        },
    ] {
        assert!(subname_storage_cursor(&payload, &other).is_err());
    }
}

#[test]
fn subname_cursor_ignores_legacy_snapshot_component() {
    let binding = default_binding();
    let mut payload = subname_cursor_payload(&name_cursor(), &binding);
    payload.snapshot = Some("legacy-snapshot".to_owned());

    assert_eq!(
        subname_storage_cursor(&payload, &binding)
            .expect("legacy snapshot component must not bind a latest-state cursor"),
        name_cursor()
    );
}
