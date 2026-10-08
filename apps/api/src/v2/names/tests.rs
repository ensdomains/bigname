use super::*;

fn timestamp(value: &str) -> UnixSeconds {
    value.parse::<UnixSeconds>().expect("timestamp must parse")
}

fn binding<'a>() -> NamesCursorBinding<'a> {
    NamesCursorBinding {
        deadline: NameCurrentDeadline::Expiry,
        namespace: "ens",
        expires_after: Some(timestamp("2026-09-01T00:00:00Z")),
        expires_before: None,
        windows: None,
        authority: None,
        parent: None,
        order: SortOrder::Asc,
    }
}

fn cursor() -> NameCurrentListCursor {
    NameCurrentListCursor {
        sort_value: NameCurrentListCursorValue::Timestamp(Some(timestamp("2026-10-01T00:00:00Z"))),
        namespace: "ens".to_owned(),
        normalized_name: "beta.eth".to_owned(),
        namehash: "0xbeta".to_owned(),
    }
}

fn read(binding: &NamesCursorBinding<'_>, cursor: &str) -> V2Result<NameCurrentListCursor> {
    let position = names_list_cursor(binding)
        .read(Some(cursor), &position_keys(binding.deadline))?
        .expect("a cursor was sent");
    names_storage_cursor(&position, binding.deadline)
}

#[test]
fn names_cursor_round_trips_and_binds_window_and_order() {
    let binding = binding();
    let cursor_text = names_list_cursor(&binding)
        .next(names_position(&cursor(), NameCurrentDeadline::Expiry).expect("position must build"));
    let payload = crate::v2::decode(&cursor_text).expect("cursor must decode");
    assert_eq!(payload.sort, "expires_at");
    assert_eq!(
        payload.filters,
        BTreeMap::from([
            ("date_family".to_owned(), "expires_at".to_owned()),
            ("namespace".to_owned(), "ens".to_owned()),
            ("expires_after".to_owned(), "1788220800".to_owned()),
            ("expires_before".to_owned(), String::new()),
            ("order".to_owned(), "asc".to_owned()),
        ])
    );
    assert_eq!(
        read(&binding, &cursor_text).expect("cursor must decode"),
        cursor()
    );

    for other in [
        NamesCursorBinding {
            namespace: "basenames",
            ..binding
        },
        NamesCursorBinding {
            expires_after: None,
            ..binding
        },
        NamesCursorBinding {
            expires_before: Some(timestamp("2027-01-01T00:00:00Z")),
            ..binding
        },
        NamesCursorBinding {
            order: SortOrder::Desc,
            ..binding
        },
    ] {
        assert!(
            read(&other, &cursor_text).is_err(),
            "{other:?} must reject a cursor bound to {binding:?}"
        );
    }

    let mut old_cursor = payload.clone();
    old_cursor.filters.remove("date_family");
    assert!(read(&binding, &crate::v2::encode(&old_cursor)).is_err());

    let mut wrong_sort = payload.clone();
    wrong_sort.sort = "name".to_owned();
    assert!(read(&binding, &crate::v2::encode(&wrong_sort)).is_err());
}

#[test]
fn names_cursor_binds_authority_and_parent_only_when_sent() {
    let ens_v1 = AuthoritySet::from(super::super::vocab::Authority::EnsV1);
    let both = AuthoritySet::from_authorities([
        super::super::vocab::Authority::EnsV1,
        super::super::vocab::Authority::EnsV0,
    ])
    .expect("non-empty set");
    let filtered = NamesCursorBinding {
        authority: Some(&both),
        parent: Some("eth"),
        ..binding()
    };
    let cursor_text = names_list_cursor(&filtered)
        .next(names_position(&cursor(), NameCurrentDeadline::Expiry).expect("position must build"));
    let filters = crate::v2::decode(&cursor_text)
        .expect("cursor must decode")
        .filters;
    assert_eq!(filters["authority"], "ens_v0,ens_v1");
    assert_eq!(filters["parent"], "eth");
    assert_eq!(
        read(&filtered, &cursor_text).expect("cursor must decode"),
        cursor()
    );
    for other in [
        binding(),
        NamesCursorBinding {
            authority: Some(&ens_v1),
            ..filtered
        },
        NamesCursorBinding {
            parent: None,
            ..filtered
        },
        NamesCursorBinding {
            parent: Some("base.eth"),
            ..filtered
        },
    ] {
        assert!(
            read(&other, &cursor_text).is_err(),
            "{other:?} must reject a cursor bound to {filtered:?}"
        );
    }
    assert!(
        !crate::v2::decode(&names_list_cursor(&binding()).next(
            names_position(&cursor(), NameCurrentDeadline::Expiry).expect("position must build")
        ))
        .expect("cursor must decode")
        .filters
        .keys()
        .any(|key| key == "authority" || key == "parent")
    );
}

#[test]
fn names_position_refuses_a_name_or_null_sort_value() {
    for sort_value in [
        NameCurrentListCursorValue::Name("beta.eth".to_owned()),
        NameCurrentListCursorValue::Timestamp(None),
    ] {
        let cursor = NameCurrentListCursor {
            sort_value,
            ..cursor()
        };
        assert!(names_position(&cursor, NameCurrentDeadline::Expiry).is_err());
    }
}
