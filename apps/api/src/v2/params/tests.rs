mod relation_tests;
use super::*;
use crate::v2::error::ErrorCode;

fn parse(raw: RawQueryParams) -> V2Result<QueryParams> {
    QueryParams::try_from(raw)
}

#[test]
fn page_size_above_max_is_rejected() {
    let error = parse(RawQueryParams {
        page_size: Some(MAX_PAGE_SIZE + 1),
        ..RawQueryParams::default()
    })
    .expect_err("oversized page must fail");

    assert_eq!(error.code(), ErrorCode::InvalidInput);
}

#[test]
fn bad_finality_source_and_order_are_rejected() {
    for raw in [
        RawQueryParams {
            finality: Some("pending".to_owned()),
            ..RawQueryParams::default()
        },
        RawQueryParams {
            source: Some("both".to_owned()),
            ..RawQueryParams::default()
        },
        RawQueryParams {
            order: Some("sideways".to_owned()),
            ..RawQueryParams::default()
        },
        RawQueryParams {
            relation: Some("token_holder".to_owned()),
            ..RawQueryParams::default()
        },
        RawQueryParams {
            dedupe: Some("surface".to_owned()),
            ..RawQueryParams::default()
        },
        RawQueryParams {
            sort: Some("expiry_date".to_owned()),
            ..RawQueryParams::default()
        },
        RawQueryParams {
            scope: Some("surface".to_owned()),
            ..RawQueryParams::default()
        },
    ] {
        let error = parse(raw).expect_err("bad enum value must fail");
        assert_eq!(error.code(), ErrorCode::InvalidInput);
    }
}

#[test]
fn address_name_controls_default_and_parse_wire_values() {
    let defaulted = parse(RawQueryParams::default()).expect("default query must parse");
    assert_eq!(defaulted.relation, None);
    assert_eq!(defaulted.q, None);
    assert_eq!(defaulted.dedupe, AddressNamesDedupe::Name);
    assert_eq!(defaulted.sort, AddressNamesSort::Name);
    assert_eq!(defaulted.order, None);

    let params = parse(RawQueryParams {
        relation: Some("owner".to_owned()),
        q: Some(" alice ".to_owned()),
        dedupe: Some("registration".to_owned()),
        sort: Some("expires_at".to_owned()),
        order: Some("desc".to_owned()),
        ..RawQueryParams::default()
    })
    .expect("address-name controls must parse");

    assert_eq!(params.relation, Some(RelationSet::from(Relation::Owner)));
    assert_eq!(params.q, Some("alice".to_owned()));
    assert_eq!(params.dedupe, AddressNamesDedupe::Registration);
    assert_eq!(params.sort, AddressNamesSort::ExpiresAt);
    assert_eq!(params.order, Some(SortOrder::Desc));
}

#[test]
fn expiry_bounds_parse_rfc3339_and_reject_other_values() {
    let params = parse(RawQueryParams {
        expires_after: Some(" 2026-01-02T03:04:05Z ".to_owned()),
        expires_before: Some("2026-02-02T03:04:05Z".to_owned()),
        sort: Some(" expires_at ".to_owned()),
        ..RawQueryParams::default()
    })
    .expect("expiry bounds must parse");
    assert_eq!(
        params.expires_after,
        Some(
            bigname_storage::parse_rfc3339_utc_timestamp("2026-01-02T03:04:05Z")
                .expect("timestamp must parse")
        )
    );
    assert_eq!(
        params.expires_before,
        Some(
            bigname_storage::parse_rfc3339_utc_timestamp("2026-02-02T03:04:05Z")
                .expect("timestamp must parse")
        )
    );
    assert_eq!(params.sort_wire.as_deref(), Some("expires_at"));

    for raw in [
        RawQueryParams {
            expires_after: Some("2026-01-02".to_owned()),
            ..RawQueryParams::default()
        },
        RawQueryParams {
            expires_before: Some("1767322445".to_owned()),
            ..RawQueryParams::default()
        },
    ] {
        let error = parse(raw).expect_err("non-RFC 3339 expiry bound must fail");
        assert_eq!(error.code(), ErrorCode::InvalidInput);
    }
}

#[test]
fn include_expired_parses_booleans_and_rejects_other_values() {
    let defaulted = parse(RawQueryParams::default()).expect("default query must parse");
    assert_eq!(defaulted.include_expired, None);
    for (wire, expected) in [("true", Some(true)), ("false", Some(false))] {
        let params = parse(RawQueryParams {
            include_expired: Some(wire.to_owned()),
            ..RawQueryParams::default()
        })
        .expect("include_expired must parse");
        assert_eq!(params.include_expired, expected);
    }
    let error = parse(RawQueryParams {
        include_expired: Some("maybe".to_owned()),
        ..RawQueryParams::default()
    })
    .expect_err("non-boolean include_expired must fail");
    assert_eq!(error.code(), ErrorCode::InvalidInput);
}

#[test]
fn relation_sets_parse_any_and_canonicalize_duplicates() {
    let params = parse(RawQueryParams {
        relation: Some("registrant,owner,owner".to_owned()),
        ..RawQueryParams::default()
    })
    .expect("relation set must parse");
    assert_eq!(
        params.relation.as_ref().map(RelationSet::canonical_value),
        Some("owner,registrant".to_owned())
    );

    let params = parse(RawQueryParams {
        relation: Some("any".to_owned()),
        ..RawQueryParams::default()
    })
    .expect("relation any must parse");
    assert_eq!(
        params.relation.as_ref().map(RelationSet::canonical_value),
        Some("owner,manager,registrant".to_owned())
    );

    let error = parse(RawQueryParams {
        relation: Some("any,invalid".to_owned()),
        ..RawQueryParams::default()
    })
    .expect_err("invalid mixed relation set must fail");
    assert_eq!(error.code(), ErrorCode::InvalidInput);
}

#[test]
fn history_scope_defaults_to_both_and_parses_wire_values() {
    let defaulted = parse(RawQueryParams::default()).expect("default query must parse");
    assert_eq!(defaulted.scope, HistoryScope::Both);

    for (wire, expected) in [
        ("name", HistoryScope::Name),
        ("registration", HistoryScope::Registration),
        ("both", HistoryScope::Both),
    ] {
        let params = parse(RawQueryParams {
            scope: Some(wire.to_owned()),
            ..RawQueryParams::default()
        })
        .expect("scope value must parse");
        assert_eq!(params.scope, expected);
    }
}

#[test]
fn latest_collection_selectors_reject_at_and_historical_finality() {
    let at = AtSelector::Timestamp("2026-07-21T00:00:00Z".to_owned());
    let at_error = validate_latest_collection_selectors(Some(&at), Finality::Latest)
        .expect_err("collection at selector must fail");
    assert_eq!(at_error.code(), ErrorCode::InvalidInput);
    assert_eq!(
        at_error.envelope().error.message,
        "at is not supported because collection routes read latest state"
    );

    for finality in [Finality::Safe, Finality::Finalized] {
        let error = validate_latest_collection_selectors(None, finality)
            .expect_err("historical collection finality must fail");
        assert_eq!(error.code(), ErrorCode::InvalidInput);
        assert_eq!(
            error.envelope().error.message,
            "finality must be latest because collection routes read latest state"
        );
    }

    validate_latest_collection_selectors(None, Finality::Latest)
        .expect("latest collection selector must remain valid");
}

#[test]
fn event_filters_parse_and_normalize_wire_values() {
    let params = parse(RawQueryParams {
        event_type: Some("registration".to_owned()),
        name: Some(" Alice.eth ".to_owned()),
        registration_id: Some("550e8400-e29b-41d4-a716-446655440000".to_owned()),
        address: Some("0x00000000000000000000000000000000000000aa".to_owned()),
        from_block: Some("10".to_owned()),
        to_block: Some("20".to_owned()),
        ..RawQueryParams::default()
    })
    .expect("event filters must parse");

    assert_eq!(
        params.event_types,
        Some(HistoryEventTypeSet::from(HistoryEventType::Registration))
    );
    assert_eq!(params.name, Some("Alice.eth".to_owned()));
    assert_eq!(
        params.registration_id,
        Some("550e8400-e29b-41d4-a716-446655440000".to_owned())
    );
    assert_eq!(
        params.address,
        Some("0x00000000000000000000000000000000000000aa".to_owned())
    );
    assert_eq!(params.from_block, Some(10));
    assert_eq!(params.to_block, Some(20));
}

#[test]
fn bad_event_filter_values_are_rejected() {
    for raw in [
        RawQueryParams {
            event_type: Some("registered".to_owned()),
            ..RawQueryParams::default()
        },
        RawQueryParams {
            registration_id: Some("not-a-uuid".to_owned()),
            ..RawQueryParams::default()
        },
        RawQueryParams {
            address: Some("0x1234".to_owned()),
            ..RawQueryParams::default()
        },
        RawQueryParams {
            from_block: Some("-1".to_owned()),
            ..RawQueryParams::default()
        },
        RawQueryParams {
            to_block: Some("latest".to_owned()),
            ..RawQueryParams::default()
        },
    ] {
        let error = parse(raw).expect_err("bad event filter must fail");
        assert_eq!(error.code(), ErrorCode::InvalidInput);
    }
}

#[test]
fn event_type_sets_parse_and_canonicalize_wire_values() {
    let params = parse(RawQueryParams {
        event_type: Some(" renewal, registration ,renewal".to_owned()),
        ..RawQueryParams::default()
    })
    .expect("type set must parse");
    assert_eq!(
        params
            .event_types
            .as_ref()
            .map(HistoryEventTypeSet::canonical_value),
        Some("registration,renewal".to_owned())
    );

    for raw in ["registration,bogus", ",", "registration,,renewal,"] {
        let result = parse(RawQueryParams {
            event_type: Some(raw.to_owned()),
            ..RawQueryParams::default()
        });
        if raw == "registration,,renewal," {
            assert!(result.is_ok(), "empty parts are ignored: {raw}");
        } else {
            let error = result.expect_err("bad type set must fail");
            assert_eq!(error.code(), ErrorCode::InvalidInput);
        }
    }
}

#[test]
fn timestamp_bounds_parse_canonicalize_and_validate_order() {
    let params = parse(RawQueryParams {
        from_timestamp: Some("2023-11-14T23:15:04+01:00".to_owned()),
        to_timestamp: Some("2023-11-14T22:15:07.500Z".to_owned()),
        ..RawQueryParams::default()
    })
    .expect("timestamp bounds must parse");
    assert_eq!(
        params
            .from_timestamp
            .as_ref()
            .map(|bound| bound.canonical.as_str()),
        Some("2023-11-14T22:15:04Z")
    );
    assert_eq!(
        params
            .to_timestamp
            .as_ref()
            .map(|bound| bound.canonical.as_str()),
        Some("2023-11-14T22:15:07.5Z")
    );

    for raw in [
        RawQueryParams {
            from_timestamp: Some("yesterday".to_owned()),
            ..RawQueryParams::default()
        },
        RawQueryParams {
            to_timestamp: Some("1700000000".to_owned()),
            ..RawQueryParams::default()
        },
        RawQueryParams {
            from_timestamp: Some("2023-11-14T22:15:07Z".to_owned()),
            to_timestamp: Some("2023-11-14T22:15:04Z".to_owned()),
            ..RawQueryParams::default()
        },
    ] {
        let error = parse(raw).expect_err("bad timestamp bound must fail");
        assert_eq!(error.code(), ErrorCode::InvalidInput);
    }
}

#[test]
fn at_selector_classifies_rfc3339_timestamp() {
    let params = parse(RawQueryParams {
        at: Some("2026-06-10T00:00:00Z".to_owned()),
        ..RawQueryParams::default()
    })
    .expect("timestamp at selector must parse");

    assert_eq!(
        params.at,
        Some(AtSelector::Timestamp("2026-06-10T00:00:00Z".to_owned()))
    );
}

#[test]
fn at_selector_classifies_opaque_snapshot_token() {
    let params = parse(RawQueryParams {
        at: Some("snapshot_abc-123".to_owned()),
        ..RawQueryParams::default()
    })
    .expect("snapshot token at selector must parse");

    assert_eq!(
        params.at,
        Some(AtSelector::SnapshotToken("snapshot_abc-123".to_owned()))
    );
}

#[test]
fn invalid_at_selector_is_rejected() {
    let error = parse(RawQueryParams {
        at: Some("not a token".to_owned()),
        ..RawQueryParams::default()
    })
    .expect_err("invalid at selector must fail");

    assert_eq!(error.code(), ErrorCode::InvalidInput);
}
