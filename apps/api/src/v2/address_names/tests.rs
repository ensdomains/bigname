use super::*;
use crate::v2::decode;
use crate::v2::name_filter::NameMatch;
use crate::v2::{AddressNamesDedupe, AddressNamesSort};
use bigname_storage::{AddressNamesCurrentSortedCursor, AddressNamesCurrentSortedCursorValue};
use sqlx::types::Uuid;

fn binding(sort: AddressNamesSort) -> AddressNamesCursorBinding<'static> {
    let relation = Box::leak(Box::new(RelationSet::from(Relation::Owner)));
    let authority = Box::leak(Box::new(AuthoritySet::from(Authority::EnsV1)));
    AddressNamesCursorBinding {
        address: "0x00000000000000000000000000000000000000aa",
        namespace: Some("ens"),
        relation: Some(relation),
        dedupe: AddressNamesDedupe::Name,
        q: Some("al"),
        name_match: NameMatch::Prefix,
        authority: Some(authority),
        is_migrated: None,
        parent: None,
        sort,
        order: SortOrder::Asc,
    }
}

#[test]
fn address_names_cursor_binds_parent_only_when_sent() {
    let cursor = AddressNamesCurrentSortedCursor {
        sort_value: AddressNamesCurrentSortedCursorValue::Name("alice.eth".to_owned()),
        logical_name_id: "ens:alice.eth".to_owned(),
        resource_id: Uuid::from_u128(0x1234),
    };
    let unfiltered = binding(AddressNamesSort::Name);
    assert!(
        !address_names_cursor_payload(&cursor, &unfiltered)
            .filters
            .contains_key("parent")
    );
    let eth = AddressNamesCursorBinding {
        parent: Some("eth"),
        ..unfiltered.clone()
    };
    let payload = address_names_cursor_payload(&cursor, &eth);
    assert_eq!(payload.filters["parent"], "eth");
    assert_eq!(
        address_names_storage_cursor(&payload, &eth).expect("cursor must decode"),
        cursor
    );
    for other in [
        unfiltered.clone(),
        AddressNamesCursorBinding {
            parent: Some("base.eth"),
            ..unfiltered
        },
    ] {
        assert!(address_names_storage_cursor(&payload, &other).is_err());
    }
}

#[test]
fn address_names_cursor_payload_round_trips_name_cursor() {
    let cursor = AddressNamesCurrentSortedCursor {
        sort_value: AddressNamesCurrentSortedCursorValue::Name("Alice.eth".to_owned()),
        logical_name_id: "ens:alice.eth".to_owned(),
        resource_id: Uuid::from_u128(0x1234),
    };
    let payload = address_names_cursor_payload(&cursor, &binding(AddressNamesSort::Name));

    assert_eq!(
        address_names_storage_cursor(&payload, &binding(AddressNamesSort::Name))
            .expect("cursor must decode"),
        cursor
    );
    assert_eq!(payload.last_item[SORT_KIND_CURSOR_KEY], SORT_KIND_NAME);
    assert!(payload.snapshot.is_none());
}

#[test]
fn address_names_cursor_payload_distinguishes_timestamp_null_and_value() {
    let null_cursor = AddressNamesCurrentSortedCursor {
        sort_value: AddressNamesCurrentSortedCursorValue::Timestamp(None),
        logical_name_id: "ens:missing-expiry.eth".to_owned(),
        resource_id: Uuid::from_u128(0x1235),
    };
    let null_payload =
        address_names_cursor_payload(&null_cursor, &binding(AddressNamesSort::ExpiresAt));
    assert_eq!(
        null_payload.last_item[SORT_KIND_CURSOR_KEY],
        SORT_KIND_TIMESTAMP_NULL
    );
    assert_eq!(null_payload.last_item[SORT_VALUE_CURSOR_KEY], "");
    assert_eq!(
        address_names_storage_cursor(&null_payload, &binding(AddressNamesSort::ExpiresAt))
            .expect("null timestamp cursor must decode"),
        null_cursor
    );

    let value = bigname_storage::parse_rfc3339_utc_timestamp("2027-01-02T03:04:05Z")
        .expect("timestamp must parse")
        .into();
    let value_cursor = AddressNamesCurrentSortedCursor {
        sort_value: AddressNamesCurrentSortedCursorValue::Timestamp(Some(value)),
        logical_name_id: "ens:alice.eth".to_owned(),
        resource_id: Uuid::from_u128(0x1236),
    };
    let value_payload =
        address_names_cursor_payload(&value_cursor, &binding(AddressNamesSort::ExpiresAt));
    assert_eq!(
        value_payload.last_item[SORT_KIND_CURSOR_KEY],
        SORT_KIND_TIMESTAMP_VALUE
    );
    assert_eq!(
        value_payload.last_item[SORT_VALUE_CURSOR_KEY],
        "2027-01-02T03:04:05Z"
    );
    assert_eq!(
        address_names_storage_cursor(&value_payload, &binding(AddressNamesSort::ExpiresAt))
            .expect("timestamp cursor must decode"),
        value_cursor
    );
}

#[test]
fn address_names_cursor_rejects_cross_sort_filter_or_order() {
    let cursor = AddressNamesCurrentSortedCursor {
        sort_value: AddressNamesCurrentSortedCursorValue::Name("Alice.eth".to_owned()),
        logical_name_id: "ens:alice.eth".to_owned(),
        resource_id: Uuid::from_u128(0x1234),
    };

    let payload = address_names_cursor_payload(&cursor, &binding(AddressNamesSort::Name));
    assert!(address_names_storage_cursor(&payload, &binding(AddressNamesSort::ExpiresAt)).is_err());

    let mut payload = address_names_cursor_payload(&cursor, &binding(AddressNamesSort::Name));
    payload.filters.insert(
        ADDRESS_FILTER_KEY.to_owned(),
        "0x00000000000000000000000000000000000000bb".to_owned(),
    );
    assert!(address_names_storage_cursor(&payload, &binding(AddressNamesSort::Name)).is_err());

    let mut payload = address_names_cursor_payload(&cursor, &binding(AddressNamesSort::Name));
    payload
        .filters
        .insert(ORDER_FILTER_KEY.to_owned(), "desc".to_owned());
    assert!(address_names_storage_cursor(&payload, &binding(AddressNamesSort::Name)).is_err());
}

#[test]
fn address_names_cursor_ignores_legacy_snapshot_component() {
    let cursor = AddressNamesCurrentSortedCursor {
        sort_value: AddressNamesCurrentSortedCursorValue::Name("Alice.eth".to_owned()),
        logical_name_id: "ens:alice.eth".to_owned(),
        resource_id: Uuid::from_u128(0x1234),
    };
    let mut payload = address_names_cursor_payload(&cursor, &binding(AddressNamesSort::Name));
    payload.snapshot = Some("legacy-snapshot".to_owned());

    assert_eq!(
        address_names_storage_cursor(&payload, &binding(AddressNamesSort::Name))
            .expect("legacy snapshot component must not bind a latest-state cursor"),
        cursor
    );
}

#[test]
fn address_names_cursor_rejects_cross_timestamp_sort_reuse() {
    let cursor = AddressNamesCurrentSortedCursor {
        sort_value: AddressNamesCurrentSortedCursorValue::Timestamp(Some(
            bigname_storage::parse_rfc3339_utc_timestamp("2027-01-02T03:04:05Z")
                .expect("timestamp must parse")
                .into(),
        )),
        logical_name_id: "ens:alice.eth".to_owned(),
        resource_id: Uuid::from_u128(0x1234),
    };
    let payload = address_names_cursor_payload(&cursor, &binding(AddressNamesSort::ExpiresAt));

    assert!(
        address_names_storage_cursor(&payload, &binding(AddressNamesSort::RegisteredAt)).is_err()
    );
}

// A prefix `q` binds the same filters it bound before `match` existed; `match=contains` adds a
// key, so neither mode's cursor resumes the other.
#[test]
fn address_names_cursor_binds_contains_match_only_when_it_narrows_q() {
    let cursor = AddressNamesCurrentSortedCursor {
        sort_value: AddressNamesCurrentSortedCursorValue::Name("Alice.eth".to_owned()),
        logical_name_id: "ens:alice.eth".to_owned(),
        resource_id: Uuid::from_u128(0x1234),
    };
    let prefix = binding(AddressNamesSort::Name);
    let contains = AddressNamesCursorBinding {
        name_match: NameMatch::Contains,
        ..prefix.clone()
    };
    let prefix_payload = address_names_cursor_payload(&cursor, &prefix);
    // Only the eight request filters.
    assert_eq!(prefix_payload.filters.len(), 8);
    assert!(!prefix_payload.filters.contains_key("match"));
    let contains_payload = address_names_cursor_payload(&cursor, &contains);
    assert_eq!(contains_payload.filters["match"], "contains");
    assert!(address_names_storage_cursor(&prefix_payload, &contains).is_err());
    assert!(address_names_storage_cursor(&contains_payload, &prefix).is_err());
    assert_eq!(
        address_names_storage_cursor(&contains_payload, &contains).expect("same match decodes"),
        cursor
    );

    // Without `q`, `match` selects nothing and binds nothing.
    let unfiltered = AddressNamesCursorBinding { q: None, ..prefix };
    let unfiltered_contains = AddressNamesCursorBinding {
        q: None,
        ..contains
    };
    let payload = address_names_cursor_payload(&cursor, &unfiltered_contains);
    assert!(address_names_storage_cursor(&payload, &unfiltered).is_ok());
}

#[test]
fn address_names_cursor_binds_the_authority_set() {
    let cursor = AddressNamesCurrentSortedCursor {
        sort_value: AddressNamesCurrentSortedCursorValue::Timestamp(None),
        logical_name_id: "ens:alice.eth".to_owned(),
        resource_id: Uuid::from_u128(0x1234),
    };
    let v0_v1 = AuthoritySet::from_authorities([Authority::EnsV1, Authority::EnsV0])
        .expect("non-empty set");
    let v0_v1_binding = AddressNamesCursorBinding {
        authority: Some(&v0_v1),
        ..binding(AddressNamesSort::CreatedAt)
    };
    let payload = address_names_cursor_payload(&cursor, &v0_v1_binding);
    assert_eq!(payload.filters["authority"], "ens_v0,ens_v1");
    assert_eq!(payload.sort, "created_at");
    assert_eq!(
        address_names_storage_cursor(&payload, &v0_v1_binding).expect("same set decodes"),
        cursor
    );
    // The one-value set binds what a single `authority` bound, and differs from the pair.
    assert!(address_names_storage_cursor(&payload, &binding(AddressNamesSort::CreatedAt)).is_err());
    assert!(
        address_names_storage_cursor(&payload, &binding(AddressNamesSort::RegisteredAt)).is_err()
    );
    let single = address_names_cursor_payload(&cursor, &binding(AddressNamesSort::CreatedAt));
    assert_eq!(single.filters["authority"], "ens_v1");
}

#[test]
fn address_names_cursor_token_decodes_to_bound_payload() {
    let cursor = AddressNamesCurrentSortedCursor {
        sort_value: AddressNamesCurrentSortedCursorValue::Name("Alice.eth".to_owned()),
        logical_name_id: "ens:alice.eth".to_owned(),
        resource_id: Uuid::from_u128(0x1234),
    };
    let payload = address_names_cursor_payload(&cursor, &binding(AddressNamesSort::Name));
    let encoded = crate::v2::encode(&payload);

    assert_eq!(
        decode(&encoded).expect("encoded cursor must decode"),
        payload
    );
}

#[test]
fn address_name_registry_operator_grant_serializes_relation() {
    let grant = AddressNameGrant {
        grant_relation: Some(GrantRelation::Operator),
        grant_scope: serde_json::json!({"kind": "account", "detail": {}}),
        powers: serde_json::json!(["registry_control"]),
    };
    let value = serde_json::to_value(grant).expect("grant must serialize");

    assert_eq!(value["grant_relation"], serde_json::json!("operator"));
}
