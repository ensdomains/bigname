use super::*;
use bigname_storage::AddressNamesCurrentSortedCursorValue;
use sqlx::types::Uuid;

fn binding(coin_type: &'static str) -> ResolvesToCursorBinding<'static> {
    ResolvesToCursorBinding {
        address: "0x00000000000000000000000000000000000000aa",
        namespace: None,
        coin_type,
        dedupe: AddressNamesDedupe::Name,
        q: None,
        authority: None,
        sort: AddressNamesSort::Name,
        order: SortOrder::Asc,
    }
}

#[test]
fn resolves_to_cursor_binds_relation_and_coin_type() {
    let cursor = AddressNamesCurrentSortedCursor {
        sort_value: AddressNamesCurrentSortedCursorValue::Name("alice.eth".to_owned()),
        logical_name_id: "ens:alice.eth".to_owned(),
        resource_id: Uuid::from_u128(0x1234),
    };
    let payload = resolves_to_cursor_payload(&cursor, &binding("60"));
    assert_eq!(payload.filters["relation"], "resolves_to");
    assert_eq!(payload.filters["coin_type"], "60");
    assert_eq!(
        resolves_to_storage_cursor(&payload, &binding("60")).expect("cursor must decode"),
        cursor
    );
    assert!(resolves_to_storage_cursor(&payload, &binding("2147483658")).is_err());
    let selected_authority = ResolvesToCursorBinding {
        authority: Some(Authority::EnsV1),
        ..binding("60")
    };
    assert!(resolves_to_storage_cursor(&payload, &selected_authority).is_err());
    let selected_payload = resolves_to_cursor_payload(&cursor, &selected_authority);
    assert_eq!(
        resolves_to_storage_cursor(&selected_payload, &selected_authority)
            .expect("same authority must decode"),
        cursor
    );
    assert!(resolves_to_storage_cursor(&selected_payload, &binding("60")).is_err());
    assert!(
        resolves_to_storage_cursor(
            &selected_payload,
            &ResolvesToCursorBinding {
                authority: Some(Authority::EnsV2),
                ..binding("60")
            },
        )
        .is_err()
    );

    // An authority-relation cursor for the same address never resumes a resolves_to page.
    let authority = crate::v2::address_names::address_names_cursor_payload(
        &cursor,
        &crate::v2::address_names::AddressNamesCursorBinding {
            address: "0x00000000000000000000000000000000000000aa",
            namespace: None,
            relation: None,
            dedupe: AddressNamesDedupe::Name,
            q: None,
            authority: None,
            is_migrated: None,
            sort: AddressNamesSort::Name,
            order: SortOrder::Asc,
        },
    );
    assert!(resolves_to_storage_cursor(&authority, &binding("60")).is_err());
}

#[test]
fn resolves_to_coin_type_defaults_to_sixty() {
    assert_eq!(
        parse_resolves_to_coin_type(None).expect("default must parse"),
        ("60".to_owned(), 60)
    );
    assert_eq!(
        parse_resolves_to_coin_type(Some("2147483658")).expect("coin type must parse"),
        ("2147483658".to_owned(), 2_147_483_658)
    );
    assert!(parse_resolves_to_coin_type(Some("-1")).is_err());
    assert!(parse_resolves_to_coin_type(Some("abc")).is_err());
}
