// EVM resolution facets and continuation contracts over a populated address fixture.

const V2_EVM_GATE_NAMES: u128 = 250;

/// Extra names, each resolving to `V2_ADDRESS` under 30 EVM coin types (60, the default 2^31, 27
/// ENSIP-11 coin types, and 2^32 - 1) and 5 coin types outside the EVM set (0, 61, 118, 2^31 - 1,
/// 2^32).
async fn seed_v2_resolves_to_evm_cost_fixture(database: &TestDatabase) -> Result<()> {
    let specs = (1..=V2_EVM_GATE_NAMES)
        .map(|index| V2AddressNameSpec {
            logical_name_id: Box::leak(format!("ens:bulk{index:04}.eth").into_boxed_str()),
            name: Box::leak(format!("bulk{index:04}.eth").into_boxed_str()),
            resource_id: Uuid::from_u128(0xf00000 + index * 3),
            token_lineage_id: Uuid::from_u128(0xf00001 + index * 3),
            surface_binding_id: Uuid::from_u128(0xf00002 + index * 3),
            block_hash: Box::leak(format!("0xbulk{index:x}").into_boxed_str()),
            block_number: (2000 + index) as i64,
            owner: V2_OTHER_ADDRESS,
            registrant: V2_OTHER_ADDRESS,
            registered_at: "2024-01-02T00:00:00Z",
            created_at: "2023-01-02T00:00:00Z",
            expires_at: "2027-01-02T00:00:00Z",
            relations: &[],
        })
        .collect::<Vec<_>>();
    seed_v2_address_name_identities(database, &specs).await?;
    publish_v2_address_name_inputs(database, &specs).await?;
    let mut target_coins = vec!["60".to_owned(), "2147483648".to_owned()];
    target_coins.extend((1..=27).map(|k| (2_147_483_648_u64 + k * 1000).to_string()));
    target_coins.push("4294967295".to_owned());
    target_coins.extend(
        ["0", "61", "118", "2147483647", "4294967296"]
            .into_iter()
            .map(str::to_owned),
    );
    let writes = specs
        .iter()
        .map(|spec| {
            let records = target_coins
                .iter()
                .map(|coin| (format!("addr:{coin}"), V2_ADDRESS.to_owned()))
                .collect::<Vec<_>>();
            (spec.name, records)
        })
        .collect::<Vec<_>>();
    write_address_name_records(database, &writes).await
}

#[tokio::test]
async fn v2_resolves_to_evm_pages_preserve_all_matching_coin_types() -> Result<()> {
    use bigname_storage::{
        AddressNamesCurrentDedupe as Dedupe, AddressNamesCurrentOrder as Order,
        AddressNamesCurrentSort as Sort,
    };

    let database = TestDatabase::new_migrated().await?;
    seed_v2_address_names_fixture(&database).await?;
    seed_v2_resolves_to_records(&database).await?;
    seed_v2_resolves_to_evm_cost_fixture(&database).await?;

    // The served page: the EVM set's boundaries, one row per name, all 30 matches kept.
    let first = v2_resolves_to_evm_rows(&database, "&q=bulk&page_size=200").await?;
    assert_eq!(first.len(), 200);
    let coins = resolutions(&first[0])
        .into_iter()
        .map(|(coin_type, _)| coin_type)
        .collect::<Vec<_>>();
    assert_eq!(coins.len(), 30, "{}", first[0]);
    assert_eq!(coins.first(), Some(&60));
    assert!(coins.contains(&2_147_483_648) && coins.contains(&4_294_967_295));
    assert!(!coins.contains(&2_147_483_647) && !coins.contains(&4_294_967_296));

    for (label, dedupe, sort, order, page_size) in [
        ("first page, name, 50", Dedupe::Surface, Sort::Name, Order::Asc, 50),
        ("first page, name, 200", Dedupe::Surface, Sort::Name, Order::Asc, 200),
        ("first page, expires_at desc, 50", Dedupe::Surface, Sort::ExpiresAt, Order::Desc, 50),
        ("first page, registration, 50", Dedupe::Resource, Sort::Name, Order::Asc, 50),
    ] {
        // A deep continuation: validate the cursor, then read the next page.
        let mut cursor = None;
        for _ in 0..3 {
            let page = bigname_storage::load_address_records_current_evm_page(
                &database.pool,
                V2_ADDRESS,
                None,
                dedupe,
                None,
                None,
                None,
                sort,
                order,
                cursor.as_ref(),
                page_size,
            )
            .await?;
            if page.next_cursor.is_none() {
                break;
            }
            cursor = page.next_cursor;
        }
        let cursor = cursor.expect("fixture must span several pages");
        let continuation = bigname_storage::load_address_records_current_evm_page(
            &database.pool, V2_ADDRESS, None, dedupe, None, None, None, sort, order,
            Some(&cursor), page_size,
        ).await?;
        assert!(!continuation.entries.is_empty(), "{label}");
    }

    // The route also carries primary claims and optional counts across the continuation.
    for query in [
        "&coin_type=evm&page_size=50",
        "&coin_type=evm&page_size=200",
        "&coin_type=evm&page_size=200&include=counts",
        "&coin_type=evm&page_size=50&sort=expires_at&order=desc",
        "&coin_type=evm&page_size=50&dedupe=registration",
        "&page_size=50",
    ] {
        let base = format!("/v1/addresses/{V2_ADDRESS}/names?relation=resolves_to{query}");
        let first = v2_address_names_payload_for_database(&database, &base).await?;
        let cursor = first["page"]["next_cursor"].as_str().expect("next page");
        v2_address_names_payload_for_database(&database, &format!("{base}&cursor={cursor}"))
            .await?;
    }

    database.cleanup().await
}

/// The storage read aggregates at most the per-row limit of coin types for a group, whatever the
/// group matched, and reports the full distinct count beside the bounded facets.
#[tokio::test]
async fn v2_resolves_to_evm_storage_bounds_the_group_aggregation() -> Result<()> {
    use bigname_storage::{
        AddressNamesCurrentDedupe as Dedupe, AddressNamesCurrentOrder as Order,
        AddressNamesCurrentSort as Sort,
    };
    let limit = bigname_storage::EVM_MATCHED_COIN_TYPES_PER_ROW_LIMIT;
    let database = TestDatabase::new_migrated().await?;
    seed_v2_address_names_fixture(&database).await?;
    seed_v2_evm_bound_fixture(&database).await?;
    let lowest = v2_evm_wide_coin_types(limit)
        .into_iter()
        .map(|coin_type| coin_type.to_string())
        .collect::<Vec<_>>();

    for (dedupe, page_size, first_name) in [
        (Dedupe::Surface, 1, "beta.eth"),
        (Dedupe::Surface, 50, "beta.eth"),
        (Dedupe::Resource, 50, "beta.eth"),
    ] {
        let page = bigname_storage::load_address_records_current_evm_page(
            &database.pool,
            V2_EVM_WIDE_ADDRESS,
            None,
            dedupe,
            None,
            None,
            None,
            Sort::Name,
            Order::Asc,
            None,
            page_size,
        )
        .await?;
        assert_eq!(page.entries[0].entry.normalized_name, first_name);
        for row in &page.entries {
            let label = format!("{dedupe:?} {page_size} {}", row.entry.normalized_name);
            assert_eq!(row.matched_coin_type_count, limit + 1, "{label}");
            assert_eq!(
                row.resolutions
                    .iter()
                    .map(|matched| matched.coin_type.clone())
                    .collect::<Vec<_>>(),
                lowest,
                "{label}"
            );
            assert_eq!(row.representative_coin_types, lowest, "{label}");
        }
    }

    // At the limit nothing is dropped.
    for dedupe in [Dedupe::Surface, Dedupe::Resource] {
        let page = bigname_storage::load_address_records_current_evm_page(
            &database.pool,
            V2_EVM_BOUNDED_ADDRESS,
            None,
            dedupe,
            None,
            None,
            None,
            Sort::Name,
            Order::Asc,
            None,
            50,
        )
        .await?;
        for row in &page.entries {
            assert_eq!(row.matched_coin_type_count, limit, "{dedupe:?}");
            assert_eq!(row.resolutions.len(), lowest.len(), "{dedupe:?}");
        }
    }

    database.cleanup().await
}
