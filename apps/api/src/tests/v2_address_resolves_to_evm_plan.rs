// Cost gate for `relation=resolves_to&coin_type=evm`: the page and continuation statements are
// served from the address-leading indexes of `address_records_current` and read only the selected
// address's rows, however many rows other addresses hold. Work is asserted from row counts in the
// executed plan, not from wall-clock time or planner node spelling beyond the scanned index.

const V2_EVM_GATE_NAMES: i32 = 250;
const V2_EVM_GATE_OTHER_NAMES: i32 = 25_000;
const V2_EVM_GATE_OTHER_ADDRESSES: i32 = 100;
const V2_EVM_GATE_ADDRESS_INDEXES: [&str; 2] = [
    "address_records_current_pkey",
    "address_records_current_address_sort_idx",
];

/// Extra names on the fixture's canonical blocks. Each of the first `V2_EVM_GATE_NAMES` resolves to
/// `V2_ADDRESS` under 30 EVM coin types (60, the default 2^31, 27 ENSIP-11 coin types, and 2^32 - 1)
/// and 5 coin types outside the EVM set (0, 61, 118, 2^31 - 1, 2^32). Each of the next
/// `V2_EVM_GATE_OTHER_NAMES` resolves to `V2_EVM_GATE_OTHER_ADDRESSES` other addresses under 10 EVM
/// coin types, so the table and the name tables are dominated by unrelated rows, as in production.
/// Returns the number of rows stored for `V2_ADDRESS`.
async fn seed_v2_resolves_to_evm_cost_fixture(database: &TestDatabase) -> Result<i64> {
    sqlx::query(
        r#"
        INSERT INTO bigname_phase.name_surfaces (
            logical_name_id, namespace, raw_name, raw_labels, dns_encoded_name, namehash,
            labelhashes, normalizer_version, visibility_state, chain_id, block_hash,
            block_number, provenance, canonicality_state
        )
        SELECT 'ens:bulk-node-' || i, 'ens', 'bulk' || lpad(i::text, 4, '0') || '.eth',
               src.raw_labels, src.dns_encoded_name, 'bulk-node-' || i, src.labelhashes,
               src.normalizer_version, 'active', src.chain_id, src.block_hash,
               src.block_number, src.provenance, src.canonicality_state
        FROM bigname_phase.name_surfaces src
        CROSS JOIN generate_series(1, $1) i
        WHERE src.raw_name = 'alpha.eth'
        "#,
    )
    .bind(V2_EVM_GATE_NAMES + V2_EVM_GATE_OTHER_NAMES)
    .execute(&database.pool)
    .await?;
    sqlx::query(
        r#"
        INSERT INTO bigname_phase.resources (
            resource_id, chain_id, block_hash, block_number, provenance, canonicality_state
        )
        SELECT ('00000000-0000-0000-0000-' || lpad(to_hex(15728640 + i), 12, '0'))::uuid,
               src.chain_id, src.block_hash, src.block_number, '{}'::jsonb,
               src.canonicality_state
        FROM bigname_phase.resources src
        CROSS JOIN generate_series(1, $1) i
        WHERE src.resource_id = $2
        "#,
    )
    .bind(V2_EVM_GATE_NAMES + V2_EVM_GATE_OTHER_NAMES)
    .bind(Uuid::from_u128(0xa100))
    .execute(&database.pool)
    .await?;
    let insert_rows = r#"
        INSERT INTO bigname_phase.address_records_current (
            address, coin_type, logical_name_id, namespace, raw_name, namehash,
            surface_binding_id, resource_id, record_resource_id, binding_kind, record_key,
            support_status, unsupported_reason, provenance, chain_positions,
            canonicality_summary, manifest_version
        )
        SELECT target.address, coin.coin_type, 'ens:bulk-node-' || i, 'ens',
               'bulk' || lpad(i::text, 4, '0') || '.eth', 'bulk-node-' || i,
               NULL, NULL,
               ('00000000-0000-0000-0000-' || lpad(to_hex(15728640 + i), 12, '0'))::uuid,
               NULL, 'addr:' || coin.coin_type, 'supported', NULL, src.provenance,
               src.chain_positions, src.canonicality_summary, 1
        FROM bigname_phase.address_records_current src
        CROSS JOIN generate_series($1, $5) i
        CROSS JOIN unnest($2::text[]) AS target(address)
        CROSS JOIN unnest($3::text[]) AS coin(coin_type)
        WHERE src.address = $4 AND src.raw_name = 'alpha.eth' AND src.coin_type = '60'
    "#;
    let mut target_coins = vec!["60".to_owned(), "2147483648".to_owned()];
    target_coins.extend((1..=27).map(|k| (2_147_483_648_u64 + k * 1000).to_string()));
    target_coins.push("4294967295".to_owned());
    target_coins.extend(
        ["0", "61", "118", "2147483647", "4294967296"]
            .into_iter()
            .map(str::to_owned),
    );
    sqlx::query(insert_rows)
        .bind(1)
        .bind(vec![V2_ADDRESS.to_owned()])
        .bind(&target_coins)
        .bind(V2_ADDRESS)
        .bind(V2_EVM_GATE_NAMES)
        .execute(&database.pool)
        .await?;
    // Each unrelated name resolves to one of the other addresses, as a real name's records do.
    let other_rows = insert_rows.replace(
        "CROSS JOIN unnest($2::text[]) AS target(address)",
        "CROSS JOIN LATERAL (SELECT ($2::text[])[1 + i % cardinality($2::text[])]) AS target(address)",
    );
    let others = (1..=V2_EVM_GATE_OTHER_ADDRESSES)
        .map(|index| format!("0x{:040x}", 0xe000 + index))
        .collect::<Vec<_>>();
    let other_coins = target_coins[..10].to_vec();
    sqlx::query(&other_rows)
        .bind(V2_EVM_GATE_NAMES + 1)
        .bind(&others)
        .bind(&other_coins)
        .bind(V2_ADDRESS)
        .bind(V2_EVM_GATE_NAMES + V2_EVM_GATE_OTHER_NAMES)
        .execute(&database.pool)
        .await?;
    for table in ["address_records_current", "name_surfaces", "resources"] {
        sqlx::query(&format!("ANALYZE bigname_phase.{table}"))
            .execute(&database.pool)
            .await?;
    }
    Ok(sqlx::query_scalar(
        "SELECT count(*) FROM bigname_phase.address_records_current WHERE address = $1",
    )
    .bind(V2_ADDRESS)
    .fetch_one(&database.pool)
    .await?)
}

fn v2_evm_plan_nodes<'a>(node: &'a Value, nodes: &mut Vec<&'a Value>) {
    nodes.push(node);
    for child in node["Plans"].as_array().into_iter().flatten() {
        v2_evm_plan_nodes(child, nodes);
    }
}

fn v2_evm_plan_count(node: &Value, key: &str) -> u64 {
    node[key].as_f64().map_or(0, |value| value as u64)
}

/// Every access to `address_records_current` is an address-leading index probe, and the rows it
/// reads (returned plus filtered out, over all loops) never exceed the selected address's rows.
fn assert_v2_evm_plan_reads_only_the_address(explain: &Value, address_rows: u64, label: &str) {
    let plan = &explain[0]["Plan"];
    let mut nodes = Vec::new();
    v2_evm_plan_nodes(plan, &mut nodes);
    let mut probes = 0;
    let mut rows_read = 0;
    for node in nodes {
        if let Some(index) = node["Index Name"].as_str()
            && index.starts_with("address_records_current")
        {
            assert!(
                V2_EVM_GATE_ADDRESS_INDEXES.contains(&index),
                "{label}: {index} is not address-leading:\n{explain:#}"
            );
            let condition = node["Index Cond"].as_str().unwrap_or_default();
            assert!(
                condition.contains("address ="),
                "{label}: {index} probe is not keyed by address:\n{explain:#}"
            );
            probes += 1;
        }
        if node["Relation Name"] != json!("address_records_current") {
            continue;
        }
        assert_ne!(
            node["Node Type"],
            json!("Seq Scan"),
            "{label}: sequential scan of address_records_current:\n{explain:#}"
        );
        rows_read += (v2_evm_plan_count(node, "Actual Rows")
            + v2_evm_plan_count(node, "Rows Removed by Filter")
            + v2_evm_plan_count(node, "Rows Removed by Index Recheck"))
            * v2_evm_plan_count(node, "Actual Loops").max(1);
    }
    assert!(probes > 0, "{label}: no address index probe:\n{explain:#}");
    assert!(
        rows_read <= address_rows,
        "{label}: read {rows_read} address_records_current rows for an address holding {address_rows}:\n{explain:#}"
    );
    eprintln!(
        "evm cost gate {label}: {rows_read} rows read of {address_rows}; planning {} ms, execution {} ms",
        explain[0]["Planning Time"], explain[0]["Execution Time"]
    );
}

#[tokio::test]
async fn v2_resolves_to_evm_page_reads_only_the_address_rows() -> Result<()> {
    use bigname_storage::{
        AddressNamesCurrentDedupe as Dedupe, AddressNamesCurrentOrder as Order,
        AddressNamesCurrentSort as Sort,
    };

    let database = TestDatabase::new_migrated().await?;
    seed_v2_address_names_fixture(&database).await?;
    seed_v2_resolves_to_records(&database).await?;
    let address_rows = u64::try_from(seed_v2_resolves_to_evm_cost_fixture(&database).await?)?;
    let table_rows: i64 =
        sqlx::query_scalar("SELECT count(*) FROM bigname_phase.address_records_current")
            .fetch_one(&database.pool)
            .await?;
    eprintln!("evm cost gate fixture: {address_rows} rows for the address, {table_rows} in the table");

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
        let plans = bigname_storage::explain_address_records_current_evm_page_for_test(
            &database.pool,
            V2_ADDRESS,
            dedupe,
            sort,
            order,
            None,
            page_size,
        )
        .await?;
        assert_eq!(plans.len(), 1);
        assert_v2_evm_plan_reads_only_the_address(&plans[0], address_rows, label);

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
        let plans = bigname_storage::explain_address_records_current_evm_page_for_test(
            &database.pool,
            V2_ADDRESS,
            dedupe,
            sort,
            order,
            Some(&cursor),
            page_size,
        )
        .await?;
        assert_eq!(plans.len(), 2);
        assert_v2_evm_plan_reads_only_the_address(
            &plans[0],
            address_rows,
            &format!("{label}, continuation cursor check"),
        );
        assert_v2_evm_plan_reads_only_the_address(
            &plans[1],
            address_rows,
            &format!("{label}, continuation page"),
        );
    }

    // Whole-request timings through the route, including primary-claim and count loads. These are
    // printed for review, never asserted.
    for query in [
        "&coin_type=evm&page_size=50",
        "&coin_type=evm&page_size=200",
        "&coin_type=evm&page_size=200&include=counts",
        "&coin_type=evm&page_size=50&sort=expires_at&order=desc",
        "&coin_type=evm&page_size=50&dedupe=registration",
        "&page_size=50",
    ] {
        let base = format!("/v1/addresses/{V2_ADDRESS}/names?relation=resolves_to{query}");
        let started = std::time::Instant::now();
        let first = v2_address_names_payload_for_database(&database, &base).await?;
        let first_elapsed = started.elapsed();
        let cursor = first["page"]["next_cursor"].as_str().expect("next page");
        let started = std::time::Instant::now();
        v2_address_names_payload_for_database(&database, &format!("{base}&cursor={cursor}"))
            .await?;
        eprintln!(
            "evm cost gate route {query}: first page {first_elapsed:?}, continuation {:?}",
            started.elapsed()
        );
    }

    database.cleanup().await
}
