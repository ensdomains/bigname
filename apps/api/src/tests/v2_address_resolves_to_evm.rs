// `relation=resolves_to&coin_type=evm`: names whose stored `addr:<coin_type>` record for any EVM
// coin type (60, or 2^31 through 2^32 - 1) resolves to the address, one row per name with the
// matched coin types in `resolutions`.

const V2_RESOLVES_TO_BASE_COIN: &str = "2147492101";

/// Extra reverse-index rows over `seed_v2_resolves_to_records`: alpha also resolves here on Base,
/// and 20-byte values under non-EVM coin types (118, 61) that the producer keeps because it checks
/// the value's shape, not the coin type.
async fn seed_v2_resolves_to_evm_records(database: &TestDatabase) -> Result<()> {
    seed_v2_resolves_to_records(database).await?;
    let specs = v2_address_name_specs();
    for (name, coin_type) in [
        ("alpha.eth", V2_RESOLVES_TO_BASE_COIN),
        ("gamma.eth", "118"),
        ("beta.eth", "61"),
    ] {
        let spec = specs
            .iter()
            .find(|spec| spec.name == name)
            .expect("fixture name");
        upsert_phase_address_records_current_row(
            &database.pool,
            V2_ADDRESS,
            "ens",
            spec.name,
            spec.surface_binding_id,
            spec.resource_id,
            coin_type,
            &format!("addr:{coin_type}"),
            json!({}),
        )
        .await?;
    }
    Ok(())
}

fn resolutions(row: &Value) -> Vec<(u64, &str)> {
    row["resolutions"]
        .as_array()
        .unwrap_or_else(|| panic!("row must carry resolutions: {row}"))
        .iter()
        .map(|entry| {
            (
                entry["coin_type"].as_u64().expect("numeric coin_type"),
                entry["record_key"].as_str().expect("record_key"),
            )
        })
        .collect()
}

fn row_named<'a>(rows: &'a [Value], name: &str) -> &'a Value {
    rows.iter()
        .find(|row| row["name"] == json!(name))
        .unwrap_or_else(|| panic!("{name} must be present"))
}

async fn v2_resolves_to_evm_rows(database: &TestDatabase, query: &str) -> Result<Vec<Value>> {
    let payload = v2_address_names_payload_for_database(
        database,
        &format!("/v1/addresses/{V2_ADDRESS}/names?relation=resolves_to&coin_type=evm{query}"),
    )
    .await?;
    Ok(payload["data"].as_array().expect("data array").clone())
}

#[tokio::test]
async fn v2_resolves_to_evm_discovers_names_across_evm_coin_types() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_address_names_fixture(&database).await?;
    seed_v2_resolves_to_evm_records(&database).await?;

    let payload = v2_address_names_payload_for_database(
        &database,
        &format!("/v1/addresses/{V2_ADDRESS}/names?relation=resolves_to&coin_type=evm"),
    )
    .await?;
    let rows = payload["data"].as_array().expect("evm data");
    // beta matches only another EVM chain and shared-one only the shadowed default record, so
    // neither appears in the default coin-60 read; here every EVM match is found in one call.
    assert_eq!(
        names(rows),
        vec!["alpha.eth", "beta.eth", "gamma.eth", "shared-one.eth"]
    );
    // One row per name with every matched EVM coin type, ascending.
    assert_eq!(
        resolutions(row_named(rows, "alpha.eth")),
        vec![(60, "addr:60"), (2_147_492_101, "addr:2147492101")]
    );
    // Coin 61 is a legacy SLIP-44 coin type, not an ENSIP-19 EVM coin type.
    assert_eq!(
        resolutions(row_named(rows, "beta.eth")),
        vec![(2_147_483_658, "addr:2147483658")]
    );
    // A non-EVM coin type with a 20-byte value is not matched.
    assert_eq!(
        resolutions(row_named(rows, "gamma.eth")),
        vec![(60, "addr:60")]
    );
    // The stored default record appears once under its own coin type, although an exact coin-60
    // entry shadows it for coin 60, and it is not expanded into the chains it would answer.
    assert_eq!(
        resolutions(row_named(rows, "shared-one.eth")),
        vec![(2_147_483_648, "addr:2147483648")]
    );
    for row in rows {
        assert!(row.get("resolution").is_none(), "{row}");
        assert_eq!(row["relations"], json!(["resolves_to"]), "{row}");
    }
    assert_eq!(payload["page"]["total_count"], Value::Null);
    assert_eq!(payload["page"]["has_more"], json!(false));
    assert_no_banned_v1_spellings(&payload);

    // The single-coin reads keep their meaning and shape.
    let default_payload = v2_address_names_payload_for_database(
        &database,
        &format!("/v1/addresses/{V2_ADDRESS}/names?relation=resolves_to"),
    )
    .await?;
    let default_rows = default_payload["data"].as_array().expect("default data");
    assert_eq!(names(default_rows), vec!["alpha.eth", "gamma.eth"]);
    assert_eq!(
        default_rows[0]["resolution"],
        json!({"coin_type": 60, "record_key": "addr:60"})
    );
    assert!(
        default_rows
            .iter()
            .all(|row| row.get("resolutions").is_none())
    );
    let cosmos = v2_address_names_payload_for_database(
        &database,
        &format!("/v1/addresses/{V2_ADDRESS}/names?relation=resolves_to&coin_type=118"),
    )
    .await?;
    assert_eq!(
        names(cosmos["data"].as_array().expect("coin 118 data")),
        vec!["gamma.eth"]
    );

    // Filters narrow the evm read before pagination exactly as they narrow a single-coin read.
    let q_rows = v2_resolves_to_evm_rows(&database, "&q=sh").await?;
    assert_eq!(names(&q_rows), vec!["shared-one.eth"]);
    let basenames = v2_resolves_to_evm_rows(&database, "&namespace=basenames").await?;
    assert!(basenames.is_empty());
    let counted = v2_resolves_to_evm_rows(&database, "&include=counts&q=alpha").await?;
    assert_eq!(names(&counted), vec!["alpha.eth"]);
    assert!(counted[0]["subname_count"].is_u64(), "{}", counted[0]);

    database.cleanup().await
}

#[tokio::test]
async fn v2_resolves_to_evm_finds_a_base_only_address() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_address_names_fixture(&database).await?;
    let beta = v2_address_name_specs()
        .into_iter()
        .find(|spec| spec.name == "beta.eth")
        .expect("beta spec");
    upsert_phase_address_records_current_row(
        &database.pool,
        V2_OTHER_ADDRESS,
        "ens",
        beta.name,
        beta.surface_binding_id,
        beta.resource_id,
        V2_RESOLVES_TO_BASE_COIN,
        "addr:2147492101",
        json!({}),
    )
    .await?;

    let default_payload = v2_address_names_payload_for_database(
        &database,
        &format!("/v1/addresses/{V2_OTHER_ADDRESS}/names?relation=resolves_to"),
    )
    .await?;
    assert_eq!(default_payload["data"], json!([]));

    // Checksum case is accepted and lowercased, as on every address route.
    let evm_payload = v2_address_names_payload_for_database(
        &database,
        "/v1/addresses/0x0000000000000000000000000000000000000DEF/names?relation=resolves_to&coin_type=evm",
    )
    .await?;
    let rows = evm_payload["data"].as_array().expect("evm data");
    assert_eq!(names(rows), vec!["beta.eth"]);
    assert_eq!(
        resolutions(&rows[0]),
        vec![(2_147_492_101, "addr:2147492101")]
    );

    database.cleanup().await
}

#[tokio::test]
async fn v2_resolves_to_evm_parameter_spelling() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_address_names_fixture(&database).await?;
    seed_v2_resolves_to_evm_records(&database).await?;
    let base = format!("/v1/addresses/{V2_ADDRESS}/names?relation=resolves_to");

    // A bare or whitespace-only coin_type stays "omitted": coin type 60.
    let omitted = v2_address_names_payload_for_database(&database, &base).await?;
    for blank in ["&coin_type=", "&coin_type=%20%20"] {
        let payload = v2_address_names_payload_for_database(&database, &format!("{base}{blank}"))
            .await?;
        assert_eq!(payload["data"], omitted["data"], "{blank}");
    }
    // Boundary whitespace is trimmed like every query value.
    let evm = v2_address_names_payload_for_database(&database, &format!("{base}&coin_type=evm"))
        .await?;
    let padded =
        v2_address_names_payload_for_database(&database, &format!("{base}&coin_type=%20evm%20"))
            .await?;
    assert_eq!(padded["data"], evm["data"]);

    for (query, fragment) in [
        ("&coin_type=EVM", "coin_type"),
        ("&coin_type=Evm", "coin_type"),
        ("&coin_type=evm,60", "coin_type"),
        ("&coin_type=60,2147492101", "coin_type"),
        ("&coin_type=any", "coin_type"),
        ("&coin_type=evm&is_migrated=true", "is_migrated"),
    ] {
        let uri = format!("{base}{query}");
        let response = v2_address_names_response_for_database(&database, &uri).await?;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{uri}");
        let payload: Value = read_json(response).await?;
        assert_eq!(payload["error"]["code"], json!("invalid_input"), "{uri}");
        let message = payload["error"]["message"].as_str().expect("message");
        assert!(message.contains(fragment), "{uri}: {message}");
    }
    let response = v2_address_names_response_for_database(
        &database,
        &format!("/v1/addresses/{V2_ADDRESS}/names?relation=owner&coin_type=evm"),
    )
    .await?;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    database.cleanup().await
}

/// Walk `coin_type=evm` with `page_size=1` for each covered sort and order and require the pages,
/// concatenated, to equal the unpaged read: nothing repeated or skipped when a name or group
/// matches several coin types.
async fn assert_v2_resolves_to_evm_page_walks(database: &TestDatabase, dedupe: &str) -> Result<()> {
    for (sort, order) in [("name", "asc"), ("name", "desc"), ("expires_at", "asc")] {
        let base = format!(
            "/v1/addresses/{V2_ADDRESS}/names?relation=resolves_to&coin_type=evm&dedupe={dedupe}&sort={sort}&order={order}"
        );
        let full = v2_address_names_payload_for_database(database, &base).await?;
        let expected = full["data"].as_array().expect("full page").clone();
        let mut paged = Vec::new();
        let mut cursor: Option<String> = None;
        loop {
            let uri = match &cursor {
                Some(cursor) => format!("{base}&page_size=1&cursor={cursor}"),
                None => format!("{base}&page_size=1"),
            };
            let page = v2_address_names_payload_for_database(database, &uri).await?;
            let rows = page["data"].as_array().expect("page rows");
            assert_eq!(rows.len(), 1, "{uri}: {page}");
            paged.push(rows[0].clone());
            match page["page"]["next_cursor"].as_str() {
                Some(next) => cursor = Some(next.to_owned()),
                None => break,
            }
        }
        let paged_names = names(&paged);
        let distinct = paged_names.iter().collect::<std::collections::BTreeSet<_>>();
        assert_eq!(distinct.len(), paged_names.len(), "{dedupe} {sort} {order}");
        assert_eq!(paged, expected, "{dedupe} {sort} {order}");
    }
    Ok(())
}

#[tokio::test]
async fn v2_resolves_to_evm_paginates_and_binds_cursor_to_the_selector() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_address_names_fixture(&database).await?;
    seed_v2_resolves_to_evm_records(&database).await?;

    assert_v2_resolves_to_evm_page_walks(&database, "name").await?;

    let first = v2_address_names_payload_for_database(
        &database,
        &format!("/v1/addresses/{V2_ADDRESS}/names?relation=resolves_to&coin_type=evm&page_size=1"),
    )
    .await?;
    let evm_cursor = first["page"]["next_cursor"].as_str().expect("evm cursor");
    let single = v2_address_names_payload_for_database(
        &database,
        &format!("/v1/addresses/{V2_ADDRESS}/names?relation=resolves_to&page_size=1"),
    )
    .await?;
    let single_cursor = single["page"]["next_cursor"].as_str().expect("single cursor");
    for uri in [
        format!(
            "/v1/addresses/{V2_ADDRESS}/names?relation=resolves_to&page_size=1&cursor={evm_cursor}"
        ),
        format!(
            "/v1/addresses/{V2_ADDRESS}/names?relation=resolves_to&coin_type=2147483648&page_size=1&cursor={evm_cursor}"
        ),
        format!(
            "/v1/addresses/{V2_ADDRESS}/names?relation=resolves_to&coin_type=evm&dedupe=registration&page_size=1&cursor={evm_cursor}"
        ),
        format!(
            "/v1/addresses/{V2_ADDRESS}/names?relation=resolves_to&coin_type=evm&page_size=1&cursor={single_cursor}"
        ),
    ] {
        let response = v2_address_names_response_for_database(&database, &uri).await?;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{uri}");
    }

    database.cleanup().await
}

#[tokio::test]
async fn v2_resolves_to_evm_registration_dedupe_keeps_the_group_matches() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_address_names_fixture(&database).await?;
    seed_v2_resolves_to_evm_records(&database).await?;
    // shared-one and shared-two share one registration resource and therefore one record
    // inventory; the reverse index gives both names the same default-record row.
    let shared_two = v2_address_name_specs()
        .into_iter()
        .find(|spec| spec.name == "shared-two.eth")
        .expect("shared-two spec");
    upsert_phase_address_records_current_row(
        &database.pool,
        V2_ADDRESS,
        "ens",
        shared_two.name,
        shared_two.surface_binding_id,
        shared_two.resource_id,
        V2_ENSIP19_DEFAULT_COIN,
        "addr:2147483648",
        json!({"ensip19_default_address": true, "shadowed_coin_types": ["60"]}),
    )
    .await?;

    let by_name = v2_resolves_to_evm_rows(&database, "").await?;
    assert_eq!(
        names(&by_name),
        vec![
            "alpha.eth",
            "beta.eth",
            "gamma.eth",
            "shared-one.eth",
            "shared-two.eth"
        ]
    );
    let by_registration = v2_resolves_to_evm_rows(&database, "&dedupe=registration").await?;
    assert_eq!(
        names(&by_registration),
        vec!["alpha.eth", "beta.eth", "gamma.eth", "shared-one.eth"]
    );
    let shared = row_named(&by_registration, "shared-one.eth");
    assert_eq!(resolutions(shared), vec![(2_147_483_648, "addr:2147483648")]);

    // The group facet is the union of every member's matches, kept before the representative is
    // chosen. The producer gives members of one group identical rows, so this extra Base row for
    // shared-two alone is a direct insert that only pins the union and representative rules.
    upsert_phase_address_records_current_row(
        &database.pool,
        V2_ADDRESS,
        "ens",
        shared_two.name,
        shared_two.surface_binding_id,
        shared_two.resource_id,
        V2_RESOLVES_TO_BASE_COIN,
        "addr:2147492101",
        json!({}),
    )
    .await?;
    upsert_primary_name_current_snapshots(
        &database.pool,
        &[v2_evm_primary_claim(V2_RESOLVES_TO_BASE_COIN, "shared-one.eth")],
    )
    .await?;
    let by_registration = v2_resolves_to_evm_rows(&database, "&dedupe=registration").await?;
    let shared = row_named(&by_registration, "shared-one.eth");
    assert_eq!(
        resolutions(shared),
        vec![
            (2_147_483_648, "addr:2147483648"),
            (2_147_492_101, "addr:2147492101")
        ]
    );
    // The representative did not match Base itself, so a Base claim naming it does not count.
    assert_eq!(shared["is_primary"], json!(false));
    upsert_primary_name_current_snapshots(
        &database.pool,
        &[v2_evm_primary_claim(V2_ENSIP19_DEFAULT_COIN, "shared-one.eth")],
    )
    .await?;
    let by_registration = v2_resolves_to_evm_rows(&database, "&dedupe=registration").await?;
    assert_eq!(
        row_named(&by_registration, "shared-one.eth")["is_primary"],
        json!(true)
    );

    database.cleanup().await
}

#[tokio::test]
async fn v2_resolves_to_evm_registration_dedupe_paginates() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_address_names_fixture(&database).await?;
    seed_v2_resolves_to_evm_records(&database).await?;
    // shared-one and shared-two form one registration group through their shared inventory;
    // alpha matches coin 60 and Base.
    let shared_two = v2_address_name_specs()
        .into_iter()
        .find(|spec| spec.name == "shared-two.eth")
        .expect("shared-two spec");
    upsert_phase_address_records_current_row(
        &database.pool,
        V2_ADDRESS,
        "ens",
        shared_two.name,
        shared_two.surface_binding_id,
        shared_two.resource_id,
        V2_ENSIP19_DEFAULT_COIN,
        "addr:2147483648",
        json!({"ensip19_default_address": true, "shadowed_coin_types": ["60"]}),
    )
    .await?;
    let rows = v2_resolves_to_evm_rows(&database, "&dedupe=registration").await?;
    assert_eq!(
        names(&rows),
        vec!["alpha.eth", "beta.eth", "gamma.eth", "shared-one.eth"]
    );
    assert_eq!(resolutions(row_named(&rows, "alpha.eth")).len(), 2);

    assert_v2_resolves_to_evm_page_walks(&database, "registration").await?;

    database.cleanup().await
}

fn v2_evm_primary_claim(coin_type: &str, name: &str) -> PrimaryNameCurrentSnapshot {
    PrimaryNameCurrentSnapshot {
        row: PrimaryNameCurrentRow {
            address: V2_ADDRESS.to_owned(),
            namespace: "ens".to_owned(),
            coin_type: coin_type.to_owned(),
            claim_status: PrimaryNameClaimStatus::Success,
            raw_claim_name: None,
            claim_provenance: json!({
                "source_family": "ens_v1_reverse_l1",
                "contract_role": "reverse_registrar",
            }),
        },
        normalized_claim_name: Some(name.to_owned()),
        claim_name_is_normalized: true,
    }
}

/// Replace a claim's served columns with a canonical-head hydration from an orphaned block and
/// keep `baseline` as the retained event claim.
async fn hydrate_v2_evm_primary_claim_on_orphaned_block(
    database: &TestDatabase,
    coin_type: &str,
    hydrated_name: &str,
    baseline: Value,
) -> Result<()> {
    sqlx::query(
        "INSERT INTO bigname_phase.chain_lineage (
             chain_id, block_hash, block_number, block_timestamp, canonicality_state
         ) VALUES ('ethereum-mainnet', '0xevm-orphaned-hydration', 42,
             '2026-04-17T00:00:42Z', 'orphaned')
         ON CONFLICT DO NOTHING",
    )
    .execute(&database.pool)
    .await?;
    sqlx::query(
        r#"
        UPDATE bigname_phase.primary_names_current
        SET claim_status = 'success', raw_claim_name = $3, claim_name_is_normalized = true,
            unsupported_reason = NULL,
            claim_provenance = claim_provenance || jsonb_build_object(
                'canonical_head_multicall_hydration', jsonb_build_object(
                    'chain_id', 'ethereum-mainnet',
                    'block_number', 42,
                    'block_hash', '0xevm-orphaned-hydration',
                    'baseline', $4::jsonb
                )
            )
        WHERE address = $1 AND namespace = 'ens' AND coin_type = $2
        "#,
    )
    .bind(V2_ADDRESS)
    .bind(coin_type)
    .bind(hydrated_name)
    .bind(baseline)
    .execute(&database.pool)
    .await?;
    Ok(())
}

#[tokio::test]
async fn v2_resolves_to_evm_is_primary_counts_only_the_row_matched_coins() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_address_names_fixture(&database).await?;
    seed_v2_resolves_to_evm_records(&database).await?;
    upsert_primary_name_current_snapshots(
        &database.pool,
        &[
            // gamma matched only coin 60, so a claim naming it for another chain does not count.
            v2_evm_primary_claim(V2_RESOLVES_TO_OTHER_EVM_COIN, "gamma.eth"),
            v2_evm_primary_claim(V2_ENSIP19_DEFAULT_COIN, "shared-one.eth"),
        ],
    )
    .await?;

    let rows = v2_resolves_to_evm_rows(&database, "").await?;
    // The fixture's coin-60 claim names alpha, which matched coin 60.
    assert_eq!(row_named(&rows, "alpha.eth")["is_primary"], json!(true));
    assert_eq!(row_named(&rows, "gamma.eth")["is_primary"], json!(false));
    assert_eq!(row_named(&rows, "beta.eth")["is_primary"], json!(false));
    assert_eq!(row_named(&rows, "shared-one.eth")["is_primary"], json!(true));

    // A hydration whose block left canonical lineage is not served; the retained baseline is.
    hydrate_v2_evm_primary_claim_on_orphaned_block(
        &database,
        V2_RESOLVES_TO_OTHER_EVM_COIN,
        "beta.eth",
        json!({
            "claim_status": "unsupported",
            "raw_claim_name": null,
            "claim_name_is_normalized": false,
            "unsupported_reason": "legacy_resolver_does_not_emit_name"
        }),
    )
    .await?;
    hydrate_v2_evm_primary_claim_on_orphaned_block(
        &database,
        V2_ENSIP19_DEFAULT_COIN,
        "alpha.eth",
        json!({
            "claim_status": "success",
            "raw_claim_name": "shared-one.eth",
            "claim_name_is_normalized": true
        }),
    )
    .await?;
    let rows = v2_resolves_to_evm_rows(&database, "").await?;
    assert_eq!(row_named(&rows, "beta.eth")["is_primary"], json!(false));
    assert_eq!(row_named(&rows, "shared-one.eth")["is_primary"], json!(true));
    // alpha stays primary through its own coin-60 claim only.
    assert_eq!(row_named(&rows, "alpha.eth")["is_primary"], json!(true));
    // The single-coin read applies the same snapshot rules.
    let single = v2_address_names_payload_for_database(
        &database,
        &format!(
            "/v1/addresses/{V2_ADDRESS}/names?relation=resolves_to&coin_type={V2_RESOLVES_TO_OTHER_EVM_COIN}"
        ),
    )
    .await?;
    assert_eq!(
        row_named(single["data"].as_array().expect("single"), "beta.eth")["is_primary"],
        json!(false)
    );

    database.cleanup().await
}
