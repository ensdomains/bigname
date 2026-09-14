// `relation=resolves_to` (feature request F8): names whose current `addr:<coin_type>` record
// resolves to an address, on `GET /v1/addresses/{address}/names` and `POST /v1/lookup`.

const V2_RESOLVES_TO_OTHER_EVM_COIN: &str = "2147483658";
const V2_ENSIP19_DEFAULT_COIN: &str = "2147483648";

#[tokio::test]
async fn v2_get_address_names_resolves_to_lists_names_whose_addr_record_points_here() -> Result<()>
{
    let database = TestDatabase::new_migrated().await?;
    seed_v2_address_names_fixture(&database).await?;
    seed_v2_resolves_to_records(&database).await?;

    let payload = v2_address_names_payload_for_database(
        &database,
        &format!("/v1/addresses/{V2_ADDRESS}/names?relation=resolves_to"),
    )
    .await?;
    let rows = payload["data"]
        .as_array()
        .expect("resolves_to data must be an array");
    // shared-one's default EVM address is shadowed for coin 60 by its exact addr:60 clear, and
    // beta resolves here only for another EVM coin type.
    assert_eq!(names(rows), vec!["alpha.eth", "gamma.eth"]);
    assert_eq!(rows[0]["relations"], json!(["resolves_to"]));
    assert_eq!(
        rows[0]["resolution"],
        json!({"coin_type": 60, "record_key": "addr:60"})
    );
    assert_eq!(rows[0]["is_primary"], json!(true));
    assert_eq!(rows[1]["is_primary"], json!(false));
    assert_eq!(
        rows[0]["owner"],
        json!("0x00000000000000000000000000000000000000a1")
    );
    assert_eq!(rows[0]["registration_status"], json!("active"));
    assert_eq!(rows[0]["expires_at"], json!("2027-01-02T00:00:00Z"));
    assert_eq!(payload["page"]["total_count"], Value::Null);
    assert_eq!(payload["page"]["has_more"], json!(false));
    assert_eq!(payload["meta"]["as_of"]["1"]["block_number"], json!(105));
    assert_no_banned_v1_spellings(&payload);

    // The authority relations are untouched: `any` stays the three authority relations and no
    // authority row carries a resolution.
    let any_payload = v2_address_names_payload_for_database(
        &database,
        &format!("/v1/addresses/{V2_ADDRESS}/names?relation=any"),
    )
    .await?;
    let any_rows = any_payload["data"].as_array().expect("any data");
    assert_eq!(
        names(any_rows),
        vec![
            "alpha.eth",
            "beta.eth",
            "gamma.eth",
            "shared-one.eth",
            "shared-two.eth"
        ]
    );
    assert!(any_rows.iter().all(|row| row.get("resolution").is_none()));
    assert!(any_rows.iter().all(|row| {
        !row["relations"]
            .as_array()
            .expect("relations")
            .contains(&json!("resolves_to"))
    }));

    // Another EVM coin type: beta's exact entry and shared-one's unshadowed ENSIP-19 default.
    let other_payload = v2_address_names_payload_for_database(
        &database,
        &format!(
            "/v1/addresses/{V2_ADDRESS}/names?relation=resolves_to&coin_type={V2_RESOLVES_TO_OTHER_EVM_COIN}"
        ),
    )
    .await?;
    let other_rows = other_payload["data"].as_array().expect("other data");
    assert_eq!(names(other_rows), vec!["beta.eth", "shared-one.eth"]);
    assert_eq!(
        other_rows[0]["resolution"],
        json!({"coin_type": 2_147_483_658_u64, "record_key": "addr:2147483658"})
    );
    assert_eq!(
        other_rows[1]["resolution"],
        json!({"coin_type": 2_147_483_658_u64, "record_key": "addr:2147483648"})
    );

    // The default entry itself answers only its own coin type.
    let default_payload = v2_address_names_payload_for_database(
        &database,
        &format!(
            "/v1/addresses/{V2_ADDRESS}/names?relation=resolves_to&coin_type={V2_ENSIP19_DEFAULT_COIN}"
        ),
    )
    .await?;
    assert_eq!(
        names(default_payload["data"].as_array().expect("default data")),
        vec!["shared-one.eth"]
    );

    // A non-EVM coin type never falls back to the default address.
    let btc_payload = v2_address_names_payload_for_database(
        &database,
        &format!("/v1/addresses/{V2_ADDRESS}/names?relation=resolves_to&coin_type=0"),
    )
    .await?;
    assert_eq!(btc_payload["data"], json!([]));

    // Prefix, namespace, and role-summary expansion behave as on the authority relations.
    let q_payload = v2_address_names_payload_for_database(
        &database,
        &format!("/v1/addresses/{V2_ADDRESS}/names?relation=resolves_to&q=ga&include=role_summary"),
    )
    .await?;
    let q_rows = q_payload["data"].as_array().expect("q data");
    assert_eq!(names(q_rows), vec!["gamma.eth"]);
    assert!(q_rows[0]["role_summary"].is_array());
    let namespace_payload = v2_address_names_payload_for_database(
        &database,
        &format!("/v1/addresses/{V2_ADDRESS}/names?relation=resolves_to&namespace=basenames"),
    )
    .await?;
    assert_eq!(namespace_payload["data"], json!([]));

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_get_address_names_resolves_to_rejects_mixed_relations_and_stray_coin_type()
-> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_address_names_fixture(&database).await?;

    for (uri, expected_message_fragment) in [
        (
            format!("/v1/addresses/{V2_ADDRESS}/names?relation=resolves_to,owner"),
            "resolves_to",
        ),
        (
            format!("/v1/addresses/{V2_ADDRESS}/names?relation=any,resolves_to"),
            "resolves_to",
        ),
        (
            format!("/v1/addresses/{V2_ADDRESS}/names?coin_type=60"),
            "coin_type requires relation=resolves_to",
        ),
        (
            format!("/v1/addresses/{V2_ADDRESS}/names?relation=owner&coin_type=60"),
            "coin_type requires relation=resolves_to",
        ),
        (
            format!("/v1/addresses/{V2_ADDRESS}/names?relation=resolves_to&coin_type=abc"),
            "coin_type",
        ),
    ] {
        let response = v2_address_names_response_for_database(&database, &uri).await?;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{uri}");
        let payload: Value = read_json(response).await?;
        assert_eq!(payload["error"]["code"], json!("invalid_input"), "{uri}");
        let message = payload["error"]["message"]
            .as_str()
            .expect("error message must be a string");
        assert!(
            message.contains(expected_message_fragment),
            "{uri}: {message}"
        );
    }

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_get_address_names_resolves_to_paginates_and_binds_cursor_to_relation_and_coin_type()
-> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_address_names_fixture(&database).await?;
    seed_v2_resolves_to_records(&database).await?;

    let first = v2_address_names_payload_for_database(
        &database,
        &format!("/v1/addresses/{V2_ADDRESS}/names?relation=resolves_to&page_size=1"),
    )
    .await?;
    assert_eq!(names(first["data"].as_array().expect("first page")), vec!["alpha.eth"]);
    assert_eq!(first["page"]["has_more"], json!(true));
    let cursor = first["page"]["next_cursor"]
        .as_str()
        .expect("first page must mint a cursor")
        .to_owned();

    let second = v2_address_names_payload_for_database(
        &database,
        &format!(
            "/v1/addresses/{V2_ADDRESS}/names?relation=resolves_to&page_size=1&cursor={cursor}"
        ),
    )
    .await?;
    assert_eq!(names(second["data"].as_array().expect("second page")), vec!["gamma.eth"]);
    assert_eq!(second["page"]["has_more"], json!(false));
    assert_eq!(second["page"]["cursor"], json!(cursor));

    for uri in [
        format!(
            "/v1/addresses/{V2_ADDRESS}/names?relation=resolves_to&coin_type={V2_RESOLVES_TO_OTHER_EVM_COIN}&page_size=1&cursor={cursor}"
        ),
        format!("/v1/addresses/{V2_ADDRESS}/names?relation=owner&page_size=1&cursor={cursor}"),
        format!("/v1/addresses/{V2_ADDRESS}/names?page_size=1&cursor={cursor}"),
    ] {
        let response = v2_address_names_response_for_database(&database, &uri).await?;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{uri}");
        let payload: Value = read_json(response).await?;
        assert_eq!(payload["error"]["code"], json!("invalid_input"), "{uri}");
    }

    // An authority-relation cursor never resumes a resolves_to page either.
    let owner_first = v2_address_names_payload_for_database(
        &database,
        &format!("/v1/addresses/{V2_ADDRESS}/names?relation=owner&page_size=1"),
    )
    .await?;
    let owner_cursor = owner_first["page"]["next_cursor"]
        .as_str()
        .expect("owner page must mint a cursor");
    let response = v2_address_names_response_for_database(
        &database,
        &format!(
            "/v1/addresses/{V2_ADDRESS}/names?relation=resolves_to&page_size=1&cursor={owner_cursor}"
        ),
    )
    .await?;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_lookup_reverse_resolves_to_returns_records_with_resolution() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let address = "0x0000000000000000000000000000000000000abc";
    seed_v2_lookup_reverse_fixture(&database, address).await?;
    seed_v2_lookup_resolves_to_records(&database, address).await?;

    let payload = v2_lookup_json(
        &database,
        json!({
            "profile": "detail",
            "inputs": [
                {"id": "eth", "address": address, "relation": "resolves_to"},
                {
                    "id": "other", "address": address, "relation": "resolves_to",
                    "coin_type": 2_147_483_658_u64
                },
                {"id": "any", "address": address, "relation": "any"}
            ]
        }),
    )
    .await?;

    assert_eq!(
        payload["data"][0]["input"],
        json!({
            "id": "eth",
            "address": address,
            "coin_type": 60,
            "relation": "resolves_to"
        })
    );
    assert_eq!(payload["data"][0]["status"], json!("ok"));
    let eth_records = payload["data"][0]["records"]
        .as_array()
        .expect("eth records must be an array");
    assert_eq!(names(eth_records), vec!["alice.eth"]);
    assert_eq!(eth_records[0]["relations"], json!(["resolves_to"]));
    assert_eq!(
        eth_records[0]["resolution"],
        json!({"coin_type": 60, "record_key": "addr:60"})
    );
    assert_eq!(eth_records[0]["is_primary"], json!(true));
    assert_eq!(payload["data"][0]["page"]["total_count"], Value::Null);
    assert_eq!(payload["data"][0]["page"]["has_more"], json!(false));

    assert_eq!(payload["data"][1]["input"]["coin_type"], json!(2_147_483_658_u64));
    let other_records = payload["data"][1]["records"]
        .as_array()
        .expect("other records must be an array");
    assert_eq!(names(other_records), vec!["bob.eth"]);
    assert_eq!(
        other_records[0]["resolution"],
        json!({"coin_type": 2_147_483_658_u64, "record_key": "addr:2147483658"})
    );
    assert_eq!(other_records[0]["is_primary"], json!(false));

    // `any` keeps its authority-relation meaning and its rows carry no resolution.
    let any_records = payload["data"][2]["records"]
        .as_array()
        .expect("any records must be an array");
    assert_eq!(names(any_records), vec!["alice.eth", "bob.eth"]);
    assert!(any_records.iter().all(|record| record.get("resolution").is_none()));
    assert_eq!(any_records[0]["relations"], json!(["owner"]));
    assert!(payload["meta"]["as_of"].is_object());

    // Feed profile keeps the relation and resolution on the reduced record.
    let feed = v2_lookup_json(
        &database,
        json!({
            "profile": "feed",
            "inputs": [{"address": address, "relation": "resolves_to"}]
        }),
    )
    .await?;
    assert_eq!(feed["data"][0]["records"][0]["relations"], json!(["resolves_to"]));
    assert_eq!(
        feed["data"][0]["records"][0]["resolution"],
        json!({"coin_type": 60, "record_key": "addr:60"})
    );
    assert!(feed["data"][0]["records"][0].get("owner").is_none());

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_lookup_reverse_resolves_to_paginates_with_a_bound_cursor() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let address = "0x0000000000000000000000000000000000000abc";
    seed_v2_lookup_reverse_fixture(&database, address).await?;
    seed_v2_lookup_resolves_to_records(&database, address).await?;
    // bob also resolves here on coin 60 so the page has two rows.
    upsert_phase_address_records_current_row(
        &database.pool,
        address,
        "ens",
        "bob.eth",
        Uuid::from_u128(0x5a0213),
        Uuid::from_u128(0x5a0211),
        "60",
        "addr:60",
        json!({}),
    )
    .await?;

    let first = v2_lookup_json(
        &database,
        json!({
            "inputs": [{"id": "p1", "address": address, "relation": "resolves_to", "page_size": 1}]
        }),
    )
    .await?;
    let first_records = first["data"][0]["records"].as_array().expect("first page");
    assert_eq!(names(first_records), vec!["alice.eth"]);
    assert_eq!(first["data"][0]["page"]["has_more"], json!(true));
    let cursor = first["data"][0]["page"]["next_cursor"]
        .as_str()
        .expect("first page must mint a cursor")
        .to_owned();

    let second = v2_lookup_json(
        &database,
        json!({
            "inputs": [{
                "id": "p2", "address": address, "relation": "resolves_to", "page_size": 1,
                "cursor": cursor
            }]
        }),
    )
    .await?;
    let second_records = second["data"][0]["records"].as_array().expect("second page");
    assert_eq!(names(second_records), vec!["bob.eth"]);
    assert_eq!(second["data"][0]["page"]["has_more"], json!(false));
    assert_eq!(second["data"][0]["page"]["cursor"], json!(cursor));

    for input in [
        json!({
            "address": address, "relation": "resolves_to", "coin_type": 2_147_483_658_u64,
            "page_size": 1, "cursor": cursor
        }),
        json!({"address": address, "relation": "any", "page_size": 1, "cursor": cursor}),
        json!({"address": address, "page_size": 1, "cursor": cursor}),
    ] {
        let response = v2_lookup_response_for_database(
            &database,
            "/v1/lookup",
            json!({"inputs": [input.clone()]}),
        )
        .await?;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{input}");
        let payload: Value = read_json(response).await?;
        assert_eq!(payload["error"]["code"], json!("invalid_input"), "{input}");
    }

    // Mixed relation sets are rejected on lookup inputs as on the collection route.
    let response = v2_lookup_response_for_database(
        &database,
        "/v1/lookup",
        json!({"inputs": [{"address": address, "relation": "owner,resolves_to"}]}),
    )
    .await?;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    database.cleanup().await?;
    Ok(())
}

/// Reverse-index rows for the `seed_v2_address_names_fixture` names: alpha and gamma resolve
/// to the address for coin 60; beta for another EVM coin type only; shared-one through the
/// ENSIP-19 default EVM address whose exact addr:60 clear shadows coin 60.
async fn seed_v2_resolves_to_records(database: &TestDatabase) -> Result<()> {
    let specs = v2_address_name_specs();
    let spec = |name: &str| {
        specs
            .iter()
            .find(|spec| spec.name == name)
            .unwrap_or_else(|| panic!("fixture must include {name}"))
    };
    for (name, coin_type, record_key, provenance_extra) in [
        ("alpha.eth", "60", "addr:60", json!({})),
        ("gamma.eth", "60", "addr:60", json!({})),
        (
            "beta.eth",
            V2_RESOLVES_TO_OTHER_EVM_COIN,
            "addr:2147483658",
            json!({}),
        ),
        (
            "shared-one.eth",
            V2_ENSIP19_DEFAULT_COIN,
            "addr:2147483648",
            json!({"ensip19_default_address": true, "shadowed_coin_types": ["60"]}),
        ),
    ] {
        let spec = spec(name);
        upsert_phase_address_records_current_row(
            &database.pool,
            V2_ADDRESS,
            "ens",
            spec.name,
            spec.surface_binding_id,
            spec.resource_id,
            coin_type,
            record_key,
            provenance_extra,
        )
        .await?;
    }
    Ok(())
}

/// Reverse-index rows for the `seed_v2_lookup_reverse_fixture` names: alice on coin 60, bob on
/// another EVM coin type.
async fn seed_v2_lookup_resolves_to_records(database: &TestDatabase, address: &str) -> Result<()> {
    upsert_phase_address_records_current_row(
        &database.pool,
        address,
        "ens",
        "alice.eth",
        Uuid::from_u128(0x5a0203),
        Uuid::from_u128(0x5a0201),
        "60",
        "addr:60",
        json!({}),
    )
    .await?;
    upsert_phase_address_records_current_row(
        &database.pool,
        address,
        "ens",
        "bob.eth",
        Uuid::from_u128(0x5a0213),
        Uuid::from_u128(0x5a0211),
        V2_RESOLVES_TO_OTHER_EVM_COIN,
        "addr:2147483658",
        json!({}),
    )
    .await?;
    Ok(())
}

/// Insert one `address_records_current` row the way Project publishes it: identity from the
/// phase `name_current` row, target position from that row's publication, coverage projected.
#[allow(clippy::too_many_arguments)]
async fn upsert_phase_address_records_current_row(
    pool: &PgPool,
    address: &str,
    namespace: &str,
    name: &str,
    surface_binding_id: Uuid,
    resource_id: Uuid,
    coin_type: &str,
    record_key: &str,
    provenance_extra: Value,
) -> Result<()> {
    let normalized_name = bigname_domain::normalization::normalize_name(name)
        .map_err(|error| anyhow::anyhow!(error.message().to_owned()))?
        .normalized_name;
    let (logical_name_id, namehash) = phase_logical_identity(namespace, &normalized_name)?;
    let chain_positions: Value = sqlx::query_scalar(
        "SELECT chain_positions FROM bigname_phase.name_current WHERE logical_name_id = $1",
    )
    .bind(&logical_name_id)
    .fetch_one(pool)
    .await
    .with_context(|| format!("phase name_current row for {logical_name_id} must exist"))?;
    let chain_id = phase_projection_source_position(&chain_positions)?
        .get("chain_id")
        .and_then(Value::as_str)
        .context("address_records_current fixture position must include chain_id")?
        .to_owned();
    let (target_block_number, target_block_hash) =
        phase_projection_target_for_chain(pool, &chain_id, &chain_positions).await?;
    let mut provenance = json!({
        "chain_id": chain_id,
        "logical_name_id": logical_name_id,
        "coverage": {"status": "projected", "exhaustiveness": "not_asserted"},
    });
    if let (Some(base), Some(extra)) = (provenance.as_object_mut(), provenance_extra.as_object())
    {
        for (key, value) in extra {
            base.insert(key.clone(), value.clone());
        }
    }
    sqlx::query(
        r#"
        INSERT INTO bigname_phase.address_records_current (
            address, coin_type, logical_name_id, namespace, raw_name, namehash,
            surface_binding_id, resource_id, record_resource_id, binding_kind, record_key,
            support_status, unsupported_reason, provenance, chain_positions,
            canonicality_summary, manifest_version
        )
        VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8, $8, 'declared_registry_path', $9,
            'supported', NULL, $10, $11, $12, 1
        )
        ON CONFLICT (address, coin_type, logical_name_id) DO UPDATE SET
            record_key = EXCLUDED.record_key,
            provenance = EXCLUDED.provenance,
            chain_positions = EXCLUDED.chain_positions,
            canonicality_summary = EXCLUDED.canonicality_summary
        "#,
    )
    .bind(address.to_ascii_lowercase())
    .bind(coin_type)
    .bind(&logical_name_id)
    .bind(namespace)
    .bind(&normalized_name)
    .bind(namehash)
    .bind(surface_binding_id)
    .bind(resource_id)
    .bind(record_key)
    .bind(provenance)
    .bind(phase_flat_projection_position(
        target_block_number,
        &target_block_hash,
    ))
    .bind(json!({
        "state": "canonical_lineage",
        "target_block_number": target_block_number,
        "target_block_hash": target_block_hash,
    }))
    .execute(pool)
    .await?;
    Ok(())
}
