// `relation=resolves_to` (feature request F8): names whose current `addr:<coin_type>` record
// resolves to an address, on `GET /v1/addresses/{address}/names` and `POST /v1/lookup`.

const V2_RESOLVES_TO_OTHER_EVM_COIN: &str = "2147483658";
const V2_ENSIP19_DEFAULT_COIN: &str = "2147483648";

#[tokio::test]
async fn v2_lookup_resolves_to_includes_root_pointer_without_authority() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let address = "0x0000000000000000000000000000000000000abc";
    seed_v2_lookup_reverse_fixture(&database, address).await?;
    seed_v2_lookup_resolves_to_records(&database, address).await?;
    // The `eth` TLD is served through its ENSv2 root-registry resolver pointer alone.
    seed_resolves_to_root_tld(&database, "eth", address).await?;
    for profile in ["feed", "detail"] {
        let request =
            json!({"profile":profile,"inputs":[{"address":address,"relation":"resolves_to"}]});
        let payload = v2_lookup_json(&database, request).await?;
        let records = payload["data"][0]["records"]
            .as_array()
            .expect("root pointer records");
        assert_eq!(names(records), vec!["alice.eth", "eth"], "{payload}");
        assert_eq!(records[1]["relations"], json!(["resolves_to"]));
        for field in ["owner", "registration_id", "authority"] {
            assert!(
                records[1].get(field).is_none_or(Value::is_null),
                "{field}: {payload}"
            );
        }
    }
    database.cleanup().await
}

#[tokio::test]
async fn v2_resolves_to_pages_names_without_authority_or_registration() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    database
        .seed_default_ens_snapshot_selector_position()
        .await?;
    seed_v2_address_name_identities(&database, &[]).await?;
    // Registry nodes whose owner was cleared keep serving their retained resolver pointer.
    for (name, seed) in [("alpha.eth", 0xa500), ("gamma.eth", 0xc500)] {
        seed_resolves_to_ownerless_name(&database, name, seed).await?;
    }
    for (dedupe, sort) in [
        ("name", "name"),
        ("registration", "name"),
        ("name", "expires_at"),
    ] {
        let uri = format!(
            "/v1/addresses/{V2_ADDRESS}/names?relation=resolves_to&dedupe={dedupe}&sort={sort}&page_size=1&include=role_summary"
        );
        let first = v2_address_names_payload_for_database(&database, &uri).await?;
        // Null timestamp ties use logical-name identity, whose order differs from name text.
        let expected = if sort == "name" {
            ["alpha.eth", "gamma.eth"]
        } else {
            ["gamma.eth", "alpha.eth"]
        };
        assert_eq!(first["data"][0]["name"], expected[0], "{first}");
        assert_eq!(
            first["data"][0]["registration_status"], "unregistered",
            "{first}"
        );
        for field in ["owner", "registrant", "authority"] {
            assert!(
                first["data"][0].get(field).is_none_or(Value::is_null),
                "{field}: {first}"
            );
        }
        assert_eq!(first["data"][0]["role_summary"], json!([]), "{first}");
        let cursor = first["page"]["next_cursor"]
            .as_str()
            .expect("ownerless cursor");
        let second =
            v2_address_names_payload_for_database(&database, &format!("{uri}&cursor={cursor}"))
                .await?;
        assert_eq!(second["data"][0]["name"], expected[1], "{second}");
        assert_eq!(second["page"]["has_more"], false);
    }
    database.cleanup().await
}

#[tokio::test]
async fn v2_get_address_names_resolves_to_filters_authority_before_pagination() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_address_names_fixture(&database).await?;
    seed_v2_resolves_to_records(&database).await?;
    let empty = v2_address_names_payload_for_database(
        &database,
        &format!("/v1/addresses/{V2_ADDRESS}/names?relation=resolves_to&authority=ens_v2"),
    )
    .await?;
    assert_eq!(empty["data"], json!([]));

    bind_address_name_ens_v2(&database, "alpha.eth", 0xa200, false).await?;
    let filtered = v2_address_names_payload_for_database(
        &database,
        &format!(
            "/v1/addresses/{V2_ADDRESS}/names?relation=resolves_to&authority=ens_v1&page_size=1"
        ),
    )
    .await?;
    assert_eq!(
        names(filtered["data"].as_array().expect("filtered names")),
        vec!["gamma.eth"]
    );
    assert_eq!(filtered["data"][0]["authority"], json!("ens_v1"));
    assert_eq!(filtered["page"]["has_more"], json!(false));

    database.cleanup().await
}

#[tokio::test]
async fn v2_get_address_names_resolves_to_role_summary_includes_restrictions() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_address_names_fixture(&database).await?;
    wrap_address_name(
        &database,
        "beta.eth",
        0xb300,
        Some(("emancipated", 65_536, 1_900_000_000)),
    )
    .await?;
    seed_v2_resolves_to_records(&database).await?;

    let owned = v2_address_names_payload_for_database(
        &database,
        &format!("/v1/addresses/{V2_ADDRESS}/names?q=beta&include=role_summary"),
    )
    .await?;
    let resolved = v2_address_names_payload_for_database(
        &database,
        &format!(
            "/v1/addresses/{V2_ADDRESS}/names?relation=resolves_to&coin_type={V2_RESOLVES_TO_OTHER_EVM_COIN}&q=beta&include=role_summary"
        ),
    )
    .await?;
    assert!(owned["data"][0]["restrictions"].is_object());
    assert_eq!(
        resolved["data"][0]["restrictions"],
        owned["data"][0]["restrictions"]
    );

    database.cleanup().await
}

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
    assert_eq!(rows[0]["owner"], json!(V2_PERMISSION_SUBJECT));
    assert_eq!(rows[0]["registration_status"], json!("active"));
    assert_eq!(rows[0]["expires_at"], json!("2027-01-02T00:00:00Z"));
    assert_eq!(payload["page"]["total_count"], Value::Null);
    assert_eq!(payload["page"]["has_more"], json!(false));
    assert_eq!(payload["meta"]["as_of"]["1"]["block_number"], json!(105));
    assert_no_banned_v1_spellings(&payload);

    // The authority relations are untouched: `any` stays the authority relations and no
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
async fn v2_get_address_names_resolves_to_rejects_mixed_relations_and_stray_coin_type() -> Result<()>
{
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
    assert_eq!(
        names(first["data"].as_array().expect("first page")),
        vec!["alpha.eth"]
    );
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
    assert_eq!(
        names(second["data"].as_array().expect("second page")),
        vec!["gamma.eth"]
    );
    assert_eq!(second["page"]["has_more"], json!(false));
    assert_eq!(second["page"]["cursor"], json!(cursor));

    for uri in [
        format!(
            "/v1/addresses/{V2_ADDRESS}/names?relation=resolves_to&coin_type={V2_RESOLVES_TO_OTHER_EVM_COIN}&page_size=1&cursor={cursor}"
        ),
        format!("/v1/addresses/{V2_ADDRESS}/names?relation=owner&page_size=1&cursor={cursor}"),
        format!("/v1/addresses/{V2_ADDRESS}/names?page_size=1&cursor={cursor}"),
        format!(
            "/v1/addresses/{V2_ADDRESS}/names?relation=resolves_to&authority=ens_v1&page_size=1&cursor={cursor}"
        ),
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

    assert_eq!(
        payload["data"][1]["input"]["coin_type"],
        json!(2_147_483_658_u64)
    );
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
    assert!(
        any_records
            .iter()
            .all(|record| record.get("resolution").is_none())
    );
    // The reverse fixture gives the address alice.eth's lease; another account controls it.
    assert_eq!(any_records[0]["relations"], json!(["owner", "registrant"]));
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
    assert_eq!(
        feed["data"][0]["records"][0]["relations"],
        json!(["resolves_to"])
    );
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
    write_lookup_resolves_to_records(
        &database,
        "bob.eth",
        address,
        &[family_fixture_record_write("addr:60", Some(json!(address)))],
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
    let second_records = second["data"][0]["records"]
        .as_array()
        .expect("second page");
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

const V2_RESOLVES_TO_RESOLVER: &str = "0x0000000000000000000000000000000000000aaa";

/// Records for the `seed_v2_address_names_fixture` names on one resolver: alpha and gamma resolve
/// to the address for coin 60; beta for another EVM coin type only; shared-one through the
/// ENSIP-19 default EVM address, whose cleared exact addr:60 shadows coin 60.
async fn seed_v2_resolves_to_records(database: &TestDatabase) -> Result<()> {
    write_address_name_records(
        database,
        &[
            ("alpha.eth", vec![("addr:60".into(), V2_ADDRESS.into())]),
            ("gamma.eth", vec![("addr:60".into(), V2_ADDRESS.into())]),
            (
                "beta.eth",
                vec![("addr:2147483658".into(), V2_ADDRESS.into())],
            ),
            (
                "shared-one.eth",
                vec![
                    ("addr:2147483648".into(), V2_ADDRESS.into()),
                    (
                        "addr:60".into(),
                        "0x0000000000000000000000000000000000000000".into(),
                    ),
                ],
            ),
        ],
    )
    .await
}

/// Write records for fixture names on `V2_RESOLVES_TO_RESOLVER` at the head block and publish
/// them. A name not yet pointing at that resolver gets the registry pointer from its current
/// binding, which gives its registry owner resolver control.
async fn write_address_name_records(
    database: &TestDatabase,
    names: &[(&str, Vec<(String, String)>)],
) -> Result<()> {
    let specs = v2_address_name_specs();
    let (block, hash) = address_fixture_head(database).await?;
    let mut events = Vec::new();
    for (name, writes) in names {
        let logical = bigname_storage::logical_name_id_for_name("ens", name);
        let resource: Uuid = sqlx::query_scalar(
            "SELECT resource_id FROM surface_bindings WHERE logical_name_id = $1 AND active_to IS NULL",
        )
        .bind(&logical)
        .fetch_one(&database.pool)
        .await?;
        let pointed: bool = sqlx::query_scalar(
            "SELECT EXISTS (SELECT 1 FROM normalized_events WHERE event_kind = 'ResolverChanged'
             AND logical_name_id = $1 AND resource_id = $2 AND after_state ->> 'resolver' = $3)",
        )
        .bind(&logical)
        .bind(resource)
        .bind(V2_RESOLVES_TO_RESOLVER)
        .fetch_one(&database.pool)
        .await?;
        if !pointed {
            let node = bigname_lookup::ens_namehash_hex(name)?;
            events.push(address_fixture_event(
                &format!("resolves-to-pointer-{name}-{resource}"),
                Some(&logical),
                Some(resource),
                "ResolverChanged",
                "ens_v1_registry_l1",
                block,
                &hash,
                NEXT_TEST_ID.fetch_add(1, Ordering::Relaxed) as i64 + 100,
                json!({"source_event":"NewResolver", "node":node, "resolver":V2_RESOLVES_TO_RESOLVER}),
            ));
            if let Some(spec) = specs.iter().find(|spec| spec.name == *name) {
                let mut grant = address_owner_grant(
                    spec,
                    json!({"kind":"resolver", "chain_id":"ethereum-mainnet",
                        "resolver_address":V2_RESOLVES_TO_RESOLVER}),
                    "resolver_control",
                    block,
                    &hash,
                )?;
                grant.resource_id = Some(resource);
                events.push(grant);
            }
        }
        let writes = writes
            .iter()
            .map(|(key, value)| family_fixture_record_write(key, Some(json!(value))))
            .collect::<Vec<_>>();
        insert_family_fixture_record_writes(
            &database.pool,
            "ens",
            "ethereum-mainnet",
            name,
            V2_RESOLVES_TO_RESOLVER,
            block,
            &hash,
            &writes,
        )
        .await?;
    }
    bigname_storage::insert_normalized_event_fixtures(&database.pool, &events).await?;
    declare_resolves_to_default_address(database).await?;
    rebuild_address_fixture(database).await
}

/// Declare the ENSIP-19 default-address read on `V2_RESOLVES_TO_RESOLVER`, as the mainnet
/// ENSv1 resolver manifest does for the public resolvers.
async fn declare_resolves_to_default_address(database: &TestDatabase) -> Result<()> {
    let (manifest, mut payload): (i64, Value) = sqlx::query_as(
        "SELECT manifest_id, manifest_payload FROM manifest_versions
         WHERE source_family = 'ens_v1_resolver_l1' AND chain_id = 'ethereum-mainnet'
           AND rollout_status = 'active'
           AND manifest_payload -> 'contracts' @> jsonb_build_array(jsonb_build_object('address', $1::text))",
    )
    .bind(V2_RESOLVES_TO_RESOLVER)
    .fetch_one(&database.pool)
    .await?;
    let contract = payload["contracts"]
        .as_array_mut()
        .and_then(|contracts| {
            contracts
                .iter_mut()
                .find(|contract| contract["address"] == V2_RESOLVES_TO_RESOLVER)
        })
        .context("the fixture resolver must be declared")?;
    if contract["read_features"] == json!(["ensip19_default_address"]) {
        return Ok(());
    }
    contract["read_features"] = json!(["ensip19_default_address"]);
    sqlx::query("UPDATE manifest_versions SET manifest_payload = $2 WHERE manifest_id = $1")
        .bind(manifest)
        .bind(&payload)
        .execute(&database.pool)
        .await?;
    seed_fixture_manifest_update(
        &database.pool,
        manifest,
        "ethereum-mainnet",
        "ens",
        "ens_v1_resolver_l1",
        &payload,
    )
    .await
}

/// Record writes for the `seed_v2_lookup_reverse_fixture` names, whose resolver is the address
/// itself, published at the Ethereum head.
async fn write_lookup_resolves_to_records(
    database: &TestDatabase,
    name: &str,
    resolver: &str,
    writes: &[Value],
) -> Result<()> {
    let (block, hash): (i64, String) = sqlx::query_as(
        "SELECT latest_block_number, latest_block_hash FROM chain_heads WHERE chain_id = 'ethereum-mainnet'",
    )
    .fetch_one(&database.pool)
    .await?;
    insert_family_fixture_record_writes(
        &database.pool,
        "ens",
        "ethereum-mainnet",
        name,
        resolver,
        block,
        &hash,
        writes,
    )
    .await?;
    rebuild_fixture_families(&database.pool, "ethereum-mainnet", block, &hash).await
}

/// The lookup fixture's names both resolve to the address on coin 60. Keep that for alice and
/// move bob to another EVM coin type.
async fn seed_v2_lookup_resolves_to_records(database: &TestDatabase, address: &str) -> Result<()> {
    write_lookup_resolves_to_records(
        database,
        "bob.eth",
        address,
        &[
            family_fixture_record_write("addr:60", Some(json!("0x"))),
            family_fixture_record_write(
                &format!("addr:{V2_RESOLVES_TO_OTHER_EVM_COIN}"),
                Some(json!(address)),
            ),
        ],
    )
    .await
}

/// A root-registry TLD with a resolver pointer and no registration binding, whose addr:60 is
/// `address` on the resolver at `address`.
async fn seed_resolves_to_root_tld(
    database: &TestDatabase,
    name: &str,
    address: &str,
) -> Result<()> {
    let (block, hash): (i64, String) = sqlx::query_as(
        "SELECT latest_block_number, latest_block_hash FROM chain_heads WHERE chain_id = 'ethereum-mainnet'",
    )
    .fetch_one(&database.pool)
    .await?;
    let (resource, binding) = (Uuid::from_u128(0x7d0100), Uuid::from_u128(0x7d0102));
    let logical = seed_family_identity_inputs(
        &database.pool,
        "ens",
        name,
        "ethereum-mainnet",
        block,
        &hash,
        resource,
        Uuid::from_u128(0x7d0101),
        binding,
        "ens_v2",
    )
    .await?;
    sqlx::query("DELETE FROM surface_bindings WHERE surface_binding_id = $1")
        .bind(binding)
        .execute(&database.pool)
        .await?;
    let pointer = address_fixture_event(
        &format!("resolves-to-root-pointer-{name}"),
        Some(&logical),
        Some(resource),
        "ResolverChanged",
        "ens_v2_root_l1",
        block,
        &hash,
        NEXT_TEST_ID.fetch_add(1, Ordering::Relaxed) as i64 + 100,
        json!({"node":bigname_lookup::ens_namehash_hex(name)?, "resolver":address}),
    );
    bigname_storage::insert_normalized_event_fixtures(&database.pool, &[pointer]).await?;
    write_lookup_resolves_to_records(
        database,
        name,
        address,
        &[family_fixture_record_write("addr:60", Some(json!(address)))],
    )
    .await
}

/// A registry-only node whose owner was set to zero: no token lineage and no binding, served
/// through its retained resolver pointer, whose addr:60 is `V2_ADDRESS`.
async fn seed_resolves_to_ownerless_name(
    database: &TestDatabase,
    name: &str,
    seed: u128,
) -> Result<()> {
    let (block, hash) = address_fixture_head(database).await?;
    let (resource, binding) = (Uuid::from_u128(seed), Uuid::from_u128(seed + 2));
    let logical = seed_family_identity_inputs(
        &database.pool,
        "ens",
        name,
        "ethereum-mainnet",
        block,
        &hash,
        resource,
        Uuid::from_u128(seed + 1),
        binding,
        "ens_v1",
    )
    .await?;
    sqlx::query("DELETE FROM surface_bindings WHERE surface_binding_id = $1")
        .bind(binding)
        .execute(&database.pool)
        .await?;
    sqlx::query("UPDATE resources SET token_lineage_id = NULL WHERE resource_id = $1")
        .bind(resource)
        .execute(&database.pool)
        .await?;
    let node = bigname_lookup::ens_namehash_hex(name)?;
    let zero = "0x0000000000000000000000000000000000000000";
    let ordinal = NEXT_TEST_ID.fetch_add(1, Ordering::Relaxed) as i64 + 100;
    let events = [
        address_fixture_event(
            &format!("resolves-to-ownerless-owner-{name}"),
            Some(&logical),
            Some(resource),
            "AuthorityTransferred",
            "ens_v1_registry_l1",
            block,
            &hash,
            ordinal,
            json!({"node":node, "source_event":"Transfer", "owner":zero, "owner_getter":zero}),
        ),
        address_fixture_event(
            &format!("resolves-to-ownerless-pointer-{name}"),
            Some(&logical),
            Some(resource),
            "ResolverChanged",
            "ens_v1_registry_l1",
            block,
            &hash,
            ordinal + 1,
            json!({"node":node, "resolver":V2_RESOLVES_TO_RESOLVER}),
        ),
    ];
    bigname_storage::insert_normalized_event_fixtures(&database.pool, &events).await?;
    insert_family_fixture_record_writes(
        &database.pool,
        "ens",
        "ethereum-mainnet",
        name,
        V2_RESOLVES_TO_RESOLVER,
        block,
        &hash,
        &[family_fixture_record_write(
            "addr:60",
            Some(json!(V2_ADDRESS)),
        )],
    )
    .await?;
    rebuild_address_fixture(database).await
}
