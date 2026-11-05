// `meta.completeness` on `GET /v1/names/{name}/records` and
// `GET /v1/resolvers/{chain_id}/{address}/records` (docs/api-v1-routes.md): `full` when an
// inventory an admitted resolver backs stands behind an indexed answer, else `unsupported` with
// the reason. A verified answer and `POST /v1/lookup` carry none of it. Every read here passes
// the OpenAPI response validation of the test router.

const COMPLETENESS_RESOLVER: &str = "0x0000000000000000000000000000000000000abc";

fn resolver_records_uri(query: &str) -> String {
    format!("/v1/resolvers/1/{COMPLETENESS_RESOLVER}/records?name=alice.eth&{query}")
}

fn assert_indexed_full(uri: &str, payload: &Value) {
    let meta = &payload["meta"];
    assert_eq!(meta["source"], json!("indexed"), "{uri}: {payload:#}");
    assert_eq!(meta["completeness"], json!("full"), "{uri}: {payload:#}");
    assert!(meta.get("unsupported_fields").is_none(), "{uri}: {payload:#}");
    assert!(meta.get("unsupported_reason").is_none(), "{uri}: {payload:#}");
}

fn assert_indexed_unsupported(uri: &str, payload: &Value, reason: &str) {
    let meta = &payload["meta"];
    assert_eq!(meta["source"], json!("indexed"), "{uri}: {payload:#}");
    assert_eq!(meta["completeness"], json!("unsupported"), "{uri}: {payload:#}");
    assert_eq!(meta["unsupported_fields"], json!(["records"]), "{uri}: {payload:#}");
    assert_eq!(meta["unsupported_reason"], json!(reason), "{uri}: {payload:#}");
}

fn assert_verified_carries_no_completeness(uri: &str, payload: &Value) {
    let meta = &payload["meta"];
    assert_eq!(meta["source"], json!("verified"), "{uri}: {payload:#}");
    for field in ["completeness", "unsupported_fields", "unsupported_reason"] {
        assert!(meta.get(field).is_none(), "{uri}: {field}: {payload:#}");
    }
}

/// An admitted ENSv1 resolver. A key's own `not_found` or `unsupported` answer does
/// not lower the field, which speaks for the inventory. Without a `source`, or with `auto`
/// answered from the index, the read is indexed.
#[tokio::test]
async fn v2_records_of_an_admitted_ens_v1_resolver_are_full() -> Result<()> {
    let database = TestDatabase::new_with_schemas(false, true).await?;
    seed_alice_name_inputs_with_writes(&database, &[
        family_fixture_record_write("addr:60", Some(json!("0x0000000000000000000000000000000000000def"))),
        family_fixture_record_write("avatar", None),
    ]).await?;
    let state = database.app_state();
    for query in [
        "keys=addr:60",
        "keys=addr:60&source=indexed",
        "keys=text:unset",
        "keys=text:avatar",
        "keys=addr:60&include=inventory",
        "source=indexed",
        "values=false",
        "keys=addr:60&source=auto",
        "source=auto",
    ] {
        let uri = format!("/v1/names/Alice.eth/records?{query}");
        let payload = records_route_json(&state, &uri).await?;
        assert_indexed_full(&uri, &payload);
    }
    let unset = "/v1/names/Alice.eth/records?keys=text:unset,text:avatar";
    let payload = records_route_json(&state, unset).await?;
    assert_eq!(payload["data"]["records"]["text:unset"]["status"], json!("not_found"));
    assert_eq!(payload["data"]["records"]["text:avatar"]["status"], json!("unsupported"));

    let verified = "/v1/names/Alice.eth/records?keys=addr:60&source=verified";
    let payload = records_route_json(&state, verified).await?;
    assert_verified_carries_no_completeness(verified, &payload);
    database.cleanup().await
}

/// An admitted ENSv2 record-ID resolver, read over the inventory's default key set.
#[tokio::test]
async fn v2_records_of_an_admitted_record_id_resolver_are_full() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_record_id_resolver_inputs(&database, &[
        family_fixture_record_write("text:url", Some(json!("https://example.test"))),
    ], false).await?;
    let state = database.app_state();
    let uri = "/v1/names/alice.eth/records";
    let payload = records_route_json(&state, uri).await?;
    assert_eq!(payload["data"]["records"]["text:url"]["status"], json!("ok"), "{payload:#}");
    assert_indexed_full(uri, &payload);
    database.cleanup().await
}

/// An on-chain resolver whose implementation is not admitted. Each key keeps the
/// row's own reason. Keyed `auto` answers those keys through the verified read.
#[tokio::test]
async fn v2_records_of_a_non_admitted_resolver_are_not_authoritative() -> Result<()> {
    let database = TestDatabase::new_with_schemas(false, true).await?;
    seed_unknown_resolver_inputs(&database, &unknown_resolver_record_writes()).await?;
    let state = database.app_state();
    for query in ["keys=addr:60", "source=indexed", "source=auto", "values=false"] {
        let uri = format!("/v1/names/Alice.eth/records?{query}");
        let payload = records_route_json(&state, &uri).await?;
        assert_indexed_unsupported(&uri, &payload, "inventory_not_authoritative");
        for answer in payload["data"]["records"].as_object().unwrap().values() {
            assert_eq!(
                answer,
                &json!({"status": "unsupported", "unsupported_reason": "resolver_implementation_unknown"}),
                "{uri}"
            );
        }
    }
    for query in ["keys=addr:60&source=auto", "keys=addr:60&source=verified"] {
        let uri = format!("/v1/names/Alice.eth/records?{query}");
        let payload = records_route_json(&state, &uri).await?;
        assert_verified_carries_no_completeness(&uri, &payload);
    }
    database.cleanup().await
}

/// A name with no resolver of its own, whose ENSv1 registry pointer is zero.
#[tokio::test]
async fn v2_records_of_a_name_with_no_resolver_of_its_own_are_not_available() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_alice_name_inputs_with_writes(&database, &[]).await?;
    append_alice_name_input(&database, "ResolverChanged", "ens_v1_registry_l1", json!({
        "node": bigname_lookup::ens_namehash_hex("alice.eth")?,
        "resolver": "0x0000000000000000000000000000000000000000"
    })).await?;
    rebuild_fixture_families(&database.pool, "ethereum-mainnet", 21_000_003, "0xbinding").await?;
    let state = database.app_state();
    for query in ["keys=addr:60", "source=indexed", "values=false"] {
        let uri = format!("/v1/names/Alice.eth/records?{query}");
        let payload = records_route_json(&state, &uri).await?;
        assert_indexed_unsupported(&uri, &payload, "inventory_not_available");
    }
    let uri = "/v1/names/Alice.eth/records?keys=addr:60";
    let payload = records_route_json(&state, uri).await?;
    assert_eq!(
        payload["data"]["records"]["addr:60"],
        json!({"status": "unsupported", "unsupported_reason": "inventory_not_available"})
    );
    database.cleanup().await
}

/// A released lease, a reservation, and an authority with no binding, which enters
/// the name-level short circuit with `inventory_not_available`.
#[tokio::test]
async fn v2_records_a_name_withholds_are_not_available() -> Result<()> {
    for (label, input) in [
        ("released", AliceInputState::Released),
        ("reservation", AliceInputState::Reserved),
        ("unbound authority", AliceInputState::Unbound),
    ] {
        let database = TestDatabase::new_migrated().await?;
        seed_alice_state_inputs(&database, input).await?;
        let state = database.app_state();
        for query in ["keys=addr:60", "values=false"] {
            let uri = format!("/v1/names/Alice.eth/records?{query}");
            let payload = records_route_json(&state, &uri).await?;
            assert_indexed_unsupported(&format!("{label} {uri}"), &payload, "inventory_not_available");
        }
        let keyed = "/v1/names/Alice.eth/records?keys=addr:60";
        let payload = records_route_json(&state, keyed).await?;
        assert_eq!(
            payload["data"]["records"]["addr:60"],
            json!({"status": "unsupported", "unsupported_reason": "inventory_not_available"}),
            "{label}"
        );
        database.cleanup().await?;
    }
    Ok(())
}

/// A name with no live ENSv2 entry keeps answering its keys `not_found`, and no inventory
/// stands behind that answer.
#[tokio::test]
async fn v2_records_of_an_unresolvable_name_are_not_available() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_alice_name_inputs(&database).await?;
    admit_ens_v2_root_registry(&database, "ethereum-mainnet").await?;
    rebuild_fixture_families(&database.pool, "ethereum-mainnet", 21_000_003, "0xbinding").await?;
    let state = database.app_state();
    let uri = "/v1/names/Alice.eth/records?keys=addr:60";
    let payload = records_route_json(&state, uri).await?;
    assert_eq!(payload["data"]["records"]["addr:60"], json!({"status": "not_found"}), "{payload:#}");
    assert_indexed_unsupported(uri, &payload, "inventory_not_available");
    database.cleanup().await
}

/// An ENSv2 TLD served through its root-registry resolver pointer stays out of the
/// name-level short circuit, so the pointer's inventory decides.
#[tokio::test]
async fn v2_records_of_a_root_registry_pointer_follow_the_inventory() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_unbound_name_inputs(&database, "eth", true).await?;
    let state = database.app_state();
    let uri = "/v1/names/eth/records?keys=addr:60";
    let payload = records_route_json(&state, uri).await?;
    assert_eq!(payload["data"]["records"]["addr:60"]["status"], json!("ok"), "{payload:#}");
    assert_indexed_full(uri, &payload);
    database.cleanup().await
}

/// The resolver records route reports the inventory it read, whatever the name
/// serves. It has no inventory-less case: a node with no writes at an admitted resolver is an
/// authoritative empty inventory.
#[tokio::test]
async fn v2_resolver_records_report_the_resolver_inventory() -> Result<()> {
    let database = TestDatabase::new_with_schemas(false, true).await?;
    seed_alice_name_inputs(&database).await?;
    let state = database.app_state();
    for query in ["keys=addr:60", "source=indexed", "values=false"] {
        let uri = resolver_records_uri(query);
        let payload = records_route_json(&state, &uri).await?;
        assert_indexed_full(&uri, &payload);
    }
    let uri = format!("/v1/resolvers/1/{COMPLETENESS_RESOLVER}/records?name=nobody.eth");
    assert_indexed_full(&uri, &records_route_json(&state, &uri).await?);
    database.cleanup().await?;

    let database = TestDatabase::new_with_schemas(false, true).await?;
    seed_unknown_resolver_inputs(&database, &unknown_resolver_record_writes()).await?;
    let state = database.app_state();
    for query in ["keys=addr:60", "source=indexed", "values=false"] {
        let uri = resolver_records_uri(query);
        let payload = records_route_json(&state, &uri).await?;
        assert_indexed_unsupported(&uri, &payload, "inventory_not_authoritative");
    }
    let uri = format!("/v1/resolvers/1/{COMPLETENESS_RESOLVER}/records?name=nobody.eth");
    let payload = records_route_json(&state, &uri).await?;
    assert_indexed_unsupported(&uri, &payload, "inventory_not_authoritative");
    database.cleanup().await
}

/// Lookup and the diagnostics route: neither gains the field. `POST /v1/lookup` reports its
/// request scope in `meta.as_of_completeness`, and each input carries its own record answers.
#[tokio::test]
async fn v2_lookup_and_the_records_diagnostic_carry_no_inventory_completeness() -> Result<()> {
    let database = TestDatabase::new_with_schemas(false, true).await?;
    seed_unknown_resolver_inputs(&database, &unknown_resolver_record_writes()).await?;
    let lookup = v2_lookup_json(
        &database,
        json!({"profile": "detail", "inputs": [{"name": "alice.eth"}]}),
    )
    .await?;
    let state = database.app_state();
    let diagnostic = records_route_get(&state, "/v1/diagnostics/names/Alice.eth/records").await?;
    assert_eq!(diagnostic.0, StatusCode::OK, "{}", diagnostic.1);
    let diagnostic: Value = serde_json::from_str(&diagnostic.1)?;
    for (label, payload) in [("lookup", &lookup), ("diagnostic", &diagnostic)] {
        assert!(payload["meta"].get("completeness").is_none(), "{label}: {payload:#}");
        assert!(payload["meta"].get("unsupported_reason").is_none(), "{label}: {payload:#}");
    }
    database.cleanup().await
}
