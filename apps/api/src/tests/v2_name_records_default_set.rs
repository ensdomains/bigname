// `GET /v1/names/{name}/records` answers per-key `records` as its only value shape. Without
// `keys` it answers the inventory-derived default key set (docs/api-v1-routes.md
// § `GET /v1/names/{name}/records`).

const RECORDS_ROUTE_REMOVED_FIELDS: [&str; 3] = ["addresses", "text_records", "content_hash"];
const RECORDS_ROUTE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(20);

async fn records_route_get(state: &AppState, uri: &str) -> Result<(StatusCode, String)> {
    let response = tokio::time::timeout(
        RECORDS_ROUTE_TIMEOUT,
        app_router(state.clone()).oneshot(
            Request::builder()
                .uri(uri)
                .body(Body::empty())
                .expect("request must build"),
        ),
    )
    .await
    .with_context(|| format!("records request did not finish in time: {uri}"))?
    .with_context(|| format!("records request failed: {uri}"))?;
    let status = response.status();
    let bytes = to_bytes(response.into_body(), usize::MAX)
        .await
        .context("failed to read records response body")?;
    Ok((status, String::from_utf8(bytes.to_vec())?))
}

async fn records_route_json(state: &AppState, uri: &str) -> Result<Value> {
    let (status, body) = records_route_get(state, uri).await?;
    let payload: Value = serde_json::from_str(&body)?;
    assert_eq!(status, StatusCode::OK, "{uri}: {payload:#}");
    assert_records_only_shape(uri, &payload);
    Ok(payload)
}

fn assert_records_only_shape(uri: &str, payload: &Value) {
    let data = payload["data"].as_object().expect("data must be an object");
    for field in RECORDS_ROUTE_REMOVED_FIELDS {
        assert!(
            data.get(field).is_none(),
            "{uri} still serves {field}: {payload}"
        );
    }
    assert!(
        data.get("records").is_some_and(Value::is_object),
        "{uri} must always serve a records object: {payload}"
    );
    for key in data.keys() {
        assert!(
            matches!(
                key.as_str(),
                "namespace" | "resolver" | "records" | "inventory"
            ),
            "{uri} serves unexpected data field {key}: {payload}"
        );
    }
}

fn record_keys(payload: &Value) -> Vec<String> {
    payload["data"]["records"]
        .as_object()
        .expect("records must be an object")
        .keys()
        .cloned()
        .collect()
}

// Stored inventory rows keep selectors unique and sorted by record key, and carry one entry per
// cacheable selector (crates/storage/src/record_inventory/validation.rs); fixtures follow that.
fn text_selector(key: &str, cacheable: bool) -> Value {
    json!({
        "record_key": format!("text:{key}"),
        "record_family": "text",
        "selector_key": key,
        "cacheable": cacheable
    })
}

fn addr_selector(coin_type: &str, cacheable: bool) -> Value {
    json!({
        "record_key": format!("addr:{coin_type}"),
        "record_family": "addr",
        "selector_key": coin_type,
        "cacheable": cacheable
    })
}







#[tokio::test]
async fn v2_get_name_records_without_keys_answers_the_inventory_default_set() -> Result<()> {
    let database = TestDatabase::new_with_schemas(false, true).await?;
    seed_alice_name_inputs_with_writes(&database,&[
        family_fixture_record_write("addr:60",Some(json!("0x00000000000000000000000000000000000ABCDE"))),
        family_fixture_record_write("avatar",None),
        family_fixture_record_write("contenthash",Some(json!("0x"))),
        family_fixture_record_write("text:description",Some(json!("Alice profile"))),
        family_fixture_record_write("text:url",Some(json!(""))),
        json!({"source_event":"PubkeyChanged","record_key":"pubkey","record_family":"pubkey","selector_key":null,"x":"0x00","y":"0x00"})
    ]).await?;
    let state = database.app_state();

    let expected_records = json!({
        "addr:60": {"status": "ok", "value": "0x00000000000000000000000000000000000abcde"},
        "text:avatar": {"status": "unsupported", "unsupported_reason": "value_not_retained"},
        "contenthash": {"status": "not_found"},
        "text:description": {"status": "ok", "value": "Alice profile"},
        "text:url": {"status": "ok", "value":""}
    });
    for uri in [
        "/v1/names/Alice.eth/records?include=inventory",
        "/v1/names/Alice.eth/records?source=indexed&keys=&include=inventory",
        "/v1/names/Alice.eth/records?source=auto&include=inventory",
    ] {
        let payload = records_route_json(&state, uri).await?;
        assert_eq!(payload["meta"]["source"], json!("indexed"), "{uri}");
        assert_eq!(payload["data"]["records"], expected_records, "{uri}");
        assert_eq!(
            payload["data"]["inventory"],
            json!({
                "known_keys": ["addr:60", "contenthash", "text:description", "text:url"],
                "unset_keys": [],
                "unsupported_keys": ["text:avatar"],
                "abi_content_types": []
            }),
            "{uri}"
        );
    }

    // Keyed reads keep their established per-key answers.
    let keyed = records_route_json(
        &state,
        "/v1/names/Alice.eth/records?keys=addr:60,text:email",
    )
    .await?;
    assert_eq!(
        keyed["data"]["records"],
        json!({
            "addr:60": {"status": "ok", "value": "0x00000000000000000000000000000000000abcde"},
            "text:email": {"status": "not_found"}
        })
    );

    database.cleanup().await
}

#[test]
fn default_requested_records_unions_all_three_sections_once() {
    let mut inventory = record_inventory_current_row("ens:alice.eth", Uuid::from_u128(0x2200));
    inventory.selectors = json!([
        addr_selector("60", true),
        text_selector("description", false)
    ]);
    inventory.entries = json!([
        {"record_key": "addr:60", "record_family": "addr", "selector_key": "60", "status": "success", "value": "0x01"},
        {"record_key": "avatar", "record_family": "avatar", "selector_key": null, "status": "success", "value": "a"}
    ]);
    inventory.explicit_gaps = json!([
        {"record_key": "contenthash", "record_family": "contenthash", "selector_key": null, "gap_reason": "not_observed_on_current_resolver"},
        {"record_key": "text:description", "record_family": "text", "selector_key": "description", "gap_reason": "not_observed_on_current_resolver"},
        {"record_key": "abi:1", "record_family": "abi", "selector_key": "1", "gap_reason": "not_observed_on_current_resolver"}
    ]);

    let keys = crate::v2::default_requested_records(Some(&inventory))
        .into_iter()
        .map(|record| record.record_key)
        .collect::<Vec<_>>();
    assert_eq!(
        keys,
        vec!["addr:60", "avatar", "contenthash", "text:description"],
        "product keys from selectors, entries, and explicit gaps, deduplicated in byte order"
    );
    assert!(crate::v2::default_requested_records(None).is_empty());
}

#[tokio::test]
async fn v2_get_name_records_default_set_does_not_enumerate_derivable_addresses() -> Result<()> {
    let database = TestDatabase::new_with_schemas(false, true).await?;
    seed_alice_name_inputs_with_writes(
        &database,
        &[family_fixture_record_write(
            "addr:2147483648",
            Some(json!("0x0000000000000000000000000000000000000DeF")),
        )],
    )
    .await?;
    enable_alice_ensip19_inputs(&database).await?;
    let state = database.app_state();

    // The default set enumerates inventory keys only; `addr:60` is derivable but not listed.
    let unkeyed = records_route_json(&state, "/v1/names/Alice.eth/records").await?;
    assert_eq!(
        unkeyed["data"]["records"],
        json!({
            "addr:2147483648": {
                "status": "ok",
                "value": "0x0000000000000000000000000000000000000def"
            }
        })
    );

    // A keyed read still derives it.
    let keyed = records_route_json(&state, "/v1/names/Alice.eth/records?keys=addr:60").await?;
    assert_eq!(
        keyed["data"]["records"]["addr:60"],
        json!({
            "status": "ok",
            "value": "0x0000000000000000000000000000000000000def",
            "meta": {
                "basis": "derived",
                "rule": "ensip19_default_address",
                "source_record_key": "addr:2147483648"
            }
        })
    );

    database.cleanup().await
}

#[tokio::test]
async fn v2_get_name_records_default_set_derives_an_enumerated_address_without_exact_value()
-> Result<()> {
    for (default_value, expected_addr60) in [
        (
            "0x0000000000000000000000000000000000000DeF",
            json!({
                "status": "ok",
                "value": "0x0000000000000000000000000000000000000def",
                "meta": {
                    "basis": "derived",
                    "rule": "ensip19_default_address",
                    "source_record_key": "addr:2147483648"
                }
            }),
        ),
        (
            // Coin 60 reads a zero default through the legacy getter as absence.
            "0x0000000000000000000000000000000000000000",
            json!({
                "status": "not_found",
                "meta": {
                    "basis": "derived",
                    "rule": "ensip19_default_address",
                    "source_record_key": "addr:2147483648"
                }
            }),
        ),
    ] {
        let database = TestDatabase::new_with_schemas(false, true).await?;
        seed_alice_name_inputs_with_writes(
            &database,
            &[
                family_fixture_record_write("addr:2147483648", Some(json!(default_value))),
                family_fixture_record_write("addr:60", Some(json!("0x"))),
            ],
        )
        .await?;
        enable_alice_ensip19_inputs(&database).await?;
        let state = database.app_state();

        for uri in [
            "/v1/names/Alice.eth/records",
            "/v1/names/Alice.eth/records?source=auto",
        ] {
            let (status, body) = records_route_get(&state, uri).await?;
            assert_eq!(status, StatusCode::OK, "{uri}: {body}");
            let payload: Value = serde_json::from_str(&body)?;
            assert_records_only_shape(uri, &payload);
            assert_eq!(payload["meta"]["source"], json!("indexed"), "{uri}");
            assert_eq!(
                record_keys(&payload),
                vec!["addr:2147483648", "addr:60"],
                "{uri}"
            );
            assert_eq!(
                payload["data"]["records"]["addr:60"], expected_addr60,
                "{uri}"
            );
            // Byte order of the record key is the serialization order.
            let default_at = body.find("\"addr:2147483648\"").expect("default key");
            let coin60_at = body.find("\"addr:60\"").expect("coin 60 key");
            assert!(default_at < coin60_at, "{uri}: {body}");
        }

        database.cleanup().await?;
    }
    Ok(())
}

#[tokio::test]
async fn v2_get_name_records_default_set_reports_unsupported_inventory_per_key() -> Result<()> {
    let database = TestDatabase::new_with_schemas(false, true).await?;
    seed_unknown_resolver_inputs(&database, &unknown_resolver_record_writes()).await?;
    let state = database.app_state();
    let refused = json!({
        "status": "unsupported",
        "unsupported_reason": "resolver_implementation_unknown"
    });

    // Indexed evaluation and unkeyed auto answer each default key with the row's reason.
    for uri in [
        "/v1/names/Alice.eth/records?include=inventory",
        "/v1/names/Alice.eth/records?source=auto&include=inventory",
    ] {
        let payload = records_route_json(&state, uri).await?;
        assert_eq!(payload["meta"]["source"], json!("indexed"), "{uri}");
        assert_eq!(
            payload["data"]["records"],
            json!({"addr:60": refused, "text:description": refused}),
            "{uri}"
        );
        assert_eq!(
            payload["data"]["inventory"],
            json!({
                "known_keys": [],
                "unset_keys": [],
                "unsupported_keys": ["addr:60", "text:description"],
                "abi_content_types": null,
                "abi_unsupported_reason": "inventory_not_authoritative"
            }),
            "{uri}"
        );
        assert_eq!(
            payload["data"]["resolver"]["address"],
            json!("0x0000000000000000000000000000000000000abc"),
            "{uri}"
        );
    }

    // Verified enumerates the same keys but answers them from verified execution, which this
    // fixture does not admit.
    let verified =
        records_route_json(&state, "/v1/names/Alice.eth/records?source=verified").await?;
    assert_eq!(verified["meta"]["source"], json!("verified"));
    let not_supported = json!({
        "status": "unsupported",
        "unsupported_reason": "verified_records_not_supported"
    });
    assert_eq!(
        verified["data"]["records"],
        json!({"addr:60": not_supported, "text:description": not_supported})
    );

    database.cleanup().await
}

#[tokio::test]
async fn v2_get_name_records_default_set_is_empty_without_enumerable_keys() -> Result<()> {
    for (label, mode, abi) in [
        (
            "no resolver inventory",
            0,
            json!({"abi_content_types":null,"abi_unsupported_reason":"inventory_not_available"}),
        ),
        (
            "authoritative empty inventory",
            1,
            json!({"abi_content_types":[]}),
        ),
        (
            "unknown implementation without product keys",
            2,
            json!({"abi_content_types":null,"abi_unsupported_reason":"inventory_not_authoritative"}),
        ),
    ] {
        let database = TestDatabase::new_migrated().await?;
        if mode == 2 {
            seed_unknown_resolver_inputs(&database, &[]).await?;
        } else {
            seed_alice_name_inputs_with_writes(&database, &[]).await?;
            if mode == 0 {
                append_alice_name_input(&database,"ResolverChanged","ens_v1_registry_l1",json!({"node":bigname_lookup::ens_namehash_hex("alice.eth")?,"resolver":"0x0000000000000000000000000000000000000000"})).await?;
                rebuild_fixture_families(
                    &database.pool,
                    "ethereum-mainnet",
                    21_000_003,
                    "0xbinding",
                )
                .await?;
            }
        }
        let state = database.app_state();
        for source in ["indexed", "auto", "verified"] {
            let uri = format!("/v1/names/Alice.eth/records?source={source}&include=inventory");
            let payload = records_route_json(&state, &uri).await?;
            assert_eq!(payload["data"]["records"], json!({}), "{label} {uri}");
            assert_eq!(
                payload["data"]["inventory"],
                {
                    let mut expected =
                        json!({"known_keys":[],"unset_keys":[],"unsupported_keys":[]});
                    expected
                        .as_object_mut()
                        .unwrap()
                        .extend(abi.as_object().unwrap().clone());
                    expected
                },
                "{label} {uri}"
            );
        }
        if mode == 0 {
            // Explicit keys are still answered, never replaced by `{}`.
            let keyed =
                records_route_json(&state, "/v1/names/Alice.eth/records?keys=addr:60").await?;
            assert_eq!(
                keyed["data"]["records"],
                json!({"addr:60": {"status": "unsupported", "unsupported_reason": "inventory_not_available"}}),
                "{label}"
            );
        }
        database.cleanup().await?;
    }
    Ok(())
}

async fn unavailable_rpc_state(database: &TestDatabase) -> Result<AppState> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let unavailable_rpc_url = format!("http://{}", listener.local_addr()?);
    drop(listener);
    database
        .app_state_with_lookup_chain_rpc_urls(bigname_lookup::ChainRpcUrls::from_entries(&[
            format!("ethereum-mainnet={unavailable_rpc_url}"),
        ])?)
        .await
}

#[tokio::test]
async fn v2_get_name_records_default_set_limit_applies_on_every_source() -> Result<()> {
    let database = TestDatabase::new_with_schemas(false, true).await?;
    let writes = (0..=crate::v2::MAX_PAGE_SIZE)
        .map(|index| family_fixture_record_write(&format!("text:key-{index}"), Some(json!(""))))
        .collect::<Vec<_>>();
    seed_alice_name_inputs_with_writes(&database, &writes).await?;
    // A provider call would fail the request with a transport error, and reaching verified
    // execution would block on the installed hook until the bounded request timeout.
    let state = unavailable_rpc_state(&database).await?;
    let (_hook_guard, _hook_control) =
        crate::v2::name_records_auto_fallback_test_hooks::install(&database.pool).await?;

    for source in ["indexed", "auto", "verified"] {
        for keys in ["", "&keys=", "&keys=%20%20"] {
            let uri = format!("/v1/names/Alice.eth/records?source={source}{keys}");
            let (status, body) = records_route_get(&state, &uri).await?;
            let payload: Value = serde_json::from_str(&body)?;
            assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{uri}: {payload}");
            assert_eq!(payload["error"]["code"], json!("unsupported"), "{uri}");
            assert_eq!(
                payload["error"]["message"],
                json!("inventory-derived record key sets support at most 200 record keys"),
                "{uri}"
            );
        }
    }
    // A caller narrows the read with explicit keys (the verified twin is covered by
    // `v2_verified_name_reads_reject_oversized_inventory_derived_selector_sets`).
    for source in ["indexed", "auto"] {
        let narrowed = records_route_json(
            &state,
            &format!("/v1/names/Alice.eth/records?source={source}&keys=text:key-0"),
        )
        .await?;
        assert_eq!(
            narrowed["data"]["records"],
            json!({"text:key-0": {"status": "ok", "value":""}})
        );
    }

    // More than 200 explicit keys stays a request error.
    let explicit = (0..=crate::v2::MAX_PAGE_SIZE)
        .map(|index| format!("text:key-{index}"))
        .collect::<Vec<_>>()
        .join(",");
    let (status, body) = records_route_get(
        &state,
        &format!("/v1/names/Alice.eth/records?keys={explicit}"),
    )
    .await?;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");

    database.cleanup().await
}

#[tokio::test]
async fn v2_get_name_records_default_set_of_exactly_200_product_keys_is_served() -> Result<()> {
    let database = TestDatabase::new_with_schemas(false, true).await?;
    let mut writes = (0..198)
        .map(|index| family_fixture_record_write(&format!("text:key-{index}"), Some(json!(""))))
        .collect::<Vec<_>>();
    writes.push(family_fixture_record_write(
        "addr:60",
        Some(json!("0x0000000000000000000000000000000000000def")),
    ));
    writes.push(family_fixture_record_write(
        "avatar",
        Some(json!("https://example.test/avatar.png")),
    ));
    writes.push(json!({"source_event":"PubkeyChanged","record_key":"pubkey","record_family":"pubkey","selector_key":null,"x":"0x00","y":"0x00"}));
    seed_alice_name_inputs_with_writes(&database, &writes).await?;
    let state = database.app_state();

    for source in ["indexed", "auto", "verified"] {
        for keys in ["", "&keys=", "&keys=%20"] {
            let uri = format!("/v1/names/Alice.eth/records?source={source}{keys}");
            let payload = records_route_json(&state, &uri).await?;
            assert_eq!(record_keys(&payload).len(), 200, "{uri}");
        }
    }

    database.cleanup().await
}

#[tokio::test]
async fn v2_get_name_records_source_auto_without_keys_stays_indexed_over_default_set() -> Result<()>
{
    let database = TestDatabase::new_with_schemas(false, true).await?;
    seed_alice_name_inputs(&database).await?;
    insert_family_fixture_record_writes(
        &database.pool,
        "ens",
        "ethereum-mainnet",
        "alice.eth",
        "0x0000000000000000000000000000000000000abc",
        21_000_003,
        "0xbinding",
        &[family_fixture_record_write("text:email", None)],
    )
    .await?;
    rebuild_fixture_families(&database.pool, "ethereum-mainnet", 21_000_003, "0xbinding").await?;
    let state = database.app_state();

    for keys in ["", "&keys=", "&keys=%20%20"] {
        let (hook_guard, _hook_control) =
            crate::v2::name_records_auto_fallback_test_hooks::install(&database.pool).await?;
        let uri = format!("/v1/names/Alice.eth/records?source=auto{keys}");
        // Reaching the fallback would block on the hook until the bounded request timeout.
        let payload = records_route_json(&state, &uri).await?;
        drop(hook_guard);
        assert_eq!(payload["meta"]["source"], json!("indexed"), "{uri}");
        assert_eq!(
            payload["data"]["records"]["addr:60"],
            json!({"status": "ok", "value": "0x0000000000000000000000000000000000000def"}),
            "{uri}"
        );
        assert_eq!(
            payload["data"]["records"]["text:email"],
            json!({"status": "unsupported", "unsupported_reason": "value_not_retained"}),
            "{uri}"
        );
        assert_eq!(record_keys(&payload).len(), 5, "{uri}");
    }

    // The same key requested explicitly still enters the verified fallback.
    let (_hook_guard, control) =
        crate::v2::name_records_auto_fallback_test_hooks::install(&database.pool).await?;
    let keyed_state = state.clone();
    let keyed = tokio::spawn(async move {
        records_route_json(
            &keyed_state,
            "/v1/names/Alice.eth/records?source=auto&keys=text:email",
        )
        .await
    });
    tokio::time::timeout(RECORDS_ROUTE_TIMEOUT, control.wait_until_reached())
        .await
        .context("explicit auto key must reach the verified fallback")?;
    control.resume().await;
    let keyed = keyed.await.context("keyed auto request task panicked")??;
    assert_eq!(keyed["meta"]["source"], json!("verified"));

    database.cleanup().await
}









#[tokio::test]
async fn v2_get_name_records_shape_matrix_serves_records_only() -> Result<()> {
    let fixture_keys = ["addr:60", "contenthash", "text:avatar", "text:description"];
    for (label, input) in [
        ("active", None),
        ("ownerless serving", Some(AliceInputState::Ownerless)),
        ("reservation", Some(AliceInputState::Reserved)),
        ("unbound authority", Some(AliceInputState::Unbound)),
    ] {
        let database = TestDatabase::new_migrated().await?;
        if let Some(input) = input {
            seed_alice_state_inputs(&database, input).await?;
        } else {
            seed_alice_name_inputs(&database).await?;
        }
        let state = database.app_state();

        for source in ["indexed", "auto", "verified"] {
            for keys in [None, Some("addr:60")] {
                let uri = match keys {
                    Some(keys) => format!(
                        "/v1/names/Alice.eth/records?source={source}&keys={keys}&include=inventory"
                    ),
                    None => {
                        format!("/v1/names/Alice.eth/records?source={source}&include=inventory")
                    }
                };
                let payload = records_route_json(&state, &uri).await?;
                let records = &payload["data"]["records"];
                let expected_keys = match (label, keys) {
                    (_, Some(key)) => vec![key],
                    ("reservation" | "unbound authority", None) => Vec::new(),
                    (_, None) => fixture_keys.to_vec(),
                };
                assert_eq!(record_keys(&payload), expected_keys, "{label} {uri}");
                assert_eq!(
                    payload["data"].get("inventory").is_some(),
                    !matches!(label, "reservation" | "unbound authority"),
                    "{label} {uri}: {payload}"
                );
                let expected_source = if source == "verified" {
                    "verified"
                } else {
                    "indexed"
                };
                assert_eq!(
                    payload["meta"]["source"],
                    json!(expected_source),
                    "{label} {uri}"
                );
                match (label, source) {
                    ("active" | "ownerless serving", "indexed" | "auto") => {
                        assert_eq!(records["addr:60"]["status"], json!("ok"), "{label} {uri}");
                        assert!(records["addr:60"]["value"].is_string(), "{label} {uri}");
                    }
                    ("active" | "ownerless serving", _) => assert_eq!(
                        records["addr:60"],
                        json!({"status": "unsupported", "unsupported_reason": "verified_records_not_supported"}),
                        "{label} {uri}"
                    ),
                    ("unbound authority", _) => {
                        for key in &expected_keys {
                            assert_eq!(
                                records[*key],
                                json!({"status": "unsupported", "unsupported_reason": "inventory_not_available"}),
                                "{label} {uri}"
                            );
                        }
                    }
                    ("reservation", "indexed") if keys.is_some() => assert_eq!(
                        records["addr:60"],
                        json!({"status": "unsupported", "unsupported_reason": "inventory_not_available"}),
                        "{label} {uri}"
                    ),
                    ("reservation", "verified") if keys.is_some() => assert_eq!(
                        records["addr:60"],
                        json!({"status": "unsupported", "unsupported_reason": "inventory_not_available"}),
                        "{label} {uri}"
                    ),
                    _ => {}
                }
            }
        }
        database.cleanup().await?;
    }
    Ok(())
}

#[tokio::test]
async fn v2_sepolia_default_sets_match_across_sources_at_one_snapshot() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_sepolia_indexed_inventory(&database).await?;
    let state = database.app_state();

    let indexed = records_route_json(
        &state,
        &format!("/v1/names/{V2_SEPOLIA_ONLY_SNAPSHOT_NAME}/records?include=inventory"),
    )
    .await?;
    let at = indexed["meta"]["as_of_token"]
        .as_str()
        .expect("snapshot token")
        .to_owned();
    assert_eq!(
        record_keys(&indexed),
        vec!["addr:60", "text:avatar", "text:com.twitter"]
    );
    assert_eq!(indexed["data"]["records"]["addr:60"]["status"], json!("ok"));
    assert_eq!(
        indexed["data"]["records"]["text:com.twitter"],
        json!({"status": "ok", "value": "sepolia-record"})
    );

    let verified = records_route_json(
        &state,
        &format!(
            "/v1/names/{V2_SEPOLIA_ONLY_SNAPSHOT_NAME}/records?source=verified&at={at}&include=inventory"
        ),
    )
    .await?;
    assert_eq!(verified["meta"]["source"], json!("verified"));
    assert_eq!(verified["meta"]["as_of"], indexed["meta"]["as_of"]);
    assert_eq!(record_keys(&verified), record_keys(&indexed));
    for key in record_keys(&verified) {
        assert_eq!(
            verified["data"]["records"][&key],
            json!({"status": "unsupported", "unsupported_reason": "verified_records_not_supported"}),
            "{key}"
        );
    }
    assert_eq!(verified["data"]["inventory"], indexed["data"]["inventory"]);

    database.cleanup().await
}

#[tokio::test]
async fn v2_sepolia_verified_records_readback_checks_the_inventory_snapshot() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_sepolia_only_phase_head_name(&database).await?;
    let state = database.app_state();
    let unkeyed = format!(
        "/v1/names/{V2_SEPOLIA_ONLY_SNAPSHOT_NAME}/records?source=verified&include=inventory"
    );
    let keyed =
        format!("/v1/names/{V2_SEPOLIA_ONLY_SNAPSHOT_NAME}/records?source=verified&keys=addr:60");

    // Missing inventory: no default keys; explicit keys are still answered.
    let missing = records_route_json(&state, &unkeyed).await?;
    assert_eq!(missing["data"]["records"], json!({}));
    let missing_keyed = records_route_json(&state, &keyed).await?;
    assert_eq!(
        missing_keyed["data"]["records"]["addr:60"],
        json!({"status": "unsupported", "unsupported_reason": "verified_records_not_supported"})
    );

    // A later family publication cannot serve the older selected snapshot.
    let token = missing["meta"]["as_of_token"]
        .as_str()
        .context("verified snapshot token")?;
    insert_v2_sepolia_indexed_inventory(&database).await?;
    database
        .seed_snapshot_selector_chain_positions(&json!({"ethereum":{
            "chain_id":"ethereum-sepolia","block_number":V2_SEPOLIA_ONLY_SNAPSHOT_BLOCK+1,
            "block_hash":"0xsepolia-next","timestamp":"2026-04-17T00:10:21Z"
        }}))
        .await?;
    rebuild_fixture_families(
        &database.pool,
        "ethereum-sepolia",
        V2_SEPOLIA_ONLY_SNAPSHOT_BLOCK + 1,
        "0xsepolia-next",
    )
    .await?;
    for uri in [unkeyed, keyed] {
        let (status, body) = records_route_get(&state, &format!("{uri}&at={token}")).await?;
        assert_eq!(status, StatusCode::CONFLICT, "{uri}: {body}");
    }

    database.cleanup().await
}
