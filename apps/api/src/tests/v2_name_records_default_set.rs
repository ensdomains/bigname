// `GET /v1/names/{name}/records` answers per-key `records` as its only value shape. Without
// `keys` it answers the inventory-derived default key set (docs/api-v2-routes.md
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

fn non_product_selectors() -> Vec<Value> {
    vec![
        json!({"record_key": "abi:1", "record_family": "abi", "selector_key": "1", "cacheable": false}),
        json!({"record_key": "pubkey", "record_family": "pubkey", "selector_key": null, "cacheable": false}),
    ]
}

fn sorted_selectors(mut selectors: Vec<Value>) -> Value {
    selectors.sort_by(|left, right| {
        left["record_key"]
            .as_str()
            .cmp(&right["record_key"].as_str())
    });
    Value::Array(selectors)
}

fn mixed_default_set_inventory(inventory: &mut bigname_storage::RecordInventoryCurrentRow) {
    // Cacheable selectors repeat in entries and count once; text:url is a selector without a
    // retained entry; abi:1 and pubkey are outside the product key grammar.
    let mut selectors = vec![
        addr_selector("60", true),
        json!({"record_key": "avatar", "record_family": "avatar", "selector_key": null, "cacheable": true}),
        json!({"record_key": "contenthash", "record_family": "contenthash", "selector_key": null, "cacheable": true}),
        text_selector("description", true),
        text_selector("url", false),
    ];
    selectors.extend(non_product_selectors());
    inventory.selectors = sorted_selectors(selectors);
    inventory.entries = json!([
        {
            "record_key": "addr:60",
            "record_family": "addr",
            "selector_key": "60",
            "status": "success",
            "value": {"coin_type": "60", "value": "0x00000000000000000000000000000000000ABCDE"}
        },
        {
            "record_key": "text:description",
            "record_family": "text",
            "selector_key": "description",
            "status": "success",
            "value": {"key": "description", "value": "Alice profile"}
        },
        {
            "record_key": "avatar",
            "record_family": "avatar",
            "selector_key": null,
            "status": "unsupported",
            "unsupported_reason": "resolver_family_pending"
        },
        {
            "record_key": "contenthash",
            "record_family": "contenthash",
            "selector_key": null,
            "status": "not_found"
        }
    ]);
    inventory.explicit_gaps = json!([]);
    inventory.unsupported_families = json!([]);
}

#[tokio::test]
async fn v2_get_name_records_without_keys_answers_the_inventory_default_set() -> Result<()> {
    let database = TestDatabase::new_with_schemas(false, true).await?;
    seed_v2_alice_name_records_fixture(&database, |_, _, inventory| {
        mixed_default_set_inventory(inventory)
    })
    .await?;
    let state = database.app_state();

    let expected_records = json!({
        "addr:60": {"status": "ok", "value": "0x00000000000000000000000000000000000abcde"},
        "avatar": {"status": "unsupported", "unsupported_reason": "resolver_family_pending"},
        "contenthash": {"status": "not_found"},
        "text:description": {"status": "ok", "value": "Alice profile"},
        "text:url": {"status": "not_found"}
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
                "unsupported_keys": ["avatar"],
                "abi_content_types": null,
                "abi_unsupported_reason": "abi_observations_not_supported"
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
    seed_v2_alice_name_records_fixture(&database, |_, _, inventory| {
        inventory.selectors = json!([addr_selector("2147483648", true)]);
        inventory.entries = json!([{
            "record_key": "addr:2147483648",
            "record_family": "addr",
            "selector_key": "2147483648",
            "status": "success",
            "value": "0x0000000000000000000000000000000000000DeF"
        }]);
        inventory.provenance["read_rules"] = json!([{
            "kind": "ensip19_default_address",
            "source_record_key": "addr:2147483648"
        }]);
        inventory.explicit_gaps = json!([]);
        inventory.unsupported_families = json!([]);
    })
    .await?;
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
        seed_v2_alice_name_records_fixture(&database, |_, _, inventory| {
            inventory.selectors = sorted_selectors(vec![
                addr_selector("60", false),
                addr_selector("2147483648", true),
            ]);
            inventory.entries = json!([{
                "record_key": "addr:2147483648",
                "record_family": "addr",
                "selector_key": "2147483648",
                "status": "success",
                "value": default_value
            }]);
            inventory.provenance["read_rules"] = json!([{
                "kind": "ensip19_default_address",
                "source_record_key": "addr:2147483648"
            }]);
            inventory.explicit_gaps = json!([]);
            inventory.unsupported_families = json!([]);
        })
        .await?;
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
    seed_v2_alice_name_records_fixture(&database, |_, _, inventory| {
        unsupported_resolver_inventory(inventory)
    })
    .await?;
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
    type Configure = fn(&mut bigname_storage::RecordInventoryCurrentRow);
    let authoritative_empty: Configure = |inventory| {
        inventory.selectors = json!([]);
        inventory.entries = json!([]);
        inventory.explicit_gaps = json!([]);
        inventory.unsupported_families = json!([]);
    };
    let non_authoritative_without_product_keys: Configure = |inventory| {
        unsupported_resolver_inventory(inventory);
        inventory.selectors = sorted_selectors(non_product_selectors());
        inventory.entries = json!([]);
    };
    for (label, configure, delete_inventory, abi_unsupported_reason) in [
        (
            "no inventory row",
            authoritative_empty,
            true,
            "inventory_not_available",
        ),
        (
            "authoritative empty inventory",
            authoritative_empty,
            false,
            "abi_observations_not_supported",
        ),
        (
            "non-authoritative inventory with no product keys",
            non_authoritative_without_product_keys,
            false,
            "inventory_not_authoritative",
        ),
    ] {
        let database = TestDatabase::new_with_schemas(false, true).await?;
        seed_v2_alice_name_records_fixture(&database, |_, _, inventory| configure(inventory))
            .await?;
        if delete_inventory {
            sqlx::query("DELETE FROM bigname_phase.record_inventory_current")
                .execute(&database.pool)
                .await?;
        }
        let state = database.app_state();
        for source in ["indexed", "auto", "verified"] {
            let uri = format!("/v1/names/Alice.eth/records?source={source}&include=inventory");
            let payload = records_route_json(&state, &uri).await?;
            assert_eq!(payload["data"]["records"], json!({}), "{label} {uri}");
            assert_eq!(
                payload["data"]["inventory"],
                json!({
                    "known_keys": [],
                    "unset_keys": [],
                    "unsupported_keys": [],
                    "abi_content_types": null,
                    "abi_unsupported_reason": abi_unsupported_reason
                }),
                "{label} {uri}"
            );
        }
        if delete_inventory {
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
    seed_v2_alice_name_records_fixture(&database, |_, _, inventory| {
        inventory.selectors = sorted_selectors(
            (0..=crate::v2::MAX_PAGE_SIZE)
                .map(|index| text_selector(&format!("key-{index}"), false))
                .collect(),
        );
        inventory.entries = json!([]);
        inventory.explicit_gaps = json!([]);
    })
    .await?;
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
            json!({"text:key-0": {"status": "not_found"}})
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
    seed_v2_alice_name_records_fixture(&database, |_, _, inventory| {
        // 198 text keys, addr:60 and avatar give 200 product keys. addr:60 and avatar repeat in
        // entries and must count once; non-product selectors do not count.
        let mut selectors = (0..198)
            .map(|index| text_selector(&format!("key-{index}"), false))
            .collect::<Vec<_>>();
        selectors.push(addr_selector("60", true));
        selectors.push(json!({
            "record_key": "avatar", "record_family": "avatar", "selector_key": null, "cacheable": true
        }));
        selectors.extend(non_product_selectors());
        inventory.selectors = sorted_selectors(selectors);
        inventory.entries = json!([
            {
                "record_key": "addr:60",
                "record_family": "addr",
                "selector_key": "60",
                "status": "success",
                "value": {"coin_type": "60", "value": "0x0000000000000000000000000000000000000def"}
            },
            {
                "record_key": "avatar",
                "record_family": "avatar",
                "selector_key": null,
                "status": "success",
                "value": {"value": "https://example.test/avatar.png"}
            }
        ]);
        inventory.explicit_gaps = json!([]);
        inventory.unsupported_families = json!([]);
    })
    .await?;
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
    seed_v2_alice_name_records_fixture(&database, |_, _, inventory| {
        let selectors = inventory.selectors.as_array_mut().expect("selectors array");
        selectors.push(text_selector("email", false));
        // text:email has no entry, so the family refusal makes it unsatisfiable by the index.
        // Project never lists a product family here (it records non-product families and
        // the resolver classification), so this row is synthetic; the test only needs a key
        // that explicit auto sends to the fallback and unkeyed auto does not.
        inventory.unsupported_families = json!([{
            "record_family": "text",
            "unsupported_reason": "resolver_family_pending"
        }]);
    })
    .await?;
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
            json!({"status": "unsupported", "unsupported_reason": "resolver_family_pending"}),
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

fn reserved_row(row: &mut bigname_storage::NameCurrentRow) {
    row.declared_summary["registration"] = json!({
        "status": "reserved",
        "expiry": 4_000_000_000_u64,
        "latest_event_kind": "RegistrationReserved"
    });
    row.declared_summary["control"] = json!({"status": "reserved"});
}

fn reserved_audit_inventory(inventory: &mut bigname_storage::RecordInventoryCurrentRow) {
    inventory.selectors = sorted_selectors(
        (0..=crate::v2::MAX_PAGE_SIZE)
            .map(|index| text_selector(&format!("audit-{index}"), false))
            .collect(),
    );
    inventory.entries = json!([]);
}

fn ownerless_serving_row(row: &mut bigname_storage::NameCurrentRow) {
    let serving_resource_id = row.resource_id.expect("fixture control resource");
    row.surface_binding_id = None;
    row.resource_id = None;
    row.serving_resource_id = Some(serving_resource_id);
    row.token_lineage_id = None;
    row.binding_kind = None;
    row.declared_summary["registration"] = json!({"status":"unregistered"});
    row.declared_summary["control"] = json!({"status":"unregistered"});
    let coverage = json!({
        "status":"projected",
        "exhaustiveness":"not_asserted",
        "enumeration_basis":"event_linked_registry_resolver",
        "unsupported_reason":null
    });
    row.declared_summary["coverage"] = coverage.clone();
    row.provenance["read_reachability"] = json!({
        "serving_resource_id":serving_resource_id,
        "basis":"retained_registry_resolver_pointer",
        "owner_getter_reason":"registry_self",
        "pointer_event_id":102
    });
    row.coverage = coverage;
}

fn authority_unsupported_row(row: &mut bigname_storage::NameCurrentRow) {
    row.coverage = json!({
        "status":"unsupported",
        "unsupported_reason":"conflicting_current_ens_authority"
    });
}

#[tokio::test]
async fn v2_get_name_records_shape_matrix_serves_records_only() -> Result<()> {
    type ConfigureRow = fn(&mut bigname_storage::NameCurrentRow);
    type ConfigureInventory = fn(&mut bigname_storage::RecordInventoryCurrentRow);
    let active: ConfigureRow = |_| {};
    let keep_inventory: ConfigureInventory = |_| {};
    let fixture_keys = ["addr:60", "avatar", "contenthash", "text:description"];
    for (label, configure_row, configure_inventory) in [
        ("active", active, keep_inventory),
        (
            "ownerless serving",
            ownerless_serving_row as ConfigureRow,
            keep_inventory,
        ),
        (
            "reservation",
            reserved_row as ConfigureRow,
            reserved_audit_inventory as ConfigureInventory,
        ),
        (
            "authority unsupported",
            authority_unsupported_row as ConfigureRow,
            keep_inventory,
        ),
    ] {
        let database = TestDatabase::new_with_schemas(false, true).await?;
        seed_v2_alice_name_records_fixture_with_row(&database, configure_row, |_, _, inventory| {
            configure_inventory(inventory)
        })
        .await?;
        if label == "ownerless serving" {
            sqlx::query(
                "UPDATE bigname_phase.resources SET token_lineage_id = NULL
                 WHERE resource_id = $1",
            )
            .bind(Uuid::from_u128(0x2200))
            .execute(&database.pool)
            .await?;
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
                    ("reservation", None) => Vec::new(),
                    (_, None) => fixture_keys.to_vec(),
                };
                assert_eq!(record_keys(&payload), expected_keys, "{label} {uri}");
                assert_eq!(
                    payload["data"].get("inventory").is_some(),
                    label != "reservation",
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
                    ("authority unsupported", _) => {
                        for key in &expected_keys {
                            assert_eq!(
                                records[*key],
                                json!({"status": "unsupported", "unsupported_reason": "conflicting_current_ens_authority"}),
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
                        json!({"status": "unsupported", "unsupported_reason": "verified_records_not_supported"}),
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
        vec!["addr:60", "avatar", "text:com.twitter"]
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

    // Inventory ahead of the selected snapshot fails the verified readback like an indexed read.
    insert_v2_sepolia_indexed_inventory(&database).await?;
    sqlx::query(
        "INSERT INTO bigname_phase.chain_lineage
         (chain_id, block_hash, block_number, block_timestamp, canonicality_state)
         VALUES ('ethereum-sepolia', '0xfuture-inventory', $1,
                 '2026-04-17T00:10:21Z', 'canonical'::bigname_phase.canonicality_state)",
    )
    .bind(V2_SEPOLIA_ONLY_SNAPSHOT_BLOCK + 1)
    .execute(&database.pool)
    .await?;
    sqlx::query(
        "UPDATE bigname_phase.record_inventory_current
         SET chain_positions = $2, canonicality_summary = $3
         WHERE resource_id = $1",
    )
    .bind(Uuid::from_u128(0x7e30))
    .bind(json!({
        "block_number": V2_SEPOLIA_ONLY_SNAPSHOT_BLOCK + 1,
        "block_hash": "0xfuture-inventory",
        "target_block_number": V2_SEPOLIA_ONLY_SNAPSHOT_BLOCK + 1,
        "target_block_hash": "0xfuture-inventory",
    }))
    .bind(json!({
        "state": "canonical_lineage",
        "target_block_number": V2_SEPOLIA_ONLY_SNAPSHOT_BLOCK + 1,
        "target_block_hash": "0xfuture-inventory",
    }))
    .execute(&database.pool)
    .await?;
    for uri in [unkeyed, keyed] {
        let (status, body) = records_route_get(&state, &uri).await?;
        assert_eq!(status, StatusCode::CONFLICT, "{uri}: {body}");
    }

    database.cleanup().await
}
