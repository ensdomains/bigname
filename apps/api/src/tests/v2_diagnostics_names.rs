struct DiagnosticRouteCase {
    suffix: &'static str,
    expected_data: Value,
}

fn diagnostic_route_cases() -> Vec<DiagnosticRouteCase> {
    vec![
        DiagnosticRouteCase {
            suffix: "coverage",
            expected_data: json!({
                "status": "projected",
                "exhaustiveness": "not_asserted",
                "source_classes_considered": ["ensv1_registry_path"],
                "enumeration_basis": "exact_name",
                "unsupported_reason": null
            }),
        },
        DiagnosticRouteCase {
            suffix: "binding",
            expected_data: json!({
                "anchors": {
                    "logical_name_id": "ens:0x787192fc5378cc32aa956ddfdedbf26b24e8d78e40109add0eea2c1a012c3dec",
                    "namehash": "0x787192fc5378cc32aa956ddfdedbf26b24e8d78e40109add0eea2c1a012c3dec",
                    "resource_id": "00000000-0000-0000-0000-000000002200",
                    "token_lineage_id": "00000000-0000-0000-0000-000000001100"
                },
                "surface_binding": {
                    "surface_binding_id": "00000000-0000-0000-0000-000000003300",
                    "binding_kind": "declared_registry_path"
                },
                "history": {}
            }),
        },
        DiagnosticRouteCase {
            suffix: "authority",
            expected_data: json!({
                "authority": {
                    "resource_id": "00000000-0000-0000-0000-000000002200",
                    "token_lineage_id": "00000000-0000-0000-0000-000000001100",
                    "binding_kind": "declared_registry_path"
                },
                "control": {
                    "registrant": "0x00000000000000000000000000000000000000aa",
                    "registry_owner": "0x00000000000000000000000000000000000000bb",
                    "latest_event_kind": "AuthorityTransferred"
                },
                "permission_lineage": {
                    "status": "unsupported",
                    "unsupported_reason": "permission_lineage_not_projected_on_name_current"
                }
            }),
        },
    ]
}

#[tokio::test]
async fn v2_diagnostics_name_routes_return_declared_state_slices() -> Result<()> {
    let database = TestDatabase::new_with_schemas(false, true).await?;
    seed_v2_diagnostics_name_fixture(&database, "ens:alice.eth", 21_000_003).await?;

    let resolver_event: i64 = sqlx::query_scalar(
        "SELECT normalized_event_id FROM normalized_events WHERE event_kind='ResolverChanged'",
    )
    .fetch_one(&database.pool)
    .await?;
    for mut case in diagnostic_route_cases() {
        if case.suffix == "binding" {
            let head = json!({"normalized_event_id":resolver_event,"event_kind":"ResolverChanged",
                "chain_position":{"chain_id":"ethereum-mainnet","block_number":21000003,
                    "block_hash":"0xbinding","timestamp":"2026-04-17T00:00:03+00:00"}});
            case.expected_data["history"] = json!({"surface_head":head,"resource_head":head});
        }
        let uri = format!("/v1/diagnostics/names/Alice.eth/{}", case.suffix);
        let payload = request_v2_diagnostics_json(&database, &uri, StatusCode::OK).await?;

        assert!(payload.get("page").is_none(), "{uri}");
        for (field, expected) in case
            .expected_data
            .as_object()
            .context("diagnostic sections")?
        {
            if field == "control" {
                for (key, value) in expected.as_object().context("diagnostic fields")? {
                    assert_eq!(payload["data"][field][key], *value, "{uri}: {payload}");
                }
            } else {
                assert_eq!(payload["data"][field], *expected, "{uri}: {payload}");
            }
        }
        assert_eq!(
            payload["meta"]["as_of"]["1"],
            json!({
                "block_number": 21_000_003,
                "block_hash": "0xbinding",
                "timestamp": "2026-04-17T00:00:03Z"
            }),
            "{uri}"
        );
    }

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_diagnostics_name_records_executes_ephemeral_lookup_without_legacy_persistence()
-> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    database.initialize_lookup_schema().await?;
    let execution_block_hash = "0x1111111111111111111111111111111111111111111111111111111111111111";
    let indexed_address = "0x0000000000000000000000000000000000000def";
    let verified_address = "0x0000000000000000000000000000000000000e0e";
    let lookup_pool = database.lookup_pool().await?;
    let _namehash = seed_schema_v2_ens_record_lookup(
        &lookup_pool,
        21_000_003,
        execution_block_hash,
        "2026-04-17T00:00:03Z",
        indexed_address,
    )
    .await?;
    let (rpc_url, rpc_handle) =
        spawn_primary_name_mock_rpc(vec![resolution_universal_resolver_addr60_response(
            verified_address,
        )])
        .await?;
    let chain_rpc_urls =
        bigname_lookup::ChainRpcUrls::from_entries(&[format!("ethereum-mainnet={rpc_url}")])?;
    let state = database
        .app_state_with_lookup_chain_rpc_urls(chain_rpc_urls)
        .await?;

    let response = app_router(state)
        .oneshot(
            Request::builder()
                .uri("/v1/diagnostics/names/Alice.eth/records?keys=addr:60")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("v2 diagnostic live records request failed")?;
    let status = response.status();
    let payload: Value = read_json(response).await?;
    assert_eq!(status, StatusCode::OK, "unexpected response: {payload}");
    assert_eq!(
        payload["data"]["comparison"],
        json!({
            "addr:60": {
                "indexed": { "status": "ok", "value": indexed_address },
                "verified": { "status": "ok", "value": verified_address }
            }
        })
    );
    assert_eq!(
        payload["data"]["value_sources"]["addr:60"],
        json!([
            { "source": "indexed", "status": "ok", "value": indexed_address },
            { "source": "verified", "status": "ok", "value": verified_address }
        ])
    );
    assert_eq!(
        join_primary_name_mock_rpc_requests(rpc_handle).await?.len(),
        1
    );
    let ledger_count: i64 =
        sqlx::query_scalar("SELECT count(*) FROM resolution_divergences WHERE cleared_at IS NULL")
            .fetch_one(&lookup_pool)
            .await?;
    assert_eq!(ledger_count, 1);

    lookup_pool.close().await;
    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_diagnostics_name_records_compares_retained_ownerless_audit_state() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let indexed_address = "0x0000000000000000000000000000000000000def";
    let verified_address = "0x0000000000000000000000000000000000000e0e";
    seed_alice_verified_inputs(
        &database,
        AliceInputState::Ownerless,
        &[family_fixture_record_write(
            "addr:60",
            Some(json!(indexed_address)),
        )],
    )
    .await?;
    let lookup_pool = database.lookup_pool().await?;
    let (rpc_url, rpc_handle) =
        spawn_primary_name_mock_rpc(vec![resolution_universal_resolver_addr60_response(
            verified_address,
        )])
        .await?;
    let chain_rpc_urls =
        bigname_lookup::ChainRpcUrls::from_entries(&[format!("ethereum-mainnet={rpc_url}")])?;

    let response = app_router(
        database
            .app_state_with_lookup_chain_rpc_urls(chain_rpc_urls)
            .await?,
    )
    .oneshot(
        Request::builder()
            .uri("/v1/diagnostics/names/Alice.eth/records?keys=addr:60")
            .body(Body::empty())
            .expect("request must build"),
    )
    .await
    .context("v2 reservation diagnostic records request failed")?;
    let status = response.status();
    let payload: Value = read_json(response).await?;
    assert_eq!(status, StatusCode::OK, "unexpected response: {payload}");
    assert_eq!(
        payload["data"]["comparison"]["addr:60"],
        json!({
            "indexed": {"status": "ok", "value": indexed_address},
            "verified": {"status": "ok", "value": verified_address}
        })
    );
    assert_eq!(
        payload["data"]["record_cache"]["entries"][0]["value"],
        json!(indexed_address)
    );
    assert_eq!(
        join_primary_name_mock_rpc_requests(rpc_handle).await?.len(),
        1
    );

    lookup_pool.close().await;
    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_diagnostics_name_records_at_or_below_cap_has_no_truncation_note() -> Result<()> {
    let database = TestDatabase::new_with_schemas(false, true).await?;
    let writes = (0..16)
        .map(|i| {
            family_fixture_record_write(
                &format!("text:key{i:02}"),
                Some(json!(format!("value{i}"))),
            )
        })
        .collect::<Vec<_>>();
    seed_alice_name_inputs_with_writes(&database, &writes).await?;

    let payload = request_v2_diagnostics_json(
        &database,
        "/v1/diagnostics/names/Alice.eth/records",
        StatusCode::OK,
    )
    .await?;

    assert_eq!(
        payload["data"]["comparison"]
            .as_object()
            .expect("comparison must be an object")
            .len(),
        16
    );
    assert!(payload["data"].get("comparison_explicit_gaps").is_none());

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_diagnostics_name_records_reuses_supported_inventory_boundary_fallback() -> Result<()> {
    let database = TestDatabase::new_with_schemas(false, true).await?;
    seed_alice_name_inputs(&database).await?;
    replace_alice_record_inputs(&database, &[]).await?;
    let event_id: i64 = sqlx::query_scalar(
        "SELECT normalized_event_id FROM normalized_events WHERE event_kind='RecordVersionChanged'",
    )
    .fetch_one(&database.pool)
    .await?;
    let expected_boundary = json!({"namespace":"ens","name":"alice.eth",
    "registration_id":Uuid::from_u128(0x2200).to_string(),"normalized_event_id":event_id,
    "event_kind":"RecordVersionChanged","chain_position":{
        "chain_id":"ethereum-mainnet","block_number":21000003,"block_hash":"0xbinding","timestamp":"2026-04-17T00:00:03+00:00"
    }});

    let payload = request_v2_diagnostics_json(
        &database,
        "/v1/diagnostics/names/alice.eth/records",
        StatusCode::OK,
    )
    .await?;

    assert_eq!(
        payload["data"]["record_inventory"]["record_version_boundary"],
        expected_boundary
    );
    assert_eq!(
        payload["data"]["record_cache"]["record_version_boundary"],
        expected_boundary
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_diagnostics_name_records_reports_non_product_family_without_comparing_it() -> Result<()>
{
    let database = TestDatabase::new_with_schemas(false, true).await?;
    seed_alice_name_inputs_with_writes(&database,&[
        family_fixture_record_write("addr:60",Some(json!("0x0000000000000000000000000000000000000def"))),
        json!({"source_event":"ABIChanged","record_key":"abi:1","record_family":"abi","selector_key":"1","content_type":"1"})
    ]).await?;

    let payload = request_v2_diagnostics_json(
        &database,
        "/v1/diagnostics/names/alice.eth/records",
        StatusCode::OK,
    )
    .await?;

    assert_eq!(
        payload["data"]["record_cache"]["entries"],
        json!([{
            "record_key":"addr:60","record_family":"addr","selector_key":"60","status":"success",
            "value":"0x0000000000000000000000000000000000000def"
        }])
    );
    assert!(
        payload["data"]["record_inventory"]["unsupported_families"]
            .as_array()
            .context("unsupported families")?
            .iter()
            .any(|entry| entry["record_family"] == "abi")
    );
    assert_eq!(
        payload["data"]["comparison"]
            .as_object()
            .expect("comparison must be an object")
            .keys()
            .cloned()
            .collect::<Vec<_>>(),
        vec!["addr:60".to_owned()]
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_diagnostics_name_routes_return_not_found_for_missing_name() -> Result<()> {
    let database = TestDatabase::new_with_schemas(false, true).await?;
    database
        .seed_default_ens_snapshot_selector_position()
        .await?;

    rebuild_fixture_families(&database.pool, "ethereum-mainnet", 21000003, "0xbinding").await?;

    for suffix in ["coverage", "binding", "authority", "records"] {
        let uri = format!("/v1/diagnostics/names/missing.eth/{suffix}");
        let payload = request_v2_diagnostics_json(&database, &uri, StatusCode::NOT_FOUND).await?;

        assert_eq!(payload["error"]["code"], json!("not_found"), "{uri}");
        assert_eq!(
            payload["error"]["message"],
            json!("name missing.eth was not found in namespace ens"),
            "{uri}"
        );
    }

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_diagnostics_name_routes_honor_snapshot_selectors() -> Result<()> {
    let database = TestDatabase::new_with_schemas(false, true).await?;
    let snapshot_token =
        seed_v2_diagnostics_name_fixture(&database, "ens:alice.eth", 21_000_003).await?;

    for suffix in ["coverage", "binding", "authority", "records"] {
        let uri = format!(
            "/v1/diagnostics/names/alice.eth/{suffix}?at={snapshot_token}&finality=finalized"
        );
        let payload = request_v2_diagnostics_json(&database, &uri, StatusCode::OK).await?;

        assert_eq!(
            payload["meta"]["as_of"]["1"],
            json!({
                "block_number": 21_000_003,
                "block_hash": "0xbinding",
                "timestamp": "2026-04-17T00:00:03Z"
            }),
            "{uri}"
        );
    }

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_diagnostics_name_routes_infer_basenames_namespace() -> Result<()> {
    let database = TestDatabase::new_with_schemas(false, true).await?;
    seed_v2_diagnostics_name_fixture(&database, "basenames:alice.base.eth", 84).await?;

    for suffix in ["coverage", "binding", "authority", "records"] {
        let uri = format!("/v1/diagnostics/names/alice.base.eth/{suffix}");
        let payload = request_v2_diagnostics_json(&database, &uri, StatusCode::OK).await?;

        assert_eq!(
            payload["meta"]["as_of"]["8453"],
            json!({
                "block_number": 84,
                "block_hash": "0xdiag54",
                "timestamp": "2026-04-17T00:00:24Z"
            }),
            "{uri}"
        );
    }

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_diagnostics_name_routes_honor_namespace_override() -> Result<()> {
    let database = TestDatabase::new_with_schemas(false, true).await?;
    seed_v2_diagnostics_name_fixture(&database, "ens:alice.base.eth", 21_000_003).await?;

    for suffix in ["coverage", "binding", "authority", "records"] {
        let uri = format!("/v1/diagnostics/names/alice.base.eth/{suffix}?namespace=ens");
        let payload = request_v2_diagnostics_json(&database, &uri, StatusCode::OK).await?;

        assert_eq!(
            payload["meta"]["as_of"]["1"],
            json!({
                "block_number": 21_000_003,
                "block_hash": "0xbinding",
                "timestamp": "2026-04-17T00:00:03Z"
            }),
            "{uri}"
        );
    }

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_diagnostics_name_routes_reject_malformed_name() -> Result<()> {
    let state = AppState::new(
        PgPool::connect_lazy_with(
            "postgres://bigname:bigname@127.0.0.1:5432/bigname"
                .parse()
                .expect("static test database URL must parse"),
        ),
        bigname_lookup::ChainRpcUrls::default(),
    );

    for suffix in ["coverage", "binding", "authority", "records"] {
        let uri = format!("/v1/diagnostics/names/bad%20name.eth/{suffix}");
        let response = app_router(state.clone())
            .oneshot(
                Request::builder()
                    .uri(&uri)
                    .body(Body::empty())
                    .expect("request must build"),
            )
            .await
            .context("v2 malformed diagnostic name request failed")?;

        assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{uri}");
        let payload: Value = read_json(response).await?;
        assert_eq!(payload["error"]["code"], json!("invalid_input"), "{uri}");
    }

    Ok(())
}

#[tokio::test]
async fn v2_diagnostics_name_routes_reject_undocumented_query_params() -> Result<()> {
    let state = AppState::new(
        PgPool::connect_lazy_with(
            "postgres://bigname:bigname@127.0.0.1:5432/bigname"
                .parse()
                .expect("static test database URL must parse"),
        ),
        bigname_lookup::ChainRpcUrls::default(),
    );

    for suffix in ["coverage", "binding", "authority"] {
        for (query, expected_message) in [
            ("source=verified", "unknown query parameter: source"),
            ("keys=addr:60", "unknown query parameter: keys"),
            ("address=bad", "unknown query parameter: address"),
            ("page_size=201", "unknown query parameter: page_size"),
        ] {
            let uri = format!("/v1/diagnostics/names/alice.eth/{suffix}?{query}");
            let response = app_router(state.clone())
                .oneshot(
                    Request::builder()
                        .uri(&uri)
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("v2 diagnostic name undocumented query request failed")?;

            assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{uri}");
            let payload: Value = read_json(response).await?;
            assert_eq!(payload["error"]["code"], json!("invalid_input"), "{uri}");
            assert_eq!(
                payload["error"]["message"],
                json!(expected_message),
                "{uri}"
            );
        }
    }

    for (query, expected_message) in [
        ("source=verified", "unknown query parameter: source"),
        ("address=bad", "unknown query parameter: address"),
        ("page_size=201", "unknown query parameter: page_size"),
    ] {
        let uri = format!("/v1/diagnostics/names/alice.eth/records?{query}");
        let response = app_router(state.clone())
            .oneshot(
                Request::builder()
                    .uri(&uri)
                    .body(Body::empty())
                    .expect("request must build"),
            )
            .await
            .context("v2 diagnostic records undocumented query request failed")?;

        assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{uri}");
        let payload: Value = read_json(response).await?;
        assert_eq!(payload["error"]["code"], json!("invalid_input"), "{uri}");
        assert_eq!(
            payload["error"]["message"],
            json!(expected_message),
            "{uri}"
        );
    }

    Ok(())
}

#[tokio::test]
async fn v2_diagnostics_name_records_rejects_malformed_duplicate_and_unknown_query_params()
-> Result<()> {
    let state = AppState::new(
        PgPool::connect_lazy_with(
            "postgres://bigname:bigname@127.0.0.1:5432/bigname"
                .parse()
                .expect("static test database URL must parse"),
        ),
        bigname_lookup::ChainRpcUrls::default(),
    );

    for (uri, expected_message) in [
        (
            "/v1/diagnostics/names/alice.eth/records?keys=bad%20key",
            "keys must contain only addr:<coin_type>, text:<key>, avatar, or contenthash",
        ),
        (
            "/v1/diagnostics/names/alice.eth/records?keys=abi",
            "keys must contain only addr:<coin_type>, text:<key>, avatar, or contenthash",
        ),
        (
            "/v1/diagnostics/names/alice.eth/records?keys=addr:060,addr:60",
            "keys must not contain duplicate record keys",
        ),
        (
            "/v1/diagnostics/names/alice.eth/records?keys=addr:60&source=verified",
            "unknown query parameter: source",
        ),
    ] {
        let response = app_router(state.clone())
            .oneshot(
                Request::builder()
                    .uri(uri)
                    .body(Body::empty())
                    .expect("request must build"),
            )
            .await
            .context("v2 records diagnostic invalid keys request failed")?;

        assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{uri}");
        let payload: Value = read_json(response).await?;
        assert_eq!(payload["error"]["code"], json!("invalid_input"), "{uri}");
        assert_eq!(
            payload["error"]["message"],
            json!(expected_message),
            "{uri}"
        );
    }

    Ok(())
}

#[tokio::test]
async fn v2_diagnostics_name_routes_reject_invalid_namespace_and_at() -> Result<()> {
    let database = TestDatabase::new_with_schemas(false, true).await?;
    seed_v2_diagnostics_name_fixture(&database, "ens:alice.eth", 21_000_003).await?;

    for suffix in ["coverage", "binding", "authority", "records"] {
        let invalid_namespace = format!("/v1/diagnostics/names/alice.eth/{suffix}?namespace=unknown");
        let payload =
            request_v2_diagnostics_json(&database, &invalid_namespace, StatusCode::BAD_REQUEST)
                .await?;
        assert_eq!(
            payload["error"]["code"],
            json!("invalid_input"),
            "{invalid_namespace}"
        );

        let invalid_at = format!("/v1/diagnostics/names/alice.eth/{suffix}?at=not-hex");
        let payload =
            request_v2_diagnostics_json(&database, &invalid_at, StatusCode::BAD_REQUEST).await?;
        assert_eq!(payload["error"]["code"], json!("invalid_input"), "{invalid_at}");
        assert_eq!(payload["error"]["message"], json!("at is invalid"), "{invalid_at}");
    }

    database.cleanup().await?;
    Ok(())
}

async fn request_v2_diagnostics_json(
    database: &TestDatabase,
    uri: &str,
    expected_status: StatusCode,
) -> Result<Value> {
    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri(uri)
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .with_context(|| format!("v2 diagnostics name request failed for {uri}"))?;
    let status = response.status();
    let payload = read_json(response).await?;

    assert_eq!(status, expected_status, "{uri}: {payload}");
    Ok(payload)
}

#[tokio::test]
async fn v2_diagnostics_name_coverage_serves_the_projected_source_classes() -> Result<()> {
    for (state, classes, basis) in [
        (
            AliceInputState::Ownerless,
            json!(["ensv1_registry_path"]),
            "event_linked_registry_resolver",
        ),
        (
            AliceInputState::Registry,
            json!([
                "ens_v2_root_l1",
                "ens_v2_registry_l1",
                "ens_v2_registrar_l1"
            ]),
            "exact_name_profile",
        ),
    ] {
        let database = TestDatabase::new_migrated().await?;
        seed_alice_state_inputs(&database, state).await?;
        let payload = request_v2_diagnostics_json(
            &database,
            "/v1/diagnostics/names/alice.eth/coverage",
            StatusCode::OK,
        )
        .await?;
        assert_eq!(
            payload["data"],
            json!({"status":"projected","exhaustiveness":"not_asserted",
            "source_classes_considered":classes,"enumeration_basis":basis,"unsupported_reason":null})
        );
        database.cleanup().await?;
    }
    Ok(())
}

async fn seed_v2_diagnostics_name_fixture(
    database: &TestDatabase,
    logical_name_id: &str,
    block_number: i64,
) -> Result<String> {
    let (namespace, name) = logical_name_id
        .split_once(':')
        .context("diagnostic name namespace")?;
    if logical_name_id == "ens:alice.eth" {
        seed_alice_name_inputs(database).await?;
    } else if namespace == "ens" {
        seed_unbound_name_inputs(database, name, false).await?;
    } else {
        let hash = format!("0xdiag{block_number:x}");
        let at = format!("2026-04-17T00:00:{:02}Z", block_number % 60);
        database.seed_snapshot_selector_chain_positions(&json!({
            "base":{"chain_id":"base-mainnet","block_number":block_number,"block_hash":hash,"timestamp":at},
            "ethereum":{"chain_id":"ethereum-mainnet","block_number":21000003,"block_hash":"0xbinding","timestamp":"2026-04-17T00:00:03Z"}
        })).await?;
        seed_record_lookup_inputs(
            &database.pool,
            "base-mainnet",
            namespace,
            name,
            Uuid::from_u128(0x2200),
            Uuid::from_u128(0x3300),
            block_number,
            &hash,
            &at,
            "0x0000000000000000000000000000000000000def",
        )
        .await?;
        rebuild_fixture_families(&database.pool, "ethereum-mainnet", 21000003, "0xbinding").await?;
    }
    let payload = v2_name_record_payload_for_database(
        database,
        &format!("/v1/names/{name}?namespace={namespace}"),
    )
    .await?;
    Ok(payload["meta"]["as_of_token"]
        .as_str()
        .context("diagnostic fixture snapshot token")?
        .to_owned())
}
