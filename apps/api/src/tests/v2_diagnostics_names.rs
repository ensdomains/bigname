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
                "source_classes_considered": [],
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
                "history": {
                    "latest_event_kind": "NameTransferred"
                }
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
                    "latest_event_kind": "NameTransferred"
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

    for case in diagnostic_route_cases() {
        let uri = format!("/v2/diagnostics/names/Alice.eth/{}", case.suffix);
        let payload = request_v2_diagnostics_json(&database, &uri, StatusCode::OK).await?;

        assert!(payload.get("page").is_none(), "{uri}");
        assert_eq!(payload["data"], case.expected_data, "{uri}");
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
async fn v2_diagnostics_name_coverage_synthesizes_missing_unsupported_reason() -> Result<()> {
    let database = TestDatabase::new_with_schemas(false, true).await?;
    let resource_id = Uuid::from_u128(0x4400);
    let token_lineage_id = Uuid::from_u128(0x5500);
    let surface_binding_id = Uuid::from_u128(0x6600);
    let logical_name_id = "ens:unsupported.eth";
    let normalized_name = "unsupported.eth";

    database
        .seed_name_current_binding(
            logical_name_id,
            "ens",
            normalized_name,
            normalized_name,
            &format!("namehash:{normalized_name}"),
            resource_id,
            token_lineage_id,
            surface_binding_id,
        )
        .await?;

    let mut row = diagnostic_name_current_row(
        logical_name_id,
        21_000_004,
        resource_id,
        token_lineage_id,
        surface_binding_id,
    );
    row.coverage = json!({
        "status": "unsupported",
        "exhaustiveness": "not_applicable",
        "source_classes_considered": [],
        "enumeration_basis": "exact_name",
        "unsupported_reason": null
    });
    database.insert_name_current_row(row).await?;

    let payload = request_v2_diagnostics_json(
        &database,
        "/v2/diagnostics/names/unsupported.eth/coverage",
        StatusCode::OK,
    )
    .await?;

    assert_eq!(payload["data"]["status"], json!("unsupported"));
    assert_eq!(
        payload["data"]["unsupported_reason"],
        json!("name_coverage_unsupported_reason_missing")
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_diagnostics_name_records_executes_ephemeral_lookup_without_legacy_persistence()
-> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    database.initialize_lookup_schema().await?;
    let execution_block_hash =
        "0x1111111111111111111111111111111111111111111111111111111111111111";
    let indexed_address = "0x0000000000000000000000000000000000000def";
    let verified_address = "0x0000000000000000000000000000000000000e0e";
    let lookup_pool = database.lookup_pool().await?;
    let namehash = seed_schema_v2_ens_record_lookup(
        &lookup_pool,
        21_000_003,
        execution_block_hash,
        "2026-04-17T00:00:03Z",
        indexed_address,
    )
    .await?;
    seed_v2_alice_name_record_fixture_migrated(
        &database,
        |row| {
            row.namehash = namehash;
            row.chain_positions = json!({
                "ethereum": {
                    "chain_id": "ethereum-mainnet",
                    "block_number": 21_000_003,
                    "block_hash": execution_block_hash,
                    "timestamp": "2026-04-17T00:00:03Z"
                }
            });
        },
        |_, _, inventory| {
            inventory.selectors = json!([{
                "record_key": "addr:60",
                "record_family": "addr",
                "selector_key": "60",
                "cacheable": true
            }]);
            inventory.entries = json!([{
                "record_key": "addr:60",
                "record_family": "addr",
                "selector_key": "60",
                "status": "success",
                "value": {
                    "coin_type": "60",
                    "value": indexed_address
                }
            }]);
            inventory.record_version_boundary["chain_position"]["block_hash"] =
                json!(execution_block_hash);
            inventory.chain_positions = json!({
                "ethereum-mainnet": {
                    "chain_id": "ethereum-mainnet",
                    "block_number": 21_000_003,
                    "block_hash": execution_block_hash,
                    "timestamp": "2026-04-17T00:00:03Z"
                }
            });
        },
    )
    .await?;
    let (rpc_url, rpc_handle) = spawn_primary_name_mock_rpc(vec![
        resolution_universal_resolver_addr60_response(verified_address),
    ])
    .await?;
    let chain_rpc_urls =
        bigname_lookup::ChainRpcUrls::from_entries(&[format!("ethereum-mainnet={rpc_url}")])?;
    let state = database
        .app_state_with_lookup_chain_rpc_urls(chain_rpc_urls)
        .await?;

    let response = app_router(state)
        .oneshot(
            Request::builder()
                .uri("/v2/diagnostics/names/Alice.eth/records?keys=addr:60")
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
    assert_eq!(join_primary_name_mock_rpc_requests(rpc_handle).await?.len(), 1);
    let ledger_count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM resolution_divergences WHERE cleared_at IS NULL",
    )
    .fetch_one(&lookup_pool)
    .await?;
    assert_eq!(ledger_count, 1);

    lookup_pool.close().await;
    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_diagnostics_name_records_compares_retained_reservation_audit_state() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    database.initialize_lookup_schema().await?;
    let execution_block_hash =
        "0x1111111111111111111111111111111111111111111111111111111111111111";
    let indexed_address = "0x0000000000000000000000000000000000000def";
    let verified_address = "0x0000000000000000000000000000000000000e0e";
    let lookup_pool = database.lookup_pool().await?;
    let namehash = seed_schema_v2_ens_record_lookup(
        &lookup_pool,
        21_000_003,
        execution_block_hash,
        "2026-04-17T00:00:03Z",
        indexed_address,
    )
    .await?;
    seed_v2_alice_name_record_fixture_migrated(
        &database,
        |row| {
            row.namehash = namehash;
            row.chain_positions = json!({
                "ethereum": {
                    "chain_id": "ethereum-mainnet",
                    "block_number": 21_000_003,
                    "block_hash": execution_block_hash,
                    "timestamp": "2026-04-17T00:00:03Z"
                }
            });
            row.declared_summary["registration"] = json!({
                "status": "reserved",
                "expiry": 4_000_000_000_u64,
                "latest_event_kind": "RegistrationReserved"
            });
            row.declared_summary["control"] = json!({"status": "reserved"});
        },
        |_, _, inventory| {
            inventory.selectors = json!([{
                "record_key": "addr:60",
                "record_family": "addr",
                "selector_key": "60",
                "cacheable": true
            }]);
            inventory.entries = json!([{
                "record_key": "addr:60",
                "record_family": "addr",
                "selector_key": "60",
                "status": "success",
                "value": {"coin_type": "60", "value": indexed_address}
            }]);
            inventory.record_version_boundary["chain_position"]["block_hash"] =
                json!(execution_block_hash);
            inventory.chain_positions = json!({
                "ethereum-mainnet": {
                    "chain_id": "ethereum-mainnet",
                    "block_number": 21_000_003,
                    "block_hash": execution_block_hash,
                    "timestamp": "2026-04-17T00:00:03Z"
                }
            });
        },
    )
    .await?;
    let (rpc_url, rpc_handle) = spawn_primary_name_mock_rpc(vec![
        resolution_universal_resolver_addr60_response(verified_address),
    ])
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
            .uri("/v2/diagnostics/names/Alice.eth/records?keys=addr:60")
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
        payload["data"]["record_cache"]["entries"][0]["value"]["value"],
        json!(indexed_address)
    );
    assert_eq!(join_primary_name_mock_rpc_requests(rpc_handle).await?.len(), 1);

    lookup_pool.close().await;
    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_diagnostics_name_records_at_or_below_cap_has_no_truncation_note() -> Result<()> {
    let database = TestDatabase::new_with_schemas(false, true).await?;
    seed_v2_alice_name_records_fixture(
        &database,
        |_, _, inventory| {
            inventory.selectors = Value::Array(diagnostic_text_record_selectors(16));
            inventory.entries = Value::Array(diagnostic_text_record_entries(16));
            inventory.explicit_gaps = json!([]);
            inventory.unsupported_families = json!([]);
        },
    )
    .await?;

    let payload = request_v2_diagnostics_json(
        &database,
        "/v2/diagnostics/names/Alice.eth/records",
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
    let logical_name_id = "ens:alice.eth";
    let resource_id = Uuid::from_u128(0x2200);
    let token_lineage_id = Uuid::from_u128(0x1100);
    let surface_binding_id = Uuid::from_u128(0x3300);
    let worker_boundary = record_inventory_boundary_with_pointer(
        logical_name_id,
        resource_id,
        Some(1201),
        Some("RecordVersionChanged"),
    );
    let expected_boundary = json!({
        "namespace": "ens",
        "name": "alice.eth",
        "registration_id": resource_id.to_string(),
        "normalized_event_id": 1201,
        "event_kind": "RecordVersionChanged",
        "chain_position": worker_boundary["chain_position"].clone(),
    });

    database
        .seed_name_current_binding(
            logical_name_id,
            "ens",
            "alice.eth",
            "Alice.eth",
            "namehash:alice.eth",
            resource_id,
            token_lineage_id,
            surface_binding_id,
        )
        .await?;
    database
        .insert_name_current_row(exact_name_row(
            logical_name_id,
            surface_binding_id,
            resource_id,
            token_lineage_id,
        ))
        .await?;
    database
        .insert_record_inventory_current_row(worker_record_inventory_current_row(
            logical_name_id,
            resource_id,
        ))
        .await?;

    let payload = request_v2_diagnostics_json(
        &database,
        "/v2/diagnostics/names/alice.eth/records",
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
async fn v2_diagnostics_name_records_cache_keeps_non_product_cacheable_selectors() -> Result<()> {
    let database = TestDatabase::new_with_schemas(false, true).await?;
    seed_v2_alice_name_records_fixture(
        &database,
        |_, _, inventory| {
            inventory.selectors = json!([
                {
                    "record_key": "addr:60",
                    "record_family": "addr",
                    "selector_key": "60",
                    "cacheable": true
                },
                {
                    "record_key": "pubkey",
                    "record_family": "pubkey",
                    "selector_key": null,
                    "cacheable": true
                }
            ]);
            inventory.entries = json!([
                {
                    "record_key": "addr:60",
                    "record_family": "addr",
                    "selector_key": "60",
                    "status": "success",
                    "value": {
                        "coin_type": "60",
                        "value": "0x0000000000000000000000000000000000000def"
                    }
                },
                {
                    "record_key": "pubkey",
                    "record_family": "pubkey",
                    "selector_key": null,
                    "status": "unsupported",
                    "unsupported_reason": "record_family_not_supported_in_phase6_projection"
                }
            ]);
            inventory.explicit_gaps = json!([]);
            inventory.unsupported_families = json!([]);
        },
    )
    .await?;

    let payload = request_v2_diagnostics_json(
        &database,
        "/v2/diagnostics/names/alice.eth/records",
        StatusCode::OK,
    )
    .await?;

    assert_eq!(
        payload["data"]["record_cache"]["entries"],
        json!([
            {
                "record_key": "addr:60",
                "record_family": "addr",
                "selector_key": "60",
                "status": "success",
                "value": {
                    "coin_type": "60",
                    "value": "0x0000000000000000000000000000000000000def"
                }
            },
            {
                "record_key": "pubkey",
                "record_family": "pubkey",
                "selector_key": null,
                "status": "unsupported",
                "unsupported_reason": "record_family_not_supported_in_phase6_projection"
            }
        ])
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
    database.seed_default_ens_snapshot_selector_position().await?;

    for suffix in ["coverage", "binding", "authority", "records"] {
        let uri = format!("/v2/diagnostics/names/missing.eth/{suffix}");
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
            "/v2/diagnostics/names/alice.eth/{suffix}?at={snapshot_token}&finality=finalized"
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
        let uri = format!("/v2/diagnostics/names/alice.base.eth/{suffix}");
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
        let uri = format!("/v2/diagnostics/names/alice.base.eth/{suffix}?namespace=ens");
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
        let uri = format!("/v2/diagnostics/names/bad%20name.eth/{suffix}");
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
            let uri = format!("/v2/diagnostics/names/alice.eth/{suffix}?{query}");
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
        let uri = format!("/v2/diagnostics/names/alice.eth/records?{query}");
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
            "/v2/diagnostics/names/alice.eth/records?keys=bad%20key",
            "keys must contain only addr:<coin_type>, text:<key>, avatar, or contenthash",
        ),
        (
            "/v2/diagnostics/names/alice.eth/records?keys=abi",
            "keys must contain only addr:<coin_type>, text:<key>, avatar, or contenthash",
        ),
        (
            "/v2/diagnostics/names/alice.eth/records?keys=addr:060,addr:60",
            "keys must not contain duplicate record keys",
        ),
        (
            "/v2/diagnostics/names/alice.eth/records?keys=addr:60&source=verified",
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
        let invalid_namespace = format!("/v2/diagnostics/names/alice.eth/{suffix}?namespace=unknown");
        let payload =
            request_v2_diagnostics_json(&database, &invalid_namespace, StatusCode::BAD_REQUEST)
                .await?;
        assert_eq!(
            payload["error"]["code"],
            json!("invalid_input"),
            "{invalid_namespace}"
        );

        let invalid_at = format!("/v2/diagnostics/names/alice.eth/{suffix}?at=not-hex");
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

fn diagnostic_text_record_selectors(count: usize) -> Vec<Value> {
    (0..count)
        .map(|index| {
            json!({
                "record_key": format!("text:key{index:02}"),
                "record_family": "text",
                "selector_key": format!("key{index:02}"),
                "cacheable": true
            })
        })
        .collect()
}

fn diagnostic_text_record_entries(count: usize) -> Vec<Value> {
    (0..count)
        .map(|index| {
            json!({
                "record_key": format!("text:key{index:02}"),
                "record_family": "text",
                "selector_key": format!("key{index:02}"),
                "status": "success",
                "value": {
                    "key": format!("key{index:02}"),
                    "value": format!("value-{index:02}")
                }
            })
        })
        .collect()
}

async fn seed_v2_diagnostics_name_fixture(
    database: &TestDatabase,
    logical_name_id: &str,
    block_number: i64,
) -> Result<String> {
    let (namespace, normalized_name) = logical_name_id
        .split_once(':')
        .expect("logical_name_id must include namespace");
    let resource_id = Uuid::from_u128(0x2200);
    let token_lineage_id = Uuid::from_u128(0x1100);
    let surface_binding_id = Uuid::from_u128(0x3300);

    database
        .seed_name_current_binding(
            logical_name_id,
            namespace,
            normalized_name,
            normalized_name,
            &format!("namehash:{normalized_name}"),
            resource_id,
            token_lineage_id,
            surface_binding_id,
        )
        .await?;

    let mut row = diagnostic_name_current_row(
        logical_name_id,
        block_number,
        resource_id,
        token_lineage_id,
        surface_binding_id,
    );
    if namespace == "basenames" {
        row.chain_positions["ethereum"] = json!({
            "chain_id": "ethereum-mainnet",
            "block_number": 21_000_003,
            "block_hash": "0xbinding",
            "timestamp": "2026-04-17T00:00:03Z"
        });
        row.canonicality_summary["chains"]["ethereum-mainnet"] = json!("finalized");
        database
            .seed_snapshot_selector_chain_positions(&row.chain_positions)
            .await?;
    }
    row.chain_positions = align_phase_chain_positions(&database.pool, &row.chain_positions).await?;
    let snapshot_token = hex::encode(
        serde_json::to_vec(&row.chain_positions).expect("chain positions must serialize"),
    );
    database.insert_name_current_row(row).await?;

    Ok(snapshot_token)
}

fn diagnostic_name_current_row(
    logical_name_id: &str,
    block_number: i64,
    resource_id: Uuid,
    token_lineage_id: Uuid,
    surface_binding_id: Uuid,
) -> bigname_storage::NameCurrentRow {
    let (namespace, normalized_name) = logical_name_id
        .split_once(':')
        .expect("logical_name_id must include namespace");
    let chain_id = chain_id_for_namespace(namespace);
    let chain_slot = chain_slot_for_namespace(namespace);
    let block_hash = format!("0xdiag{block_number:x}");

    bigname_storage::NameCurrentRow {
        logical_name_id: logical_name_id.to_owned(),
        namespace: namespace.to_owned(),
        canonical_display_name: normalized_name.to_owned(),
        normalized_name: normalized_name.to_owned(),
        namehash: format!("namehash:{normalized_name}"),
        surface_binding_id: Some(surface_binding_id),
        resource_id: Some(resource_id),
        token_lineage_id: Some(token_lineage_id),
        binding_kind: Some(bigname_storage::SurfaceBindingKind::DeclaredRegistryPath),
        declared_summary: json!({
            "control": {
                "registrant": "0x00000000000000000000000000000000000000aa",
                "registry_owner": "0x00000000000000000000000000000000000000bb",
                "latest_event_kind": "NameTransferred"
            },
            "history": {
                "latest_event_kind": "NameTransferred"
            }
        }),
        provenance: json!({}),
        coverage: json!({
            "status": "full",
            "exhaustiveness": "authoritative",
            "source_classes_considered": [source_family_for_namespace(namespace)],
            "enumeration_basis": "exact_name",
            "unsupported_reason": null
        }),
        chain_positions: json!({
            chain_slot: {
                "chain_id": chain_id,
                "block_number": block_number,
                "block_hash": block_hash,
                "timestamp": format!("2026-04-17T00:00:{:02}Z", block_number % 60)
            }
        }),
        canonicality_summary: json!({
            "status": "finalized",
            "chains": {
                chain_id: "finalized"
            }
        }),
        manifest_version: 1,
        last_recomputed_at: timestamp(1_717_176_000 + block_number),
    }
}
