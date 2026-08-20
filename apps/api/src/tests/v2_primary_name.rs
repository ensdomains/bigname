#[tokio::test]
async fn v2_get_primary_name_executes_lookup_each_time_without_legacy_persistence() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    database.initialize_lookup_schema().await?;
    database
        .seed_default_ens_primary_name_fallback_context()
        .await?;
    let lookup_pool = database.lookup_pool().await?;
    seed_schema_v2_ens_primary_name_authority(
        &lookup_pool,
        21_000_003,
        "0xbinding",
        "2026-04-17T00:00:03Z",
    )
    .await?;
    let (rpc_url, rpc_handle) = spawn_primary_name_mock_rpc(vec![
        json!("0x000000000000000000000000a2c122be93b0074270ebee7f6b7292c7deb45047"),
        primary_name_reverse_name_response("taytems.eth"),
        primary_name_universal_resolver_addr60_response(V2_ON_DEMAND_PRIMARY_NAME_ADDRESS),
        json!("0x000000000000000000000000a2c122be93b0074270ebee7f6b7292c7deb45047"),
        primary_name_reverse_name_response("taytems.eth"),
        primary_name_universal_resolver_addr60_response(V2_ON_DEMAND_PRIMARY_NAME_ADDRESS),
        json!("0x000000000000000000000000a2c122be93b0074270ebee7f6b7292c7deb45047"),
        primary_name_reverse_name_response("taytems.eth"),
        primary_name_universal_resolver_addr60_response(V2_ON_DEMAND_PRIMARY_NAME_ADDRESS),
    ])
    .await?;
    let chain_rpc_urls =
        bigname_lookup::ChainRpcUrls::from_entries(&[format!("ethereum-mainnet={rpc_url}")])?;
    let state = database
        .app_state_with_lookup_chain_rpc_urls(chain_rpc_urls)
        .await?;

    for _ in 0..2 {
        let verified_response = app_router(state.clone())
        .oneshot(
            Request::builder()
                .uri(format!(
                    "/v2/addresses/{V2_ON_DEMAND_PRIMARY_NAME_ADDRESS}/primary-name?source=verified"
                ))
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("v2 verified schema-v2 primary-name request failed")?;
        let status = verified_response.status();
        let verified_payload: Value = read_json(verified_response).await?;
        assert_eq!(status, StatusCode::OK, "unexpected response: {verified_payload}");
        assert_eq!(
            verified_payload["data"]["answers"],
            json!([{
                "source": "verified",
                "status": "ok",
                "name": "taytems.eth"
            }])
        );
        assert_primary_name_snapshot_meta_chain_ids(&verified_payload, &["1"]);
    }

    let mixed_response = app_router(state)
        .oneshot(
            Request::builder()
                .uri(format!(
                    "/v2/addresses/{V2_ON_DEMAND_PRIMARY_NAME_ADDRESS}/primary-name"
                ))
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("v2 mixed schema-v2 primary-name request failed")?;
    let status = mixed_response.status();
    let mixed_payload: Value = read_json(mixed_response).await?;
    assert_eq!(status, StatusCode::OK, "unexpected response: {mixed_payload}");
    assert_eq!(
        mixed_payload["data"]["answers"],
        json!([
            {
                "source": "indexed",
                "status": "not_found"
            },
            {
                "source": "verified",
                "status": "ok",
                "name": "taytems.eth"
            }
        ])
    );

    let rpc_requests = join_primary_name_mock_rpc_requests(rpc_handle).await?;
    assert_eq!(
        rpc_requests.len(),
        9,
        "v2 primary-name verification must execute again on every request"
    );
    let ledger_count: i64 = sqlx::query_scalar("SELECT count(*) FROM resolution_divergences")
        .fetch_one(&lookup_pool)
        .await?;
    assert_eq!(ledger_count, 0, "primary-name lookup has no ledger comparison target");

    lookup_pool.close().await;
    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_get_primary_name_uses_one_phase_position_without_legacy_checkpoint() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    database.initialize_lookup_schema().await?;
    database
        .seed_default_ens_primary_name_fallback_context()
        .await?;
    database
        .insert_primary_name_current_claim_row(
            V2_ON_DEMAND_PRIMARY_NAME_ADDRESS,
            "ens",
            "60",
            PrimaryNameClaimStatus::Success,
            Some("legacy-worker.eth"),
        )
        .await?;
    database
        .insert_primary_name_current_normalized_claim_name(
            V2_ON_DEMAND_PRIMARY_NAME_ADDRESS,
            "ens",
            "60",
            Some("legacy-worker.eth"),
            true,
        )
        .await?;
    let lookup_pool = database.lookup_pool().await?;
    seed_schema_v2_ens_primary_name_authority(
        &lookup_pool,
        21_000_004,
        "0xnewer-phase-head",
        "2026-04-17T00:00:04Z",
    )
    .await?;
    seed_schema_v2_primary_name_claim(
        &lookup_pool,
        V2_ON_DEMAND_PRIMARY_NAME_ADDRESS,
        "ens",
        "60",
        "taytems.eth",
        true,
    )
    .await?;
    let (rpc_url, rpc_handle) = spawn_primary_name_mock_rpc(vec![
        json!("0x000000000000000000000000a2c122be93b0074270ebee7f6b7292c7deb45047"),
        primary_name_reverse_name_response("taytems.eth"),
        primary_name_universal_resolver_addr60_response(V2_ON_DEMAND_PRIMARY_NAME_ADDRESS),
    ])
    .await?;
    let chain_rpc_urls =
        bigname_lookup::ChainRpcUrls::from_entries(&[format!("ethereum-mainnet={rpc_url}")])?;
    let state = database
        .app_state_with_lookup_chain_rpc_urls(chain_rpc_urls)
        .await?;

    let response = app_router(state.clone())
        .oneshot(
            Request::builder()
                .uri(format!(
                    "/v2/addresses/{V2_ON_DEMAND_PRIMARY_NAME_ADDRESS}/primary-name"
                ))
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("v2 mixed primary-name request failed")?;
    let status = response.status();
    let payload: Value = read_json(response).await?;
    assert_eq!(status, StatusCode::OK, "unexpected response: {payload}");
    assert_eq!(payload["meta"]["as_of"]["1"]["block_number"], 21_000_004);
    assert_eq!(
        payload["data"]["answers"],
        json!([
            {
                "source": "indexed",
                "status": "ok",
                "name": "taytems.eth"
            },
            {
                "source": "verified",
                "status": "ok",
                "name": "taytems.eth"
            }
        ])
    );

    let indexed_response = app_router(state)
        .oneshot(
            Request::builder()
                .uri(format!(
                    "/v2/addresses/{V2_ON_DEMAND_PRIMARY_NAME_ADDRESS}/primary-name?source=indexed"
                ))
                .body(Body::empty())
                .expect("request must build"),
        )
        .await?;
    assert_eq!(indexed_response.status(), StatusCode::OK);
    let indexed_payload: Value = read_json(indexed_response).await?;
    assert_eq!(
        indexed_payload["data"]["answers"],
        json!([{
            "source": "indexed",
            "status": "ok",
            "name": "taytems.eth"
        }])
    );

    assert_eq!(join_primary_name_mock_rpc_requests(rpc_handle).await?.len(), 3);
    lookup_pool.close().await;
    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_get_primary_name_returns_mixed_answers_at_one_position() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    database.initialize_lookup_schema().await?;
    database
        .seed_default_ens_primary_name_fallback_context()
        .await?;
    database
        .insert_primary_name_current_claim_row(
            V2_ON_DEMAND_PRIMARY_NAME_ADDRESS,
            "ens",
            "60",
            PrimaryNameClaimStatus::Success,
            Some("taytems.eth"),
        )
        .await?;
    database
        .insert_primary_name_current_normalized_claim_name(
            V2_ON_DEMAND_PRIMARY_NAME_ADDRESS,
            "ens",
            "60",
            Some("taytems.eth"),
            true,
        )
        .await?;
    let lookup_pool = database.lookup_pool().await?;
    seed_schema_v2_ens_primary_name_authority(
        &lookup_pool,
        21_000_003,
        "0xbinding",
        "2026-04-17T00:00:03Z",
    )
    .await?;
    seed_schema_v2_primary_name_claim(
        &lookup_pool,
        V2_ON_DEMAND_PRIMARY_NAME_ADDRESS,
        "ens",
        "60",
        "taytems.eth",
        true,
    )
    .await?;
    let (rpc_url, rpc_handle) = spawn_primary_name_mock_rpc(vec![
        json!("0x000000000000000000000000a2c122be93b0074270ebee7f6b7292c7deb45047"),
        primary_name_reverse_name_response("taytems.eth"),
        primary_name_universal_resolver_addr60_response(V2_ON_DEMAND_PRIMARY_NAME_ADDRESS),
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
                .uri(format!(
                    "/v2/addresses/{V2_ON_DEMAND_PRIMARY_NAME_ADDRESS}/primary-name"
                ))
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("v2 mixed primary-name request failed")?;
    let status = response.status();
    let payload: Value = read_json(response).await?;
    assert_eq!(status, StatusCode::OK, "unexpected response: {payload}");
    assert_eq!(
        payload["data"]["answers"],
        json!([
            {
                "source": "indexed",
                "status": "ok",
                "name": "taytems.eth"
            },
            {
                "source": "verified",
                "status": "ok",
                "name": "taytems.eth"
            }
        ])
    );
    assert_eq!(payload["meta"]["as_of"]["1"]["block_hash"], json!("0xbinding"));
    assert_eq!(join_primary_name_mock_rpc_requests(rpc_handle).await?.len(), 3);

    lookup_pool.close().await;
    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_get_primary_name_normalizes_schema_v2_successful_claim() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    database
        .seed_snapshot_selector_chain_positions(&json!({
            "ethereum": {
                "chain_id": "ethereum-mainnet",
                "block_number": 21_000_005,
                "block_hash": "0xprimary-nonnormalized",
                "timestamp": "2026-04-17T00:00:05Z"
            }
        }))
        .await?;
    seed_schema_v2_primary_name_claim(
        &database.lookup_pool,
        V2_ON_DEMAND_PRIMARY_NAME_ADDRESS,
        "ens",
        "60",
        "Taytems.eth",
        false,
    )
    .await?;

    let payload = v2_primary_name_payload_for_database(
        &database,
        &format!(
            "/v2/addresses/{V2_ON_DEMAND_PRIMARY_NAME_ADDRESS}/primary-name?source=indexed"
        ),
    )
    .await?;
    assert_eq!(
        payload["data"]["answers"],
        json!([{
            "source": "indexed",
            "status": "ok",
            "name": "taytems.eth"
        }])
    );

    database.cleanup().await
}

#[tokio::test]
async fn v2_get_primary_name_reports_an_unnormalizable_stored_claim_as_invalid_name() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    database
        .seed_snapshot_selector_chain_positions(&json!({
            "ethereum": {
                "chain_id": "ethereum-mainnet",
                "block_number": 21_000_005,
                "block_hash": "0xprimary-unnormalizable",
                "timestamp": "2026-04-17T00:00:05Z"
            }
        }))
        .await?;
    // The projection classifies an unnormalizable claim `invalid_name`, so a stored `success` row
    // that no longer normalizes is only reachable mid-normalizer-revision. Report it with the same
    // vocabulary rather than a name-less `ok` or a failed read.
    seed_schema_v2_primary_name_claim(
        &database.lookup_pool,
        V2_ON_DEMAND_PRIMARY_NAME_ADDRESS,
        "ens",
        "60",
        "taytems..eth",
        false,
    )
    .await?;

    let payload = v2_primary_name_payload_for_database(
        &database,
        &format!(
            "/v2/addresses/{V2_ON_DEMAND_PRIMARY_NAME_ADDRESS}/primary-name?source=indexed"
        ),
    )
    .await?;
    assert_eq!(
        payload["data"]["answers"],
        json!([{
            "source": "indexed",
            "status": "invalid_name",
            "raw_claim_name": "taytems..eth"
        }])
    );

    database.cleanup().await
}

#[tokio::test]
async fn v2_get_primary_name_publishes_an_already_normalized_claim_as_stored() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    database
        .seed_snapshot_selector_chain_positions(&json!({
            "ethereum": {
                "chain_id": "ethereum-mainnet",
                "block_number": 21_000_005,
                "block_hash": "0xprimary-prenormalized",
                "timestamp": "2026-04-17T00:00:05Z"
            }
        }))
        .await?;
    // The marker asserts the stored bytes are the projection's normalized form, so the read path
    // publishes them unchanged. Seeding bytes the current normalizer would rewrite is what makes
    // the two branches distinguishable: a re-normalizing reader would answer "taytems.eth" and
    // silently restate an already-published name after a normalizer revision.
    seed_schema_v2_primary_name_claim(
        &database.lookup_pool,
        V2_ON_DEMAND_PRIMARY_NAME_ADDRESS,
        "ens",
        "60",
        "Taytems.eth",
        true,
    )
    .await?;

    let payload = v2_primary_name_payload_for_database(
        &database,
        &format!(
            "/v2/addresses/{V2_ON_DEMAND_PRIMARY_NAME_ADDRESS}/primary-name?source=indexed"
        ),
    )
    .await?;
    assert_eq!(
        payload["data"]["answers"],
        json!([{
            "source": "indexed",
            "status": "ok",
            "name": "Taytems.eth"
        }])
    );

    database.cleanup().await
}

#[tokio::test]
async fn v2_get_primary_name_excludes_lower_height_orphaned_project_target() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    database
        .seed_snapshot_selector_chain_positions(&json!({
            "ethereum": {
                "chain_id": "ethereum-mainnet",
                "block_number": 21_000_005,
                "block_hash": "0xprimary-readable-head",
                "timestamp": "2026-04-17T00:00:05Z"
            }
        }))
        .await?;
    seed_schema_v2_primary_name_claim(
        &database.lookup_pool,
        V2_ON_DEMAND_PRIMARY_NAME_ADDRESS,
        "ens",
        "60",
        "orphaned.eth",
        true,
    )
    .await?;
    sqlx::query(
        r#"
        INSERT INTO bigname_phase.chain_lineage (
            chain_id, block_hash, block_number, block_timestamp, canonicality_state
        ) VALUES (
            'ethereum-mainnet', '0xorphaned-primary-target', 21000004,
            '2026-04-17T00:00:04Z', 'orphaned'
        )
        "#,
    )
    .execute(&database.lookup_pool)
    .await?;
    let updated = sqlx::query(
        r#"
        UPDATE bigname_phase.primary_names_current
        SET claim_provenance = claim_provenance || jsonb_build_object(
                'chain_id', 'ethereum-mainnet',
                'target_block_number', 21000004,
                'target_block_hash', '0xorphaned-primary-target'
            )
        WHERE address = lower($1)
          AND namespace = 'ens'
          AND coin_type = '60'
        "#,
    )
    .bind(V2_ON_DEMAND_PRIMARY_NAME_ADDRESS)
    .execute(&database.lookup_pool)
    .await?;
    assert_eq!(updated.rows_affected(), 1);

    let payload = v2_primary_name_payload_for_database(
        &database,
        &format!(
            "/v2/addresses/{V2_ON_DEMAND_PRIMARY_NAME_ADDRESS}/primary-name?source=indexed"
        ),
    )
    .await?;
    assert_eq!(
        payload["data"]["answers"],
        json!([{"source": "indexed", "status": "not_found"}])
    );

    database.cleanup().await
}

#[tokio::test]
async fn v2_get_primary_name_rejects_project_change_after_indexed_read() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    database
        .seed_snapshot_selector_chain_positions(&json!({
            "ethereum": {
                "chain_id": "ethereum-mainnet",
                "block_number": 21_000_006,
                "block_hash": "0xprimary-before-publication",
                "timestamp": "2026-04-17T00:00:06Z"
            }
        }))
        .await?;
    seed_schema_v2_primary_name_claim(
        &database.lookup_pool,
        V2_ON_DEMAND_PRIMARY_NAME_ADDRESS,
        "ens",
        "60",
        "before.eth",
        true,
    )
    .await?;
    let (_guard, control) =
        crate::v2::support::indexed_read_test_hooks::install(&database.lookup_pool).await?;
    let state = database.app_state();
    let request_task = tokio::spawn(async move {
        app_router(state)
            .oneshot(
                Request::builder()
                    .uri(format!(
                        "/v2/addresses/{V2_ON_DEMAND_PRIMARY_NAME_ADDRESS}/primary-name?source=indexed"
                    ))
                    .body(Body::empty())
                    .expect("request must build"),
            )
            .await
    });

    control.wait_until_reached().await;
    seed_schema_v2_primary_name_claim(
        &database.lookup_pool,
        V2_ON_DEMAND_PRIMARY_NAME_ADDRESS,
        "ens",
        "60",
        "after.eth",
        true,
    )
    .await?;
    sqlx::query(
        "UPDATE chain_phase_state
         SET updated_at = clock_timestamp()
         WHERE chain_id = 'ethereum-mainnet' AND phase_name = 'project'",
    )
    .execute(&database.lookup_pool)
    .await?;
    control.resume().await;

    let response = request_task
        .await
        .context("indexed primary-name request task panicked")?
        .context("indexed primary-name request failed")?;
    assert_eq!(response.status(), StatusCode::CONFLICT);
    let payload: Value = read_json(response).await?;
    assert_eq!(payload["error"]["code"], json!("stale"));

    database.cleanup().await
}

#[tokio::test]
async fn v2_get_primary_name_rejects_same_head_republication_during_mixed_read() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    database.initialize_lookup_schema().await?;
    database
        .seed_default_ens_primary_name_fallback_context()
        .await?;
    let lookup_pool = database.lookup_pool().await?;
    seed_schema_v2_ens_primary_name_authority(
        &lookup_pool,
        21_000_007,
        "0xprimary-mixed-publication",
        "2026-04-17T00:00:07Z",
    )
    .await?;
    seed_schema_v2_primary_name_claim(
        &lookup_pool,
        V2_ON_DEMAND_PRIMARY_NAME_ADDRESS,
        "ens",
        "60",
        "taytems.eth",
        true,
    )
    .await?;
    let (rpc_url, rpc_handle) = spawn_primary_name_mock_rpc(vec![
        json!("0x000000000000000000000000a2c122be93b0074270ebee7f6b7292c7deb45047"),
        primary_name_reverse_name_response("taytems.eth"),
        primary_name_universal_resolver_addr60_response(V2_ON_DEMAND_PRIMARY_NAME_ADDRESS),
    ])
    .await?;
    let chain_rpc_urls =
        bigname_lookup::ChainRpcUrls::from_entries(&[format!("ethereum-mainnet={rpc_url}")])?;
    let (_guard, control) =
        crate::v2::support::indexed_read_test_hooks::install(&database.lookup_pool).await?;
    let state = database
        .app_state_with_lookup_chain_rpc_urls(chain_rpc_urls)
        .await?;
    let request_task = tokio::spawn(async move {
        app_router(state)
            .oneshot(
                Request::builder()
                    .uri(format!(
                        "/v2/addresses/{V2_ON_DEMAND_PRIMARY_NAME_ADDRESS}/primary-name"
                    ))
                    .body(Body::empty())
                    .expect("request must build"),
            )
            .await
    });

    control.wait_until_reached().await;
    sqlx::query(
        "UPDATE chain_phase_state
         SET updated_at = clock_timestamp()
         WHERE chain_id = 'ethereum-mainnet' AND phase_name = 'project'",
    )
    .execute(&database.lookup_pool)
    .await?;
    control.resume().await;

    let response = request_task
        .await
        .context("mixed primary-name request task panicked")?
        .context("mixed primary-name request failed")?;
    assert_eq!(response.status(), StatusCode::CONFLICT);
    let payload: Value = read_json(response).await?;
    assert_eq!(payload["error"]["code"], json!("stale"));
    assert_eq!(join_primary_name_mock_rpc_requests(rpc_handle).await?.len(), 3);

    lookup_pool.close().await;
    database.cleanup().await
}

#[tokio::test]
async fn v2_get_primary_name_keeps_provider_response_timeout_in_band_without_persistence()
-> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    database.initialize_lookup_schema().await?;
    database
        .seed_default_ens_primary_name_fallback_context()
        .await?;
    let lookup_pool = database.lookup_pool().await?;
    seed_schema_v2_ens_primary_name_authority(
        &lookup_pool,
        21_000_003,
        "0xbinding",
        "2026-04-17T00:00:03Z",
    )
    .await?;
    let (rpc_url, rpc_handle) = spawn_hanging_primary_name_rpc().await?;
    let chain_rpc_urls =
        bigname_lookup::ChainRpcUrls::from_entries(&[format!("ethereum-mainnet={rpc_url}")])?
            .with_http_timeouts(
                std::time::Duration::from_millis(10),
                std::time::Duration::from_millis(25),
            )?;
    let state = database
        .app_state_with_lookup_chain_rpc_urls(chain_rpc_urls)
        .await?;

    let response = app_router(state)
        .oneshot(
            Request::builder()
                .uri(format!(
                    "/v2/addresses/{V2_ON_DEMAND_PRIMARY_NAME_ADDRESS}/primary-name"
                ))
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("v2 timed-out primary-name request failed")?;
    rpc_handle.abort();
    let status = response.status();
    let payload: Value = read_json(response).await?;
    assert_eq!(status, StatusCode::OK, "unexpected response: {payload}");
    assert_eq!(
        payload["data"]["answers"],
        json!([
            {
                "source": "indexed",
                "status": "not_found"
            },
            {
                "source": "verified",
                "status": "failed",
                "failure_reason": "resolver_call_failed"
            }
        ])
    );
    let ledger_count: i64 = sqlx::query_scalar("SELECT count(*) FROM resolution_divergences")
        .fetch_one(&lookup_pool)
        .await?;
    assert_eq!(ledger_count, 0);

    lookup_pool.close().await;
    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_get_primary_name_aborts_provider_transport_failure_without_persistence() -> Result<()>
{
    let database = TestDatabase::new_migrated().await?;
    database.initialize_lookup_schema().await?;
    database
        .seed_default_ens_primary_name_fallback_context()
        .await?;
    let lookup_pool = database.lookup_pool().await?;
    seed_schema_v2_ens_primary_name_authority(
        &lookup_pool,
        21_000_003,
        "0xbinding",
        "2026-04-17T00:00:03Z",
    )
    .await?;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let unavailable_rpc_url = format!("http://{}", listener.local_addr()?);
    drop(listener);
    let chain_rpc_urls = bigname_lookup::ChainRpcUrls::from_entries(&[format!(
        "ethereum-mainnet={unavailable_rpc_url}"
    )])?;
    let state = database
        .app_state_with_lookup_chain_rpc_urls(chain_rpc_urls)
        .await?;

    let response = app_router(state)
        .oneshot(
            Request::builder()
                .uri(format!(
                    "/v2/addresses/{V2_ON_DEMAND_PRIMARY_NAME_ADDRESS}/primary-name?source=verified"
                ))
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("v2 transport-failed primary-name request failed")?;
    let status = response.status();
    let payload: Value = read_json(response).await?;
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR, "{payload}");
    assert_eq!(payload["error"]["code"], json!("internal_error"));
    let ledger_count: i64 = sqlx::query_scalar("SELECT count(*) FROM resolution_divergences")
        .fetch_one(&lookup_pool)
        .await?;
    assert_eq!(ledger_count, 0);

    lookup_pool.close().await;
    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_get_basenames_primary_name_verified_is_explicitly_unsupported_and_base_scoped()
-> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let address = "0x0000000000000000000000000000000000000bcd";
    seed_v2_basenames_primary_name_snapshot_positions(&database).await?;
    seed_v2_basenames_primary_name_claim(&database, address).await?;

    let verified = v2_primary_name_payload_for_database(
        &database,
        &format!(
            "/v2/addresses/{address}/primary-name?namespace=basenames&coin_type={V2_BASENAMES_PRIMARY_COIN_TYPE}&source=verified"
        ),
    )
    .await?;
    assert_eq!(
        verified["data"]["answers"],
        json!([{
            "source": "verified",
            "status": "unsupported",
            "unsupported_reason": "verified primary-name entrypoint is not yet supported"
        }])
    );
    assert_primary_name_snapshot_meta_chain_ids(&verified, &["8453"]);
    assert_primary_name_snapshot_token_slots(&verified, &["base"]);
    assert_eq!(
        verified["meta"]["as_of"]["8453"]["block_hash"],
        json!("0xprimary-base")
    );

    let indexed = v2_primary_name_payload_for_database(
        &database,
        &format!(
            "/v2/addresses/{address}/primary-name?namespace=basenames&coin_type={V2_BASENAMES_PRIMARY_COIN_TYPE}&source=indexed"
        ),
    )
    .await?;
    assert_primary_name_snapshot_meta_chain_ids(&indexed, &["8453"]);
    assert_primary_name_snapshot_token_slots(&indexed, &["base"]);

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_get_basenames_primary_name_normalization_gate_keeps_meta_base_scoped() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let address = "0x0000000000000000000000000000000000000bcf";
    seed_v2_basenames_primary_name_snapshot_positions(&database).await?;
    seed_v2_basenames_primary_name_claim(&database, address).await?;
    database
        .insert_primary_name_current_normalized_claim_name(
            address,
            "basenames",
            V2_BASENAMES_PRIMARY_COIN_TYPE,
            Some("alice.base.eth"),
            false,
        )
        .await?;
    let verified = v2_primary_name_payload_for_database(
        &database,
        &format!(
            "/v2/addresses/{address}/primary-name?namespace=basenames&coin_type={V2_BASENAMES_PRIMARY_COIN_TYPE}&source=verified"
        ),
    )
    .await?;

    assert_eq!(
        verified["data"],
        json!({
            "address": address,
            "coin_type": 2_147_492_101_u64,
            "namespace": "basenames",
            "answers": [{
                "source": "verified",
                "status": "unsupported",
                "unsupported_reason": "verified primary-name entrypoint is not yet supported"
            }]
        })
    );
    assert_primary_name_snapshot_meta_chain_ids(&verified, &["8453"]);
    assert_primary_name_snapshot_token_slots(&verified, &["base"]);

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_get_basenames_primary_name_without_persisted_verified_stays_base_scoped() -> Result<()>
{
    let database = TestDatabase::new_migrated().await?;
    let address = "0x0000000000000000000000000000000000000bce";
    seed_v2_basenames_primary_name_base_snapshot_position(&database).await?;
    database
        .insert_primary_name_current_claim_row(
            address,
            "basenames",
            V2_BASENAMES_PRIMARY_COIN_TYPE,
            PrimaryNameClaimStatus::Success,
            Some("alice.base.eth"),
        )
        .await?;
    database
        .insert_primary_name_current_normalized_claim_name(
            address,
            "basenames",
            V2_BASENAMES_PRIMARY_COIN_TYPE,
            Some("alice.base.eth"),
            true,
        )
        .await?;
    seed_schema_v2_primary_name_claim(
        &database.lookup_pool,
        address,
        "basenames",
        V2_BASENAMES_PRIMARY_COIN_TYPE,
        "alice.base.eth",
        true,
    )
    .await?;

    let verified = v2_primary_name_payload_for_database(
        &database,
        &format!(
            "/v2/addresses/{address}/primary-name?namespace=basenames&coin_type={V2_BASENAMES_PRIMARY_COIN_TYPE}&source=verified"
        ),
    )
    .await?;
    assert_eq!(
        verified["data"]["answers"],
        json!([{
            "source": "verified",
            "status": "unsupported",
            "unsupported_reason": "verified primary-name entrypoint is not yet supported"
        }])
    );
    assert_primary_name_snapshot_meta_chain_ids(&verified, &["8453"]);
    assert_primary_name_snapshot_token_slots(&verified, &["base"]);

    let omitted_source = v2_primary_name_payload_for_database(
        &database,
        &format!(
            "/v2/addresses/{address}/primary-name?namespace=basenames&coin_type={V2_BASENAMES_PRIMARY_COIN_TYPE}"
        ),
    )
    .await?;
    assert_eq!(
        omitted_source["data"]["answers"],
        json!([
            {
                "source": "indexed",
                "status": "ok",
                "name": "alice.base.eth"
            },
            {
                "source": "verified",
                "status": "unsupported",
                "unsupported_reason": "verified primary-name entrypoint is not yet supported"
            }
        ])
    );
    assert_primary_name_snapshot_meta_chain_ids(&omitted_source, &["8453"]);
    assert_primary_name_snapshot_token_slots(&omitted_source, &["base"]);

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_get_primary_name_rejects_malformed_address() -> Result<()> {
    let database = TestDatabase::new(false).await?;

    let response =
        v2_primary_name_response_for_database(&database, "/v2/addresses/not-an-address/primary-name")
            .await?;

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    assert_eq!(
        read_json::<Value>(response).await?["error"]["code"],
        json!("invalid_input")
    );

    database.cleanup().await?;
    Ok(())
}

// Forward verification consults the claimed name's selected exact-name authority before it
// dispatches anything. The RPC endpoint here is dead, so reaching a provider at all would fail the
// whole request with 500: a successful in-band unsupported answer is the proof no call went out.
#[tokio::test]
async fn v2_get_primary_name_refuses_an_unsupported_claim_without_provider_dispatch() -> Result<()>
{
    let database = TestDatabase::new_migrated().await?;
    database.initialize_lookup_schema().await?;
    database
        .seed_default_ens_primary_name_fallback_context()
        .await?;
    let lookup_pool = database.lookup_pool().await?;
    seed_schema_v2_ens_primary_name_authority(
        &lookup_pool,
        21_000_003,
        "0xbinding",
        "2026-04-17T00:00:03Z",
    )
    .await?;
    seed_phase_primary_name_snapshot(
        &database,
        V2_ON_DEMAND_PRIMARY_NAME_ADDRESS,
        "ens",
        "60",
        bigname_storage::PrimaryNameClaimStatus::Success,
        Some("taytems.eth"),
        true,
    )
    .await?;
    // The claimed name's exact-name authority is unsupported: no registration is selected for it,
    // so there is nothing for forward verification to resolve through.
    seed_schema_v2_unsupported_name(&lookup_pool, "ens", "taytems.eth").await?;

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let unavailable_rpc_url = format!("http://{}", listener.local_addr()?);
    drop(listener);
    let chain_rpc_urls = bigname_lookup::ChainRpcUrls::from_entries(&[format!(
        "ethereum-mainnet={unavailable_rpc_url}"
    )])?;
    let state = database
        .app_state_with_lookup_chain_rpc_urls(chain_rpc_urls)
        .await?;

    let response = app_router(state)
        .oneshot(
            Request::builder()
                .uri(format!(
                    "/v2/addresses/{V2_ON_DEMAND_PRIMARY_NAME_ADDRESS}/primary-name?source=verified"
                ))
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("v2 authority-gated primary-name request failed")?;
    let status = response.status();
    let payload: Value = read_json(response).await?;
    assert_eq!(status, StatusCode::OK, "{payload}");

    let verified = payload["data"]["answers"]
        .as_array()
        .expect("answers must be an array")
        .iter()
        .find(|answer| answer["source"] == json!("verified"))
        .expect("a verified answer must be present");
    assert_eq!(verified["status"], json!("unsupported"), "{payload}");
    assert_eq!(
        verified["unsupported_reason"],
        json!("conflicting_current_ens_authority")
    );
    // No provider call ran, so there is no verification outcome to report.
    assert!(payload["data"].get("verification").is_none(), "{payload}");

    lookup_pool.close().await;
    database.cleanup().await?;
    Ok(())
}

const V2_PRIMARY_NAME_ADDRESS: &str = "0x0000000000000000000000000000000000000abc";
const V2_ON_DEMAND_PRIMARY_NAME_ADDRESS: &str = "0x8e8db5ccef88cca9d624701db544989c996e3216";
const V2_BASENAMES_PRIMARY_COIN_TYPE: &str = "2147492101";

async fn v2_primary_name_payload_for_database(
    database: &TestDatabase,
    uri: &str,
) -> Result<Value> {
    let response = v2_primary_name_response_for_database(database, uri).await?;
    assert_eq!(response.status(), StatusCode::OK);
    read_json(response).await
}

async fn v2_primary_name_response_for_database(
    database: &TestDatabase,
    uri: &str,
) -> Result<Response> {
    app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri(uri)
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("v2 primary-name request failed")
}

fn assert_primary_name_snapshot_meta(payload: &Value) {
    assert!(
        payload["meta"]["as_of"].is_object(),
        "primary-name response must include meta.as_of"
    );
    let token = payload["meta"]["as_of_token"]
        .as_str()
        .expect("primary-name response must include meta.as_of_token");
    assert!(!token.is_empty(), "meta.as_of_token must not be empty");
    assert!(
        token
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b'~')),
        "meta.as_of_token must be URL-safe"
    );
}

fn assert_primary_name_snapshot_meta_chain_ids(payload: &Value, expected_chain_ids: &[&str]) {
    assert_primary_name_snapshot_meta(payload);
    let actual = payload["meta"]["as_of"]
        .as_object()
        .expect("primary-name meta.as_of must be an object")
        .keys()
        .map(String::as_str)
        .collect::<std::collections::BTreeSet<_>>();
    let expected = expected_chain_ids
        .iter()
        .copied()
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(actual, expected);
}

fn assert_primary_name_snapshot_token_slots(payload: &Value, expected_slots: &[&str]) {
    let token = payload["meta"]["as_of_token"]
        .as_str()
        .expect("primary-name response must include meta.as_of_token");
    let bigname_storage::SnapshotAt::ResolvedPositions(chain_positions) =
        crate::v2::decode_at_token(token).expect("primary-name token must decode")
    else {
        panic!("primary-name token must contain resolved chain positions");
    };
    let actual = chain_positions
        .as_map()
        .keys()
        .map(String::as_str)
        .collect::<std::collections::BTreeSet<_>>();
    let expected = expected_slots
        .iter()
        .copied()
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(actual, expected);
}

async fn seed_v2_basenames_primary_name_snapshot_positions(database: &TestDatabase) -> Result<()> {
    database
        .seed_snapshot_selector_chain_positions(&json!({
            "base": {
                "chain_id": "base-mainnet",
                "block_number": 84_530_001,
                "block_hash": "0xprimary-base",
                "timestamp": "2026-04-17T00:10:01Z"
            },
            "ethereum": {
                "chain_id": "ethereum-mainnet",
                "block_number": 21_000_010,
                "block_hash": "0xprimary-ethereum",
                "timestamp": "2026-04-17T00:10:00Z"
            }
        }))
        .await
}

async fn seed_v2_basenames_primary_name_base_snapshot_position(
    database: &TestDatabase,
) -> Result<()> {
    database
        .seed_snapshot_selector_chain_positions(&json!({
            "base": {
                "chain_id": "base-mainnet",
                "block_number": 84_530_001,
                "block_hash": "0xprimary-base",
                "timestamp": "2026-04-17T00:10:01Z"
            }
        }))
        .await
}

// Projects one name whose exact-name authority the projection does not support, so a route can be
// asked what it serves for a claim with no selected registration.
async fn seed_schema_v2_unsupported_name(
    pool: &PgPool,
    namespace: &str,
    name: &str,
) -> Result<()> {
    let chain_id = "ethereum-mainnet";
    let (block_number, block_hash): (i64, String) = sqlx::query_as(
        "SELECT block_number, block_hash FROM bigname_phase.chain_lineage \
         WHERE chain_id = $1 \
           AND canonicality_state IN ('canonical', 'safe', 'finalized') \
         ORDER BY block_number DESC, block_hash LIMIT 1",
    )
    .bind(chain_id)
    .fetch_one(pool)
    .await?;
    let namehash = bigname_lookup::ens_namehash_hex(name)?;
    let logical_name_id = format!("{namespace}:{namehash}");
    let labels = name.split('.').collect::<Vec<_>>();
    sqlx::query(
        "INSERT INTO bigname_phase.name_surfaces (
             logical_name_id, namespace, raw_name, raw_labels, dns_encoded_name, namehash,
             labelhashes, normalizer_version, visibility_state,
             chain_id, block_hash, block_number, canonicality_state
         ) VALUES ($1, $2, $3, $4, '\\x00', $5, $6, 'test', 'active', $7, $8, $9, 'canonical')
         ON CONFLICT (logical_name_id) DO NOTHING",
    )
    .bind(&logical_name_id)
    .bind(namespace)
    .bind(name)
    .bind(&labels)
    .bind(&namehash)
    .bind(
        labels
            .iter()
            .enumerate()
            .map(|(index, _)| format!("0x{:064x}", index + 1))
            .collect::<Vec<_>>(),
    )
    .bind(chain_id)
    .bind(&block_hash)
    .bind(block_number)
    .execute(pool)
    .await?;
    sqlx::query(
        "INSERT INTO bigname_phase.name_current (
             logical_name_id, namespace, raw_name, namehash, declared_summary,
             support_status, unsupported_reason, provenance, chain_positions,
             canonicality_summary, manifest_version
         ) VALUES (
             $1, $2, $3, $4, '{}'::jsonb, 'unsupported', 'conflicting_current_ens_authority',
             jsonb_build_object('chain_id', $5::text), $6, $7, 1
         )
         ON CONFLICT (logical_name_id) DO UPDATE SET
             support_status = EXCLUDED.support_status,
             unsupported_reason = EXCLUDED.unsupported_reason",
    )
    .bind(&logical_name_id)
    .bind(namespace)
    .bind(name)
    .bind(&namehash)
    .bind(chain_id)
    .bind(json!({
        "ethereum": {
            "chain_id": chain_id,
            "block_number": block_number,
            "block_hash": block_hash
        }
    }))
    .bind(json!({
        "state": "canonical_lineage",
        "target_block_number": block_number,
        "target_block_hash": block_hash
    }))
    .execute(pool)
    .await?;
    Ok(())
}

async fn seed_schema_v2_primary_name_claim(
    pool: &PgPool,
    address: &str,
    namespace: &str,
    coin_type: &str,
    name: &str,
    claim_name_is_normalized: bool,
) -> Result<()> {
    let chain_id = if namespace == "basenames" {
        "base-mainnet"
    } else {
        "ethereum-mainnet"
    };
    let (target_block_number, target_block_hash): (i64, String) = sqlx::query_as(
        "SELECT block_number, block_hash FROM bigname_phase.chain_lineage \
         WHERE chain_id = $1 \
           AND canonicality_state IN ('canonical', 'safe', 'finalized') \
         ORDER BY block_number DESC, block_hash LIMIT 1",
    )
    .bind(chain_id)
    .fetch_one(pool)
    .await?;
    sqlx::query(
        r#"
        INSERT INTO primary_names_current (
            address,
            coin_type,
            namespace,
            claim_status,
            raw_claim_name,
            claim_name_is_normalized,
            claim_provenance
        )
        VALUES ($1, $3, $2, 'success', $4, $5, $6)
        ON CONFLICT (address, coin_type, namespace) DO UPDATE SET
            claim_status = EXCLUDED.claim_status,
            raw_claim_name = EXCLUDED.raw_claim_name,
            claim_name_is_normalized = EXCLUDED.claim_name_is_normalized,
            unsupported_reason = NULL,
            claim_provenance = EXCLUDED.claim_provenance
        "#,
    )
    .bind(address)
    .bind(namespace)
    .bind(coin_type)
    .bind(name)
    .bind(claim_name_is_normalized)
    .bind(json!({
        "chain_id": chain_id,
        "target_block_number": target_block_number,
        "target_block_hash": target_block_hash,
    }))
    .execute(pool)
    .await?;
    Ok(())
}

async fn seed_v2_basenames_primary_name_claim(
    database: &TestDatabase,
    address: &str,
) -> Result<()> {
    database
        .insert_primary_name_current_claim_row(
            address,
            "basenames",
            V2_BASENAMES_PRIMARY_COIN_TYPE,
            PrimaryNameClaimStatus::Success,
            Some("alice.base.eth"),
        )
        .await?;
    database
        .insert_primary_name_current_normalized_claim_name(
            address,
            "basenames",
            V2_BASENAMES_PRIMARY_COIN_TYPE,
            Some("alice.base.eth"),
            true,
        )
        .await?;
    Ok(())
}
