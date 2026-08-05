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
    assert_eq!(
        persisted_route_local_primary_name_counts(
            &database,
            V2_ON_DEMAND_PRIMARY_NAME_ADDRESS,
        )
        .await?,
        (0, 0)
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
async fn v2_get_primary_name_rejects_mixed_answers_from_different_positions() -> Result<()> {
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
            None,
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
        21_000_004,
        "0xnewer-phase-head",
        "2026-04-17T00:00:04Z",
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
    assert_eq!(status, StatusCode::CONFLICT, "unexpected response: {payload}");
    assert_eq!(payload["error"]["code"], json!("stale"));

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
            None,
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
    assert_eq!(
        persisted_route_local_primary_name_counts(
            &database,
            V2_ON_DEMAND_PRIMARY_NAME_ADDRESS,
        )
        .await?,
        (0, 0)
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
    assert_eq!(
        persisted_route_local_primary_name_counts(
            &database,
            V2_ON_DEMAND_PRIMARY_NAME_ADDRESS,
        )
        .await?,
        (0, 0)
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
async fn v2_get_basenames_primary_name_verified_is_explicitly_unsupported_and_base_scoped()
-> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let address = "0x0000000000000000000000000000000000000bcd";
    seed_v2_basenames_primary_name_snapshot_positions(&database).await?;
    seed_v2_basenames_primary_name_persisted_verified(&database, address).await?;
    assert_basenames_primary_execution_artifact_slots(&database, address, &["ethereum"]).await?;

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
    seed_v2_basenames_primary_name_persisted_verified(&database, address).await?;
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
            None,
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

async fn persisted_route_local_primary_name_counts(
    database: &TestDatabase,
    address: &str,
) -> Result<(i64, i64)> {
    let request_key = format!("ens:{address}:60");
    let trace_count = sqlx::query_scalar::<_, i64>(
        r#"
        SELECT COUNT(*)
        FROM execution_traces
        WHERE request_type = 'verified_primary_name'
          AND namespace = 'ens'
          AND request_key = $1
        "#,
    )
    .bind(&request_key)
    .fetch_one(&database.pool)
    .await?;
    let outcome_count = sqlx::query_scalar::<_, i64>(
        r#"
        SELECT COUNT(*)
        FROM execution_cache_outcomes
        WHERE request_type = 'verified_primary_name'
          AND namespace = 'ens'
          AND request_key = $1
        "#,
    )
    .bind(request_key)
    .fetch_one(&database.pool)
    .await?;
    Ok((trace_count, outcome_count))
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

async fn assert_basenames_primary_execution_artifact_slots(
    database: &TestDatabase,
    address: &str,
    expected_slots: &[&str],
) -> Result<()> {
    let request_key = format!("basenames:{address}:{V2_BASENAMES_PRIMARY_COIN_TYPE}");
    let requested_positions: Value = sqlx::query_scalar(
        r#"
        SELECT requested_chain_positions
        FROM execution_cache_outcomes
        WHERE request_type = $1
          AND namespace = 'basenames'
          AND request_key = $2
        "#,
    )
    .bind(VERIFIED_PRIMARY_NAME_REQUEST_TYPE)
    .bind(request_key)
    .fetch_one(&database.pool)
    .await?;
    let actual = requested_positions
        .as_array()
        .expect("primary-name requested chain positions must be an array")
        .iter()
        .filter_map(|position| {
            position
                .get("chain_id")
                .and_then(Value::as_str)
                .and_then(|chain_id| chain_id.strip_suffix("-mainnet"))
        })
        .collect::<std::collections::BTreeSet<_>>();
    let expected = expected_slots
        .iter()
        .copied()
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(actual, expected);
    Ok(())
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

async fn seed_v2_basenames_primary_name_persisted_verified(
    database: &TestDatabase,
    address: &str,
) -> Result<()> {
    let execution_trace_id = Uuid::from_u128(0x0e7ec7ace0000000000000000000004a);
    let verified_primary_name = json!({
        "status": "success",
        "name": {
            "logical_name_id": "basenames:alice.base.eth",
            "namespace": "basenames",
            "normalized_name": "alice.base.eth",
            "canonical_display_name": "Alice.base.eth",
            "namehash": "0x0000000000000000000000000000000000000000000000000000000000000b45",
            "resource_id": "00000000-0000-0000-0000-000000000654",
            "binding_kind": "declared_registry_path"
        }
    });

    database
        .insert_primary_name_current_claim_row(
            address,
            "basenames",
            V2_BASENAMES_PRIMARY_COIN_TYPE,
            PrimaryNameClaimStatus::Success,
            None,
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

    let finished_at = timestamp(1_717_172_410);
    let trace = primary_name_execution_trace(
        execution_trace_id,
        "basenames",
        address,
        V2_BASENAMES_PRIMARY_COIN_TYPE,
        verified_primary_name.clone(),
        finished_at,
    );
    let outcome = primary_name_execution_outcome(
        execution_trace_id,
        "basenames",
        address,
        V2_BASENAMES_PRIMARY_COIN_TYPE,
        verified_primary_name,
        finished_at,
    );
    upsert_execution_trace(&database.pool, &trace).await?;
    upsert_execution_outcome(&database.pool, &outcome).await?;
    Ok(())
}
