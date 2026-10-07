use super::*;

#[path = "v2_read_only_lookup/prepared.rs"]
mod prepared;

const READ_ONLY_GUARD: &str = "bigname_phase.revalidate_resolution_lookup_state_read_only(text,bigint,text,jsonb,jsonb,uuid,text,text)";

async fn read_only_pool(database: &TestDatabase) -> Result<(PgPool, String)> {
    read_only_pool_with(database, 2).await
}

async fn read_only_pool_with(
    database: &TestDatabase,
    max_connections: u32,
) -> Result<(PgPool, String)> {
    let role = format!("readonly_{}", database.database_name);
    sqlx::raw_sql(&format!(
        "CREATE ROLE {role} NOLOGIN; GRANT USAGE ON SCHEMA bigname_phase TO {role};
         GRANT SELECT ON ALL TABLES IN SCHEMA bigname_phase TO {role};
         REVOKE SELECT ON bigname_phase.resolution_divergences FROM {role};
         GRANT EXECUTE ON FUNCTION {READ_ONLY_GUARD} TO {role}"
    ))
    .execute(&database.pool)
    .await?;
    let set_role = format!("SET ROLE {role}");
    let pool = PgPoolOptions::new()
        .max_connections(max_connections)
        .after_connect(move |connection, _| {
            let set_role = set_role.clone();
            Box::pin(async move {
                sqlx::query(&set_role).execute(&mut *connection).await?;
                sqlx::query("SET default_transaction_read_only = on")
                    .execute(connection)
                    .await?;
                Ok(())
            })
        })
        .connect_with(database.pool.connect_options().as_ref().clone())
        .await?;
    assert_eq!(
        sqlx::query_scalar::<_, String>("SHOW transaction_read_only")
            .fetch_one(&pool)
            .await?,
        "on"
    );
    assert!(
        bigname_storage::load_missing_api_lookup_ddl(&pool)
            .await?
            .is_empty()
    );
    // The reader has neither the ledger writer function nor table access.
    let writable: bool = sqlx::query_scalar("SELECT has_table_privilege(current_user, 'bigname_phase.resolution_divergences', 'INSERT,UPDATE,SELECT') OR has_function_privilege(current_user, 'bigname_phase.write_resolution_divergence(uuid,text,text,text,bigint,text,jsonb,text,text,text,text,jsonb,jsonb,boolean)', 'EXECUTE')")
        .fetch_one(&pool).await?;
    assert!(!writable);
    let guard_access: (bool, bool, bool) = sqlx::query_as(
        "SELECT has_function_privilege(current_user, $1, 'EXECUTE'),
         has_function_privilege(current_user,
           'bigname_phase.revalidate_resolution_lookup_state(text,bigint,text,jsonb,jsonb,uuid,text,text,boolean)',
           'EXECUTE'),
         has_function_privilege(current_user,
           'bigname_phase.revalidate_resolution_lookup_state(text,bigint,text,jsonb,jsonb,uuid,text,text)',
           'EXECUTE')",
    )
    .bind(READ_ONLY_GUARD)
    .fetch_one(&pool)
    .await?;
    assert_eq!(guard_access, (true, false, false));
    Ok((pool, role))
}

async fn cleanup_role(database: &TestDatabase, pool: PgPool, role: String) -> Result<()> {
    pool.close().await;
    sqlx::raw_sql(&format!("DROP OWNED BY {role}; DROP ROLE {role}"))
        .execute(&database.pool)
        .await?;
    Ok(())
}

#[tokio::test]
async fn read_only_api_records_verify_without_changing_ledger() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let id = seed_family_lookup_fixture(&database).await?;
    // Retained non-API callers may still write diagnostics. API agreement must not clear
    // this observation, and another disagreement must not refresh its timestamps.
    let (url, handle) =
        spawn_primary_name_mock_rpc(vec![resolution_universal_resolver_addr60_response(
            FAMILY_BOB,
        )])
        .await?;
    let writer = bigname_lookup::LookupEngine::new(
        database.pool.clone(),
        bigname_lookup::ChainRpcUrls::from_entries(&[format!("{FAMILY_CHAIN}={url}")])?,
    );
    assert_eq!(
        writer
            .lookup(bigname_lookup::LookupRequest::new(id, ["addr:60"])?)
            .await?
            .records[0]
            .ledger_action,
        bigname_lookup::LedgerAction::Written
    );
    join_primary_name_mock_rpc_requests(handle).await?;
    let before: Value =
        sqlx::query_scalar("SELECT jsonb_agg(to_jsonb(d)) FROM resolution_divergences d")
            .fetch_one(&database.pool)
            .await?;
    let (pool, role) = read_only_pool(&database).await?;
    let (url, handle) = spawn_primary_name_mock_rpc(vec![
        resolution_universal_resolver_addr60_response(FAMILY_ALICE),
        resolution_universal_resolver_addr60_response(FAMILY_BOB),
    ])
    .await?;
    let state = AppState::new_with_rpc_urls(
        pool.clone(),
        bigname_lookup::ChainRpcUrls::from_entries(&[format!("{FAMILY_CHAIN}={url}")])?,
    )
    .with_public_namespaces_for_test(["ens"]);
    for (query, key, expected) in [
        ("source=verified&keys=addr:60", "addr:60", FAMILY_ALICE),
        ("source=verified&keys=addr:60", "addr:60", FAMILY_BOB),
    ] {
        let response = app_router(state.clone())
            .oneshot(
                Request::builder()
                    .uri(format!("/v1/names/alpha.eth/records?{query}"))
                    .body(Body::empty())?,
            )
            .await?;
        let status = response.status();
        let body: Value = read_json(response).await?;
        assert_eq!(status, StatusCode::OK, "{query}: {body:#}");
        assert_eq!(body["data"]["records"][key]["status"], "ok", "{body:#}");
        assert_eq!(body["data"]["records"][key]["value"], expected, "{body:#}");
    }
    assert_eq!(join_primary_name_mock_rpc_requests(handle).await?.len(), 2);
    let after: Value =
        sqlx::query_scalar("SELECT jsonb_agg(to_jsonb(d)) FROM resolution_divergences d")
            .fetch_one(&database.pool)
            .await?;
    assert_eq!(
        before, after,
        "API reads leave retained diagnostics unchanged"
    );
    drop(state);
    cleanup_role(&database, pool, role).await?;
    database.cleanup().await
}

#[tokio::test]
async fn read_only_api_primary_name_supports_verified_and_default_source() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    database
        .seed_default_ens_primary_name_fallback_context()
        .await?;
    seed_schema_v2_ens_primary_name_authority(
        &database.pool,
        21_000_003,
        "0xbinding",
        "2026-04-17T00:00:03Z",
    )
    .await?;
    let (pool, role) = read_only_pool(&database).await?;
    let replies = (0..2)
        .flat_map(|_| {
            [
                json!("0x000000000000000000000000a2c122be93b0074270ebee7f6b7292c7deb45047"),
                primary_name_reverse_name_response("taytems.eth"),
                primary_name_universal_resolver_addr60_response(V2_ON_DEMAND_PRIMARY_NAME_ADDRESS),
            ]
        })
        .collect();
    let (url, handle) = spawn_primary_name_mock_rpc(replies).await?;
    let state = AppState::new_with_rpc_urls(
        pool.clone(),
        bigname_lookup::ChainRpcUrls::from_entries(&[format!("ethereum-mainnet={url}")])?,
    )
    .with_public_namespaces_for_test(["ens"]);
    for suffix in ["?source=verified", ""] {
        let response = app_router(state.clone())
            .oneshot(
                Request::builder()
                    .uri(format!(
                        "/v1/addresses/{V2_ON_DEMAND_PRIMARY_NAME_ADDRESS}/primary-name{suffix}"
                    ))
                    .body(Body::empty())?,
            )
            .await?;
        let status = response.status();
        let body: Value = read_json(response).await?;
        assert_eq!(status, StatusCode::OK, "{suffix}: {body:#}");
        let verified = body["data"]["answers"]
            .as_array()
            .context("answers")?
            .iter()
            .find(|answer| answer["source"] == "verified")
            .context("verified answer")?;
        assert_eq!(
            verified,
            &json!({"source":"verified","status":"ok","name":"taytems.eth"})
        );
    }
    assert_eq!(join_primary_name_mock_rpc_requests(handle).await?.len(), 6);
    drop(state);
    cleanup_role(&database, pool, role).await?;
    database.cleanup().await
}

#[tokio::test]
async fn read_only_api_rechecks_publication_and_manifest_after_rpc() -> Result<()> {
    for mutation in [
        "UPDATE project_family_marker SET sequence = sequence + 1",
        "UPDATE manifest_versions SET manifest_payload = manifest_payload",
    ] {
        let database = TestDatabase::new_migrated().await?;
        seed_family_lookup_fixture(&database).await?;
        let (pool, role) = read_only_pool(&database).await?;
        let (url, reached, release, handle) =
            spawn_primary_name_mock_rpc_with_last_response_gate(vec![
                resolution_universal_resolver_addr60_response(FAMILY_BOB),
            ])
            .await?;
        let state = AppState::new_with_rpc_urls(
            pool.clone(),
            bigname_lookup::ChainRpcUrls::from_entries(&[format!("{FAMILY_CHAIN}={url}")])?,
        )
        .with_public_namespaces_for_test(["ens"]);
        let request = tokio::spawn(async move {
            app_router(state)
                .oneshot(
                    Request::builder()
                        .uri("/v1/names/alpha.eth/records?source=verified&keys=addr:60")
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
        });
        tokio::time::timeout(std::time::Duration::from_secs(10), reached).await??;
        sqlx::query(mutation).execute(&database.pool).await?;
        release
            .send(())
            .map_err(|_| anyhow::anyhow!("RPC gate closed"))?;
        let response = request.await??;
        let status = response.status();
        let body: Value = read_json(response).await?;
        assert_eq!(status, StatusCode::CONFLICT, "{mutation}: {body:#}");
        assert_eq!(body["error"]["code"], "stale", "{body:#}");
        join_primary_name_mock_rpc_requests(handle).await?;
        cleanup_role(&database, pool, role).await?;
        database.cleanup().await?;
    }
    Ok(())
}

#[tokio::test]
async fn read_only_api_auto_fallback_uses_universal_resolver_discovery() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_schema_v2_ens_record_lookup(
        &database.pool,
        21_000_003,
        "0x1111111111111111111111111111111111111111111111111111111111111111",
        "2026-04-17T00:00:03Z",
        "0x0000000000000000000000000000000000000def",
    )
    .await?;
    append_name_resolver_input(
        &database,
        "ens",
        "alice.eth",
        "0x0000000000000000000000000000000000000000",
    )
    .await?;
    let (pool, role) = read_only_pool(&database).await?;
    let (url, handle) =
        spawn_primary_name_mock_rpc(vec![resolution_universal_resolver_text_response(
            "live-only text",
        )])
        .await?;
    let state = AppState::new_with_rpc_urls(
        pool.clone(),
        bigname_lookup::ChainRpcUrls::from_entries(&[format!("ethereum-mainnet={url}")])?,
    )
    .with_public_namespaces_for_test(["ens"]);
    let response = app_router(state)
        .oneshot(
            Request::builder()
                .uri("/v1/names/alice.eth/records?source=auto&keys=text:url")
                .body(Body::empty())?,
        )
        .await?;
    let status = response.status();
    let body: Value = read_json(response).await?;
    assert_eq!(status, StatusCode::OK, "{body:#}");
    assert_eq!(
        body["data"]["records"]["text:url"]["status"], "ok",
        "{body:#}"
    );
    assert_eq!(
        body["data"]["records"]["text:url"]["value"], "live-only text",
        "{body:#}"
    );
    assert_eq!(body["meta"]["source"], "verified", "{body:#}");
    assert_eq!(join_primary_name_mock_rpc_requests(handle).await?.len(), 1);
    cleanup_role(&database, pool, role).await?;
    database.cleanup().await
}

#[tokio::test]
async fn read_only_connection_reports_misconfigured_non_api_writer() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let id = seed_family_lookup_fixture(&database).await?;
    let (pool, role) = read_only_pool(&database).await?;
    sqlx::query(&format!("GRANT EXECUTE ON FUNCTION bigname_phase.revalidate_resolution_lookup_state(text,bigint,text,jsonb,jsonb,uuid,text,text) TO {role}"))
        .execute(&database.pool).await?;
    let (url, handle) =
        spawn_primary_name_mock_rpc(vec![resolution_universal_resolver_addr60_response(
            FAMILY_BOB,
        )])
        .await?;
    let writer = bigname_lookup::LookupEngine::new(
        pool.clone(),
        bigname_lookup::ChainRpcUrls::from_entries(&[format!("{FAMILY_CHAIN}={url}")])?,
    );
    let error = writer
        .lookup(bigname_lookup::LookupRequest::new(id, ["addr:60"])?)
        .await
        .expect_err("a locking writer cannot run in a read-only transaction");
    assert_eq!(error.kind(), bigname_lookup::ErrorKind::Configuration);
    assert!(error.message().contains("read-only database"), "{error}");
    join_primary_name_mock_rpc_requests(handle).await?;
    drop(writer);
    cleanup_role(&database, pool, role).await?;
    database.cleanup().await
}

/// The namespace route's resolution read and its publication fence run in one read-only snapshot
/// on a single connection.
#[tokio::test]
async fn read_only_namespace_reports_resolution_on_one_connection() -> Result<()> {
    let database = TestDatabase::new(true).await?;
    sync_checked_in_manifests(&database, "sepolia").await?;
    for (block, upgrade) in [
        (11821679, (RESOLUTION_TOP, RESOLUTION_MANAGED)),
        (11821680, (RESOLUTION_MANAGED, RESOLUTION_ADMITTED)),
    ] {
        publish_resolution_block(&database.pool, "ethereum-sepolia", block, Some(upgrade)).await?;
    }
    let (pool, role) = read_only_pool_with(&database, 1).await?;
    let state = AppState::new_with_rpc_urls(pool.clone(), bigname_lookup::ChainRpcUrls::default())
        .with_public_namespaces_for_test(["ens"]);
    let response = app_router(state)
        .oneshot(
            Request::builder()
                .uri("/v1/namespaces/ens")
                .body(Body::empty())?,
        )
        .await?;
    let status = response.status();
    let payload: Value = read_json(response).await?;
    assert_eq!(status, StatusCode::OK, "{payload:#}");
    assert_eq!(
        payload["data"]["networks"],
        sepolia_network(Some(
            json!({ "protocol": "ens_v2", "since_block": 11821680 })
        ))
    );
    cleanup_role(&database, pool, role).await?;
    database.cleanup().await
}
