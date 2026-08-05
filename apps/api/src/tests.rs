include!("tests/support.rs");

fn expected_health_identity() -> Value {
    json!({
        "version": SOFTWARE_VERSION,
        "build_sha": BUILD_SHA,
        "schema_migration_version": bigname_storage::latest_migration_version(),
        "projection_replay_version": bigname_storage::CURRENT_PROJECTION_REPLAY_VERSION,
        "projection_publication_versions": {
            "permissions_current": bigname_storage::PERMISSIONS_CURRENT_PUBLICATION_VERSION,
        },
    })
}

async fn register_ready_health_loops(database: &TestDatabase) -> Result<()> {
    for (service_name, instance_id) in [
        (bigname_storage::INDEXER_SERVICE_NAME, "api-health-indexer"),
        (bigname_storage::WORKER_SERVICE_NAME, "api-health-worker"),
    ] {
        bigname_storage::register_service_loop(&database.pool, service_name, instance_id).await?;
    }
    Ok(())
}

#[tokio::test]
async fn healthz_reports_ready_when_database_is_reachable() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    register_ready_health_loops(&database).await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/healthz")
                .body(Body::empty())
                .unwrap(),
        )
        .await?;

    assert_eq!(response.status(), StatusCode::OK);
    let payload: Value = read_json(response).await?;
    assert_eq!(payload.get("service"), Some(&json!("api")));
    assert_eq!(payload.get("identity"), Some(&expected_health_identity()));
    assert!(payload.get("phase").is_none());
    assert_eq!(payload.get("status"), Some(&json!("ready")));
    assert_eq!(payload.get("api_status"), Some(&json!("ready")));
    assert_eq!(
        payload.get("process"),
        Some(&json!({
            "status": "running",
        }))
    );
    assert_eq!(
        payload.get("database"),
        Some(&json!({
            "status": "reachable",
            "reachable": true,
            "check": "select_1",
            "error": null,
        }))
    );
    for service_name in ["indexer", "worker"] {
        let loop_health = &payload["loops"][service_name];
        assert_eq!(loop_health["status"], json!("running"));
        assert_eq!(loop_health["phase"], Value::Null);
        assert!(loop_health["started_at"].is_string());
        assert!(loop_health["heartbeat_at"].is_string());
        assert!(loop_health["heartbeat_age_seconds"].is_number());
        assert_eq!(loop_health["max_age_seconds"], json!(20));
    }

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn documented_api_role_persists_retained_v1_cache_misses() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let suffix = database
        .database_name
        .chars()
        .rev()
        .take(40)
        .collect::<String>()
        .chars()
        .rev()
        .collect::<String>();
    let role_name = format!("api_v1_{suffix}");
    let role = format!(r#""{}""#, role_name.replace('"', r#""""#));
    raw_sql(&format!(
        "CREATE ROLE {role}
             NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT
             NOREPLICATION NOBYPASSRLS;
         GRANT {role} TO CURRENT_USER;
         GRANT USAGE ON SCHEMA public TO {role};
         GRANT SELECT ON ALL TABLES IN SCHEMA public TO {role};
         GRANT EXECUTE ON FUNCTION public.bigname_lock_primary_name_anchor(
             text, text, text
         ) TO {role};
         GRANT INSERT ON TABLE
             public.execution_traces,
             public.execution_steps,
             public.execution_cache_outcomes
         TO {role};
         GRANT UPDATE ON TABLE public.execution_cache_outcomes TO {role};
         GRANT INSERT, UPDATE ON TABLE public.raw_call_snapshots TO {role};
         GRANT USAGE ON SEQUENCE public.raw_call_snapshots_raw_call_snapshot_id_seq
         TO {role};"
    ))
    .execute(&database.pool)
    .await?;

    let connect_options = bigname_storage::stamp_projection_replay_version(
        database
            .pool
            .connect_options()
            .as_ref()
            .clone()
            .options([("role", role_name.as_str())]),
    );
    let role_pool = PgPoolOptions::new()
        .max_connections(2)
        .connect_with(connect_options)
        .await?;
    let projection_write_error = sqlx::query(
        "UPDATE public.primary_names_current
         SET claim_status = claim_status
         WHERE false",
    )
    .execute(&role_pool)
    .await
    .expect_err("the API role must not update the retained primary-name projection directly");
    assert_eq!(
        projection_write_error
            .as_database_error()
            .and_then(|error| error.code().map(|code| code.into_owned()))
            .as_deref(),
        Some("42501")
    );
    let mut snapshot = bigname_storage::RawCallSnapshot {
        chain_id: "ethereum-mainnet".to_owned(),
        block_hash: format!("0x{}", "11".repeat(32)),
        block_number: 21_000_003,
        request_hash: format!("0x{}", "22".repeat(32)),
        request_payload: json!({
            "to": "0x0000000000000000000000000000000000000001",
            "data": "0x1234"
        }),
        response_hash: format!("0x{}", "33".repeat(32)),
        response_payload: json!({ "result": "0xabcd" }),
        canonicality_state: CanonicalityState::Observed,
    };
    bigname_storage::upsert_raw_call_snapshots(&role_pool, &[snapshot.clone()]).await?;
    snapshot.canonicality_state = CanonicalityState::Canonical;
    let refreshed =
        bigname_storage::upsert_raw_call_snapshots(&role_pool, &[snapshot.clone()]).await?;
    assert_eq!(refreshed, vec![snapshot]);

    let claim = bigname_execution::RouteLocalEnsPrimaryNameClaim::NotFound;
    let evidence = bigname_execution::OnDemandEnsPrimaryNameExecutionEvidence {
        contracts_called: vec![json!({
            "chain_id": bigname_execution::ETHEREUM_MAINNET_CHAIN_ID,
            "contract_address": bigname_execution::ENS_REGISTRY_ADDRESS,
            "selector": "0x0178b8bf",
        })],
        ..Default::default()
    };
    let primary_name_request =
        bigname_execution::build_on_demand_ens_verified_primary_name_request(
            bigname_execution::BuildOnDemandEnsVerifiedPrimaryNameRequest {
                normalized_address: "0x00000000000000000000000000000000000000af",
                claim: &claim,
                verified_primary_name: json!({ "status": "not_found" }),
                block_number: 21_000_003,
                block_hash: "0xabc123",
                block_timestamp: "2026-08-05T00:00:00Z",
                manifest_versions: json!([{
                    "source_family": bigname_execution::ENS_EXECUTION_SOURCE_FAMILY,
                    "manifest_version": 1,
                }]),
                forward_call_attempted: false,
                reverse_latency_ms: 2,
                forward_latency_ms: None,
                execution_evidence: &evidence,
            },
        )?;
    let persisted =
        bigname_execution::persist_ens_verified_primary_name(&role_pool, &primary_name_request)
            .await?;
    assert_eq!(
        persisted.execution_trace_id,
        primary_name_request.trace.execution_trace_id
    );

    role_pool.close().await;
    raw_sql(&format!(
        "DROP OWNED BY {role};
         REVOKE {role} FROM CURRENT_USER;
         DROP ROLE {role};"
    ))
    .execute(&database.pool)
    .await?;
    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn healthz_returns_ready_within_probe_window_when_request_pool_is_exhausted() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    register_ready_health_loops(&database).await?;
    let request_pool = bigname_storage::connect_with_application_name_and_statement_timeout(
        &database.database_config(2)?,
        "bigname-api-exhausted-pool-test",
        std::time::Duration::from_secs(25),
    )
    .await?;
    let health_pool = bigname_storage::connect_reserved_readiness_pool(
        &database.database_config(2)?,
        "bigname-api-health-exhausted-pool-test",
        HEALTH_DATABASE_CHECK_TIMEOUT,
    )
    .await?;
    let state = AppState::new(
        request_pool.clone(),
        bigname_execution::ChainRpcUrls::default(),
    );

    let mut held_connections = Vec::new();
    for _ in 0..request_pool.options().get_max_connections() {
        held_connections.push(request_pool.acquire().await?);
    }
    assert_eq!(request_pool.num_idle(), 0);

    let started = tokio::time::Instant::now();
    let response = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        app_router_with_health_pool(state, health_pool.clone()).oneshot(
            Request::builder()
                .uri("/healthz")
                .body(Body::empty())
                .unwrap(),
        ),
    )
    .await
    .context("health request exceeded the compose probe's five-second window")??;
    let elapsed = started.elapsed();

    assert!(elapsed < std::time::Duration::from_secs(5));
    assert_eq!(response.status(), StatusCode::OK);
    let payload: Value = read_json(response).await?;
    assert_eq!(payload.get("status"), Some(&json!("ready")));
    assert_eq!(payload["database"]["status"], json!("reachable"));
    assert_eq!(payload["database"]["reachable"], json!(true));

    drop(held_connections);
    request_pool.close().await;
    health_pool.close().await;
    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn healthz_reports_degraded_within_probe_window_when_health_pool_is_exhausted() -> Result<()>
{
    let database = TestDatabase::new_migrated().await?;
    let database_url = database
        .database_config(1)?
        .database_url
        .context("health pool test database URL must be configured")?;
    let health_pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .acquire_timeout(std::time::Duration::from_secs(10))
        .connect_with(bigname_storage::stamp_projection_replay_version(
            database_url.parse()?,
        ))
        .await?;
    let held_health_connection = health_pool.acquire().await?;

    let started = tokio::time::Instant::now();
    let response = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        app_router_with_health_pool(database.app_state(), health_pool.clone()).oneshot(
            Request::builder()
                .uri("/healthz")
                .body(Body::empty())
                .unwrap(),
        ),
    )
    .await
    .context("timed-out database check exceeded the compose probe's five-second window")??;
    let elapsed = started.elapsed();

    assert!(elapsed >= std::time::Duration::from_millis(1_900));
    assert!(elapsed < std::time::Duration::from_secs(5));
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    let payload: Value = read_json(response).await?;
    assert_eq!(payload.get("status"), Some(&json!("degraded")));
    assert_eq!(payload["database"]["status"], json!("unreachable"));
    assert_eq!(payload["database"]["reachable"], json!(false));

    drop(held_health_connection);
    health_pool.close().await;
    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn healthz_reports_degraded_when_database_is_unreachable() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let state = database.app_state();
    state.pool.close().await;

    let response = app_router(state)
        .oneshot(
            Request::builder()
                .uri("/healthz")
                .body(Body::empty())
                .unwrap(),
        )
        .await?;

    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    let payload: Value = read_json(response).await?;
    assert_eq!(payload.get("service"), Some(&json!("api")));
    assert_eq!(payload.get("identity"), Some(&expected_health_identity()));
    assert!(payload.get("phase").is_none());
    assert_eq!(payload.get("status"), Some(&json!("degraded")));
    assert_eq!(payload.get("api_status"), Some(&json!("degraded")));
    assert_eq!(
        payload.get("process"),
        Some(&json!({
            "status": "running",
        }))
    );
    assert_eq!(
        payload.get("database"),
        Some(&json!({
            "status": "unreachable",
            "reachable": false,
            "check": "select_1",
            "error": "database readiness query failed",
        }))
    );
    assert_eq!(payload["loops"]["indexer"]["status"], json!("unavailable"));
    assert_eq!(payload["loops"]["worker"]["status"], json!("unavailable"));

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn healthz_distinguishes_not_started_and_stale_service_loops() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/healthz")
                .body(Body::empty())
                .unwrap(),
        )
        .await?;
    assert_eq!(response.status(), StatusCode::OK);
    let payload: Value = read_json(response).await?;
    assert_eq!(payload["status"], json!("degraded"));
    assert_eq!(payload["api_status"], json!("ready"));
    assert_eq!(payload["loops"]["indexer"]["status"], json!("not_started"));
    assert_eq!(payload["loops"]["worker"]["status"], json!("not_started"));

    register_ready_health_loops(&database).await?;
    sqlx::query(
        r#"
        UPDATE service_loop_heartbeats
        SET started_at = clock_timestamp() - INTERVAL '2 minutes',
            heartbeat_at = clock_timestamp() - INTERVAL '1 minute'
        WHERE service_name = 'indexer'
          AND instance_id = 'api-health-indexer'
        "#,
    )
    .execute(&database.pool)
    .await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/healthz")
                .body(Body::empty())
                .unwrap(),
        )
        .await?;
    assert_eq!(response.status(), StatusCode::OK);
    let payload: Value = read_json(response).await?;
    assert_eq!(payload["status"], json!("degraded"));
    assert_eq!(payload["api_status"], json!("ready"));
    assert_eq!(payload["loops"]["indexer"]["status"], json!("stale"));
    assert_eq!(payload["loops"]["worker"]["status"], json!("running"));
    assert!(payload["loops"]["indexer"]["started_at"].is_string());
    assert!(payload["loops"]["indexer"]["heartbeat_at"].is_string());
    assert!(
        payload["loops"]["indexer"]["heartbeat_age_seconds"]
            .as_i64()
            .is_some_and(|age| age >= 60)
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn healthz_honors_the_indexer_chain_threshold_independently_of_process_age() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    register_ready_health_loops(&database).await?;
    let wedged_chain = "ethereum-mainnet";
    let peer_chain = "base-mainnet";
    bigname_storage::record_service_loop_heartbeat(
        &database.pool,
        bigname_storage::INDEXER_SERVICE_NAME,
        "api-health-indexer",
        &[wedged_chain.to_owned(), peer_chain.to_owned()],
    )
    .await?;
    sqlx::query(
        r#"
        UPDATE service_loop_heartbeats
        SET started_at = clock_timestamp() - INTERVAL '3 minutes',
            heartbeat_at = clock_timestamp() - INTERVAL '2 minutes'
        WHERE service_name = 'indexer'
          AND instance_id = 'api-health-indexer'
          AND scope_kind = 'chain'
          AND scope_id = $1
        "#,
    )
    .bind(wedged_chain)
    .execute(&database.pool)
    .await?;
    let response = app_router(
        database
            .app_state()
            .with_heartbeat_max_age_secs(3_600)
            .with_indexer_chain_heartbeat_max_age_secs(60),
    )
    .oneshot(
        Request::builder()
            .uri("/healthz")
            .body(Body::empty())
            .unwrap(),
    )
    .await?;
    assert_eq!(response.status(), StatusCode::OK);
    let payload: Value = read_json(response).await?;
    assert_eq!(payload["status"], json!("degraded"));
    assert_eq!(payload["api_status"], json!("ready"));
    assert_eq!(payload["loops"]["indexer"]["status"], json!("stale"));
    assert_eq!(payload["loops"]["indexer"]["phase"], Value::Null);
    assert_eq!(payload["loops"]["indexer"]["max_age_seconds"], json!(60));
    assert!(
        payload["loops"]["indexer"]["heartbeat_age_seconds"]
            .as_i64()
            .is_some_and(|age| age >= 2 * 60)
    );
    assert_eq!(payload["loops"]["worker"]["status"], json!("running"));

    database.cleanup().await
}

#[tokio::test]
async fn api_pool_applies_statement_timeout_to_every_connection() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let pool = bigname_storage::connect_with_application_name_and_statement_timeout(
        &database.database_config(3)?,
        "bigname-api-test",
        std::time::Duration::from_millis(75),
    )
    .await?;
    let mut connections = Vec::new();
    for _ in 0..3 {
        connections.push(pool.acquire().await?);
    }
    for connection in &mut connections {
        let timeout = sqlx::query_scalar::<_, String>("SHOW statement_timeout")
            .fetch_one(&mut **connection)
            .await?;
        assert_eq!(timeout, "75ms");
    }
    drop(connections);

    let timeout_error = sqlx::query("SELECT pg_sleep(0.2)")
        .execute(&pool)
        .await
        .expect_err("statement timeout must cancel a slow query");
    assert!(matches!(
        timeout_error,
        sqlx::Error::Database(ref error) if error.code().as_deref() == Some("57014")
    ));

    pool.close().await;
    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn lookup_pool_applies_phase_schema_and_statement_timeout_to_every_connection() -> Result<()>
{
    let database = TestDatabase::new_migrated().await?;
    database.initialize_lookup_schema().await?;
    let pool = crate::state::connect_lookup_pool(
        &database.database_config(2)?,
        "bigname-api-lookup-test",
        std::time::Duration::from_millis(75),
    )
    .await?;
    let mut connections = Vec::new();
    for _ in 0..2 {
        connections.push(pool.acquire().await?);
    }
    for connection in &mut connections {
        let search_path = sqlx::query_scalar::<_, String>("SHOW search_path")
            .fetch_one(&mut **connection)
            .await?;
        let timeout = sqlx::query_scalar::<_, String>("SHOW statement_timeout")
            .fetch_one(&mut **connection)
            .await?;
        assert_eq!(search_path, "bigname_phase");
        assert_eq!(timeout, "75ms");
    }
    drop(connections);

    let timeout_error = sqlx::query("SELECT pg_sleep(0.2)")
        .execute(&pool)
        .await
        .expect_err("lookup statement timeout must cancel a slow query");
    assert!(matches!(
        timeout_error,
        sqlx::Error::Database(ref error) if error.code().as_deref() == Some("57014")
    ));

    pool.close().await;
    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn healthz_uses_the_worker_phase_threshold_during_monolithic_rebuild_work() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    register_ready_health_loops(&database).await?;
    bigname_storage::begin_service_loop_phase(
        &database.pool,
        bigname_storage::WORKER_SERVICE_NAME,
        "api-health-worker",
        "resolver_current.publish",
    )
    .await?;
    sqlx::query(
        r#"
        UPDATE service_loop_heartbeats
        SET started_at = clock_timestamp() - INTERVAL '2 minutes',
            heartbeat_at = clock_timestamp() - INTERVAL '1 minute'
        WHERE service_name = 'worker'
          AND instance_id = 'api-health-worker'
          AND scope_kind = 'process'
        "#,
    )
    .execute(&database.pool)
    .await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/healthz")
                .body(Body::empty())
                .unwrap(),
        )
        .await?;
    assert_eq!(response.status(), StatusCode::OK);
    let payload: Value = read_json(response).await?;
    assert_eq!(payload["status"], json!("ready"));
    assert_eq!(payload["api_status"], json!("ready"));
    assert_eq!(payload["loops"]["worker"]["status"], json!("running"));
    assert_eq!(
        payload["loops"]["worker"]["phase"],
        json!("resolver_current.publish")
    );
    assert_eq!(
        payload["loops"]["worker"]["max_age_seconds"],
        json!(bigname_storage::DEFAULT_WORKER_REBUILD_PHASE_MAX_AGE_SECS)
    );

    database.cleanup().await
}

#[tokio::test]
async fn healthz_reaps_a_dead_worker_phase_when_a_replacement_registers() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    bigname_storage::register_service_loop(
        &database.pool,
        bigname_storage::INDEXER_SERVICE_NAME,
        "api-health-indexer",
    )
    .await?;
    bigname_storage::register_service_loop(
        &database.pool,
        bigname_storage::WORKER_SERVICE_NAME,
        "dead-mid-phase-worker",
    )
    .await?;
    bigname_storage::begin_service_loop_phase(
        &database.pool,
        bigname_storage::WORKER_SERVICE_NAME,
        "dead-mid-phase-worker",
        "name_current.publish",
    )
    .await?;
    sqlx::query(
        r#"
        UPDATE service_loop_heartbeats
        SET started_at = clock_timestamp() - INTERVAL '2 minutes',
            heartbeat_at = clock_timestamp() - INTERVAL '1 minute'
        WHERE service_name = 'worker'
          AND instance_id = 'dead-mid-phase-worker'
        "#,
    )
    .execute(&database.pool)
    .await?;

    bigname_storage::register_service_loop(
        &database.pool,
        bigname_storage::WORKER_SERVICE_NAME,
        "replacement-worker",
    )
    .await?;
    let orphaned_phase_count = sqlx::query_scalar::<_, i64>(
        r#"
        SELECT COUNT(*)::BIGINT
        FROM service_loop_heartbeats
        WHERE service_name = 'worker'
          AND instance_id = 'dead-mid-phase-worker'
          AND scope_kind = 'phase'
        "#,
    )
    .fetch_one(&database.pool)
    .await?;
    assert_eq!(
        orphaned_phase_count, 0,
        "replacement registration must reap the dead predecessor's phase"
    );

    sqlx::query(
        r#"
        UPDATE service_loop_heartbeats
        SET started_at = clock_timestamp() - INTERVAL '2 minutes',
            heartbeat_at = clock_timestamp() - INTERVAL '1 minute'
        WHERE service_name = 'worker'
          AND scope_kind = 'process'
        "#,
    )
    .execute(&database.pool)
    .await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/healthz")
                .body(Body::empty())
                .unwrap(),
        )
        .await?;
    assert_eq!(response.status(), StatusCode::OK);
    let payload: Value = read_json(response).await?;
    assert_eq!(payload["status"], json!("degraded"));
    assert_eq!(payload["loops"]["worker"]["status"], json!("stale"));
    assert_eq!(payload["loops"]["worker"]["phase"], Value::Null);

    database.cleanup().await
}

#[tokio::test]
async fn healthz_prefers_a_healthy_worker_phase_over_a_newer_stale_replica() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    register_ready_health_loops(&database).await?;

    bigname_storage::register_service_loop(
        &database.pool,
        bigname_storage::WORKER_SERVICE_NAME,
        "newer-stale-worker",
    )
    .await?;
    sqlx::query(
        r#"
        UPDATE service_loop_heartbeats
        SET started_at = clock_timestamp() - INTERVAL '90 minutes',
            heartbeat_at = clock_timestamp() - INTERVAL '1 hour'
        WHERE service_name = 'worker'
          AND instance_id = 'newer-stale-worker'
        "#,
    )
    .execute(&database.pool)
    .await?;

    bigname_storage::begin_service_loop_phase(
        &database.pool,
        bigname_storage::WORKER_SERVICE_NAME,
        "api-health-worker",
        "resolver_current.publish",
    )
    .await?;
    sqlx::query(
        r#"
        UPDATE service_loop_heartbeats
        SET started_at = clock_timestamp() - INTERVAL '3 hours',
            heartbeat_at = clock_timestamp() - INTERVAL '2 hours'
        WHERE service_name = 'worker'
          AND instance_id = 'api-health-worker'
        "#,
    )
    .execute(&database.pool)
    .await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/healthz")
                .body(Body::empty())
                .unwrap(),
        )
        .await?;
    assert_eq!(response.status(), StatusCode::OK);
    let payload: Value = read_json(response).await?;
    assert_eq!(payload["status"], json!("ready"));
    assert_eq!(payload["loops"]["worker"]["status"], json!("running"));
    assert_eq!(
        payload["loops"]["worker"]["phase"],
        json!("resolver_current.publish")
    );

    database.cleanup().await
}

#[tokio::test]
async fn healthz_ranks_indexer_phases_with_the_indexer_threshold() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    register_ready_health_loops(&database).await?;
    bigname_storage::register_service_loop(
        &database.pool,
        bigname_storage::INDEXER_SERVICE_NAME,
        "newer-stale-indexer",
    )
    .await?;
    bigname_storage::begin_service_loop_phase(
        &database.pool,
        bigname_storage::INDEXER_SERVICE_NAME,
        "api-health-indexer",
        "full_closure_replay_lock.wait",
    )
    .await?;
    sqlx::query(
        r#"
        UPDATE service_loop_heartbeats
        SET started_at = clock_timestamp() - INTERVAL '3 minutes',
            heartbeat_at = clock_timestamp() - INTERVAL '2 minutes'
        WHERE service_name = 'indexer'
          AND instance_id = 'api-health-indexer'
        "#,
    )
    .execute(&database.pool)
    .await?;
    sqlx::query(
        r#"
        UPDATE service_loop_heartbeats
        SET started_at = clock_timestamp() - INTERVAL '2 minutes',
            heartbeat_at = clock_timestamp() - INTERVAL '1 minute'
        WHERE service_name = 'indexer'
          AND instance_id = 'newer-stale-indexer'
        "#,
    )
    .execute(&database.pool)
    .await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/healthz")
                .body(Body::empty())
                .unwrap(),
        )
        .await?;
    assert_eq!(response.status(), StatusCode::OK);
    let payload: Value = read_json(response).await?;
    assert_eq!(payload["status"], json!("degraded"));
    assert_eq!(payload["loops"]["indexer"]["status"], json!("stale"));
    assert_eq!(payload["loops"]["indexer"]["phase"], Value::Null);
    assert!(
        payload["loops"]["indexer"]["heartbeat_age_seconds"]
            .as_i64()
            .is_some_and(|age| (60..120).contains(&age))
    );

    database.cleanup().await
}

include!("tests/exact_name.rs");

include!("tests/resolution.rs");

include!("tests/collections.rs");

include!("tests/names_collection.rs");

include!("tests/graphql.rs");

include!("tests/graphql_contract.rs");

include!("tests/records.rs");

include!("tests/identity.rs");

include!("tests/events.rs");

include!("tests/roles.rs");

include!("tests/resolvers.rs");

include!("tests/history.rs");

include!("tests/namespaces.rs");

include!("tests/primary_names.rs");

include!("tests/v2_name_record.rs");

include!("tests/v2_diagnostics_names.rs");

include!("tests/v2_history.rs");

include!("tests/v2_diag_events.rs");

include!("tests/v2_address_names.rs");

include!("tests/v2_permissions.rs");

include!("tests/v2_resolvers.rs");

include!("tests/v2_primary_name.rs");

include!("tests/v2_lookup.rs");

include!("tests/v2_query_params.rs");

include!("tests/v2_envelope_conformance.rs");

include!("tests/openapi.rs");
