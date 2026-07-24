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
        openapi::app_router_with_health_pool(state, health_pool.clone()).oneshot(
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
        .connect(&database_url)
        .await?;
    let held_health_connection = health_pool.acquire().await?;

    let started = tokio::time::Instant::now();
    let response = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        openapi::app_router_with_health_pool(database.app_state(), health_pool.clone()).oneshot(
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
