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
        assert!(loop_health["started_at"].is_string());
        assert!(loop_health["heartbeat_at"].is_string());
        assert!(loop_health["heartbeat_age_seconds"].is_number());
        assert_eq!(loop_health["max_age_seconds"], json!(20));
    }

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
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    let payload: Value = read_json(response).await?;
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
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    let payload: Value = read_json(response).await?;
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
