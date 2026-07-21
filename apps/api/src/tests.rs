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

#[tokio::test]
async fn healthz_reports_ready_when_database_is_reachable() -> Result<()> {
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
