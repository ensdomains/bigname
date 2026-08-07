include!("tests/support.rs");

#[tokio::test]
async fn healthz_reports_phase_runner_health_from_the_phase_schema() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_expected_phase_chains(&database, &["1"]).await?;
    seed_phase_runner_heartbeat(&database, "1", "now()").await?;

    let payload = healthz_payload(&database).await?;
    assert_eq!(payload["status"], json!("ready"));
    assert_eq!(payload["api_status"], json!("ready"));
    assert_eq!(payload["loops"]["phase_runner"]["status"], json!("running"));
    assert_eq!(payload["loops"]["phase_runner"]["phase"], json!("live"));
    assert_eq!(
        payload["identity"]["interpreter_content_hash"],
        json!(bigname_content_hash::INTERPRETER_CONTENT_HASH)
    );

    database.cleanup().await
}

#[tokio::test]
async fn healthz_reports_not_started_without_any_phase_runner_heartbeat() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_expected_phase_chains(&database, &["1", "8453"]).await?;

    let payload = healthz_payload(&database).await?;
    assert_eq!(
        payload["loops"]["phase_runner"]["status"],
        json!("not_started")
    );
    assert_eq!(payload["status"], json!("degraded"));
    assert_eq!(payload["api_status"], json!("ready"));

    database.cleanup().await
}

#[tokio::test]
async fn healthz_judges_the_worst_expected_chain_not_the_freshest_heartbeat() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_expected_phase_chains(&database, &["1", "8453"]).await?;
    seed_phase_runner_heartbeat(&database, "1", "now()").await?;

    let missing = healthz_payload(&database).await?;
    let missing_loop = &missing["loops"]["phase_runner"];
    assert_eq!(missing_loop["status"], json!("stale"));
    assert_eq!(missing["status"], json!("degraded"));
    assert_eq!(missing["api_status"], json!("ready"));
    // A chain with no heartbeat is not described by another chain's evidence. The keys stay
    // present and carry null, so index-and-compare would also pass on a dropped key.
    for field in [
        "phase",
        "started_at",
        "heartbeat_at",
        "heartbeat_age_seconds",
    ] {
        let value = missing_loop
            .get(field)
            .unwrap_or_else(|| panic!("{field} must be present"));
        assert!(value.is_null(), "{field} must be null, got {value}");
    }
    assert_eq!(missing_loop["max_age_seconds"], json!(60));

    seed_phase_runner_heartbeat(&database, "8453", "now() - interval '10 minutes'").await?;
    let stalled = healthz_payload(&database).await?;
    assert_eq!(stalled["loops"]["phase_runner"]["status"], json!("stale"));
    assert!(
        stalled["loops"]["phase_runner"]["heartbeat_age_seconds"]
            .as_i64()
            .expect("stale heartbeat age must be reported")
            >= 600
    );

    sqlx::query(
        "UPDATE bigname_phase.service_heartbeats SET heartbeat_at = now() \
         WHERE service_name = 'phase-runner' AND chain_id = '8453'",
    )
    .execute(&database.lookup_pool)
    .await?;
    let recovered = healthz_payload(&database).await?;
    assert_eq!(
        recovered["loops"]["phase_runner"]["status"],
        json!("running")
    );
    assert_eq!(recovered["status"], json!("ready"));

    database.cleanup().await
}

/// The phase runner only ever heartbeats a chain that already has a running phase row, so every
/// heartbeat fixture needs the phase state that admits it.
async fn seed_expected_phase_chains(database: &TestDatabase, chain_ids: &[&str]) -> Result<()> {
    for chain_id in chain_ids {
        sqlx::query(
            "INSERT INTO bigname_phase.chain_phase_state ( \
                 chain_id, phase_name, phase_status, started_at \
             ) VALUES ($1, 'live', 'running', now())",
        )
        .bind(chain_id)
        .execute(&database.lookup_pool)
        .await?;
    }
    Ok(())
}

async fn seed_phase_runner_heartbeat(
    database: &TestDatabase,
    chain_id: &str,
    heartbeat_at_sql: &str,
) -> Result<()> {
    sqlx::query(&format!(
        "INSERT INTO bigname_phase.service_heartbeats ( \
             service_name, instance_id, chain_id, phase_name, started_at, heartbeat_at \
         ) VALUES ('phase-runner', 'api-health', $1, 'live', {heartbeat_at_sql}, \
         {heartbeat_at_sql})"
    ))
    .bind(chain_id)
    .execute(&database.lookup_pool)
    .await?;
    Ok(())
}

async fn healthz_payload(database: &TestDatabase) -> Result<Value> {
    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/healthz")
                .body(Body::empty())
                .expect("health request must build"),
        )
        .await?;
    assert_eq!(response.status(), StatusCode::OK);
    read_json(response).await
}

include!("tests/graphql.rs");
include!("tests/graphql_contract.rs");
include!("tests/v2_name_record.rs");
include!("tests/v2_diagnostics_names.rs");
include!("tests/v2_history.rs");
include!("tests/v2_diag_events.rs");
include!("tests/v2_address_names.rs");
include!("tests/v2_permissions.rs");
include!("tests/v2_resolvers.rs");
include!("tests/v2_primary_name.rs");
include!("tests/v2_lookup.rs");
include!("tests/v2_search.rs");
include!("tests/v2_query_params.rs");
include!("tests/v2_status.rs");
include!("tests/v2_envelope_conformance.rs");
