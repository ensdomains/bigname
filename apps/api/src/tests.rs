include!("tests/support.rs");

#[tokio::test]
async fn healthz_reports_phase_runner_health_from_the_phase_schema() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    sqlx::query(
        r#"
        INSERT INTO bigname_phase.service_heartbeats (
            service_name, instance_id, chain_id, phase_name, started_at, heartbeat_at
        )
        VALUES ('phase-runner', 'api-health', '1', 'live', now(), now())
        "#,
    )
    .execute(&database.lookup_pool)
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
    assert_eq!(payload["loops"]["phase_runner"]["status"], json!("running"));
    assert_eq!(payload["loops"]["phase_runner"]["phase"], json!("live"));
    assert_eq!(
        payload["identity"]["interpreter_content_hash"],
        json!(bigname_content_hash::INTERPRETER_CONTENT_HASH)
    );

    database.cleanup().await
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
include!("tests/v2_query_params.rs");
include!("tests/v2_status.rs");
include!("tests/v2_envelope_conformance.rs");
