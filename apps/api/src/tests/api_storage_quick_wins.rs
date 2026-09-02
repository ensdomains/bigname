#[tokio::test]
async fn api_startup_preflight_reports_missing_lookup_relation() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    sqlx::query(
        "ALTER TABLE bigname_phase.record_inventory_current \
         RENAME TO record_inventory_current_preflight_missing",
    )
    .execute(&database.lookup_pool)
    .await?;

    let first_error = crate::startup_preflight::ensure_api_storage_compatible(&database.lookup_pool)
        .await
        .expect_err("startup must reject a missing lookup relation");
    let second_error =
        crate::startup_preflight::ensure_api_storage_compatible(&database.lookup_pool)
            .await
            .expect_err("startup diagnostics must be repeatable");
    let expected = "API storage compatibility preflight failed: required lookup DDL is missing\n\
                    relation: bigname_phase.record_inventory_current";

    assert_eq!(format!("{first_error:#}"), expected);
    assert_eq!(format!("{second_error:#}"), expected);
    database.cleanup().await
}

#[tokio::test]
async fn api_startup_preflight_reports_missing_lookup_guard_function() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    sqlx::query(
        "ALTER FUNCTION bigname_phase.revalidate_resolution_lookup_state( \
             text, bigint, text, jsonb, jsonb, uuid, text, text \
         ) RENAME TO revalidate_resolution_lookup_state_preflight_missing",
    )
    .execute(&database.lookup_pool)
    .await?;

    let error = crate::startup_preflight::ensure_api_storage_compatible(&database.lookup_pool)
        .await
        .expect_err("startup must reject a missing guarded lookup function");

    assert_eq!(
        format!("{error:#}"),
        "API storage compatibility preflight failed: required lookup DDL is missing\n\
         function: bigname_phase.revalidate_resolution_lookup_state(text,bigint,text,jsonb,jsonb,uuid,text,text)"
    );
    database.cleanup().await
}

#[tokio::test]
async fn api_startup_preflight_accepts_current_lookup_schema() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;

    crate::startup_preflight::ensure_api_storage_compatible(&database.lookup_pool).await?;

    database.cleanup().await
}

#[tokio::test]
async fn healthz_bounds_phase_runner_heartbeat_query() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_expected_phase_chains(&database, &["1"]).await?;
    seed_phase_runner_heartbeat(&database, "1", "now()").await?;
    let mut lock_transaction = database.lookup_pool.begin().await?;
    sqlx::query("LOCK TABLE bigname_phase.service_heartbeats IN ACCESS EXCLUSIVE MODE")
        .execute(&mut *lock_transaction)
        .await?;

    let response = tokio::time::timeout(
        std::time::Duration::from_secs(4),
        app_router(database.app_state()).oneshot(
            Request::builder()
                .uri("/healthz")
                .body(Body::empty())
                .expect("health request must build"),
        ),
    )
    .await;
    lock_transaction.rollback().await?;
    let response = response.context("health request exceeded its outer test timeout")??;
    let status = response.status();
    let payload: Value = read_json(response).await?;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(payload["api_status"], json!("ready"));
    assert_eq!(payload["status"], json!("degraded"));
    assert_eq!(
        payload["loops"]["phase_runner"]["status"],
        json!("unavailable")
    );
    database.cleanup().await
}

#[tokio::test]
async fn v2_address_names_rejects_unrecognized_namespace() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri(
                    "/v2/addresses/0x0000000000000000000000000000000000000001/names?namespace=not-served",
                )
                .body(Body::empty())?,
        )
        .await?;
    let status = response.status();
    let payload: Value = read_json(response).await?;

    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(payload["error"]["code"], json!("not_found"));
    database.cleanup().await
}

#[tokio::test]
async fn v2_address_history_rejects_unrecognized_namespace() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri(
                    "/v2/addresses/0x0000000000000000000000000000000000000001/history?namespace=not-served",
                )
                .body(Body::empty())?,
        )
        .await?;
    let status = response.status();
    let payload: Value = read_json(response).await?;

    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(payload["error"]["code"], json!("not_found"));
    database.cleanup().await
}
