#[tokio::test]
async fn v2_history_routes_refuse_while_interpret_redo_is_in_progress() -> Result<()> {
    const ADDRESS: &str = "0x00000000000000000000000000000000000000cc";
    const MESSAGE: &str =
        "history is temporarily unavailable while Interpret redo is in progress";
    let database = TestDatabase::new_migrated().await?;
    seed_v2_history_fixture(&database).await?;
    let routes = [
        "/v2/events?name=history.eth&page_size=2".to_owned(),
        "/v2/names/history.eth/history?page_size=2".to_owned(),
        format!("/v2/addresses/{ADDRESS}/history?page_size=2"),
    ];

    for route in &routes {
        assert_eq!(
            v2_history_response_for_database(&database, route).await?.status(),
            StatusCode::OK,
            "pre-redo route: {route}"
        );
    }
    database
        .simulate_interpret_redo_begin("ethereum-mainnet", "recompute_flags")
        .await?;
    sqlx::query(
        "DELETE FROM bigname_phase.normalized_events
         WHERE chain_id = 'ethereum-mainnet' AND block_number BETWEEN 105 AND 108",
    )
    .execute(&database.pool)
    .await?;

    for route in &routes {
        let response = v2_history_response_for_database(&database, route).await?;
        let status = response.status();
        let payload: Value = read_json(response).await?;
        assert_eq!(status, StatusCode::CONFLICT, "route: {route}; payload: {payload}");
        assert_eq!(payload["error"]["code"], json!("stale"), "route: {route}");
        assert_eq!(payload["error"]["message"], json!(MESSAGE), "route: {route}");
        assert!(payload.get("data").is_none(), "route: {route}");
        assert!(payload.get("page").is_none(), "route: {route}");
    }

    database.cleanup().await
}

#[tokio::test]
async fn v2_name_history_rejects_malformed_cursor_before_active_redo_fence() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_history_fixture(&database).await?;
    database
        .simulate_interpret_redo_begin("ethereum-mainnet", "recompute_flags")
        .await?;

    let response = v2_history_response_for_database(
        &database,
        "/v2/names/history.eth/history?cursor=garbage",
    )
    .await?;
    let status = response.status();
    let payload: Value = read_json(response).await?;
    assert_eq!(status, StatusCode::BAD_REQUEST, "payload: {payload}");
    assert_eq!(payload["error"]["code"], json!("invalid_input"));

    database.cleanup().await
}

#[tokio::test]
async fn v2_name_history_returns_stale_not_404_when_redo_orphans_the_surface() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_history_fixture(&database).await?;
    let route = "/v2/names/history.eth/history?page_size=2";

    assert_eq!(
        v2_history_response_for_database(&database, route)
            .await?
            .status(),
        StatusCode::OK
    );
    database
        .simulate_interpret_redo_begin("ethereum-mainnet", "recompute_flags")
        .await?;
    sqlx::query(
        "UPDATE bigname_phase.name_surfaces
         SET canonicality_state = 'orphaned'
         WHERE raw_name = 'history.eth'",
    )
    .execute(&database.pool)
    .await?;

    let response = v2_history_response_for_database(&database, route).await?;
    let status = response.status();
    let payload: Value = read_json(response).await?;
    assert_eq!(status, StatusCode::CONFLICT, "payload: {payload}");
    assert_eq!(payload["error"]["code"], json!("stale"));
    assert!(payload.get("data").is_none());

    database.cleanup().await
}

#[tokio::test]
async fn v2_history_routes_refuse_when_redo_finishes_after_anchor_resolution() -> Result<()> {
    const ADDRESS: &str = "0x00000000000000000000000000000000000000cc";
    let database = TestDatabase::new_migrated().await?;
    seed_v2_history_fixture(&database).await?;
    let routes = [
        "/v2/events?name=history.eth&page_size=2".to_owned(),
        "/v2/names/history.eth/history?page_size=2".to_owned(),
        format!("/v2/addresses/{ADDRESS}/history?page_size=2"),
    ];

    for route in routes {
        let (_guard, control) =
            bigname_storage::history_anchor_read_test_hooks::install(
                &database.lookup_pool,
                bigname_storage::history_anchor_read_test_hooks::HistoryReadHookPoint::AfterAnchors,
            )
            .await?;
        let state = database.app_state();
        let request_task = tokio::spawn(async move {
            app_router(state)
                .oneshot(
                    Request::builder()
                        .uri(route)
                        .body(Body::empty())
                        .expect("history request must build"),
                )
                .await
        });

        control.wait_until_reached().await;
        database
            .simulate_interpret_redo_begin("ethereum-mainnet", "recompute_flags")
            .await?;
        database
            .simulate_interpret_redo_finish("ethereum-mainnet")
            .await?;
        control.resume().await;

        let response = request_task
            .await
            .context("history anchor-race request task panicked")?
            .context("history anchor-race request failed")?;
        let status = response.status();
        let payload: Value = read_json(response).await?;
        assert_eq!(status, StatusCode::CONFLICT, "payload: {payload}");
        assert_eq!(payload["error"]["code"], json!("stale"));
        assert!(payload.get("data").is_none());
    }

    database.cleanup().await
}

#[tokio::test]
async fn v2_event_and_address_history_refuse_redo_before_name_enrichment() -> Result<()> {
    const ADDRESS: &str = "0x00000000000000000000000000000000000000cc";
    let database = TestDatabase::new_migrated().await?;
    seed_v2_history_fixture(&database).await?;

    for route in [
        "/v2/events?name=history.eth&page_size=2".to_owned(),
        format!("/v2/addresses/{ADDRESS}/history?page_size=2"),
    ] {
        let (_guard, control) =
            bigname_storage::history_anchor_read_test_hooks::install(
                &database.lookup_pool,
                bigname_storage::history_anchor_read_test_hooks::HistoryReadHookPoint::AfterPage,
            )
            .await?;
        let state = database.app_state();
        let request_task = tokio::spawn(async move {
            app_router(state)
                .oneshot(
                    Request::builder()
                        .uri(route)
                        .body(Body::empty())
                        .expect("history request must build"),
                )
                .await
        });

        control.wait_until_reached().await;
        database
            .simulate_interpret_redo_begin("ethereum-mainnet", "recompute_flags")
            .await?;
        control.resume().await;

        let response = request_task
            .await
            .context("history enrichment-race request task panicked")?
            .context("history enrichment-race request failed")?;
        let status = response.status();
        let payload: Value = read_json(response).await?;
        assert_eq!(status, StatusCode::CONFLICT, "payload: {payload}");
        assert_eq!(payload["error"]["code"], json!("stale"));
        assert!(payload.get("data").is_none());

        database
            .simulate_interpret_redo_finish("ethereum-mainnet")
            .await?;
    }

    database.cleanup().await
}
