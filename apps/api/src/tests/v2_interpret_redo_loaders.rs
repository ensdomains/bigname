#[tokio::test]
async fn v2_lookup_returns_stale_when_interpret_redo_begins_before_served_head_revalidation()
-> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_lookup_reverse_fixture(
        &database,
        "0x0000000000000000000000000000000000000abc",
    )
    .await?;
    let (_guard, control) =
        crate::v2::lookup_served_head_revalidation_test_hooks::install(&database.lookup_pool)
            .await?;
    let state = database.app_state();
    let request_task = tokio::spawn(async move {
        app_router(state)
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v2/lookup")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"inputs":[{"name":"alice.eth"}]}"#))
                    .expect("lookup request must build"),
            )
            .await
    });

    control.wait_until_reached().await;
    database
        .simulate_interpret_redo_begin("ethereum-mainnet", "recompute_flags")
        .await?;
    control.resume().await;
    let response = request_task.await??;
    assert_eq!(response.status(), StatusCode::CONFLICT);
    let payload: Value = read_json(response).await?;
    assert_eq!(payload["error"]["code"], json!("stale"));
    assert!(payload.get("data").is_none());

    database.cleanup().await
}

#[tokio::test]
async fn v2_get_resolver_latest_refuses_while_interpret_redo_is_in_progress() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_resolver_bound_names_fixture(&database).await?;
    database
        .simulate_interpret_redo_begin("ethereum-mainnet", "recompute_flags")
        .await?;

    let response = v2_resolver_response_for_database(
        &database,
        &format!("/v2/resolvers/1/{V2_RESOLVER_ADDRESS}"),
    )
    .await?;
    assert_eq!(response.status(), StatusCode::CONFLICT);
    let payload: Value = read_json(response).await?;
    assert_eq!(payload["error"]["code"], json!("stale"));
    assert!(payload.get("data").is_none());

    database.cleanup().await
}
