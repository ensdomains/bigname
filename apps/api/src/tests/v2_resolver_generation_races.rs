/// Reach the handler's generation recheck before finish(), then change the actual publication.
async fn resolver_generation_race(
    database: &TestDatabase,
    uri: &str,
    mutation: impl std::future::Future<Output = Result<()>>,
) -> Result<(StatusCode, Value)> {
    let (_guard, control) = crate::v2::resolver_generation_test_hooks::install(&database.pool).await?;
    let state = database.app_state();
    let uri = uri.to_owned();
    let request = tokio::spawn(async move {
        app_router(state).oneshot(Request::builder().uri(uri).body(Body::empty()).unwrap()).await
    });
    tokio::time::timeout(std::time::Duration::from_secs(30), control.wait_until_reached())
        .await.context("request did not reach resolver generation recheck")?;
    let mutated = mutation.await;
    control.resume().await;
    let response = request.await.context("resolver task panicked")??;
    mutated?;
    Ok((response.status(), read_json(response).await?))
}

async fn publish_newer_resolver_block(database: &TestDatabase) -> Result<()> {
    let (number, timestamp): (i64, OffsetDateTime) = sqlx::query_as(
        "SELECT marker.current_block_number, lineage.block_timestamp
         FROM bigname_phase.project_family_marker marker JOIN chain_lineage lineage
         ON lineage.chain_id = marker.chain_id AND lineage.block_hash = marker.current_block_hash
         WHERE marker.chain_id = 'ethereum-mainnet'",
    ).fetch_one(&database.pool).await?;
    let next = number + 1;
    database.seed_snapshot_selector_chain_positions(&json!({"ethereum": {
        "chain_id":"ethereum-mainnet", "block_number":next,
        "block_hash":format!("0xresolver-race-{next}"),
        "timestamp":bigname_storage::UnixSeconds::from(timestamp + time::Duration::seconds(1)).internal_string()
    }})).await?;
    publish_test_families_on(&database.pool, "ethereum-mainnet", next).await
}

#[tokio::test]
async fn v2_resolver_collection_cursor_retries_newer_publication_before_generation_recheck() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_resolver_roles_pages(&database).await?;
    let base = format!("/v1/resolvers/1/{V2_RESOLVER_ADDRESS}/roles?page_size=1");
    assert_resolver_cursor_retries_newer_generation(&database, &base, "").await?;
    database.cleanup().await
}

#[tokio::test]
async fn v2_resolver_overview_cursor_retries_newer_publication_before_generation_recheck() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_resolver_bound_names_fixture(&database).await?;
    seed_v2_resolver_overview(&database, true).await?;
    let base = format!("/v1/resolvers/1/{V2_RESOLVER_ADDRESS}?page_size=1");
    assert_resolver_cursor_retries_newer_generation(&database, &base, "/data/bound_names").await?;
    database.cleanup().await
}

async fn assert_resolver_cursor_retries_newer_generation(database: &TestDatabase, base: &str, holder: &str) -> Result<()> {
    let first = v2_resolver_payload_for_database(database, base).await?;
    let cursor = collection_next_cursor(first.pointer(holder).context("first page")?).context("cursor")?;
    let uri = format!("{base}&cursor={cursor}");
    let at = first["meta"]["as_of_token"].as_str().context("selected token")?;
    let pinned_base = format!("{base}&at={at}");
    let pinned = v2_resolver_payload_for_database(database, &pinned_base).await?;
    let pinned_cursor = collection_next_cursor(pinned.pointer(holder).context("pinned page")?).context("pinned cursor")?;
    let pinned_uri = format!("{pinned_base}&cursor={pinned_cursor}");

    let (status, error) = resolver_generation_race(database, &uri, publish_newer_resolver_block(database)).await?;
    assert_eq!(status, StatusCode::CONFLICT, "{error:#}");
    assert_eq!(error["error"]["code"], "stale");
    assert_eq!(error["error"]["message"], RETRY_REQUEST);
    let continued = v2_resolver_payload_for_database(database, &uri).await?;
    assert_eq!(continued["meta"]["as_of"]["1"]["block_number"].as_i64(), first["meta"]["as_of"]["1"]["block_number"].as_i64().map(|n| n + 1));
    assert_ne!(continued["meta"]["as_of"]["1"]["block_hash"], first["meta"]["as_of"]["1"]["block_hash"]);
    assert_ne!(continued.pointer(holder).unwrap()["data"][0], first.pointer(holder).unwrap()["data"][0]);
    // Repeating a genuinely unavailable explicit pin must not promise retry will restore it.
    let response = v2_resolver_response_for_database(database, &pinned_uri).await?;
    assert_eq!(response.status(), StatusCode::CONFLICT);
    let error: Value = read_json(response).await?;
    assert_eq!(error["error"]["code"], "stale");
    assert!(!error["error"]["message"].as_str().unwrap().contains("retry"));
    Ok(())
}

#[tokio::test]
async fn v2_resolver_overview_cursor_retries_same_block_generation_mismatch_before_finish() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_resolver_bound_names_fixture(&database).await?;
    seed_v2_resolver_overview(&database, true).await?;
    let base = format!("/v1/resolvers/1/{V2_RESOLVER_ADDRESS}?page_size=1");
    let first = v2_resolver_payload_for_database(&database, &base).await?;
    let cursor = collection_next_cursor(&first["data"]["bound_names"]).context("cursor")?;
    let uri = format!("{base}&cursor={cursor}");
    let (status, error) = resolver_generation_race(&database, &uri, publish_resolver_collection_inputs(&database)).await?;
    assert_eq!(status, StatusCode::CONFLICT, "{error:#}");
    assert_eq!(error["error"]["message"], RETRY_REQUEST);
    v2_resolver_payload_for_database(&database, &uri).await?;
    database.cleanup().await
}

#[tokio::test]
async fn v2_resolver_missing_overview_retries_generation_mismatch_before_not_found() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_resolver_overview(&database, true).await?;
    let uri = "/v1/resolvers/1/0x0000000000000000000000000000000000000fff";
    let (status, error) = resolver_generation_race(&database, uri, publish_resolver_collection_inputs(&database)).await?;
    assert_eq!(status, StatusCode::CONFLICT, "{error:#}");
    assert_eq!(error["error"]["message"], RETRY_REQUEST);
    let response = v2_resolver_response_for_database(&database, uri).await?;
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    database.cleanup().await
}

#[tokio::test]
async fn v2_resolver_collection_generation_recheck_preserves_database_errors() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_resolver_roles_pages(&database).await?;
    let uri = format!("/v1/resolvers/1/{V2_RESOLVER_ADDRESS}/roles?page_size=1");
    let (status, error) = resolver_generation_race(&database, &uri, async {
        sqlx::query("ALTER TABLE bigname_phase.project_family_marker RENAME TO hidden_marker_for_test")
            .execute(&database.pool).await?;
        Ok(())
    }).await?;
    sqlx::query("ALTER TABLE bigname_phase.hidden_marker_for_test RENAME TO project_family_marker")
        .execute(&database.pool).await?;
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR, "{error:#}");
    assert_eq!(error["error"]["code"], "internal_error");
    assert_eq!(error["error"]["message"], "failed to validate lookup data");
    database.cleanup().await
}

#[tokio::test]
async fn v2_resolver_explicit_at_preserves_unavailability_during_generation_recheck() -> Result<()> {
    for is_overview in [false, true] {
        let database = TestDatabase::new_migrated().await?;
        let (suffix, holder) = if is_overview {
            seed_v2_resolver_bound_names_fixture(&database).await?;
            seed_v2_resolver_overview(&database, true).await?;
            ("", "/data/bound_names")
        } else {
            seed_v2_resolver_roles_pages(&database).await?;
            ("/roles", "")
        };
        let base = format!("/v1/resolvers/1/{V2_RESOLVER_ADDRESS}{suffix}?page_size=1");
        let first = v2_resolver_payload_for_database(&database, &base).await?;
        let at = first["meta"]["as_of_token"].as_str().context("at token")?;
        let pinned_base = format!("{base}&at={at}");
        let pinned = v2_resolver_payload_for_database(&database, &pinned_base).await?;
        let cursor = collection_next_cursor(pinned.pointer(holder).context("pinned page")?).context("pinned cursor")?;
        let uri = format!("{pinned_base}&cursor={cursor}");
        let (status, error) = resolver_generation_race(&database, &uri, publish_newer_resolver_block(&database)).await?;
        assert_eq!(status, StatusCode::CONFLICT, "{error:#}");
        assert_eq!(error["error"]["code"], "stale");
        assert_eq!(error["error"]["message"], "served data is not available at the selected snapshot");
        let response = v2_resolver_response_for_database(&database, &uri).await?;
        assert_eq!(response.status(), StatusCode::CONFLICT);
        v2_resolver_payload_for_database(&database, &base).await?;
        database.cleanup().await?;
    }
    Ok(())
}
