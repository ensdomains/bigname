// Current-data positions continue through completed rebuilds, vanished anchors and read races.
#[tokio::test]
async fn v2_collection_cursor_address_position_outlives_removed_owner() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_family_names_fixture(&database).await?;
    let base = format!("/v1/addresses/{FAMILY_ALICE}/names?relation=owner&namespace=ens&page_size=1");
    let first = v2_resolver_payload_for_database(&database, &base).await?;
    assert_eq!(first["data"][0]["name"], "alpha.eth");
    let cursor = collection_next_cursor(&first).context("second owned name")?;
    let alpha = bigname_storage::logical_name_id_for_name("ens", "alpha.eth");
    // Model a reorg removing alpha's registration, then run the actual family publisher.
    sqlx::query("UPDATE normalized_events SET canonicality_state = 'orphaned' WHERE logical_name_id = $1")
        .bind(alpha).execute(&database.pool).await?;
    rebuild_fixture_families(&database.pool, FAMILY_CHAIN, 240, "0xhistory240").await?;
    let fresh = v2_resolver_payload_for_database(&database, &base).await?;
    assert_eq!(fresh["data"][0]["name"], "beta.eth");
    let next = v2_resolver_payload_for_database(&database, &format!("{base}&cursor={cursor}")).await?;
    assert_eq!(next["data"][0]["name"], "beta.eth");
    assert_eq!(next["page"]["has_more"], false);
    database.cleanup().await
}

#[tokio::test]
async fn v2_collection_cursor_resolves_to_outlives_removed_record_and_retries_race() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_address_names_fixture(&database).await?;
    seed_v2_resolves_to_records(&database).await?;
    let mut issued = Vec::new();
    for coin in ["60", "evm"] {
        let base = format!("/v1/addresses/{V2_ADDRESS}/names?relation=resolves_to&coin_type={coin}&page_size=1");
        let first = v2_resolver_payload_for_database(&database, &base).await?;
        assert_eq!(first["data"][0]["name"], "alpha.eth");
        issued.push((base, collection_next_cursor(&first).context("second resolved name")?));
    }
    write_address_name_records(&database, &[("alpha.eth", vec![("addr:60".into(), V2_OTHER_ADDRESS.into())])]).await?;
    for (base, cursor) in issued {
        let fresh = v2_resolver_payload_for_database(&database, &base).await?;
        assert_ne!(fresh["data"][0]["name"], "alpha.eth");
        let continuation = format!("{base}&cursor={cursor}");
        let next = v2_resolver_payload_for_database(&database, &continuation).await?;
        assert_eq!(next["data"], fresh["data"]);
        assert_eq!(resolver_publication_replaced_before_read(&database, continuation.clone()).await?, RETRY_REQUEST);
        v2_resolver_payload_for_database(&database, &continuation).await?;
    }
    database.cleanup().await
}

#[tokio::test]
async fn v2_collection_cursor_legacy_routes_retry_request_races_and_bind_queries() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_permissions_fixture(&database).await?;
    let base = "/v1/permissions?name=perms.eth&page_size=1";
    let first = v2_resolver_payload_for_database(&database, base).await?;
    let cursor = collection_next_cursor(&first).context("second permission")?;
    let mut payload = crate::v2::decode(&cursor).expect("position");
    assert!(!payload.filters.contains_key("registration_id"));
    payload.snapshot = Some("old-publication".into());
    payload.filters.insert("registration_id".into(), v2_permissions_current_resource_id().to_string());
    let legacy = crate::v2::encode(&payload);
    // Old name-only and explicit name+registration cursors have the same fields. Require
    // the stored explicit selector rather than silently dropping it on a name-only request.
    let response = v2_resolver_response_for_database(&database, &format!("{base}&cursor={legacy}")).await?;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let pinned = format!("{base}&registration_id={}", v2_permissions_current_resource_id());
    let continuation = format!("{pinned}&cursor={legacy}");
    let before = v2_resolver_payload_for_database(&database, &continuation).await?;
    assert_eq!(resolver_publication_replaced_before_read(&database, continuation.clone()).await?, RETRY_REQUEST);
    let after = v2_resolver_payload_for_database(&database, &continuation).await?;
    assert_eq!(before["data"], after["data"]);
    let wrong = format!("{base}&registration_id={}&cursor={legacy}", v2_permissions_stale_resource_id());
    let response = v2_resolver_response_for_database(&database, &wrong).await?;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let mut unknown = serde_json::to_value(payload)?;
    unknown["unknown_generation"] = json!("ignored?");
    let response = v2_resolver_response_for_database(&database, &format!("{pinned}&cursor={}", hex::encode(serde_json::to_vec(&unknown)?))).await?;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    database.cleanup().await
}

#[tokio::test]
async fn v2_collection_cursor_subnames_and_labels_retry_same_position() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_registry_fixture(&database).await?;
    for base in ["/v1/names/alpha.eth/subnames?page_size=1".to_owned(), format!("/v1/registries/1/{ALPHA_REGISTRY}/labels?page_size=1")] {
        let first = v2_resolver_payload_for_database(&database, &base).await?;
        let cursor = collection_next_cursor(&first).context("second child")?;
        let continuation = format!("{base}&cursor={cursor}");
        assert_eq!(resolver_publication_replaced_before_read(&database, continuation.clone()).await?, RETRY_REQUEST);
        v2_resolver_payload_for_database(&database, &continuation).await?;
    }
    database.cleanup().await
}

#[tokio::test]
async fn v2_collection_cursor_registry_references_select_current_block_unless_at_is_explicit() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_registry_fixture(&database).await?;
    let zed = registry_logical_name_id("zed.alpha.eth");
    bigname_storage::insert_normalized_event_fixtures(&database.pool, &[registry_event(
        "cursor-second-registry-reference", Some(&zed), "SubregistryChanged", 83, ROOT_REGISTRY,
        json!({"source_event":"SubregistryUpdated", "subregistry":ALPHA_REGISTRY}),
    )]).await?;
    publish_test_families_on(&database.pool, REGISTRY_CHAIN_ID, 83).await?;
    let base = format!("/v1/registries/1/{ALPHA_REGISTRY}?page_size=1");
    let first = registry_payload(&database, &base).await?;
    let holder = &first["data"]["referenced_by"];
    assert_eq!(holder["data"][0]["name"], "alpha.eth");
    let cursor = collection_next_cursor(holder).context("second registry reference")?;
    let position = crate::v2::decode(&cursor).expect("position");
    assert!(position.snapshot.is_none());
    assert!(!position.filters.contains_key("at"));
    let at = first["meta"]["as_of_token"].as_str().context("at token")?;
    let pinned_base = format!("{base}&at={at}");
    let pinned_first = registry_payload(&database, &pinned_base).await?;
    let pinned = collection_next_cursor(&pinned_first["data"]["referenced_by"]).context("pinned cursor")?;
    assert_registry_error(&database, &format!("{base}&cursor={pinned}"), StatusCode::BAD_REQUEST, "invalid_input").await?;
    assert_registry_error(&database, &format!("{pinned_base}&cursor={cursor}"), StatusCode::BAD_REQUEST, "invalid_input").await?;

    let mut legacy = crate::v2::decode(&pinned).expect("legacy reference position");
    legacy.snapshot = Some("old-publication".to_owned());
    legacy.evaluated_at = Some("2023-11-14T22:14:43Z".to_owned());
    let legacy = crate::v2::encode(&legacy);
    assert_registry_error(&database, &format!("{base}&cursor={legacy}"), StatusCode::BAD_REQUEST, "invalid_input").await?;
    registry_payload(&database, &format!("{pinned_base}&cursor={legacy}")).await?;

    upsert_phase_raw_blocks(&database.pool, &[raw_block(REGISTRY_CHAIN_ID, "0xregistry84", None, 84, 1_700_000_084)]).await?;
    database.seed_snapshot_selector_chain_positions(&json!({"ethereum": {
        "chain_id":REGISTRY_CHAIN_ID, "block_number":84, "block_hash":"0xregistry84", "timestamp":"2023-11-14T22:14:44Z"
    }})).await?;
    publish_test_families_on(&database.pool, REGISTRY_CHAIN_ID, 84).await?;
    let continued = registry_payload(&database, &format!("{base}&cursor={cursor}")).await?;
    assert_eq!(continued["data"]["referenced_by"]["data"][0]["name"], "zed.alpha.eth");
    assert_eq!(continued["meta"]["as_of"]["1"]["block_number"], 84);
    let at_84 = continued["meta"]["as_of_token"].as_str().context("new position")?;
    assert_registry_error(&database, &format!("{base}&at={at_84}&cursor={legacy}"), StatusCode::BAD_REQUEST, "invalid_input").await?;
    let pinned_page = registry_payload(&database, &format!("{pinned_base}&cursor={pinned}")).await?;
    assert_eq!(pinned_page["meta"]["as_of"]["1"]["block_number"], 83);
    let continuation = format!("{base}&cursor={cursor}");
    assert_eq!(resolver_publication_replaced_before_read(&database, continuation.clone()).await?, RETRY_REQUEST);
    registry_payload(&database, &continuation).await?;
    database.cleanup().await
}

#[tokio::test]
async fn v2_collection_cursor_resolver_roles_bind_explicit_at_but_survive_rebuild() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_resolver_roles_pages(&database).await?;
    let base = format!("/v1/resolvers/1/{V2_RESOLVER_ADDRESS}/roles?page_size=1");
    let first = v2_resolver_payload_for_database(&database, &base).await?;
    let at = first["meta"]["as_of_token"].as_str().context("selected position")?;
    let pinned_base = format!("{base}&at={at}");
    let pinned_first = v2_resolver_payload_for_database(&database, &pinned_base).await?;
    let cursor = collection_next_cursor(&pinned_first).context("pinned roles")?;
    let response = v2_resolver_response_for_database(&database, &format!("{base}&cursor={cursor}")).await?;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    publish_resolver_collection_inputs(&database).await?;
    let continued = v2_resolver_payload_for_database(&database, &format!("{pinned_base}&cursor={cursor}")).await?;
    assert_ne!(continued["data"][0], pinned_first["data"][0]);
    database.cleanup().await
}
