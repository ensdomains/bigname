#[tokio::test]
async fn v2_search_durable_refuses_missing_summary_instead_of_omitting_a_match() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_search_fixture(&database).await?;
    let uri = "/v1/search?q=alpha&namespace=ens";
    let before = v2_search_payload_for_database(&database, uri).await?;
    assert_eq!(before["data"].as_array().unwrap().len(), 1);
    sqlx::query("DELETE FROM bigname_phase.project_name_summary summary USING bigname_phase.name_surfaces surface
        WHERE summary.logical_name_id=surface.logical_name_id AND summary.chain_id=surface.chain_id
          AND surface.raw_name='alpha.eth'").execute(&database.pool).await?;
    let response = v2_search_response_for_database(&database, uri).await?;
    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
    let payload: Value = read_json(response).await?;
    assert!(payload.get("data").is_none());
    database.cleanup().await
}

#[tokio::test]
async fn v2_search_durable_empty_page_keeps_the_existing_explicit_publication_metadata()
-> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_search_fixture(&database).await?;
    let populated =
        v2_search_payload_for_database(&database, "/v1/search?q=alpha&namespace=ens").await?;
    let empty = v2_search_payload_for_database(
        &database,
        "/v1/search?q=no-such-search-spelling&namespace=ens",
    )
    .await?;
    assert_eq!(empty["data"], json!([]));
    assert_eq!(empty["meta"], populated["meta"]);
    assert_eq!(empty["page"]["has_more"], false);
    assert!(empty["page"]["next_cursor"].is_null());
    database.cleanup().await
}

#[tokio::test]
async fn v2_search_durable_unavailable_scope_checks_unsupported_matches_but_keeps_empty_pages()
-> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_search_fixture(&database).await?;
    let binding = Uuid::from_u128(0x3300);
    let logical = seed_family_identity_inputs(
        &database.pool,
        "ens",
        "almost.eth",
        "ethereum-mainnet",
        100,
        "0xsearch-created-0",
        Uuid::from_u128(0x2200),
        Uuid::from_u128(0x1100),
        binding,
        "ens_v2",
    )
    .await?;
    sqlx::query("DELETE FROM bigname_phase.surface_bindings WHERE surface_binding_id = $1")
        .bind(binding)
        .execute(&database.pool)
        .await?;
    rebuild_fixture_families(
        &database.pool,
        "ethereum-mainnet",
        210,
        "0xsearch-public-ethereum",
    )
    .await?;
    let uri = "/v1/search?q=almost&namespace=ens";
    assert_eq!(
        v2_search_payload_for_database(&database, uri).await?["data"],
        json!([])
    );
    begin_search_family_rebuild(
        &database,
        "ethereum-mainnet",
        210,
        "0xsearch-public-ethereum",
    )
    .await?;
    let eligibility: (String, bool, bool) = sqlx::query_as(
        "SELECT document.name, summary.search_supported,
                marker.current_block_number >= surface.block_number
         FROM bigname_phase.name_search_documents document
         JOIN bigname_phase.name_surfaces surface USING (logical_name_id, chain_id)
         JOIN bigname_phase.project_name_summary summary USING (logical_name_id, chain_id)
         JOIN bigname_phase.project_family_marker marker USING (chain_id)
         WHERE document.logical_name_id = $1",
    )
    .bind(&logical)
    .fetch_one(&database.pool)
    .await?;
    assert_eq!(eligibility, ("almost.eth".to_owned(), false, true));
    let response = v2_search_response_for_database(&database, uri).await?;
    assert_eq!(response.status(), StatusCode::CONFLICT);
    assert_eq!(
        read_json::<Value>(response).await?["error"]["code"],
        "stale"
    );
    let empty = v2_search_payload_for_database(
        &database,
        "/v1/search?q=no-such-search-spelling&namespace=ens",
    )
    .await?;
    assert_eq!(empty["data"], json!([]));
    assert_search_meta_chains(&empty, &[], &["1"]);
    database.cleanup().await
}

#[tokio::test]
async fn v2_search_empty_page_rejects_a_snapshot_scope_change() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_family_routes_fixture(&database).await?;
    let uri = "/v1/search?q=no-such-search-spelling";
    let (status, body) = read_family_response(&database, uri).await?;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["data"], json!([]));
    for flip in [
        "UPDATE bigname_phase.project_family_marker SET state = 'bootstrap_pending'",
        "UPDATE bigname_phase.project_family_marker SET input_content_hash = 'another-build'",
    ] {
        let (status, body) = v2_get_with_marker_flip_after_fence(&database, uri, flip).await?;
        assert_eq!(status, StatusCode::CONFLICT, "{flip}: {body}");
        assert_eq!(body["error"]["code"], "conflict", "{flip}: {body}");
        assert!(body.get("data").is_none(), "{flip}: {body}");
    }
    database.cleanup().await
}
