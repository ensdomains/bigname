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
    seed_unbound_name_inputs(&database, "almost.eth", false).await?;
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
