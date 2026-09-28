// A topology rebind can outlive the token's original observation. Its emitted SurfaceBound and
// RegistrationGranted carry the resource but not token_lineage_id (v2_registry/topology.rs).
// Redo of the original observation therefore reanchors the resource from the surviving event,
// while the token remains orphaned. The later binding still selects an authority arm.
#[tokio::test]
async fn v2_child_relations_keep_the_selected_arm_after_token_anchor_redo() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_switch_children_fixture(&database).await?;
    let one = bigname_storage::logical_name_id_for_name("ens", "one.alpha.eth");
    let alpha = bigname_storage::logical_name_id_for_name("ens", "alpha.eth");
    let (resource, token): (Uuid, Uuid) = sqlx::query_as(
        "SELECT resource.resource_id, resource.token_lineage_id FROM resources resource
         JOIN surface_bindings binding USING (resource_id) WHERE binding.logical_name_id = $1",
    )
    .bind(&one)
    .fetch_one(&database.pool)
    .await?;
    sqlx::query(
        "INSERT INTO chain_lineage (chain_id, block_hash, block_number, block_timestamp,
             canonicality_state) VALUES ($1, '0xhistory199', 199, to_timestamp(1700000199),
             'canonical') ON CONFLICT DO NOTHING",
    )
    .bind(SWITCH_CHAIN)
    .execute(&database.pool)
    .await?;
    // The first token/resource observation predates the later surfaced topology binding.
    sqlx::query("UPDATE token_lineages SET block_number = 199, block_hash = '0xhistory199' WHERE token_lineage_id = $1")
        .bind(token).execute(&database.pool).await?;
    sqlx::query("UPDATE resources SET block_number = 199, block_hash = '0xhistory199' WHERE resource_id = $1")
        .bind(resource).execute(&database.pool).await?;
    sqlx::query("UPDATE name_surfaces SET block_number = 205, block_hash = '0xhistory205' WHERE logical_name_id = $1")
        .bind(&one).execute(&database.pool).await?;
    sqlx::query("UPDATE surface_bindings SET block_number = 205, block_hash = '0xhistory205', active_from = to_timestamp(1700000205) WHERE logical_name_id = $1")
        .bind(&one).execute(&database.pool).await?;
    sqlx::query(
        "UPDATE normalized_events SET resource_id = $1 WHERE event_identity = 'children-one'",
    )
    .bind(resource)
    .execute(&database.pool)
    .await?;
    bigname_storage::insert_normalized_event_fixtures(
        &database.pool,
        &[
            switch_event(
                "child-original-token-observation",
                None,
                Some(resource),
                "TokenControlTransferred",
                "ens_v2_registry_l1",
                199,
                0,
                json!({"token_lineage_id": token, "owner": CHILD_OWNER}),
            ),
            switch_event(
                "child-competing-v1-edge",
                Some(&one),
                None,
                "SubregistryChanged",
                "ens_v1_registry_l1",
                205,
                1,
                json!({"source_event": "NewOwner", "node": alpha.trim_start_matches("ens:"),
                       "child_node": one.trim_start_matches("ens:"),
                       "labelhash": child_labelhash("one"), "owner": CHILD_OWNER}),
            ),
        ],
    )
    .await?;
    reset_switch_families(&database).await?;
    publish_test_families(&database, 240).await?;
    let uri = "/v1/names/alpha.eth/subnames";
    let (status, before) = read_family_response(&database, uri).await?;
    assert_eq!(status, StatusCode::OK, "{before:#}");
    assert_eq!(before["data"].as_array().map(Vec::len), Some(4));

    // The old token observation is no longer produced. Use the actual Interpret redo writer,
    // including its stable-identity reanchoring, rather than manually orphaning the token.
    database
        .insert_manifest(
            "ens",
            "ens_v2_registry_l1",
            SWITCH_CHAIN,
            "fixture",
            1,
            "active",
            bigname_domain::normalization::ENS_NORMALIZER_VERSION,
        )
        .await?;
    bigname_interpret::Engine::new(database.pool.clone())
        .run_batch(bigname_interpret::BatchRequest {
            chain_id: SWITCH_CHAIN.to_owned(),
            from_block: 199,
            to_block: 199,
            resume_current: None,
            mode: bigname_interpret::RunMode::Redo,
        })
        .await?;
    let identity: (String, String, i64) = sqlx::query_as(
        "SELECT token.canonicality_state::text, resource.canonicality_state::text,
                resource.block_number FROM resources resource JOIN token_lineages token
         USING (token_lineage_id) WHERE resource.resource_id = $1",
    )
    .bind(resource)
    .fetch_one(&database.pool)
    .await?;
    assert_eq!(
        identity,
        ("orphaned".to_owned(), "canonical".to_owned(), 205)
    );
    reset_switch_families(&database).await?;
    publish_test_families(&database, 240).await?;
    let (status, absent) = read_family_response(&database, "/v1/names/one.alpha.eth").await?;
    assert_eq!(status, StatusCode::NOT_FOUND, "{absent:#}");
    for uri in [
        uri.to_owned(),
        "/v1/names/alpha.eth?include=counts".to_owned(),
        format!("/v1/registries/1/{CHILD_ALPHA_REGISTRY}/labels"),
        format!("/v1/registries/1/{CHILD_ALPHA_REGISTRY}?include=counts"),
    ] {
        let (status, body) = read_family_response(&database, &uri).await?;
        assert_eq!(status, StatusCode::OK, "{uri}: {body:#}");
    }
    database.cleanup().await
}
