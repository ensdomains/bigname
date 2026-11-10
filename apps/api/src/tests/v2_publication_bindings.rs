#[tokio::test]
async fn v2_published_bindings_survive_later_interpret_closures() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_address_names_fixture(&database).await?;
    seed_v2_resolves_to_records(&database).await?;
    upsert_phase_raw_blocks(
        &database.pool,
        &[raw_block(
            "ethereum-mainnet",
            "0xbinding106",
            None,
            106,
            1_800_000_106,
        )],
    )
    .await?;
    sqlx::query(
        "UPDATE chain_heads SET latest_block_number=106, latest_block_hash='0xbinding106'
         WHERE chain_id='ethereum-mainnet'",
    )
    .execute(&database.pool)
    .await?;

    let routes = [
        format!("/v1/addresses/{V2_ADDRESS}/names"),
        format!("/v1/addresses/{V2_ADDRESS}/names?relation=resolves_to"),
        format!("/v1/addresses/{V2_ADDRESS}/names?relation=resolves_to&coin_type=evm"),
        "/v1/names/alpha.eth".to_owned(),
    ];
    let mut before = Vec::new();
    for route in &routes {
        before.push(v2_address_names_payload_for_database(&database, route).await?);
    }
    let reverse_input = json!({"inputs": [{"address": V2_ADDRESS}]});
    let reverse_before = v2_lookup_json(&database, reverse_input.clone()).await?;
    let generation_before: String = sqlx::query_scalar(
        "SELECT xmin::text FROM chain_phase_state
         WHERE chain_id='ethereum-mainnet' AND phase_name='project'",
    )
    .fetch_one(&database.pool)
    .await?;
    let alpha = v2_address_name_specs()
        .into_iter()
        .find(|spec| spec.name == "alpha.eth")
        .expect("fixture includes alpha");
    // Stage the committed binding closure that Interpret writes before Project publishes.
    sqlx::query(
        "UPDATE surface_bindings SET active_to=to_timestamp(1800000106), observed_at=now()
         WHERE surface_binding_id=$1",
    )
    .bind(alpha.surface_binding_id)
    .execute(&database.pool)
    .await?;
    for (route, expected) in routes.iter().zip(before) {
        let actual = v2_address_names_payload_for_database(&database, route).await?;
        assert_eq!(
            actual, expected,
            "Interpret changed the published response for {route}"
        );
    }
    assert_eq!(
        v2_lookup_json(&database, reverse_input).await?,
        reverse_before
    );
    let generation_after: String = sqlx::query_scalar(
        "SELECT xmin::text FROM chain_phase_state
         WHERE chain_id='ethereum-mainnet' AND phase_name='project'",
    )
    .fetch_one(&database.pool)
    .await?;
    assert_eq!(generation_after, generation_before);

    // An orphaned binding hides the name from the resolution read at once. The ownership read
    // serves the stored listing, which checks the name and its resource but not the binding, so
    // in this hand-made state the name stays listed. In the pipeline only an Interpret redo
    // orphans a binding, and a redo that overlaps the family marker makes the marker unservable,
    // so the ownership read refuses until Project republishes the rewritten row.
    sqlx::query(
        "UPDATE surface_bindings SET canonicality_state='orphaned' WHERE surface_binding_id=$1",
    )
    .bind(alpha.surface_binding_id)
    .execute(&database.pool)
    .await?;
    let ownership = v2_address_names_payload_for_database(&database, &routes[0]).await?;
    let resolution = v2_address_names_payload_for_database(&database, &routes[1]).await?;
    assert!(names(ownership["data"].as_array().expect("names")).contains(&"alpha.eth"));
    assert!(!names(resolution["data"].as_array().expect("names")).contains(&"alpha.eth"));
    let redo = sqlx::query(
        "UPDATE chain_phase_state
         SET phase_status = 'running', redo_in_progress = true,
             redo_attempt_generation = redo_attempt_generation + 1, redo_mode = 'redo',
             redo_previous_phase_status = phase_status, redo_previous_last_error = last_error,
             redo_previous_started_at = started_at, redo_previous_finished_at = finished_at,
             redo_from_block_number = 0, redo_to_block_number = current_block_number,
             started_at = now(), finished_at = NULL, updated_at = now()
         WHERE chain_id = 'ethereum-mainnet' AND phase_name = 'interpret'",
    )
    .execute(&database.pool)
    .await?;
    assert_eq!(redo.rows_affected(), 1);
    let refused = v2_address_names_response_for_database(&database, &routes[0]).await?;
    assert_eq!(refused.status(), StatusCode::CONFLICT);
    database.cleanup().await
}
