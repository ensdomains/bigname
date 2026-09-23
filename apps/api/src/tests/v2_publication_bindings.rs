#[tokio::test]
async fn v2_published_bindings_survive_later_interpret_closures() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_address_names_fixture(&database).await?;
    seed_v2_resolves_to_records(&database).await?;
    // The collection fixture's untimed primary claim is deliberately ineligible for reverse
    // lookup. This test covers binding publication, so start without that unrelated claim.
    sqlx::query("DELETE FROM primary_names_current WHERE address=$1")
        .bind(V2_ADDRESS)
        .execute(&database.pool)
        .await?;
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

    // Keeping the selected binding does not bypass fork invalidation.
    sqlx::query(
        "UPDATE surface_bindings SET canonicality_state='orphaned' WHERE surface_binding_id=$1",
    )
    .bind(alpha.surface_binding_id)
    .execute(&database.pool)
    .await?;
    let ownership = v2_address_names_payload_for_database(&database, &routes[0]).await?;
    let resolution = v2_address_names_payload_for_database(&database, &routes[1]).await?;
    assert!(!names(ownership["data"].as_array().expect("names")).contains(&"alpha.eth"));
    assert!(!names(resolution["data"].as_array().expect("names")).contains(&"alpha.eth"));
    database.cleanup().await
}
