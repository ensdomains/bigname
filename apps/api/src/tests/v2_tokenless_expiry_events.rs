// An ENSv2 registry `ExpiryUpdated` whose token has no interpreted state is stored for the
// registry entry row and left out of product event reads, listing and count alike.
#[tokio::test]
async fn tokenless_registry_expiry_feeds_the_entry_row_and_no_event_read() -> Result<()> {
    const REGISTRY: &str = "0x00000000000000000000000000000000000000e9";
    let database = TestDatabase::new_migrated().await?;
    g_seed_records(&database).await?;
    let token = |low: &str| format!("0x{}{low}", "ab".repeat(28));
    let instance = Uuid::from_u128(0xe9).to_string();
    let registry_event = |identity: &str, kind: &str, block: i64, after: Value| {
        event_data_event(
            identity,
            None,
            None,
            kind,
            "ens_v2_registry_l1",
            block,
            &format!("0xtx{block}"),
            7,
            REGISTRY,
            after,
        )
    };
    let unregistered = registry_event(
        "tokenless-unregistered",
        "RegistrationReleased",
        143,
        json!({"source_event": "LabelUnregistered", "token_id": token("00000000"),
               "sender": G_ADDRESS, "registry_contract_instance_id": instance}),
    );
    let tokenless = registry_event(
        "tokenless-renewal",
        "ExpiryChanged",
        144,
        json!({"source_event": "ExpiryUpdated", "token_id": token("00000001"),
               "expiry": 1_900_000_000_u64, "sender": G_ADDRESS, "token_state_absent": true,
               "registry_contract_instance_id": instance}),
    );
    // The same shape without the marker is an ordinary nameless expiry and stays listed.
    let ordinary = registry_event(
        "ordinary-renewal",
        "ExpiryChanged",
        145,
        json!({"source_event": "ExpiryUpdated", "token_id": token("00000001"),
               "expiry": 1_900_000_001_u64, "sender": G_ADDRESS,
               "registry_contract_instance_id": instance}),
    );
    bigname_storage::insert_normalized_event_fixtures(
        &database.pool,
        &[unregistered, tokenless, ordinary],
    )
    .await?;
    rebuild_fixture_families(&database.pool, EVENT_DATA_CHAIN, 145, "0xhistory145").await?;

    let stored: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM bigname_phase.normalized_events
         WHERE event_identity = 'tokenless-renewal' AND consumer_visibility = 'activated'",
    )
    .fetch_one(&database.pool)
    .await?;
    assert_eq!(stored, 1, "the renewal is a stored, activated event");
    let (status, token_id, expiry): (String, String, sqlx::types::BigDecimal) = sqlx::query_as(
        "SELECT status, token_id, expiry FROM bigname_phase.project_ens_v2_entry_owner
         WHERE chain_id = $1 AND registry = $2",
    )
    .bind(EVENT_DATA_CHAIN)
    .bind(REGISTRY)
    .fetch_one(&database.pool)
    .await?;
    assert_eq!(status, "reserved", "the renewal revived the unregistered entry");
    assert_eq!(token_id, token("00000001"));
    assert_eq!(expiry.to_string(), "1900000001");

    let by_contract = hk_ok(
        &database,
        &format!("/v1/events?contract_address={REGISTRY}&include=total_count&page_size=200"),
    )
    .await?;
    let rows = by_contract["data"].as_array().unwrap();
    assert_eq!(by_contract["page"]["total_count"], json!(rows.len()));
    let expiries = rows
        .iter()
        .filter(|row| row["type"] == "expiry")
        .collect::<Vec<_>>();
    assert_eq!(expiries.len(), 1, "only the ordinary renewal is listed: {rows:#?}");
    assert_eq!(expiries[0]["block_number"], json!(145), "{expiries:#?}");
    let with_ordinary_removed = hk_ok(
        &database,
        &format!(
            "/v1/events?contract_address={REGISTRY}&include=total_count&to_block=144&type=expiry"
        ),
    )
    .await?;
    assert_eq!(with_ordinary_removed["page"]["total_count"], json!(0));
    assert_eq!(with_ordinary_removed["data"], json!([]));

    let unanchored = hk_ok(&database, "/v1/events?type=expiry&page_size=200").await?;
    let blocks = unanchored["data"]
        .as_array()
        .unwrap()
        .iter()
        .map(|row| row["block_number"].clone())
        .collect::<Vec<_>>();
    assert_eq!(blocks, vec![json!(145)], "{unanchored:#}");
    database.cleanup().await
}
