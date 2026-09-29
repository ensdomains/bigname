// TYR-76: contract-only events opt into an exact filtered row count; ordinary pages do not count.
#[tokio::test]
async fn contract_event_counts_are_optional_filtered_and_independent_of_page_size() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    g_seed_records(&database).await?;
    let contract = format!("/v1/events?contract_address={G_RESOLVER}");
    assert_eq!(
        hk_ok(&database, &contract).await?["page"]["total_count"],
        Value::Null
    );
    assert_eq!(
        hk_ok(&database, "/v1/events?include=total_count").await?["page"]["total_count"],
        Value::Null
    );
    for (filters, count) in [
        ("", 8),
        ("&type=record", 8),
        ("&record_key=addr:60", 6),
        ("&record_key=addr:60&kind=RecordChanged", 3),
        ("&record_key=addr:60&kind=RecordVersionChanged", 3),
        ("&record_key=addr:60&name=history-key.eth", 4),
        ("&exclude_type=record", 0),
        ("&kind=MigrationApplied", 0),
        ("&record_key=addr:60&from_block=133&to_block=135", 3),
    ] {
        let base = format!("{contract}{filters}&include=total_count");
        let (ids, total) = hkw_baseline(&database, &base).await?;
        assert_eq!(ids.len(), count, "{base}");
        assert_eq!(total, json!(count), "{base}");
        assert_eq!(hkw_rest(&database, &base, None, Some(&total)).await?, ids);
    }
    let first = hk_ok(
        &database,
        &format!("{contract}&record_key=addr:60&include=total_count&page_size=1"),
    )
    .await?;
    let cursor = hk_next_cursor(&first)?;
    let rest = hk_ok(
        &database,
        &format!("{contract}&record_key=addr:60&cursor={cursor}"),
    )
    .await?;
    assert_eq!(rest["page"]["total_count"], Value::Null);
    assert_eq!(rest["data"].as_array().unwrap().len(), 5);
    database.cleanup().await
}

#[tokio::test]
async fn contract_event_count_shares_the_page_read_and_its_publication_bounds() -> Result<()> {
    use bigname_storage::history_anchor_read_test_hooks::{HistoryReadHookPoint, install};
    let database = TestDatabase::new_migrated().await?;
    g_seed_records(&database).await?;
    seed_v2_history_blocks(&database, 146..=146).await?;
    let node = bigname_lookup::ens_namehash_hex(G_NAME)?;
    let above = event_data_event(
        "g-above-publication",
        None,
        None,
        "RecordVersionChanged",
        "ens_v1_resolver_l1",
        146,
        "0xtx146",
        0,
        G_RESOLVER,
        version_after(&node, G_RESOLVER, 3),
    );
    bigname_storage::insert_normalized_event_fixtures(&database.pool, &[above]).await?;
    let uri = format!(
        "/v1/events?contract_address={G_RESOLVER}&record_key=addr:60&include=total_count&page_size=200"
    );
    let bounded = hk_ok(&database, &uri).await?;
    assert_eq!(bounded["page"]["total_count"], json!(6));
    assert_eq!(hk_ids(&bounded).len(), 6);
    let (guard, control) = install(&database.pool, HistoryReadHookPoint::AfterPage).await?;
    let app = app_router(database.app_state());
    let request_uri = uri.clone();
    let read = tokio::spawn(async move {
        app.oneshot(
            Request::builder()
                .uri(request_uri)
                .body(Body::empty())
                .unwrap(),
        )
        .await
    });
    control.wait_until_reached().await;
    // A later completed publication is legal during an ordinary history read. Both count and
    // rows remain from its earlier transaction and are bounded to the captured block.
    seed_schema_v2_ens_lookup_head(&database.pool, 146, "0xhistory146", "2023-11-14T22:15:46Z")
        .await?;
    rebuild_fixture_families(&database.pool, EVENT_DATA_CHAIN, 146, "0xhistory146").await?;
    control.resume().await;
    let response = read.await??;
    assert_eq!(response.status(), StatusCode::OK);
    let during: Value = read_json(response).await?;
    assert_eq!(during["page"]["total_count"], json!(6));
    assert_eq!(hk_ids(&during), hk_ids(&bounded));
    drop(guard);
    let later = hk_ok(&database, &uri).await?;
    assert_eq!(later["page"]["total_count"], json!(7));
    assert_eq!(hk_ids(&later).len(), 7);
    database.cleanup().await
}
