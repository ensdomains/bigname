// Address history renders the page's names on its pinned snapshot without composing topology.
#[tokio::test]
async fn v2_textless_address_history_enriches_names_without_changing_events_or_cursors() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let fixture = seed_textless_fixture(&database, true).await?;
    let base = format!(
        "/v1/addresses/{RC_OWNER}/history?namespace=ens&relation=owner&scope=both&order=desc"
    );
    let full_before = tl_get(&database, &format!("{base}&page_size=200")).await?;
    let rows = full_before["data"].as_array().expect("history rows");
    assert!(rows.len() > 2, "{full_before:#}");
    let named = rows.iter().find(|row| row["type"] == "resolver").expect("named resolver event");
    assert_eq!(named["name"], json!(fixture.first_name), "{full_before:#}");
    assert_eq!(full_before["page"]["total_count"], Value::Null);

    let uri = format!("{base}&page_size=2");
    let before = tl_get(&database, &uri).await?;
    assert_eq!(before["data"], json!(&rows[..2]));
    assert_eq!(before["page"]["has_more"], json!(true));
    assert_eq!(before["page"]["total_count"], Value::Null);
    let cursor = before["page"]["next_cursor"].as_str().expect("continuation");
    let continued_uri = format!("{uri}&cursor={cursor}");
    let continued_before = tl_get(&database, &continued_uri).await?;
    let exact = tl_get(&database, &format!("{uri}&include=total_count")).await?;
    assert_eq!(exact["data"], before["data"]);
    assert_eq!(exact["page"]["total_count"], json!(rows.len()));
    assert_eq!(exact["page"]["next_cursor"], before["page"]["next_cursor"]);

    let marker_before: Value = sqlx::query_scalar(
        "SELECT to_jsonb(marker) FROM project_family_marker marker WHERE chain_id = 'ethereum-mainnet'",
    )
    .fetch_one(&database.pool)
    .await?;
    // A verified preimage changes only the served spelling, with no new Project publication.
    insert_family_label_preimage(&database.pool, b"first").await?;
    let renamed = |mut data: Value| {
        for row in data.as_array_mut().expect("history rows") {
            if let Some(name) = row["name"].as_str() {
                row["name"] = json!(name.replace(&fixture.first_name, "first.alpha.eth"));
            }
        }
        data
    };
    let full_after = tl_get(&database, &format!("{base}&page_size=200")).await?;
    assert_eq!(full_after["data"], renamed(full_before["data"].clone()));
    assert_eq!(full_after["page"], full_before["page"]);
    let after = tl_get(&database, &uri).await?;
    assert_eq!(after["data"], renamed(before["data"].clone()));
    assert_eq!(after["page"], before["page"]);
    let continued_after = tl_get(&database, &continued_uri).await?;
    assert_eq!(continued_after["data"], renamed(continued_before["data"].clone()));
    assert_eq!(continued_after["page"], continued_before["page"]);
    let marker_after: Value = sqlx::query_scalar(
        "SELECT to_jsonb(marker) FROM project_family_marker marker WHERE chain_id = 'ethereum-mainnet'",
    )
    .fetch_one(&database.pool)
    .await?;
    assert_eq!(marker_after, marker_before);
    database.cleanup().await
}
