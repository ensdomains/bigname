// A lookup batch with a served head reads every row and enrichment on one snapshot that serves
// the captured publication. Each row below commits a change at a named point of the batch and
// asserts what the batch answered.

use crate::v2::lookup_served_head_revalidation_test_hooks::Stage as LookupStage;

const LOOKUP_PUBLICATION_MOVED: &str = "served publication changed before the lookup read began";
const LOOKUP_ADDRESS: &str = "0x0000000000000000000000000000000000000abc";

async fn lookup_with_state(state: AppState, body: Value) -> Result<(StatusCode, Value)> {
    let response = app_router(state)
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/lookup")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&body)?))?,
        )
        .await?;
    let status = response.status();
    Ok((status, read_json(response).await?))
}

/// Answers `body` with the batch paused at `stage` while `inject` commits a change.
async fn lookup_with_mid_read<Fut>(
    database: &TestDatabase,
    state: AppState,
    stage: LookupStage,
    body: Value,
    inject: impl FnOnce() -> Fut,
) -> Result<(StatusCode, Value)>
where
    Fut: std::future::Future<Output = Result<()>>,
{
    let (_guard, control) = crate::v2::lookup_served_head_revalidation_test_hooks::install_at(
        &database.lookup_pool,
        stage,
    )
    .await?;
    let request = tokio::spawn(lookup_with_state(state, body));
    tokio::time::timeout(
        std::time::Duration::from_secs(15),
        control.wait_until_reached(),
    )
    .await
    .context("lookup read hook was not reached")?;
    inject().await?;
    control.resume().await;
    request.await.context("lookup request task panicked")?
}

/// Ethereum publishes block 43, at which the address registers carol.eth. Every row a batch
/// read after this commit would see the new name, a relation of the address and a later head.
async fn publish_carol(database: &TestDatabase) -> Result<()> {
    seed_relation_name(
        database,
        "carol.eth",
        0x5a0681,
        43,
        RelationNameAccounts {
            registrant: LOOKUP_ADDRESS,
            controller: V2_LOOKUP_OTHER_ACCOUNT,
            resolver: LOOKUP_ADDRESS,
        },
    )
    .await
}

fn served_block(payload: &Value) -> &Value {
    &payload["meta"]["as_of"]["1"]["block_number"]
}

fn page_names(payload: &Value, index: usize) -> Vec<&str> {
    payload["data"][index]["records"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|record| record["name"].as_str())
        .collect()
}

/// The answer before anything moves, then the batch with carol.eth published after its snapshot
/// began. The batch must equal the earlier answer, and the next request sees block 43.
async fn assert_block_mid_read_serves_the_capture(body: Value) -> Result<(Value, Value)> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_lookup_reverse_fixture(&database, LOOKUP_ADDRESS).await?;
    let (status, baseline) = lookup_with_state(database.app_state(), body.clone()).await?;
    assert_eq!(status, StatusCode::OK, "{baseline:#}");
    assert_eq!(served_block(&baseline), &json!(42), "{baseline:#}");
    let (status, raced) = lookup_with_mid_read(
        &database,
        database.app_state(),
        LookupStage::MidRead,
        body.clone(),
        || publish_carol(&database),
    )
    .await?;
    assert_eq!(status, StatusCode::OK, "{raced:#}");
    assert_eq!(raced, baseline);
    let (status, later) = lookup_with_state(database.app_state(), body).await?;
    assert_eq!(status, StatusCode::OK, "{later:#}");
    assert_eq!(served_block(&later), &json!(43), "{later:#}");
    database.cleanup().await?;
    Ok((baseline, later))
}

/// Row N-a. Name-only batch, namespace inferred.
#[tokio::test]
async fn v2_lookup_name_only_serves_the_captured_publication_after_a_block_lands_mid_read()
-> Result<()> {
    let (baseline, later) = assert_block_mid_read_serves_the_capture(
        json!({"inputs": [{"name": "alice.eth"}, {"name": "carol.eth"}]}),
    )
    .await?;
    assert_eq!(baseline["data"][1]["status"], json!("not_found"), "{baseline:#}");
    assert_eq!(later["data"][1]["status"], json!("ok"), "{later:#}");
    Ok(())
}

/// Row X-a. Name-only batch with an explicit namespace.
#[tokio::test]
async fn v2_lookup_explicit_namespace_serves_the_captured_publication_after_a_block_lands_mid_read()
-> Result<()> {
    let (baseline, later) = assert_block_mid_read_serves_the_capture(json!({
        "namespace": "ens",
        "inputs": [{"name": "alice.eth"}, {"name": "carol.eth"}]
    }))
    .await?;
    assert_eq!(baseline["data"][1]["status"], json!("not_found"), "{baseline:#}");
    assert_eq!(later["data"][1]["status"], json!("ok"), "{later:#}");
    Ok(())
}

/// Row A-a. Public reverse lookup over the storage-exact relations.
#[tokio::test]
async fn v2_lookup_public_reverse_serves_the_captured_publication_after_a_block_lands_mid_read()
-> Result<()> {
    let (baseline, later) =
        assert_block_mid_read_serves_the_capture(json!({"inputs": [{"address": LOOKUP_ADDRESS}]}))
            .await?;
    assert_eq!(page_names(&baseline, 0), ["alice.eth", "bob.eth"], "{baseline:#}");
    assert!(page_names(&later, 0).contains(&"carol.eth"), "{later:#}");
    Ok(())
}

/// Row A-a2. `relation=resolves_to` and a relation-filtered input in one batch.
#[tokio::test]
async fn v2_lookup_resolves_to_and_filtered_relation_serve_the_captured_publication_mid_read()
-> Result<()> {
    let (baseline, later) = assert_block_mid_read_serves_the_capture(json!({"inputs": [
        {"address": LOOKUP_ADDRESS, "relation": "resolves_to"},
        {"address": LOOKUP_ADDRESS, "relation": "owner"},
    ]}))
    .await?;
    assert!(!page_names(&baseline, 1).contains(&"carol.eth"), "{baseline:#}");
    assert!(page_names(&later, 1).contains(&"carol.eth"), "{later:#}");
    Ok(())
}

/// Row M-a. Names and addresses in one batch share one `meta.as_of`.
#[tokio::test]
async fn v2_lookup_mixed_batch_serves_the_captured_publication_after_a_block_lands_mid_read()
-> Result<()> {
    let (baseline, later) = assert_block_mid_read_serves_the_capture(json!({"inputs": [
        {"name": "carol.eth"},
        {"address": LOOKUP_ADDRESS},
    ]}))
    .await?;
    assert_eq!(baseline["data"][0]["status"], json!("not_found"), "{baseline:#}");
    assert_eq!(later["data"][0]["status"], json!("ok"), "{later:#}");
    Ok(())
}

/// Row pre. A block published between the capture and the snapshot is the one retryable window.
#[tokio::test]
async fn v2_lookup_retries_a_block_published_before_the_snapshot_begins() -> Result<()> {
    for body in [
        json!({"inputs": [{"name": "alice.eth"}]}),
        json!({"inputs": [{"address": LOOKUP_ADDRESS}]}),
    ] {
        let database = TestDatabase::new_migrated().await?;
        seed_v2_lookup_reverse_fixture(&database, LOOKUP_ADDRESS).await?;
        let (status, payload) = lookup_with_mid_read(
            &database,
            database.app_state(),
            LookupStage::BeforeSnapshot,
            body.clone(),
            || publish_carol(&database),
        )
        .await?;
        assert_eq!(status, StatusCode::CONFLICT, "{body}: {payload:#}");
        assert_search_error(payload, "stale", LOOKUP_PUBLICATION_MOVED);
        let (status, retried) = lookup_with_state(database.app_state(), body).await?;
        assert_eq!(status, StatusCode::OK, "{retried:#}");
        assert_eq!(served_block(&retried), &json!(43), "{retried:#}");
        database.cleanup().await?;
    }
    Ok(())
}

/// Row A-c. An Interpret redo that begins during a public reverse read refuses the batch.
#[tokio::test]
async fn v2_lookup_public_reverse_returns_stale_when_interpret_redo_begins_mid_read() -> Result<()>
{
    let database = TestDatabase::new_migrated().await?;
    seed_v2_lookup_reverse_fixture(&database, LOOKUP_ADDRESS).await?;
    let (status, payload) = lookup_with_mid_read(
        &database,
        database.app_state(),
        LookupStage::MidRead,
        json!({"inputs": [{"address": LOOKUP_ADDRESS}]}),
        || database.simulate_interpret_redo_begin("ethereum-mainnet", "recompute_flags"),
    )
    .await?;
    assert_eq!(status, StatusCode::CONFLICT, "{payload:#}");
    assert_search_error(payload, "stale", INTERPRET_REDO_STALE);
    database.cleanup().await
}

/// Row pre-redo. An Interpret redo that begins between the capture and the snapshot is caught
/// by the snapshot's own publication check, so it answers the before-the-read message.
#[tokio::test]
async fn v2_lookup_retries_an_interpret_redo_begun_before_the_snapshot_begins() -> Result<()> {
    for body in [
        json!({"inputs": [{"name": "alice.eth"}]}),
        json!({"inputs": [{"address": LOOKUP_ADDRESS}]}),
    ] {
        let database = TestDatabase::new_migrated().await?;
        seed_v2_lookup_reverse_fixture(&database, LOOKUP_ADDRESS).await?;
        let (status, payload) = lookup_with_mid_read(
            &database,
            database.app_state(),
            LookupStage::BeforeSnapshot,
            body.clone(),
            || database.simulate_interpret_redo_begin("ethereum-mainnet", "recompute_flags"),
        )
        .await?;
        assert_eq!(status, StatusCode::CONFLICT, "{body}: {payload:#}");
        assert_search_error(payload, "stale", LOOKUP_PUBLICATION_MOVED);
        database.cleanup().await?;
    }
    Ok(())
}

/// Row cur. The reverse cursor binds the input and the namespace list, not a publication, so a
/// block between pages continues the walk at the new head.
#[tokio::test]
async fn v2_lookup_reverse_cursor_continues_after_a_block_lands_between_pages() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_lookup_reverse_fixture(&database, LOOKUP_ADDRESS).await?;
    let first = v2_lookup_json(
        &database,
        json!({"inputs": [{"address": LOOKUP_ADDRESS, "page_size": 1}]}),
    )
    .await?;
    assert_eq!(page_names(&first, 0), ["alice.eth"], "{first:#}");
    let cursor = first["data"][0]["page"]["next_cursor"]
        .as_str()
        .context("the first page has more")?;
    advance_mainnet_fixture_publication(&database, 43, "0xlookup-between-pages").await?;
    let second = v2_lookup_json(
        &database,
        json!({"inputs": [{"address": LOOKUP_ADDRESS, "page_size": 1, "cursor": cursor}]}),
    )
    .await?;
    assert_eq!(page_names(&second, 0), ["bob.eth"], "{second:#}");
    assert_eq!(second["data"][0]["page"]["has_more"], json!(false), "{second:#}");
    assert_eq!(served_block(&second), &json!(43), "{second:#}");
    database.cleanup().await
}
