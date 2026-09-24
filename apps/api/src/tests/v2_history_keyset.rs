// History cursors are keyset anchors (docs/api-v1-routes.md, "Shared Route Rules"). A
// continuation resumes after the anchor's position in the history order against whatever is
// published when it runs: new blocks, redos, a deleted anchor, and a publication during the read
// never make it fail. A cursor issued before this rule resumes through its anchor row and
// restarts once when that row is gone.

const HK_ADDRESS: &str = "0x00000000000000000000000000000000000000cc";
const HK_CHAIN: &str = "ethereum-mainnet";
/// A chain outside the `ens` publication scope, for a redo only the collection-wide check sees.
const HK_OTHER_CHAIN: &str = "base-mainnet";
/// The fixture's publication: `seed_default_ens_snapshot_selector_position`.
const HK_PUBLISHED_BLOCK: i64 = 21_000_003;
const HK_RESTART: &str =
    "collection publication is no longer available; restart pagination without a cursor";
const HK_REDO_RETRY: &str = "history is temporarily unavailable while Interpret redo is in progress";

fn hk_routes() -> [String; 4] {
    [
        "/v1/events?name=history.eth".to_owned(),
        "/v1/names/history.eth/history?scope=both".to_owned(),
        // The child arm keeps its own copy of the cursor position.
        "/v1/names/history.eth/history?scope=both&include=child_registrations".to_owned(),
        format!("/v1/addresses/{HK_ADDRESS}/history?scope=both"),
    ]
}

async fn hk_get(database: &TestDatabase, uri: &str) -> Result<(StatusCode, Value)> {
    let response = v2_history_response_for_database(database, uri).await?;
    let status = response.status();
    Ok((status, read_json(response).await?))
}

async fn hk_ok(database: &TestDatabase, uri: &str) -> Result<Value> {
    let (status, payload) = hk_get(database, uri).await?;
    anyhow::ensure!(status == StatusCode::OK, "{uri}: {status} {payload}");
    Ok(payload)
}

fn hk_next_cursor(payload: &Value) -> Result<String> {
    Ok(payload["page"]["next_cursor"]
        .as_str()
        .context("page must continue")?
        .to_owned())
}

fn hk_ids(payload: &Value) -> Vec<String> {
    payload["data"]
        .as_array()
        .expect("history data")
        .iter()
        .map(|row| row["id"].as_str().expect("row id").to_owned())
        .collect()
}

fn hk_timestamp(block: i64) -> i64 {
    1_776_384_000 + (block - 21_000_000)
}

/// Every remaining page from `cursor`: the row ids and the publication each page read.
async fn hk_walk(
    database: &TestDatabase,
    base: &str,
    mut cursor: String,
) -> Result<Vec<(Vec<String>, Value)>> {
    let mut pages = Vec::new();
    loop {
        let page = hk_ok(database, &format!("{base}&cursor={cursor}")).await?;
        pages.push((hk_ids(&page), page["meta"]["as_of"].clone()));
        let Some(next) = page["page"]["next_cursor"].as_str() else {
            return Ok(pages);
        };
        cursor = next.to_owned();
    }
}

/// Project publishes `block`: the chain head, lineage, and Project position all move to it.
/// With `with_row`, Interpret first writes a new product-visible row for `history.eth` there.
async fn hk_publish_block(database: &TestDatabase, block: i64, with_row: bool) -> Result<()> {
    let hash = format!("0xhistory{block}");
    upsert_phase_raw_blocks(
        &database.pool,
        &[raw_block(HK_CHAIN, &hash, None, block, hk_timestamp(block))],
    )
    .await?;
    if with_row {
        bigname_storage::insert_normalized_event_fixtures(
            &database.pool,
            &[v2_history_event(
                &format!("hk-record-{block}"),
                Some("ens:history.eth"),
                None,
                "RecordChanged",
                block,
            )],
        )
        .await?;
    }
    let timestamp = sqlx::types::time::OffsetDateTime::from_unix_timestamp(hk_timestamp(block))?;
    seed_schema_v2_ens_lookup_head(
        &database.pool,
        block,
        &hash,
        &crate::v2::format_timestamp(timestamp),
    )
    .await
}

/// A redo of `phase` begins and finishes, moving the phase row as the phase runner does.
async fn hk_phase_redo(database: &TestDatabase, phase: &str) -> Result<()> {
    hk_begin_redo(database, HK_CHAIN, phase).await?;
    hk_finish_redo(database, HK_CHAIN, phase).await
}

async fn hk_begin_redo(database: &TestDatabase, chain: &str, phase: &str) -> Result<()> {
    hk_phase_state(
        database,
        chain,
        phase,
        "UPDATE bigname_phase.chain_phase_state
         SET phase_status = 'running',
             redo_in_progress = true,
             redo_attempt_generation = redo_attempt_generation + 1,
             redo_mode = 'redo',
             redo_previous_phase_status = phase_status,
             redo_previous_last_error = last_error,
             redo_previous_started_at = started_at,
             redo_previous_finished_at = finished_at,
             redo_from_block_number = 0,
             redo_to_block_number = current_block_number,
             started_at = now(),
             finished_at = NULL,
             updated_at = now()
         WHERE chain_id = $1 AND phase_name = $2",
    )
    .await
}

async fn hk_finish_redo(database: &TestDatabase, chain: &str, phase: &str) -> Result<()> {
    hk_phase_state(
        database,
        chain,
        phase,
        "UPDATE bigname_phase.chain_phase_state
         SET phase_status = redo_previous_phase_status,
             last_error = redo_previous_last_error,
             started_at = redo_previous_started_at,
             finished_at = redo_previous_finished_at,
             redo_in_progress = false,
             redo_mode = NULL,
             redo_previous_phase_status = NULL,
             redo_previous_last_error = NULL,
             redo_previous_started_at = NULL,
             redo_previous_finished_at = NULL,
             redo_from_block_number = NULL,
             redo_to_block_number = NULL,
             updated_at = now()
         WHERE chain_id = $1 AND phase_name = $2 AND redo_in_progress",
    )
    .await
}

async fn hk_phase_state(
    database: &TestDatabase,
    chain: &str,
    phase: &str,
    statement: &str,
) -> Result<()> {
    let result = sqlx::query(statement)
        .bind(chain)
        .bind(phase)
        .execute(&database.lookup_pool)
        .await?;
    anyhow::ensure!(result.rows_affected() == 1, "{phase} phase state");
    Ok(())
}

/// The cursor as a client holding one issued before this rule would send it: the anchor's
/// numeric id and identity, a publication token, and an evaluation time.
async fn hk_legacy_cursor(database: &TestDatabase, cursor: &str) -> Result<String> {
    let mut value: Value = serde_json::from_slice(&hex::decode(cursor)?)?;
    let event_identity = value["last_item"]["event_identity"]
        .as_str()
        .context("cursor anchor")?
        .to_owned();
    let normalized_event_id: i64 = sqlx::query_scalar(
        "SELECT normalized_event_id FROM bigname_phase.normalized_events WHERE event_identity = $1",
    )
    .bind(&event_identity)
    .fetch_one(&database.pool)
    .await?;
    value["last_item"] = json!({
        "normalized_event_id": normalized_event_id.to_string(),
        "event_identity": event_identity,
    });
    // A cursor that already carries a publication token and evaluation time keeps them, so the
    // same test can run against a build that still checks them.
    if value["snapshot"].is_null() {
        value["snapshot"] = json!(format!("publication-0x{}", "ab".repeat(32)));
        value["evaluated_at"] = json!("2026-09-01T00:00:00Z");
    }
    Ok(hex::encode(serde_json::to_vec(&value)?))
}

fn hk_anchor(cursor: &str) -> Result<String> {
    let value: Value = serde_json::from_slice(&hex::decode(cursor)?)?;
    Ok(value["last_item"]["event_identity"]
        .as_str()
        .context("cursor anchor")?
        .to_owned())
}

async fn hk_delete_event(database: &TestDatabase, event_identity: &str) -> Result<()> {
    let deleted = sqlx::query("DELETE FROM bigname_phase.normalized_events WHERE event_identity = $1")
        .bind(event_identity)
        .execute(&database.pool)
        .await?
        .rows_affected();
    anyhow::ensure!(deleted == 1, "anchor {event_identity} must exist");
    Ok(())
}

/// An oldest-first walk continues across new publications and picks up the rows published
/// after its first page; every page reports the publication it read.
#[tokio::test]
async fn v2_history_continuation_reads_the_current_publication() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_history_fixture(&database).await?;
    let mut block = HK_PUBLISHED_BLOCK;
    for route in hk_routes() {
        let base = format!("{route}&order=asc&page_size=1");
        let first = hk_ok(&database, &base).await?;
        assert_eq!(first["meta"]["as_of"]["1"]["block_number"], json!(block));
        let cursor = hk_next_cursor(&first)?;
        block += 1;
        hk_publish_block(&database, block, true).await?;
        let pages = hk_walk(&database, &base, cursor).await?;
        for (_, as_of) in &pages {
            assert_eq!(as_of["1"]["block_number"], json!(block), "{base}");
        }
        if !route.starts_with("/v1/addresses/") {
            let newest = pages.last().and_then(|(ids, _)| ids.last()).cloned();
            let published = hk_ok(&database, &format!("{route}&page_size=1")).await?;
            assert_eq!(newest, hk_ids(&published).first().cloned(), "{base}");
        }
    }
    database.cleanup().await
}

/// A newest-first continuation after a new publication resumes after its anchor: the new row
/// sorts before the anchor, so the next page is the one it would have been.
#[tokio::test]
async fn v2_history_continuation_resumes_after_its_anchor() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_history_fixture(&database).await?;
    let mut block = HK_PUBLISHED_BLOCK;
    for route in hk_routes() {
        let base = format!("{route}&page_size=1");
        let cursor = hk_next_cursor(&hk_ok(&database, &base).await?)?;
        let expected = hk_ok(&database, &format!("{base}&cursor={cursor}")).await?;
        block += 1;
        hk_publish_block(&database, block, true).await?;
        let continued = hk_ok(&database, &format!("{base}&cursor={cursor}")).await?;
        assert_eq!(hk_ids(&continued), hk_ids(&expected), "{base}");
        assert_eq!(continued["meta"]["as_of"]["1"]["block_number"], json!(block));
    }
    database.cleanup().await
}

/// A redo that rotates every normalized-event id and drops the anchor row leaves a
/// continuation where it was.
#[tokio::test]
async fn v2_history_continuation_survives_a_redo() -> Result<()> {
    for route in hk_routes() {
        let database = TestDatabase::new_migrated().await?;
        seed_v2_history_fixture(&database).await?;
        let base = format!("{route}&page_size=1");
        let cursor = hk_next_cursor(&hk_ok(&database, &base).await?)?;
        let pages = hk_walk(&database, &base, cursor.clone()).await?;
        hk_begin_redo(&database, HK_CHAIN, "interpret").await?;
        sqlx::query("UPDATE bigname_phase.normalized_events SET normalized_event_id = DEFAULT")
            .execute(&database.pool)
            .await?;
        hk_delete_event(&database, &hk_anchor(&cursor)?).await?;
        hk_finish_redo(&database, HK_CHAIN, "interpret").await?;
        hk_phase_redo(&database, "project").await?;
        let after = hk_walk(&database, &base, cursor).await?;
        assert_eq!(
            after.iter().map(|(ids, _)| ids).collect::<Vec<_>>(),
            pages.iter().map(|(ids, _)| ids).collect::<Vec<_>>(),
            "{base}"
        );
        database.cleanup().await?;
    }
    Ok(())
}

/// A continuation whose anchor row was deleted continues after the anchor's position.
#[tokio::test]
async fn v2_history_continuation_outlives_its_anchor() -> Result<()> {
    for route in hk_routes() {
        let database = TestDatabase::new_migrated().await?;
        seed_v2_history_fixture(&database).await?;
        let base = format!("{route}&page_size=1");
        let cursor = hk_next_cursor(&hk_ok(&database, &base).await?)?;
        let expected = hk_ok(&database, &format!("{base}&cursor={cursor}")).await?;
        hk_delete_event(&database, &hk_anchor(&cursor)?).await?;
        let continued = hk_ok(&database, &format!("{base}&cursor={cursor}")).await?;
        assert_eq!(hk_ids(&continued), hk_ids(&expected), "{base}");
        database.cleanup().await?;
    }
    Ok(())
}

/// A cursor issued before this rule ignores its publication token and resumes through its
/// anchor row; when that row is gone it restarts once.
#[tokio::test]
async fn v2_history_legacy_cursor_resumes_through_its_anchor() -> Result<()> {
    for route in hk_routes() {
        let database = TestDatabase::new_migrated().await?;
        seed_v2_history_fixture(&database).await?;
        let base = format!("{route}&page_size=1");
        let cursor = hk_next_cursor(&hk_ok(&database, &base).await?)?;
        let expected = hk_ok(&database, &format!("{base}&cursor={cursor}")).await?;
        let legacy = hk_legacy_cursor(&database, &cursor).await?;
        let continued = hk_ok(&database, &format!("{base}&cursor={legacy}")).await?;
        assert_eq!(hk_ids(&continued), hk_ids(&expected), "{base}");
        assert_eq!(
            continued["page"]["next_cursor"], expected["page"]["next_cursor"],
            "{base}"
        );

        hk_delete_event(&database, &hk_anchor(&cursor)?).await?;
        let (status, payload) = hk_get(&database, &format!("{base}&cursor={legacy}")).await?;
        assert_eq!(status, StatusCode::CONFLICT, "{base}: {payload}");
        assert_eq!(payload["error"]["message"], json!(HK_RESTART), "{base}");
        database.cleanup().await?;
    }
    Ok(())
}

/// A redo that removes a legacy cursor's anchor answers with the redo retry while it runs, as
/// the redo check comes before the anchor is looked up; once it finishes, the same cursor gets
/// the one-time restart. The redo runs on a chain outside the request's publication scope, so
/// admission still finds its publication and only the collection-wide redo check can refuse.
#[tokio::test]
async fn v2_history_legacy_cursor_retries_a_redo_before_restarting() -> Result<()> {
    for route in hk_routes() {
        let database = TestDatabase::new_migrated().await?;
        seed_v2_history_fixture(&database).await?;
        let base = format!("{route}&page_size=1");
        let cursor = hk_next_cursor(&hk_ok(&database, &base).await?)?;
        let legacy = hk_legacy_cursor(&database, &cursor).await?;
        let uri = format!("{base}&cursor={legacy}");

        sqlx::query(
            "INSERT INTO bigname_phase.chain_phase_state
                 (chain_id, phase_name, phase_status, current_block_number,
                  current_block_hash, target_block_number, target_block_hash,
                  input_content_hash, started_at, finished_at)
             VALUES ($1, 'interpret', 'completed', 1, '0xother1', 1, '0xother1', $2, now(), now())
             ON CONFLICT (chain_id, phase_name) DO NOTHING",
        )
        .bind(HK_OTHER_CHAIN)
        .bind(bigname_content_hash::INTERPRETER_CONTENT_HASH)
        .execute(&database.lookup_pool)
        .await?;
        hk_begin_redo(&database, HK_OTHER_CHAIN, "interpret").await?;
        hk_delete_event(&database, &hk_anchor(&cursor)?).await?;
        let (status, payload) = hk_get(&database, &uri).await?;
        assert_eq!(status, StatusCode::CONFLICT, "{base}: {payload}");
        assert_eq!(payload["error"]["message"], json!(HK_REDO_RETRY), "{base}");

        hk_finish_redo(&database, HK_OTHER_CHAIN, "interpret").await?;
        let (status, payload) = hk_get(&database, &uri).await?;
        assert_eq!(status, StatusCode::CONFLICT, "{base}: {payload}");
        assert_eq!(payload["error"]["message"], json!(HK_RESTART), "{base}");
        database.cleanup().await?;
    }
    Ok(())
}

/// A publication that lands during the read leaves the page as read: 200, reporting the
/// publication the page read, on a first page and on a continuation.
#[tokio::test]
async fn v2_history_pages_ignore_a_publication_during_the_read() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_history_fixture(&database).await?;
    let mut block = HK_PUBLISHED_BLOCK;
    for route in hk_routes() {
        let base = format!("{route}&page_size=1");
        let cursor = hk_next_cursor(&hk_ok(&database, &base).await?)?;
        for uri in [base.clone(), format!("{base}&cursor={cursor}")] {
            let (guard, control) =
                crate::v2::collection_snapshot::finish_test_hooks::install(&database.lookup_pool)
                    .await?;
            let state = database.app_state_with_public_namespaces(&["ens"]);
            let request_uri = uri.clone();
            let request = tokio::spawn(async move {
                let response = app_router(state)
                    .oneshot(Request::builder().uri(request_uri).body(Body::empty())?)
                    .await?;
                let status = response.status();
                anyhow::Ok((status, read_json::<Value>(response).await?))
            });
            tokio::time::timeout(std::time::Duration::from_secs(10), control.wait_until_reached())
                .await
                .context("history request did not reach its finish hook")?;
            hk_publish_block(&database, block + 1, false).await?;
            control.resume().await;
            let (status, payload) = request.await.context("request task")??;
            drop(guard);
            assert_eq!(status, StatusCode::OK, "{uri}: {payload}");
            assert_eq!(payload["meta"]["as_of"]["1"]["block_number"], json!(block), "{uri}");
            block += 1;
        }
    }
    database.cleanup().await
}

/// Replaces one `last_item` value of an issued cursor, keeping everything else.
fn hk_with_last_item(cursor: &str, key: &str, value: &str) -> Result<String> {
    let mut payload: Value = serde_json::from_slice(&hex::decode(cursor)?)?;
    payload["last_item"][key] = json!(value);
    Ok(hex::encode(serde_json::to_vec(&payload)?))
}

/// A malformed cursor is refused with 400 before publication admission, so an Interpret redo
/// that leaves no publication to admit still answers 400 for it, in either cursor layout.
#[tokio::test]
async fn v2_history_malformed_cursors_precede_publication_admission() -> Result<()> {
    for route in hk_routes() {
        let database = TestDatabase::new_migrated().await?;
        seed_v2_history_fixture(&database).await?;
        let base = format!("{route}&page_size=1");
        let cursor = hk_next_cursor(&hk_ok(&database, &base).await?)?;
        let legacy = hk_legacy_cursor(&database, &cursor).await?;
        hk_begin_redo(&database, HK_CHAIN, "interpret").await?;

        let (status, payload) = hk_get(&database, &format!("{base}&cursor={cursor}")).await?;
        assert_eq!(status, StatusCode::CONFLICT, "{base}: {payload}");
        for malformed in [
            "garbage".to_owned(),
            hk_with_last_item(&cursor, "block_number", "not-a-number")?,
            hk_with_last_item(&legacy, "normalized_event_id", "not-a-number")?,
            hk_with_last_item(&cursor, "event_identity", "   ")?,
            hk_with_last_item(&legacy, "event_identity", "   ")?,
        ] {
            let (status, payload) =
                hk_get(&database, &format!("{base}&cursor={malformed}")).await?;
            assert_eq!(status, StatusCode::BAD_REQUEST, "{base}: {payload}");
            assert_eq!(payload["error"]["code"], json!("invalid_input"), "{base}");
        }
        database.cleanup().await?;
    }
    Ok(())
}

/// A cursor issued before the walk rule recovers its position from its anchor row even when
/// that row is no longer one the request reads, orphaned or detached from the name, and the
/// walk continues from there.
#[tokio::test]
async fn v2_history_legacy_cursor_resumes_from_an_anchor_outside_the_collection() -> Result<()> {
    for route in hk_routes() {
        for detach in [
            "UPDATE bigname_phase.normalized_events SET canonicality_state = 'orphaned'
             WHERE event_identity = $1",
            "UPDATE bigname_phase.normalized_events SET logical_name_id = NULL, resource_id = NULL
             WHERE event_identity = $1",
        ] {
            let database = TestDatabase::new_migrated().await?;
            seed_v2_history_fixture(&database).await?;
            let base = format!("{route}&page_size=1");
            let cursor = hk_next_cursor(&hk_ok(&database, &base).await?)?;
            let expected = hk_ok(&database, &format!("{base}&cursor={cursor}")).await?;
            let legacy = hk_legacy_cursor(&database, &cursor).await?;
            let anchor = hk_anchor(&cursor)?;
            let updated = sqlx::query(detach)
                .bind(&anchor)
                .execute(&database.pool)
                .await?
                .rows_affected();
            anyhow::ensure!(updated == 1, "anchor {anchor} must exist");
            let first = hk_ok(&database, &base).await?;
            assert!(!hk_ids(&first).is_empty(), "{base}");
            let continued = hk_ok(&database, &format!("{base}&cursor={legacy}")).await?;
            assert_eq!(hk_ids(&continued), hk_ids(&expected), "{base}: {detach}");
            database.cleanup().await?;
        }
    }
    Ok(())
}

/// `/v1/diagnostics/events` keeps its own cursor: exactly the boundary row's numeric id and
/// identity, no publication token, and a 400 once that row is gone.
#[tokio::test]
async fn v2_diagnostic_events_keep_their_anchor_cursor() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_history_fixture(&database).await?;
    let base = "/v1/diagnostics/events?name=history.eth&page_size=1";
    let first = hk_ok(&database, base).await?;
    let cursor = hk_next_cursor(&first)?;
    let payload: Value = serde_json::from_slice(&hex::decode(&cursor)?)?;
    let last_item = payload["last_item"].as_object().context("last_item")?;
    assert_eq!(
        last_item.keys().map(String::as_str).collect::<Vec<_>>(),
        vec!["event_identity", "normalized_event_id"]
    );
    let anchor = hk_anchor(&cursor)?;
    let normalized_event_id: i64 = sqlx::query_scalar(
        "SELECT normalized_event_id FROM bigname_phase.normalized_events WHERE event_identity = $1",
    )
    .bind(&anchor)
    .fetch_one(&database.pool)
    .await?;
    assert_eq!(last_item["normalized_event_id"], json!(normalized_event_id.to_string()));
    assert_eq!(payload["snapshot"], Value::Null);
    assert!(payload.get("evaluated_at").is_none(), "{payload}");

    let next = hk_ok(&database, &format!("{base}&cursor={cursor}")).await?;
    assert_eq!(next["data"].as_array().map(Vec::len), Some(1));

    hk_delete_event(&database, &anchor).await?;
    let (status, payload) = hk_get(&database, &format!("{base}&cursor={cursor}")).await?;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{payload}");
    assert_eq!(payload["error"]["code"], json!("invalid_input"));
    database.cleanup().await
}
