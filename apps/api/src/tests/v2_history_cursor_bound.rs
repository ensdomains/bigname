// History cursors bound to a block. A history walk reads, on every page, the history as it
// stood at the block its first page was served at. New blocks, a running Project, and a
// projection swap during the read never break the walk; a changed redo generation, an active
// redo, a manifest change, an unreadable bound block, or a cursor issued before this rule
// expires it (docs/api-v2-routes.md, "Shared Route Rules").

const HB_ADDRESS: &str = "0x00000000000000000000000000000000000000cc";
const HB_CHAIN: &str = "ethereum-mainnet";
/// The fixture's publication: `seed_default_ens_snapshot_selector_position`.
const HB_PUBLISHED_BLOCK: i64 = 21_000_003;
const HB_LEGACY: &str =
    "this cursor was issued before position-bound pagination; restart without a cursor";
const HB_RESTART: &str =
    "collection publication is no longer available; restart pagination without a cursor";
const HB_PROJECT_REDO: &str = "history is temporarily unavailable while Project redo is in progress";

fn hb_routes() -> [String; 3] {
    [
        "/v1/events?name=history.eth".to_owned(),
        "/v1/names/history.eth/history?scope=both".to_owned(),
        format!("/v1/addresses/{HB_ADDRESS}/history?scope=both"),
    ]
}

async fn hb_get(database: &TestDatabase, uri: &str) -> Result<(StatusCode, Value)> {
    let response = v2_history_response_for_database(database, uri).await?;
    let status = response.status();
    Ok((status, read_json(response).await?))
}

async fn hb_ok(database: &TestDatabase, uri: &str) -> Result<Value> {
    let (status, payload) = hb_get(database, uri).await?;
    anyhow::ensure!(status == StatusCode::OK, "{uri}: {status} {payload}");
    Ok(payload)
}

fn hb_next_cursor(payload: &Value) -> Result<String> {
    Ok(payload["page"]["next_cursor"]
        .as_str()
        .context("page must continue")?
        .to_owned())
}

fn hb_ids(payload: &Value) -> Vec<String> {
    payload["data"]
        .as_array()
        .expect("history data")
        .iter()
        .map(|row| row["id"].as_str().expect("row id").to_owned())
        .collect()
}

fn hb_blocks(payload: &Value) -> Vec<i64> {
    payload["data"]
        .as_array()
        .expect("history data")
        .iter()
        .map(|row| row["block_number"].as_i64().expect("block number"))
        .collect()
}

fn hb_timestamp(block: i64) -> i64 {
    1_776_384_000 + (block - 21_000_000)
}

/// Rewrites one field of an issued cursor, as a client holding an old or edited cursor would.
fn hb_edit_cursor(cursor: &str, edit: impl FnOnce(&mut serde_json::Map<String, Value>)) -> String {
    let mut value: Value =
        serde_json::from_slice(&hex::decode(cursor).expect("cursor is hex")).expect("json");
    edit(value.as_object_mut().expect("cursor object"));
    hex::encode(serde_json::to_vec(&value).expect("cursor serializes"))
}

/// Project publishes `block`: the chain head, lineage, and Project position all move to it.
/// With `with_row`, Interpret first writes a new product-visible row for `history.eth` there.
async fn hb_publish_block(database: &TestDatabase, block: i64, with_row: bool) -> Result<()> {
    let hash = format!("0xhistory{block}");
    upsert_phase_raw_blocks(
        &database.pool,
        &[raw_block(HB_CHAIN, &hash, None, block, hb_timestamp(block))],
    )
    .await?;
    if with_row {
        bigname_storage::insert_normalized_event_fixtures(
            &database.pool,
            &[v2_history_event(
                &format!("hb-record-{block}"),
                Some("ens:history.eth"),
                None,
                "RecordChanged",
                block,
            )],
        )
        .await?;
    }
    let timestamp = sqlx::types::time::OffsetDateTime::from_unix_timestamp(hb_timestamp(block))?;
    seed_schema_v2_ens_lookup_head(
        &database.pool,
        block,
        &hash,
        &crate::v2::format_timestamp(timestamp),
    )
    .await
}

async fn hb_phase_redo_begin(database: &TestDatabase, phase: &str) -> Result<()> {
    let result = sqlx::query(
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
    .bind(HB_CHAIN)
    .bind(phase)
    .execute(&database.lookup_pool)
    .await?;
    anyhow::ensure!(result.rows_affected() == 1, "missing {phase} phase state");
    Ok(())
}

async fn hb_phase_redo_finish(database: &TestDatabase, phase: &str) -> Result<()> {
    let result = sqlx::query(
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
    .bind(HB_CHAIN)
    .bind(phase)
    .execute(&database.lookup_pool)
    .await?;
    anyhow::ensure!(result.rows_affected() == 1, "no active {phase} redo");
    Ok(())
}

/// Project marks its row `running` while it builds the next block, without moving position.
async fn hb_project_running(database: &TestDatabase) -> Result<()> {
    sqlx::query(
        "UPDATE bigname_phase.chain_phase_state
         SET phase_status = 'running', finished_at = NULL, updated_at = now()
         WHERE chain_id = $1 AND phase_name = 'project'",
    )
    .bind(HB_CHAIN)
    .execute(&database.lookup_pool)
    .await?;
    Ok(())
}

/// A continuation walks the history as it stood at its first page's block while the chain
/// advances between every page: same rows, same count, `as_of` at the bound. A fresh first page
/// then sees the new blocks.
#[tokio::test]
async fn v2_history_cursors_continue_across_new_blocks() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_history_fixture(&database).await?;
    let mut block = HB_PUBLISHED_BLOCK;
    let variants = [
        ("", 2),
        ("&type=record,authority,resolver", 1),
        ("&from_timestamp=2023-11-14T22:15:03Z", 2),
    ];
    let mut routes = hb_routes().to_vec();
    routes.push("/v1/names/history.eth/history?scope=both&include=child_registrations".to_owned());
    for route in routes {
        for order in ["desc", "asc"] {
            for (filter, page_size) in variants {
                let include = if route.contains("include=") { ",total_count" } else { "&include=total_count" };
                let base = format!("{route}{include}&order={order}{filter}");
                let baseline = hb_ok(&database, &format!("{base}&page_size=200")).await?;
                let first = hb_ok(&database, &format!("{base}&page_size={page_size}")).await?;
                let bound_as_of = first["meta"]["as_of"].clone();
                let bound = bound_as_of["1"]["block_number"].as_i64().context("as_of")?;
                let total = first["page"]["total_count"].clone();
                assert_eq!(total, baseline["page"]["total_count"], "{base}");
                let mut walked = hb_ids(&first);
                let mut cursor = hb_next_cursor(&first).with_context(|| format!("{base}: {first}"))?;
                let mut pages = 1;
                loop {
                    block += 1;
                    hb_publish_block(&database, block, pages == 1).await?;
                    let page = hb_ok(
                        &database,
                        &format!("{base}&page_size={page_size}&cursor={cursor}"),
                    )
                    .await?;
                    pages += 1;
                    assert_eq!(page["meta"]["as_of"], bound_as_of, "{base}: {page}");
                    assert_eq!(page["page"]["total_count"], total, "{base}: {page}");
                    assert!(
                        hb_blocks(&page).iter().all(|number| *number <= bound),
                        "{base}: a row above the bound: {page}"
                    );
                    walked.extend(hb_ids(&page));
                    match page["page"]["next_cursor"].as_str() {
                        Some(next) => cursor = next.to_owned(),
                        None => break,
                    }
                }
                assert!(pages >= 3, "{base}: walk too short to test ({pages} pages)");
                assert_eq!(walked, hb_ids(&baseline), "{base}");

                let fresh = hb_ok(&database, &format!("{base}&page_size=200")).await?;
                assert_ne!(fresh["meta"]["as_of"], bound_as_of, "{base}");
                assert!(
                    hb_blocks(&fresh).iter().any(|number| *number > bound),
                    "{base}: a fresh first page must see the new blocks: {fresh}"
                );
            }
        }
    }
    database.cleanup().await
}

/// Cursors issued before this rule, and cursors carrying both the old publication token and
/// a block binding, which no server mints.
#[tokio::test]
async fn v2_history_rejects_legacy_and_mixed_cursors() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_history_fixture(&database).await?;
    for route in hb_routes() {
        let base = format!("{route}&page_size=2");
        let cursor = hb_next_cursor(&hb_ok(&database, &base).await?)?;
        let legacy = hb_edit_cursor(&cursor, |object| {
            object.remove("binding");
            object.insert("snapshot".to_owned(), json!("publication-0x01"));
        });
        let neither = hb_edit_cursor(&cursor, |object| {
            object.remove("binding");
            object.insert("snapshot".to_owned(), Value::Null);
        });
        for old in [legacy, neither] {
            let (status, payload) = hb_get(&database, &format!("{base}&cursor={old}")).await?;
            assert_eq!(status, StatusCode::CONFLICT, "{base}: {payload}");
            assert_eq!(payload["error"]["code"], json!("stale"), "{base}");
            assert_eq!(payload["error"]["message"], json!(HB_LEGACY), "{base}");
        }

        let mixed = hb_edit_cursor(&cursor, |object| {
            object.insert("snapshot".to_owned(), json!("publication-0x01"));
            object.entry("binding").or_insert_with(|| {
                json!({"policy": "history-bound-v1", "manifests": format!("0x{}", "0".repeat(64)), "chains": {}})
            });
        });
        let (status, payload) = hb_get(&database, &format!("{base}&cursor={mixed}")).await?;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{base}: {payload}");
        assert_eq!(payload["error"]["code"], json!("invalid_input"), "{base}");
    }
    database.cleanup().await
}

/// A Project redo rewrites the projections history membership reads. Active at capture it
/// refuses a first page; begun and finished between pages it expires the cursor.
#[tokio::test]
async fn v2_history_refuses_and_expires_on_project_redo() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_history_fixture(&database).await?;
    for route in hb_routes() {
        let base = format!("{route}&page_size=2");
        hb_phase_redo_begin(&database, "project").await?;
        let (status, payload) = hb_get(&database, &base).await?;
        assert_eq!(status, StatusCode::CONFLICT, "{base}: {payload}");
        assert_eq!(payload["error"]["code"], json!("stale"), "{base}");
        assert_eq!(payload["error"]["message"], json!(HB_PROJECT_REDO), "{base}");
        assert!(payload.get("page").is_none(), "{base}");
        hb_phase_redo_finish(&database, "project").await?;

        let cursor = hb_next_cursor(&hb_ok(&database, &base).await?)?;
        hb_phase_redo_begin(&database, "project").await?;
        hb_phase_redo_finish(&database, "project").await?;
        let (status, payload) = hb_get(&database, &format!("{base}&cursor={cursor}")).await?;
        assert_eq!(status, StatusCode::CONFLICT, "{base}: {payload}");
        assert_eq!(payload["error"]["message"], json!(HB_RESTART), "{base}");

        let fresh = hb_ok(&database, &base).await?;
        hb_ok(&database, &format!("{base}&cursor={}", hb_next_cursor(&fresh)?)).await?;
    }
    database.cleanup().await
}

/// An Interpret redo that begins and finishes between pages leaves a new generation, so the
/// continuation cannot prove its rows were not rewritten.
#[tokio::test]
async fn v2_history_expires_cursor_after_interpret_redo() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_history_fixture(&database).await?;
    for route in hb_routes() {
        let base = format!("{route}&page_size=2");
        let cursor = hb_next_cursor(&hb_ok(&database, &base).await?)?;
        database
            .simulate_interpret_redo_begin(HB_CHAIN, "recompute_flags")
            .await?;
        let (status, payload) = hb_get(&database, &format!("{base}&cursor={cursor}")).await?;
        assert_eq!(status, StatusCode::CONFLICT, "{base}: {payload}");
        assert_eq!(payload["error"]["code"], json!("stale"), "{base}");
        database.simulate_interpret_redo_finish(HB_CHAIN).await?;
        let (status, payload) = hb_get(&database, &format!("{base}&cursor={cursor}")).await?;
        assert_eq!(status, StatusCode::CONFLICT, "{base}: {payload}");
        assert_eq!(payload["error"]["message"], json!(HB_RESTART), "{base}");

        let fresh = hb_ok(&database, &base).await?;
        hb_ok(&database, &format!("{base}&cursor={}", hb_next_cursor(&fresh)?)).await?;
    }
    database.cleanup().await
}

/// Project flips to `running` and publishes the next block while a page is being read (after
/// anchors are resolved, before the page transaction). The page still answers at its bound.
#[tokio::test]
async fn v2_history_pages_survive_a_publication_during_the_read() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_history_fixture(&database).await?;
    let mut block = HB_PUBLISHED_BLOCK;
    for route in hb_routes() {
        let base = format!("{route}&page_size=2&include=total_count");
        let first = hb_ok(&database, &base).await?;
        let cursor = hb_next_cursor(&first)?;
        for uri in [base.clone(), format!("{base}&cursor={cursor}")] {
            let expected_as_of = hb_ok(&database, &base).await?["meta"]["as_of"].clone();
            let (_guard, control) = bigname_storage::history_anchor_read_test_hooks::install(
                &database.lookup_pool,
                bigname_storage::history_anchor_read_test_hooks::HistoryReadHookPoint::AfterAnchors,
            )
            .await?;
            let state = database.app_state_with_public_namespaces(&["ens"]);
            let request_uri = uri.clone();
            let request = tokio::spawn(async move {
                app_router(state)
                    .oneshot(
                        Request::builder()
                            .uri(request_uri)
                            .body(Body::empty())
                            .expect("history request must build"),
                    )
                    .await
            });
            tokio::time::timeout(std::time::Duration::from_secs(10), control.wait_until_reached())
                .await
                .context("history request did not reach its read hook")?;
            hb_project_running(&database).await?;
            block += 1;
            hb_publish_block(&database, block, true).await?;
            control.resume().await;
            let response = request.await.context("request task")?.context("request")?;
            let status = response.status();
            let payload: Value = read_json(response).await?;
            assert_eq!(status, StatusCode::OK, "{uri}: {payload}");
            let as_of = if uri.contains("cursor=") {
                first["meta"]["as_of"].clone()
            } else {
                expected_as_of
            };
            assert_eq!(payload["meta"]["as_of"], as_of, "{uri}");
            assert!(hb_blocks(&payload).iter().all(|number| *number < block), "{uri}");
        }
    }
    database.cleanup().await
}

/// The missing-name `404` is answered only after the full recheck: a Project redo that begins
/// and finishes before the answer turns it into `409 stale`.
#[tokio::test]
async fn v2_name_history_missing_parent_rechecks_project_redo() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_history_fixture(&database).await?;
    let (_guard, control) =
        crate::v2::collection_snapshot::finish_test_hooks::install(&database.lookup_pool).await?;
    let state = database.app_state_with_public_namespaces(&["ens"]);
    let request = tokio::spawn(async move {
        app_router(state)
            .oneshot(
                Request::builder()
                    .uri("/v1/names/missing-history.eth/history")
                    .body(Body::empty())
                    .expect("history request must build"),
            )
            .await
    });
    tokio::time::timeout(std::time::Duration::from_secs(10), control.wait_until_reached())
        .await
        .context("missing-name history never ran its recheck")?;
    hb_phase_redo_begin(&database, "project").await?;
    hb_phase_redo_finish(&database, "project").await?;
    control.resume().await;
    let response = request.await.context("request task")?.context("request")?;
    let status = response.status();
    let payload: Value = read_json(response).await?;
    assert_eq!(status, StatusCode::CONFLICT, "{payload}");
    assert_eq!(payload["error"]["code"], json!("stale"));

    let (status, _) = hb_get(&database, "/v1/names/missing-history.eth/history").await?;
    assert_eq!(status, StatusCode::NOT_FOUND);
    database.cleanup().await
}


/// Every redo counter of `HB_CHAIN` moves, as a reorg's orphaned-suffix stamp does.
async fn hb_bump_generations(database: &TestDatabase) -> Result<()> {
    sqlx::query(
        "UPDATE bigname_phase.chain_phase_state
         SET redo_attempt_generation = redo_attempt_generation + 1
         WHERE chain_id = $1 AND phase_name IN ('interpret', 'project')",
    )
    .bind(HB_CHAIN)
    .execute(&database.lookup_pool)
    .await?;
    Ok(())
}

/// Replace the canonical block at `block` with a sibling and publish the sibling. The head
/// steps back to `parent` while the old block is orphaned, as a reorg does.
async fn hb_reorg(database: &TestDatabase, block: i64, old_hash: &str, parent: &str) -> Result<()> {
    sqlx::query(
        "UPDATE bigname_phase.chain_heads
         SET latest_block_hash = $2, latest_block_number = $3
         WHERE chain_id = $1",
    )
    .bind(HB_CHAIN)
    .bind(parent)
    .bind(block - 1)
    .execute(&database.lookup_pool)
    .await?;
    sqlx::query(
        "UPDATE bigname_phase.chain_lineage SET canonicality_state = 'orphaned'
         WHERE chain_id = $1 AND block_hash = $2",
    )
    .bind(HB_CHAIN)
    .bind(old_hash)
    .execute(&database.lookup_pool)
    .await?;
    let hash = format!("{old_hash}-sibling");
    upsert_phase_raw_blocks(
        &database.pool,
        &[raw_block(HB_CHAIN, &hash, None, block, hb_timestamp(block))],
    )
    .await?;
    let timestamp = sqlx::types::time::OffsetDateTime::from_unix_timestamp(hb_timestamp(block))?;
    seed_schema_v2_ens_lookup_head(&database.pool, block, &hash, &crate::v2::format_timestamp(timestamp))
        .await?;
    hb_bump_generations(database).await
}

async fn hb_expect_stale(database: &TestDatabase, uri: &str, message: &str) -> Result<()> {
    let (status, payload) = hb_get(database, uri).await?;
    assert_eq!(status, StatusCode::CONFLICT, "{uri}: {payload}");
    assert_eq!(payload["error"]["code"], json!("stale"), "{uri}");
    assert_eq!(payload["error"]["message"], json!(message), "{uri}");
    Ok(())
}

/// A reorg that replaces the bound block, or one entirely above it, expires the cursor: both
/// move the redo counters, and the first also leaves the bound block unreadable.
#[tokio::test]
async fn v2_history_cursor_expires_on_reorg() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_history_fixture(&database).await?;
    let base = format!("{}&page_size=2", hb_routes()[1]);
    // The fixture's own block is finalized; reorg the unfinalized blocks above it.
    let bound = HB_PUBLISHED_BLOCK + 1;
    hb_publish_block(&database, bound, false).await?;

    let at_bound = hb_next_cursor(&hb_ok(&database, &base).await?)?;
    hb_reorg(&database, bound, &format!("0xhistory{bound}"), "0xbinding").await?;
    hb_expect_stale(&database, &format!("{base}&cursor={at_bound}"), HB_RESTART).await?;

    let fresh = hb_ok(&database, &base).await?;
    assert_eq!(fresh["meta"]["as_of"]["1"]["block_hash"], json!(format!("0xhistory{bound}-sibling")));
    let above_bound = hb_next_cursor(&fresh)?;
    hb_publish_block(&database, bound + 1, false).await?;
    hb_reorg(
        &database,
        bound + 1,
        &format!("0xhistory{}", bound + 1),
        &format!("0xhistory{bound}-sibling"),
    )
    .await?;
    // Conservative: the replaced block is above the bound, but the counters moved.
    hb_expect_stale(&database, &format!("{base}&cursor={above_bound}"), HB_RESTART).await?;
    hb_ok(&database, &format!("{base}&cursor={}", hb_next_cursor(&hb_ok(&database, &base).await?)?))
        .await?;
    database.cleanup().await
}

/// Edited bindings: the chain set must be the server's, a bound above the publication or on
/// an unknown block is refused, a bound below the cursor row is a malformed cursor, and a lower
/// bound that still holds the cursor row answers at that bound.
#[tokio::test]
async fn v2_history_checks_edited_bindings() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_history_fixture(&database).await?;
    let lower = HB_PUBLISHED_BLOCK;
    let upper = HB_PUBLISHED_BLOCK + 1;
    hb_publish_block(&database, upper, true).await?;
    // A lineage block above the publication that Project has not published.
    upsert_phase_raw_blocks(
        &database.pool,
        &[raw_block(HB_CHAIN, "0xunpublished", None, upper + 1, hb_timestamp(upper + 1))],
    )
    .await?;
    let set_bound = |cursor: &str, number: i64, hash: &str| {
        let hash = hash.to_owned();
        hb_edit_cursor(cursor, move |object| {
            let chain = &mut object["binding"]["chains"][HB_CHAIN];
            chain["block_number"] = json!(number);
            chain["block_hash"] = json!(hash);
        })
    };
    for route in hb_routes() {
        let desc = format!("{route}&page_size=1&include=total_count");
        let first = hb_ok(&database, &desc).await?;
        assert_eq!(hb_blocks(&first), vec![upper], "{desc}");
        let cursor = hb_next_cursor(&first)?;

        let dropped = hb_edit_cursor(&cursor, |object| {
            object["binding"]["chains"].as_object_mut().unwrap().remove(HB_CHAIN);
        });
        let added = hb_edit_cursor(&cursor, |object| {
            let chains = object["binding"]["chains"].as_object_mut().unwrap();
            let copy = chains[HB_CHAIN].clone();
            chains.insert("base-mainnet".to_owned(), copy);
        });
        let raised = set_bound(&cursor, upper + 1, "0xunpublished");
        let unknown = set_bound(&cursor, upper, "0xnot-a-block");
        for edited in [dropped, added, raised, unknown] {
            hb_expect_stale(&database, &format!("{desc}&cursor={edited}"), HB_RESTART).await?;
        }

        // The cursor row sits at `upper`: a bound at `lower` cannot contain it.
        let below_anchor = set_bound(&cursor, lower, "0xbinding");
        let (status, payload) = hb_get(&database, &format!("{desc}&cursor={below_anchor}")).await?;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{desc}: {payload}");

        // Oldest first, the cursor row is old enough to survive the lower bound.
        let asc = format!("{desc}&order=asc");
        let first = hb_ok(&database, &asc).await?;
        let lowered = set_bound(&hb_next_cursor(&first)?, lower, "0xbinding");
        let page = hb_ok(&database, &format!("{asc}&cursor={lowered}")).await?;
        assert_eq!(page["meta"]["as_of"]["1"]["block_number"], json!(lower), "{asc}");
        assert_eq!(page["meta"]["as_of"]["1"]["block_hash"], json!("0xbinding"), "{asc}");
        assert_eq!(
            page["page"]["total_count"].as_u64(),
            first["page"]["total_count"].as_u64().map(|total| total - 1),
            "{asc}: the lowered bound drops the row at {upper}"
        );
    }
    database.cleanup().await
}

/// Malformed bindings are `400`: an unknown policy, a keyset binding on a history route, a
/// negative counter, a missing chain field, and a cursor without its chains.
#[tokio::test]
async fn v2_history_rejects_malformed_bindings() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_history_fixture(&database).await?;
    for route in hb_routes() {
        let base = format!("{route}&page_size=2");
        let cursor = hb_next_cursor(&hb_ok(&database, &base).await?)?;
        type CursorEdit = Box<dyn Fn(&mut serde_json::Map<String, Value>)>;
        let edits: [CursorEdit; 5] = [
            Box::new(|object| object["binding"]["policy"] = json!("frozen-v1")),
            Box::new(|object| {
                object["binding"]["policy"] = json!("keyset-v1");
                object["binding"].as_object_mut().unwrap().remove("chains");
            }),
            Box::new(|object| {
                object["binding"]["chains"][HB_CHAIN]["project_generation"] = json!(-1);
            }),
            Box::new(|object| {
                object["binding"]["chains"][HB_CHAIN]
                    .as_object_mut()
                    .unwrap()
                    .remove("block_hash");
            }),
            Box::new(|object| {
                object["binding"].as_object_mut().unwrap().remove("chains");
            }),
        ];
        for edit in edits {
            let edited = hb_edit_cursor(&cursor, |object| edit(object));
            let (status, payload) = hb_get(&database, &format!("{base}&cursor={edited}")).await?;
            assert_eq!(status, StatusCode::BAD_REQUEST, "{base}: {payload}");
            assert_eq!(payload["error"]["code"], json!("invalid_input"), "{base}");
        }
    }
    database.cleanup().await
}

/// The manifest digest binds the manifest set: new blocks keep it, a new manifest expires the
/// cursor. Uses the manifest-backed namespace set, not the test override.
#[tokio::test]
async fn v2_history_cursor_expires_on_manifest_change() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_history_fixture(&database).await?;
    database
        .insert_manifest(
            "ens",
            "ens_v1_registry_l1",
            HB_CHAIN,
            "ens_v1",
            1,
            "active",
            bigname_domain::normalization::ENS_NORMALIZER_VERSION,
        )
        .await?;
    let get = |uri: String| {
        let state = AppState::new_with_rpc_urls(
            database.lookup_pool.clone(),
            bigname_lookup::ChainRpcUrls::default(),
        );
        async move {
            let response = app_router(state)
                .oneshot(Request::builder().uri(uri).body(Body::empty()).expect("request"))
                .await?;
            let status = response.status();
            anyhow::Ok((status, read_json::<Value>(response).await?))
        }
    };
    for route in hb_routes() {
        let base = format!("{route}&page_size=1");
        let (status, first) = get(base.clone()).await?;
        assert_eq!(status, StatusCode::OK, "{base}: {first}");
        let cursor = hb_next_cursor(&first)?;
        let (status, second) = get(format!("{base}&cursor={cursor}")).await?;
        assert_eq!(status, StatusCode::OK, "{base}: {second}");
        let next = hb_next_cursor(&second)?;
        database
            .insert_manifest(
                "ens",
                "ens_v1_resolver_l1",
                HB_CHAIN,
                "ens_v1",
                1,
                "active",
                bigname_domain::normalization::ENS_NORMALIZER_VERSION,
            )
            .await?;
        let (status, payload) = get(format!("{base}&cursor={next}")).await?;
        assert_eq!(status, StatusCode::CONFLICT, "{base}: {payload}");
        assert_eq!(payload["error"]["message"], json!(HB_RESTART), "{base}");
        sqlx::query(
            "DELETE FROM bigname_phase.manifest_versions WHERE source_family = 'ens_v1_resolver_l1'",
        )
        .execute(&database.pool)
        .await?;
    }
    database.cleanup().await
}
