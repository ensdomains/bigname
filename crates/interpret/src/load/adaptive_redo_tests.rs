//! Adaptive redo batches over the Lifecycle history. With `config()` the planner cuts
//! [0] and [1] alone (14 and 5 logs, both over 4), [2, 3, 4] at the five-block planning
//! horizon, [5..=8] before block 9's logs would pass 4, then [9] and [10, 11]: an oversized
//! first and middle block, a log split, a horizon edge and sparse spans. Only the stored
//! rows are compared with fixed batches. The boundaries are what may differ.
use std::{
    collections::BTreeMap,
    io::{self, Write},
    num::NonZeroU64,
    sync::{Arc, Mutex},
};

use super::*;
use crate::{AdaptiveBatchConfig, ErrorKind};

fn config() -> AdaptiveBatchConfig {
    AdaptiveBatchConfig {
        target_logs: NonZeroU64::new(4).unwrap(),
        target_input_bytes: NonZeroU64::new(64 * 1024 * 1024).unwrap(),
        max_pending_input_bytes: NonZeroU64::new(64 * 1024 * 1024).unwrap(),
        max_blocks: NonZeroU32::new(5).unwrap(),
    }
}

const ADAPTIVE_ENDS: [i64; 6] = [0, 1, 4, 8, 9, 11];

fn last_block() -> i64 {
    History::Lifecycle.last_block()
}

fn ends(offsets: &[i64]) -> Vec<i64> {
    offsets.iter().map(|offset| FIRST_BLOCK + offset).collect()
}

fn engine(pool: &PgPool, force_full_state: bool, adaptive: bool) -> Engine {
    let engine = Engine::new(pool.clone()).with_full_state_loader_forced(force_full_state);
    if adaptive {
        engine.with_adaptive_batching(config())
    } else {
        engine.with_blocks_per_batch(NonZeroU32::new(3).unwrap())
    }
}

fn request(resume: Option<Marker>, mode: RunMode) -> BatchRequest {
    BatchRequest {
        chain_id: CHAIN.to_owned(),
        from_block: FIRST_BLOCK,
        to_block: last_block(),
        resume_current: resume,
        mode,
    }
}

/// Runs batches until the request completes and returns each batch's last block.
async fn walk(engine: &Engine, mut resume: Option<Marker>, mode: RunMode) -> TestResult<Vec<i64>> {
    let mut ends = Vec::new();
    loop {
        let outcome = engine.run_batch(request(resume, mode)).await?;
        ends.push(outcome.current.number);
        if outcome.complete {
            return Ok(ends);
        }
        resume = Some(outcome.current);
    }
}

fn marker(number: i64) -> Marker {
    Marker {
        number,
        hash: block_hash(number),
    }
}

async fn seeded(prefix: &str) -> TestResult<TestDatabase> {
    let database = database(prefix).await?;
    seed_history(database.pool(), History::Lifecycle).await?;
    walk(&engine(database.pool(), true, false), None, RunMode::Normal).await?;
    Ok(database)
}

/// Replaces block 1, inside the first planning horizon, with a block that carries the same
/// transaction and logs under another hash, and records the orphaning as Live does.
async fn reorganize(pool: &PgPool) -> TestResult<String> {
    let orphaned = FIRST_BLOCK + 1;
    let replacement = format!("0x{:064x}", orphaned + 1_000_000);
    for statement in [
        "INSERT INTO chain_lineage (
             chain_id, block_hash, parent_hash, block_number, block_timestamp, canonicality_state
         )
         SELECT chain_id, $3, parent_hash, block_number, block_timestamp, 'observed'
         FROM chain_lineage WHERE chain_id = $1 AND block_number = $2",
        "INSERT INTO raw_transactions (
             chain_id, block_hash, block_number, transaction_hash,
             transaction_index, from_address, to_address
         )
         SELECT chain_id, $3, block_number, transaction_hash,
                transaction_index, from_address, to_address
         FROM raw_transactions WHERE chain_id = $1 AND block_number = $2",
        "INSERT INTO raw_logs (
             chain_id, block_hash, block_number, transaction_hash,
             transaction_index, log_index, emitting_address, topics, data
         )
         SELECT chain_id, $3, block_number, transaction_hash,
                transaction_index, log_index, emitting_address, topics, data
         FROM raw_logs WHERE chain_id = $1 AND block_number = $2",
        "UPDATE chain_lineage SET canonicality_state = 'orphaned'
         WHERE chain_id = $1 AND block_number = $2 AND block_hash <> $3",
        "UPDATE chain_lineage SET canonicality_state = 'canonical'
         WHERE chain_id = $1 AND block_hash = $3",
    ] {
        sqlx::query(statement)
            .bind(CHAIN)
            .bind(orphaned)
            .bind(&replacement)
            .execute(pool)
            .await?;
    }
    sqlx::query(
        "INSERT INTO chain_heads (
             chain_id, latest_block_hash, latest_block_number, lineage_orphaning_epoch
         ) VALUES ($1, $2, $3, 1)
         ON CONFLICT (chain_id) DO UPDATE
         SET lineage_orphaning_epoch = chain_heads.lineage_orphaning_epoch + 1",
    )
    .bind(CHAIN)
    .bind(block_hash(last_block()))
    .bind(last_block())
    .execute(pool)
    .await?;
    Ok(replacement)
}

/// The rows a fixed three-block redo stores over the seeded and walked history.
async fn fixed_redo(prefix: &str) -> TestResult<Snapshot> {
    let database = seeded(prefix).await?;
    let ends = walk(&engine(database.pool(), true, false), None, RunMode::Redo).await?;
    assert_eq!(ends, self::ends(&[2, 5, 8, 11]));
    let stored = snapshot(database.pool()).await?;
    database.cleanup().await?;
    Ok(stored)
}

#[derive(Clone, Default)]
struct Logs(Arc<Mutex<Vec<u8>>>);

impl Logs {
    fn text(&self) -> String {
        String::from_utf8(self.0.lock().expect("log lock").clone()).expect("UTF-8 logs")
    }
}

impl Write for Logs {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        self.0.lock().expect("log lock").extend_from_slice(buffer);
        Ok(buffer.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

/// Fresh and reused engines, under both loaders, store exactly what fixed batches store.
/// The reused engine has already run the Normal walk with the option set.
#[tokio::test]
async fn adaptive_redo_stores_what_fixed_redo_stores() -> TestResult {
    let expected = fixed_redo("interpret_adaptive_fixed").await?;
    for force_full_state in [true, false] {
        for reuse in [false, true] {
            let database = database("interpret_adaptive_redo").await?;
            let pool = database.pool();
            seed_history(pool, History::Lifecycle).await?;
            let normal = engine(pool, force_full_state, true);
            assert_eq!(
                walk(&normal, None, RunMode::Normal).await?,
                ends(&[11]),
                "Normal follow keeps the fixed 500-block batch"
            );
            let fresh;
            let redo = if reuse {
                &normal
            } else {
                fresh = engine(pool, force_full_state, true);
                &fresh
            };
            let logs = Logs::default();
            let subscriber = tracing_subscriber::fmt()
                .with_ansi(false)
                .with_writer({
                    let logs = logs.clone();
                    move || logs.clone()
                })
                .finish();
            let guard = tracing::subscriber::set_default(subscriber);
            let ends = walk(redo, None, RunMode::Redo).await?;
            drop(guard);
            let case = format!("full state forced: {force_full_state}, reused: {reuse}");
            assert_eq!(ends, self::ends(&ADAPTIVE_ENDS), "{case}");
            let planned = logs.text();
            assert_eq!(
                planned.matches("oversized_single_block=true").count(),
                2,
                "{case}: {planned}"
            );
            if !force_full_state {
                assert_eq!(
                    redo.chosen_loader(CHAIN)?,
                    Some(StateLoader::Lookahead),
                    "{case}"
                );
            }
            assert_eq!(snapshot(pool).await?, expected, "{case}");
            database.cleanup().await?;
        }
    }
    Ok(())
}

/// Normal follow and recompute-flags ignore the option even on an engine that has it.
#[tokio::test]
async fn only_redo_plans_adaptive_batches() -> TestResult {
    let database = database("interpret_adaptive_normal_guard").await?;
    let pool = database.pool();
    seed_history(pool, History::Lifecycle).await?;
    let engine = Engine::new(pool.clone()).with_adaptive_batching(AdaptiveBatchConfig {
        max_blocks: NonZeroU32::new(1).unwrap(),
        ..AdaptiveBatchConfig::default()
    });
    let normal = engine.run_batch(request(None, RunMode::Normal)).await?;
    assert_eq!(
        (normal.current.number, normal.complete),
        (last_block(), true),
        "Normal follow interprets the whole 12-block history as one fixed batch"
    );
    let recompute = engine
        .run_batch(request(None, RunMode::RecomputeFlags))
        .await?;
    assert_eq!(
        (recompute.current.number, recompute.complete),
        (last_block(), true)
    );
    let redo = engine.run_batch(request(None, RunMode::Redo)).await?;
    assert_eq!(
        (redo.current.number, redo.complete),
        (FIRST_BLOCK, false),
        "a redo on the same engine plans one block per batch"
    );
    database.cleanup().await?;
    Ok(())
}

/// A new process resumes from the last committed marker and replans from there, so its
/// boundaries can differ from an uninterrupted run while the stored rows cannot.
#[tokio::test]
async fn a_resumed_adaptive_redo_replans_from_the_committed_marker() -> TestResult {
    let expected = fixed_redo("interpret_adaptive_resume_fixed").await?;
    let database = seeded("interpret_adaptive_resume").await?;
    let pool = database.pool();
    let first = engine(pool, true, true)
        .run_batch(request(None, RunMode::Redo))
        .await?;
    assert_eq!(first.current, marker(FIRST_BLOCK));
    let resumed = walk(
        &engine(pool, true, true),
        Some(first.current),
        RunMode::Redo,
    )
    .await?;
    assert_eq!(resumed, ends(&[1, 5, 8, 10, 11]));
    assert_eq!(snapshot(pool).await?, expected);
    database.cleanup().await?;
    Ok(())
}

/// A reorganization inside the cached planning horizon fails the batch that reaches it
/// before anything is written, and the retry from the same marker plans from fresh lineage.
#[tokio::test]
async fn a_reorg_inside_the_planning_horizon_fails_before_writing_and_replans() -> TestResult {
    let expected = {
        // Fixed one-block batches meet the same reorganization at the same point: after
        // the redo's first batch, before block 1.
        let database = seeded("interpret_adaptive_reorg_fixed").await?;
        let fixed = Engine::new(database.pool().clone())
            .with_blocks_per_batch(NonZeroU32::MIN)
            .with_full_state_loader_forced(true);
        let first = fixed.run_batch(request(None, RunMode::Redo)).await?;
        assert_eq!(first.current, marker(FIRST_BLOCK));
        reorganize(database.pool()).await?;
        let rest = walk(&fixed, Some(first.current), RunMode::Redo).await?;
        assert_eq!(rest, ends(&[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11]));
        let stored = snapshot(database.pool()).await?;
        database.cleanup().await?;
        stored
    };
    let database = seeded("interpret_adaptive_reorg").await?;
    let pool = database.pool();
    let engine = engine(pool, true, true);
    let first = engine.run_batch(request(None, RunMode::Redo)).await?;
    assert_eq!(first.current, marker(FIRST_BLOCK));
    let replacement = reorganize(pool).await?;
    let before = snapshot(pool).await?;
    let error = engine
        .run_batch(request(Some(first.current.clone()), RunMode::Redo))
        .await
        .expect_err("the cached marker of block 1 is no longer canonical");
    assert_eq!(error.kind(), ErrorKind::Transient, "{error}");
    assert!(error.to_string().contains("lineage changed"), "{error}");
    assert_eq!(
        snapshot(pool).await?,
        before,
        "the failed batch wrote nothing"
    );
    let retry = engine
        .run_batch(request(Some(first.current), RunMode::Redo))
        .await?;
    assert_eq!(
        retry.current,
        Marker {
            number: FIRST_BLOCK + 1,
            hash: replacement,
        },
        "the retry plans block 1 again from fresh lineage"
    );
    let rest = walk(&engine, Some(retry.current), RunMode::Redo).await?;
    assert_eq!(rest, ends(&[5, 8, 10, 11]));
    assert_eq!(snapshot(pool).await?, expected);
    database.cleanup().await?;
    Ok(())
}

type Snapshot = BTreeMap<String, Vec<String>>;

/// Every Interpret table except generated identities and wall-clock stamps.
async fn snapshot(pool: &PgPool) -> TestResult<Snapshot> {
    let mut result = BTreeMap::new();
    let ignored = "ARRAY['normalized_event_id','contract_instance_address_id','discovery_edge_id','search_id','observed_at','admitted_at','deactivated_at','created_at','updated_at','inserted_at','first_seen_at','last_seen_at','detected_at','acknowledged_at']::text[]";
    for table in [
        "normalized_events",
        "interpret_decode_skips",
        "contract_instances",
        "contract_instance_addresses",
        "discovery_edges",
        "name_surfaces",
        "token_lineages",
        "resources",
        "surface_bindings",
        "migration_event_associations",
        "migration_discovery_associations",
        "migration_candidate_identity_effects",
        "migration_candidate_discovery_effects",
        "name_search_documents",
        "label_preimages",
        "ens_names",
        "discovery_watch_admissions",
    ] {
        let exists: bool = sqlx::query_scalar("SELECT to_regclass($1) IS NOT NULL")
            .bind(table)
            .fetch_one(pool)
            .await?;
        assert!(exists, "{table} is missing from the test schema");
        let mut canonical = format!(
            "(to_jsonb(row) - {ignored}) || CASE WHEN to_jsonb(row) ? 'deactivated_at' THEN jsonb_build_object('_deactivated', to_jsonb(row)->'deactivated_at' <> 'null'::jsonb) ELSE '{{}}'::jsonb END"
        );
        if table == "name_surfaces" {
            canonical.push_str(" || jsonb_build_object('deactivated_at', row.deactivated_at)");
        }
        let filtered = !matches!(table, "label_preimages" | "ens_names");
        let predicate = if filtered { " WHERE chain_id = $1" } else { "" };
        let statement = format!(
            "SELECT ({canonical})::text FROM {table} row{predicate} ORDER BY ({canonical})::text"
        );
        let query = sqlx::query_scalar(&statement);
        let rows = if filtered {
            query.bind(CHAIN).fetch_all(pool).await?
        } else {
            query.fetch_all(pool).await?
        };
        result.insert(table.into(), rows);
    }
    let postings = sqlx::query_scalar(
        "SELECT ((to_jsonb(posting) - 'search_id') || jsonb_build_object('logical_name_id', document.logical_name_id))::text
         FROM name_search_postings posting JOIN name_search_documents document USING(search_id)
         WHERE document.chain_id = $1 ORDER BY 1",
    )
    .bind(CHAIN)
    .fetch_all(pool)
    .await?;
    result.insert("name_search_postings".into(), postings);
    let rows = result.values().map(Vec::len).sum::<usize>();
    assert!(rows > 0, "the snapshot holds stored rows");
    Ok(result)
}
