#[allow(dead_code)]
mod support;

use std::time::Instant;

use anyhow::Result;
use bigname_interpret::{BatchRequest, Engine, RunMode};
use serde_json::json;
use support::ScratchDatabase;

const RETAINED_EVENTS: i64 = 1_000_000;
const BATCHES: i64 = 10;
const BLOCKS_PER_BATCH: i64 = 500;
const TARGET_BLOCK: i64 = BATCHES * BLOCKS_PER_BATCH;
const NORMALIZER: &str = "ensip15@ens-normalize-0.1.1";

#[tokio::test]
#[ignore = "diagnostic deep-state benchmark; run explicitly"]
async fn profile_one_million_retained_events_across_ten_batches() -> Result<()> {
    let scratch = ScratchDatabase::create("interpret_fold_performance").await?;
    let chain = "interpret-fold-performance";
    let seed_started = Instant::now();
    seed_fixture(scratch.pool(), chain).await?;
    let retained: i64 =
        sqlx::query_scalar("SELECT count(*) FROM normalized_events WHERE chain_id = $1")
            .bind(chain)
            .fetch_one(scratch.pool())
            .await?;
    eprintln!(
        "interpret-fold-profile phase=seed elapsed_ms={} retained_events={retained}",
        seed_started.elapsed().as_millis()
    );
    assert_eq!(retained, RETAINED_EVENTS);

    let engine = Engine::new(scratch.pool().clone());
    let mut resume_current = None;
    for batch in 1..=BATCHES {
        let batch_started = Instant::now();
        let outcome = engine
            .run_batch(BatchRequest {
                chain_id: chain.to_owned(),
                from_block: 1,
                to_block: TARGET_BLOCK,
                resume_current,
                mode: RunMode::Normal,
            })
            .await?;
        let expected_current = batch * BLOCKS_PER_BATCH;
        assert_eq!(outcome.current.number, expected_current);
        assert_eq!(outcome.complete, batch == BATCHES);
        eprintln!(
            "interpret-fold-benchmark batch={batch} elapsed_ms={} rss_kib={}",
            batch_started.elapsed().as_millis(),
            rss_kib(),
        );
        resume_current = Some(outcome.current);
    }
    scratch.cleanup().await
}

fn rss_kib() -> u64 {
    std::fs::read_to_string("/proc/self/status")
        .ok()
        .and_then(|status| {
            status.lines().find_map(|line| {
                line.strip_prefix("VmRSS:")?
                    .split_whitespace()
                    .next()?
                    .parse::<u64>()
                    .ok()
            })
        })
        .unwrap_or_default()
}

async fn seed_fixture(pool: &sqlx::PgPool, chain_id: &str) -> Result<()> {
    sqlx::query(
        "
        INSERT INTO chain_lineage (
            chain_id, block_hash, parent_hash, block_number, block_timestamp,
            canonicality_state
        )
        SELECT $1,
               $1 || '-block-' || number,
               CASE WHEN number = 0 THEN NULL ELSE $1 || '-block-' || (number - 1) END,
               number,
               to_timestamp(number),
               'canonical'::canonicality_state
        FROM generate_series(0, $2::bigint) AS number
        ",
    )
    .bind(chain_id)
    .bind(TARGET_BLOCK)
    .execute(pool)
    .await?;
    sqlx::query(
        "
        INSERT INTO manifest_versions (
            manifest_version, namespace, source_family, chain_id, deployment_label,
            rollout_status, normalizer_version, file_path, manifest_payload
        )
        VALUES (1, 'ens', 'performance_fixture', $1, 'fixture', 'active', $2, $3, $4)
        ",
    )
    .bind(chain_id)
    .bind(NORMALIZER)
    .bind(format!("tests/{chain_id}.toml"))
    .bind(json!({"abi": {"events": []}}))
    .execute(pool)
    .await?;
    sqlx::query(
        "
        INSERT INTO normalized_events (
            event_identity, namespace, event_kind, source_family, manifest_version,
            chain_id, block_number, block_hash, raw_fact_ref, derivation_kind,
            canonicality_state, before_state, after_state
        )
        SELECT 'fixture-event-' || number,
               'ens',
               'RecordChanged',
               'performance_fixture',
               1,
               $1,
               0,
               $1 || '-block-0',
               jsonb_build_object(
                   'interpreter_state_key', 'fixture-state-' || number,
                   'state_scope', 'fixture-scope-' || number
               ),
               'raw_log_preimage_observation',
               'canonical'::canonicality_state,
               '{}'::jsonb,
               jsonb_build_object('value', number)
        FROM generate_series(1, $2::bigint) AS number
        ",
    )
    .bind(chain_id)
    .bind(RETAINED_EVENTS)
    .execute(pool)
    .await?;
    Ok(())
}
