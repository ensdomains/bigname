#[allow(dead_code)]
mod support;

use anyhow::Result;
use phase_runner::{
    inspect::{InspectionKind, InspectionRequest, inspect},
    phase::BlockRange,
};
use serde_json::{Value, json};

use support::ScratchDatabase;

const CHAIN: &str = "inspection-fixture";
const CANONICAL_HASH: &str = "inspection-canonical-5";
const ORPHANED_HASH: &str = "inspection-orphaned-5";

#[tokio::test]
async fn block_canonicality_window_labels_the_orphaned_fork() -> Result<()> {
    let scratch = ScratchDatabase::create("phase_runner_inspect_canonicality").await?;
    seed_inspection_fixture(scratch.pool()).await?;

    let output = inspect(scratch.pool(), &request(InspectionKind::BlockCanonicality)?).await?;
    let blocks = output["blocks"].as_array().expect("block inspection rows");
    assert_eq!(blocks.len(), 2);
    let canonical = row_by_hash(blocks, CANONICAL_HASH);
    let orphaned = row_by_hash(blocks, ORPHANED_HASH);
    assert_eq!(canonical["canonicality_state"], "canonical");
    assert_eq!(orphaned["canonicality_state"], "orphaned");
    assert_eq!(canonical["raw_fact_counts"]["transactions"], 1);
    assert_eq!(canonical["raw_fact_counts"]["receipts"], 1);
    assert_eq!(canonical["raw_fact_counts"]["logs"], 1);
    assert_eq!(orphaned["normalized_event_count"], 1);
    assert_eq!(canonical["header_audit_present"], true);
    assert_eq!(orphaned["header_audit_present"], true);
    scratch.cleanup().await
}

#[tokio::test]
async fn stored_lineage_window_returns_bounded_header_audit_rows() -> Result<()> {
    let scratch = ScratchDatabase::create("phase_runner_inspect_lineage").await?;
    seed_inspection_fixture(scratch.pool()).await?;

    let output = inspect(scratch.pool(), &request(InspectionKind::StoredLineage)?).await?;
    let blocks = output["blocks"]
        .as_array()
        .expect("lineage inspection rows");
    assert_eq!(blocks.len(), 2);
    let canonical = row_by_hash(blocks, CANONICAL_HASH);
    let orphaned = row_by_hash(blocks, ORPHANED_HASH);
    assert_eq!(canonical["block_number"], 5);
    assert_eq!(canonical["parent_hash"], "inspection-parent-4");
    assert_eq!(
        canonical["header_audit"]["transactions_root"],
        "0xtx-canonical"
    );
    assert_eq!(
        orphaned["header_audit"]["transactions_root"],
        "0xtx-orphaned"
    );
    assert_eq!(orphaned["canonicality_state"], "orphaned");
    scratch.cleanup().await
}

#[tokio::test]
async fn raw_event_window_joins_raw_facts_and_normalized_event_context_read_only() -> Result<()> {
    let scratch = ScratchDatabase::create("phase_runner_inspect_raw_events").await?;
    seed_inspection_fixture(scratch.pool()).await?;
    let counts_before: (i64, i64, i64, i64) = sqlx::query_as(
        "SELECT
             (SELECT count(*) FROM chain_lineage WHERE chain_id = $1),
             (SELECT count(*) FROM raw_transactions WHERE chain_id = $1),
             (SELECT count(*) FROM raw_logs WHERE chain_id = $1),
             (SELECT count(*) FROM normalized_events WHERE chain_id = $1)",
    )
    .bind(CHAIN)
    .fetch_one(scratch.pool())
    .await?;

    let output = inspect(scratch.pool(), &request(InspectionKind::RawEvents)?).await?;
    let events = output["events"].as_array().expect("raw-event rows");
    assert_eq!(events.len(), 2);
    let canonical = row_by_hash(events, CANONICAL_HASH);
    let orphaned = row_by_hash(events, ORPHANED_HASH);
    assert_eq!(
        canonical["transaction"]["transaction_hash"],
        "inspection-canonical-tx"
    );
    assert_eq!(canonical["receipt"]["status"], true);
    assert_eq!(canonical["log"]["data"], "0xcafe");
    assert_eq!(
        canonical["normalized_events"][0]["event_kind"],
        "RecordChanged"
    );
    assert_eq!(
        canonical["normalized_events"][0]["canonicality_state"],
        "canonical"
    );
    assert_eq!(orphaned["canonicality_state"], "orphaned");
    assert_eq!(
        orphaned["normalized_events"][0]["canonicality_state"],
        "orphaned"
    );

    let counts_after: (i64, i64, i64, i64) = sqlx::query_as(
        "SELECT
             (SELECT count(*) FROM chain_lineage WHERE chain_id = $1),
             (SELECT count(*) FROM raw_transactions WHERE chain_id = $1),
             (SELECT count(*) FROM raw_logs WHERE chain_id = $1),
             (SELECT count(*) FROM normalized_events WHERE chain_id = $1)",
    )
    .bind(CHAIN)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(counts_after, counts_before);
    scratch.cleanup().await
}

fn request(kind: InspectionKind) -> Result<InspectionRequest> {
    Ok(InspectionRequest {
        kind,
        chain_id: CHAIN.to_owned(),
        range: BlockRange::new(5, 5)?,
    })
}

fn row_by_hash<'a>(rows: &'a [Value], hash: &str) -> &'a Value {
    rows.iter()
        .find(|row| row["block_hash"] == hash)
        .expect("inspection row for block hash")
}

async fn seed_inspection_fixture(pool: &sqlx::PgPool) -> Result<()> {
    sqlx::query(
        "INSERT INTO chain_lineage (
             chain_id, block_hash, parent_hash, block_number,
             block_timestamp, canonicality_state
         ) VALUES
             ($1, 'inspection-parent-4', NULL, 4, to_timestamp(4), 'canonical'),
             ($1, $2, 'inspection-parent-4', 5, to_timestamp(5), 'canonical'),
             ($1, $3, 'inspection-parent-4', 5, to_timestamp(6), 'orphaned')",
    )
    .bind(CHAIN)
    .bind(CANONICAL_HASH)
    .bind(ORPHANED_HASH)
    .execute(pool)
    .await?;
    sqlx::query(
        "INSERT INTO chain_header_audit (
             chain_id, block_hash, logs_bloom, transactions_root,
             receipts_root, state_root
         ) VALUES
             ($1, $2, decode('aa', 'hex'), '0xtx-canonical',
              '0xreceipt-canonical', '0xstate-canonical'),
             ($1, $3, decode('bb', 'hex'), '0xtx-orphaned',
              '0xreceipt-orphaned', '0xstate-orphaned')",
    )
    .bind(CHAIN)
    .bind(CANONICAL_HASH)
    .bind(ORPHANED_HASH)
    .execute(pool)
    .await?;
    seed_fork_facts(pool, CANONICAL_HASH, "canonical").await?;
    seed_fork_facts(pool, ORPHANED_HASH, "orphaned").await?;
    Ok(())
}

async fn seed_fork_facts(pool: &sqlx::PgPool, block_hash: &str, state: &str) -> Result<()> {
    let suffix = if state == "canonical" {
        "canonical"
    } else {
        "orphaned"
    };
    let transaction_hash = format!("inspection-{suffix}-tx");
    sqlx::query(
        "INSERT INTO raw_transactions (
             chain_id, block_hash, block_number, transaction_hash,
             transaction_index, from_address, to_address, input, value
         ) VALUES ($1, $2, 5, $3, 0, '0xfrom', '0xto', decode('abcd', 'hex'), 7)",
    )
    .bind(CHAIN)
    .bind(block_hash)
    .bind(&transaction_hash)
    .execute(pool)
    .await?;
    sqlx::query(
        "INSERT INTO raw_receipts (
             chain_id, block_hash, block_number, transaction_hash,
             transaction_index, status, gas_used, cumulative_gas_used, logs_bloom
         ) VALUES ($1, $2, 5, $3, 0, true, 21, 34, decode('beef', 'hex'))",
    )
    .bind(CHAIN)
    .bind(block_hash)
    .bind(&transaction_hash)
    .execute(pool)
    .await?;
    sqlx::query(
        "INSERT INTO raw_logs (
             chain_id, block_hash, block_number, transaction_hash,
             transaction_index, log_index, emitting_address, topics, data
         ) VALUES ($1, $2, 5, $3, 0, 0, '0xemitter', ARRAY['0xtopic'], decode('cafe', 'hex'))",
    )
    .bind(CHAIN)
    .bind(block_hash)
    .bind(&transaction_hash)
    .execute(pool)
    .await?;
    sqlx::query(
        "INSERT INTO normalized_events (
             event_identity, namespace, event_kind, source_family,
             manifest_version, chain_id, block_number, block_hash,
             transaction_hash, transaction_index, log_index, raw_fact_ref,
             derivation_kind, canonicality_state, before_state, after_state
         ) VALUES ($1, 'ens', 'RecordChanged', 'inspection_fixture', 1,
                   $2, 5, $3, $4, 0, 0, $5, 'ens_v1_unwrapped_authority',
                   $6::canonicality_state, '{}'::jsonb, $7)",
    )
    .bind(format!("inspection:{suffix}:record"))
    .bind(CHAIN)
    .bind(block_hash)
    .bind(&transaction_hash)
    .bind(json!({"transaction_hash": transaction_hash, "log_index": 0}))
    .bind(state)
    .bind(json!({"fixture": suffix}))
    .execute(pool)
    .await?;
    Ok(())
}
