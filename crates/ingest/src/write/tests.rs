use anyhow::Result;
use bigname_test_support::{TestDatabase, TestDatabaseConfig};

use super::*;
use crate::{
    ErrorKind,
    fetching::FetchedBatch,
    manifest::WatchQuery,
    provider::{Block, Log, Receipt, Transaction},
};

const CHAIN_ID: &str = "coinbase-recount-test";
const BLOCK_HASH: &str = "0x0000000000000000000000000000000000000000000000000000000000000001";
const TRANSACTION_HASH: &str = "0x0000000000000000000000000000000000000000000000000000000000000002";
const ADDRESS: &str = "0x0000000000000000000000000000000000000003";
const TOPIC: &str = "0x0000000000000000000000000000000000000000000000000000000000000004";
const SUPERSEDED_BLOCK_HASH: &str =
    "0x0000000000000000000000000000000000000000000000000000000000000011";
const SUPERSEDED_TRANSACTION_HASH: &str =
    "0x0000000000000000000000000000000000000000000000000000000000000012";
const REPLACEMENT_BLOCK_HASH: &str =
    "0x0000000000000000000000000000000000000000000000000000000000000021";

#[tokio::test]
async fn coinbase_recount_rejects_provider_count_below_materialized_count() -> Result<()> {
    let database = database("ingest_coinbase_recount").await?;
    let facts = facts(vec![log(0, vec![1]), log(1, vec![2])]);
    let provider_logs = vec![log(0, vec![1])];
    let queries = vec![query(1)];

    let error = store(
        database.pool(),
        CHAIN_ID,
        &facts,
        Some((1, 1, &provider_logs, &queries)),
    )
    .await
    .expect_err("a stored-count mismatch must stop Coinbase ingest");

    assert_eq!(error.kind(), ErrorKind::DataIntegrity);
    let lineage_count: i64 = sqlx::query_scalar("SELECT count(*) FROM chain_lineage")
        .fetch_one(database.pool())
        .await?;
    assert_eq!(lineage_count, 0, "the failed bulk load must roll back");
    database.cleanup().await
}

#[tokio::test]
async fn coinbase_recount_ignores_rpc_rows_at_the_dual_coverage_seam() -> Result<()> {
    let database = database("ingest_coinbase_recount_seam").await?;
    let seam = crate::BASE_COINBASE_SEAM_BLOCK;
    let rpc_facts = facts_at(
        BLOCK_HASH,
        TRANSACTION_HASH,
        seam,
        vec![log_at(BLOCK_HASH, TRANSACTION_HASH, seam, 0, vec![1])],
    );
    store(database.pool(), CHAIN_ID, &rpc_facts, None).await?;

    store(
        database.pool(),
        CHAIN_ID,
        &block_only_facts(BLOCK_HASH, seam),
        Some((seam, seam, &[], &[query(seam)])),
    )
    .await?;

    let stored_count: i64 = sqlx::query_scalar("SELECT count(*) FROM raw_logs WHERE chain_id = $1")
        .bind(CHAIN_ID)
        .fetch_one(database.pool())
        .await?;
    assert_eq!(stored_count, 1);
    database.cleanup().await
}

#[tokio::test]
async fn coinbase_recount_ignores_rows_from_a_superseded_hash() -> Result<()> {
    let database = database("ingest_coinbase_recount_reorg").await?;
    let block_number = 7;
    let old_facts = facts_at(
        SUPERSEDED_BLOCK_HASH,
        SUPERSEDED_TRANSACTION_HASH,
        block_number,
        vec![log_at(
            SUPERSEDED_BLOCK_HASH,
            SUPERSEDED_TRANSACTION_HASH,
            block_number,
            0,
            vec![1],
        )],
    );
    store(database.pool(), CHAIN_ID, &old_facts, None).await?;

    store(
        database.pool(),
        CHAIN_ID,
        &block_only_facts(REPLACEMENT_BLOCK_HASH, block_number),
        Some((block_number, block_number, &[], &[query(block_number)])),
    )
    .await?;

    let hashes: Vec<String> = sqlx::query_scalar(
        "
        SELECT block_hash
        FROM raw_logs
        WHERE chain_id = $1
        ORDER BY block_hash
        ",
    )
    .bind(CHAIN_ID)
    .fetch_all(database.pool())
    .await?;
    assert_eq!(hashes, vec![SUPERSEDED_BLOCK_HASH]);
    database.cleanup().await
}

#[tokio::test]
async fn immutable_raw_fact_conflict_is_terminal() -> Result<()> {
    let database = database("ingest_immutable_raw_fact").await?;
    let original = facts(vec![log(0, vec![1])]);
    store(database.pool(), CHAIN_ID, &original, None).await?;
    let changed = facts(vec![log(0, vec![2])]);

    let error = store(database.pool(), CHAIN_ID, &changed, None)
        .await
        .expect_err("an existing raw fact cannot be overwritten");

    assert_eq!(error.kind(), ErrorKind::DataIntegrity);
    let stored: Vec<u8> = sqlx::query_scalar(
        "
        SELECT data
        FROM raw_logs
        WHERE chain_id = $1
          AND block_hash = $2
          AND log_index = 0
        ",
    )
    .bind(CHAIN_ID)
    .bind(BLOCK_HASH)
    .fetch_one(database.pool())
    .await?;
    assert_eq!(stored, vec![1]);
    database.cleanup().await
}

async fn database(name: &str) -> Result<TestDatabase> {
    let database = TestDatabase::create(TestDatabaseConfig::new(name)).await?;
    sqlx::raw_sql(include_str!("../../../../schema-v2/baseline/01_chain.sql"))
        .execute(database.pool())
        .await?;
    sqlx::raw_sql(include_str!(
        "../../../../schema-v2/baseline/02_raw_facts.sql"
    ))
    .execute(database.pool())
    .await?;
    Ok(database)
}

fn facts(logs: Vec<Log>) -> FetchedBatch {
    facts_at(BLOCK_HASH, TRANSACTION_HASH, 1, logs)
}

fn facts_at(
    block_hash: &str,
    transaction_hash: &str,
    block_number: i64,
    logs: Vec<Log>,
) -> FetchedBatch {
    FetchedBatch {
        blocks: vec![Block {
            hash: block_hash.to_owned(),
            parent_hash: None,
            number: block_number,
            timestamp_unix_secs: block_number,
            logs_bloom: None,
            transactions_root: None,
            receipts_root: None,
            state_root: None,
        }],
        transactions: vec![Transaction {
            hash: transaction_hash.to_owned(),
            block_hash: block_hash.to_owned(),
            block_number,
            index: 0,
            from: ADDRESS.to_owned(),
            to: Some(ADDRESS.to_owned()),
            input: vec![0xde, 0xad],
            value: "7".to_owned(),
        }],
        receipts: vec![Receipt {
            transaction_hash: transaction_hash.to_owned(),
            block_hash: block_hash.to_owned(),
            block_number,
            transaction_index: 0,
            contract_address: None,
            status: Some(true),
            cumulative_gas_used: Some("21000".to_owned()),
            gas_used: Some("21000".to_owned()),
            logs_bloom: None,
        }],
        logs,
    }
}

fn block_only_facts(block_hash: &str, block_number: i64) -> FetchedBatch {
    FetchedBatch {
        blocks: vec![Block {
            hash: block_hash.to_owned(),
            parent_hash: None,
            number: block_number,
            timestamp_unix_secs: block_number,
            logs_bloom: None,
            transactions_root: None,
            receipts_root: None,
            state_root: None,
        }],
        ..FetchedBatch::default()
    }
}

fn log(log_index: i64, data: Vec<u8>) -> Log {
    log_at(BLOCK_HASH, TRANSACTION_HASH, 1, log_index, data)
}

fn log_at(
    block_hash: &str,
    transaction_hash: &str,
    block_number: i64,
    log_index: i64,
    data: Vec<u8>,
) -> Log {
    Log {
        block_hash: block_hash.to_owned(),
        block_number,
        transaction_hash: transaction_hash.to_owned(),
        transaction_index: 0,
        log_index,
        address: ADDRESS.to_owned(),
        topics: vec![TOPIC.to_owned()],
        data,
    }
}

fn query(block_number: i64) -> WatchQuery {
    WatchQuery {
        from_block: block_number,
        to_block: block_number,
        addresses: vec![ADDRESS.to_owned()],
        topic0s: vec![TOPIC.to_owned()],
    }
}
