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

#[tokio::test]
async fn coinbase_recount_mismatch_is_terminal_and_rolls_back() -> Result<()> {
    let database = database("ingest_coinbase_recount").await?;
    let facts = facts(vec![log(0, vec![1])]);
    let provider_logs = vec![log(0, Vec::new()), log(1, Vec::new())];
    let queries = vec![WatchQuery {
        from_block: 1,
        to_block: 1,
        addresses: vec![ADDRESS.to_owned()],
        topic0s: vec![TOPIC.to_owned()],
    }];

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
    FetchedBatch {
        blocks: vec![Block {
            hash: BLOCK_HASH.to_owned(),
            parent_hash: None,
            number: 1,
            timestamp_unix_secs: 1,
            logs_bloom: None,
            transactions_root: None,
            receipts_root: None,
            state_root: None,
        }],
        transactions: vec![Transaction {
            hash: TRANSACTION_HASH.to_owned(),
            block_hash: BLOCK_HASH.to_owned(),
            block_number: 1,
            index: 0,
            from: ADDRESS.to_owned(),
            to: Some(ADDRESS.to_owned()),
            input: vec![0xde, 0xad],
            value: "7".to_owned(),
        }],
        receipts: vec![Receipt {
            transaction_hash: TRANSACTION_HASH.to_owned(),
            block_hash: BLOCK_HASH.to_owned(),
            block_number: 1,
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

fn log(log_index: i64, data: Vec<u8>) -> Log {
    Log {
        block_hash: BLOCK_HASH.to_owned(),
        block_number: 1,
        transaction_hash: TRANSACTION_HASH.to_owned(),
        transaction_index: 0,
        log_index,
        address: ADDRESS.to_owned(),
        topics: vec![TOPIC.to_owned()],
        data,
    }
}
