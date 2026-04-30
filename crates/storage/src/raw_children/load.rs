use anyhow::{Context, Result};
use sqlx::{Executor, Postgres, Row};

use super::{
    decode::{decode_raw_log, decode_raw_receipt, decode_raw_transaction},
    types::{RawLog, RawReceipt, RawTransaction},
};

pub(super) async fn load_raw_transaction_internal<'e, E>(
    executor: E,
    chain_id: &str,
    block_hash: &str,
    transaction_index: i64,
) -> Result<Option<RawTransaction>>
where
    E: Executor<'e, Database = Postgres>,
{
    let row = sqlx::query(
        r#"
        SELECT
            chain_id,
            block_hash,
            block_number,
            transaction_hash,
            transaction_index,
            from_address,
            to_address,
            canonicality_state::TEXT AS canonicality_state
        FROM raw_transactions
        WHERE chain_id = $1
          AND block_hash = $2
          AND transaction_index = $3
        "#,
    )
    .bind(chain_id)
    .bind(block_hash)
    .bind(transaction_index)
    .fetch_optional(executor)
    .await
    .with_context(|| {
        format!(
            "failed to load raw transaction for chain {chain_id} block {block_hash} index {transaction_index}"
        )
    })?;

    row.map(decode_raw_transaction).transpose()
}

pub(super) async fn load_raw_receipt_internal<'e, E>(
    executor: E,
    chain_id: &str,
    block_hash: &str,
    transaction_index: i64,
) -> Result<Option<RawReceipt>>
where
    E: Executor<'e, Database = Postgres>,
{
    let row = sqlx::query(
        r#"
        SELECT
            chain_id,
            block_hash,
            block_number,
            transaction_hash,
            transaction_index,
            contract_address,
            status,
            gas_used,
            cumulative_gas_used,
            logs_bloom,
            canonicality_state::TEXT AS canonicality_state
        FROM raw_receipts
        WHERE chain_id = $1
          AND block_hash = $2
          AND transaction_index = $3
        "#,
    )
    .bind(chain_id)
    .bind(block_hash)
    .bind(transaction_index)
    .fetch_optional(executor)
    .await
    .with_context(|| {
        format!(
            "failed to load raw receipt for chain {chain_id} block {block_hash} index {transaction_index}"
        )
    })?;

    row.map(decode_raw_receipt).transpose()
}

pub(super) async fn load_raw_log_internal<'e, E>(
    executor: E,
    chain_id: &str,
    block_hash: &str,
    log_index: i64,
) -> Result<Option<RawLog>>
where
    E: Executor<'e, Database = Postgres>,
{
    let row = sqlx::query(
        r#"
        SELECT
            chain_id,
            block_hash,
            block_number,
            transaction_hash,
            transaction_index,
            log_index,
            emitting_address,
            topics,
            data,
            canonicality_state::TEXT AS canonicality_state
        FROM raw_logs
        WHERE chain_id = $1
          AND block_hash = $2
          AND log_index = $3
        "#,
    )
    .bind(chain_id)
    .bind(block_hash)
    .bind(log_index)
    .fetch_optional(executor)
    .await
    .with_context(|| {
        format!("failed to load raw log for chain {chain_id} block {block_hash} log {log_index}")
    })?;

    row.map(decode_raw_log).transpose()
}

pub(super) async fn load_raw_block_hash_path<'e, E>(
    executor: E,
    chain_id: &str,
    from_hash: &str,
    stop_before_hash: Option<&str>,
) -> Result<Vec<String>>
where
    E: Executor<'e, Database = Postgres>,
{
    let rows = sqlx::query(
        r#"
        WITH RECURSIVE raw_block_path AS (
            SELECT chain_id, block_hash, parent_hash, 0 AS depth
            FROM chain_lineage
            WHERE chain_id = $1
              AND block_hash = $2

            UNION ALL

            SELECT parent.chain_id, parent.block_hash, parent.parent_hash, raw_block_path.depth + 1
            FROM chain_lineage AS parent
            JOIN raw_block_path
              ON parent.chain_id = raw_block_path.chain_id
             AND parent.block_hash = raw_block_path.parent_hash
            WHERE $3::TEXT IS NULL
               OR parent.block_hash <> $3::TEXT
        )
        SELECT block_hash
        FROM raw_block_path
        ORDER BY depth
        "#,
    )
    .bind(chain_id)
    .bind(from_hash)
    .bind(stop_before_hash)
    .fetch_all(executor)
    .await?;

    rows.into_iter()
        .map(|row| {
            row.try_get::<String, _>("block_hash")
                .context("missing block_hash in raw block path")
        })
        .collect()
}
