use anyhow::{Context, Result};
use sqlx::{PgPool, Postgres};

use super::{
    decode::decode_raw_receipt,
    load::load_raw_receipt_internal,
    types::RawReceipt,
    validation::{ensure_raw_receipt_identity_matches, merge_canonicality, validate_raw_receipt},
};

/// Insert missing raw receipt rows or refresh canonicality for already observed
/// block-scoped receipts.
pub async fn upsert_raw_receipts(
    pool: &PgPool,
    receipts: &[RawReceipt],
) -> Result<Vec<RawReceipt>> {
    if receipts.is_empty() {
        return Ok(Vec::new());
    }

    let mut transaction = pool
        .begin()
        .await
        .context("failed to open transaction for raw receipt upsert")?;

    let mut snapshots = Vec::with_capacity(receipts.len());
    for raw_receipt in receipts {
        validate_raw_receipt(raw_receipt)?;
        snapshots.push(upsert_raw_receipt(&mut transaction, raw_receipt).await?);
    }

    transaction
        .commit()
        .await
        .context("failed to commit raw receipt upsert")?;

    Ok(snapshots)
}

async fn upsert_raw_receipt(
    executor: &mut sqlx::Transaction<'_, Postgres>,
    receipt: &RawReceipt,
) -> Result<RawReceipt> {
    if let Some(snapshot) = sqlx::query(
        r#"
        INSERT INTO raw_receipts (
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
            canonicality_state
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11::canonicality_state)
        ON CONFLICT (chain_id, block_hash, transaction_index) DO NOTHING
        RETURNING
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
        "#,
    )
    .bind(&receipt.chain_id)
    .bind(&receipt.block_hash)
    .bind(receipt.block_number)
    .bind(&receipt.transaction_hash)
    .bind(receipt.transaction_index)
    .bind(&receipt.contract_address)
    .bind(receipt.status)
    .bind(receipt.gas_used)
    .bind(receipt.cumulative_gas_used)
    .bind(&receipt.logs_bloom)
    .bind(receipt.canonicality_state.as_str())
    .fetch_optional(&mut **executor)
    .await
    .with_context(|| {
        format!(
            "failed to insert raw receipt for chain {} block {} transaction {}",
            receipt.chain_id, receipt.block_hash, receipt.transaction_hash
        )
    })? {
        return decode_raw_receipt(snapshot);
    }

    let existing = load_raw_receipt_internal(
        &mut **executor,
        &receipt.chain_id,
        &receipt.block_hash,
        receipt.transaction_index,
    )
    .await?
    .with_context(|| {
        format!(
            "failed to reload existing raw receipt for chain {} block {} index {} after insert conflict",
            receipt.chain_id, receipt.block_hash, receipt.transaction_index
        )
    })?;

    ensure_raw_receipt_identity_matches(&existing, receipt)?;
    let next_state = merge_canonicality(existing.canonicality_state, receipt.canonicality_state);

    let snapshot = sqlx::query(
        r#"
        UPDATE raw_receipts
        SET
            canonicality_state = $4::canonicality_state,
            observed_at = now()
        WHERE chain_id = $1
          AND block_hash = $2
          AND transaction_index = $3
        RETURNING
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
        "#,
    )
    .bind(&receipt.chain_id)
    .bind(&receipt.block_hash)
    .bind(receipt.transaction_index)
    .bind(next_state.as_str())
    .fetch_one(&mut **executor)
    .await
    .with_context(|| {
        format!(
            "failed to refresh raw receipt for chain {} block {} index {}",
            receipt.chain_id, receipt.block_hash, receipt.transaction_index
        )
    })?;

    decode_raw_receipt(snapshot)
}
