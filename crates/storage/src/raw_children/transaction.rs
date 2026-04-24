use anyhow::{Context, Result};
use sqlx::{PgPool, Postgres};

use super::{
    decode::decode_raw_transaction,
    load::load_raw_transaction_internal,
    types::RawTransaction,
    validation::{
        ensure_raw_transaction_identity_matches, merge_canonicality, validate_raw_transaction,
    },
};

/// Insert missing raw transaction rows or refresh canonicality for already
/// observed block-scoped transactions.
pub async fn upsert_raw_transactions(
    pool: &PgPool,
    transactions: &[RawTransaction],
) -> Result<Vec<RawTransaction>> {
    if transactions.is_empty() {
        return Ok(Vec::new());
    }

    let mut transaction = pool
        .begin()
        .await
        .context("failed to open transaction for raw transaction upsert")?;

    let mut snapshots = Vec::with_capacity(transactions.len());
    for raw_transaction in transactions {
        validate_raw_transaction(raw_transaction)?;
        snapshots.push(upsert_raw_transaction(&mut transaction, raw_transaction).await?);
    }

    transaction
        .commit()
        .await
        .context("failed to commit raw transaction upsert")?;

    Ok(snapshots)
}

async fn upsert_raw_transaction(
    executor: &mut sqlx::Transaction<'_, Postgres>,
    transaction: &RawTransaction,
) -> Result<RawTransaction> {
    if let Some(snapshot) = sqlx::query(
        r#"
        INSERT INTO raw_transactions (
            chain_id,
            block_hash,
            block_number,
            transaction_hash,
            transaction_index,
            from_address,
            to_address,
            canonicality_state
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8::canonicality_state)
        ON CONFLICT (chain_id, block_hash, transaction_index) DO NOTHING
        RETURNING
            chain_id,
            block_hash,
            block_number,
            transaction_hash,
            transaction_index,
            from_address,
            to_address,
            canonicality_state::TEXT AS canonicality_state
        "#,
    )
    .bind(&transaction.chain_id)
    .bind(&transaction.block_hash)
    .bind(transaction.block_number)
    .bind(&transaction.transaction_hash)
    .bind(transaction.transaction_index)
    .bind(&transaction.from_address)
    .bind(&transaction.to_address)
    .bind(transaction.canonicality_state.as_str())
    .fetch_optional(&mut **executor)
    .await
    .with_context(|| {
        format!(
            "failed to insert raw transaction for chain {} block {} transaction {}",
            transaction.chain_id, transaction.block_hash, transaction.transaction_hash
        )
    })? {
        return decode_raw_transaction(snapshot);
    }

    let existing = load_raw_transaction_internal(
        &mut **executor,
        &transaction.chain_id,
        &transaction.block_hash,
        transaction.transaction_index,
    )
    .await?
    .with_context(|| {
        format!(
            "failed to reload existing raw transaction for chain {} block {} index {} after insert conflict",
            transaction.chain_id, transaction.block_hash, transaction.transaction_index
        )
    })?;

    ensure_raw_transaction_identity_matches(&existing, transaction)?;
    let next_state =
        merge_canonicality(existing.canonicality_state, transaction.canonicality_state);

    let snapshot = sqlx::query(
        r#"
        UPDATE raw_transactions
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
            from_address,
            to_address,
            canonicality_state::TEXT AS canonicality_state
        "#,
    )
    .bind(&transaction.chain_id)
    .bind(&transaction.block_hash)
    .bind(transaction.transaction_index)
    .bind(next_state.as_str())
    .fetch_one(&mut **executor)
    .await
    .with_context(|| {
        format!(
            "failed to refresh raw transaction for chain {} block {} index {}",
            transaction.chain_id, transaction.block_hash, transaction.transaction_index
        )
    })?;

    decode_raw_transaction(snapshot)
}
