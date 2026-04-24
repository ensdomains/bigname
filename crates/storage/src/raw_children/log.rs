use anyhow::{Context, Result};
use sqlx::{PgPool, Postgres};

use super::{
    decode::decode_raw_log,
    load::load_raw_log_internal,
    types::RawLog,
    validation::{ensure_raw_log_identity_matches, merge_canonicality, validate_raw_log},
};

/// Insert missing raw log rows or refresh canonicality for already observed
/// block-scoped logs.
pub async fn upsert_raw_logs(pool: &PgPool, logs: &[RawLog]) -> Result<Vec<RawLog>> {
    if logs.is_empty() {
        return Ok(Vec::new());
    }

    let mut transaction = pool
        .begin()
        .await
        .context("failed to open transaction for raw log upsert")?;

    let mut snapshots = Vec::with_capacity(logs.len());
    for raw_log in logs {
        validate_raw_log(raw_log)?;
        snapshots.push(upsert_raw_log(&mut transaction, raw_log).await?);
    }

    transaction
        .commit()
        .await
        .context("failed to commit raw log upsert")?;

    Ok(snapshots)
}

async fn upsert_raw_log(
    executor: &mut sqlx::Transaction<'_, Postgres>,
    log: &RawLog,
) -> Result<RawLog> {
    if let Some(snapshot) = sqlx::query(
        r#"
        INSERT INTO raw_logs (
            chain_id,
            block_hash,
            block_number,
            transaction_hash,
            transaction_index,
            log_index,
            emitting_address,
            topics,
            data,
            canonicality_state
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10::canonicality_state)
        ON CONFLICT (chain_id, block_hash, log_index) DO NOTHING
        RETURNING
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
        "#,
    )
    .bind(&log.chain_id)
    .bind(&log.block_hash)
    .bind(log.block_number)
    .bind(&log.transaction_hash)
    .bind(log.transaction_index)
    .bind(log.log_index)
    .bind(&log.emitting_address)
    .bind(&log.topics)
    .bind(&log.data)
    .bind(log.canonicality_state.as_str())
    .fetch_optional(&mut **executor)
    .await
    .with_context(|| {
        format!(
            "failed to insert raw log for chain {} block {} log {}",
            log.chain_id, log.block_hash, log.log_index
        )
    })? {
        return decode_raw_log(snapshot);
    }

    let existing = load_raw_log_internal(
        &mut **executor,
        &log.chain_id,
        &log.block_hash,
        log.log_index,
    )
    .await?
    .with_context(|| {
        format!(
            "failed to reload existing raw log for chain {} block {} log {} after insert conflict",
            log.chain_id, log.block_hash, log.log_index
        )
    })?;

    ensure_raw_log_identity_matches(&existing, log)?;
    let next_state = merge_canonicality(existing.canonicality_state, log.canonicality_state);

    let snapshot = sqlx::query(
        r#"
        UPDATE raw_logs
        SET
            canonicality_state = $4::canonicality_state,
            observed_at = now()
        WHERE chain_id = $1
          AND block_hash = $2
          AND log_index = $3
        RETURNING
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
        "#,
    )
    .bind(&log.chain_id)
    .bind(&log.block_hash)
    .bind(log.log_index)
    .bind(next_state.as_str())
    .fetch_one(&mut **executor)
    .await
    .with_context(|| {
        format!(
            "failed to refresh raw log for chain {} block {} log {}",
            log.chain_id, log.block_hash, log.log_index
        )
    })?;

    decode_raw_log(snapshot)
}
