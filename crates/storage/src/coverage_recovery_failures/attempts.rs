use anyhow::{Context, Result, ensure};
use sqlx::{Executor, Postgres};

use super::{
    CoverageRecoveryFailureKey, CoverageRecoveryFailureRecord, decode_failure, validate_key,
};

pub(super) async fn load_job_attempt_watermark(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    key: &CoverageRecoveryFailureKey,
    backfill_job_id: i64,
) -> Result<i64> {
    load_job_attempt_watermark_with(&mut **transaction, key, backfill_job_id).await
}

pub async fn load_coverage_recovery_job_attempt_watermark(
    pool: &sqlx::PgPool,
    key: &CoverageRecoveryFailureKey,
    backfill_job_id: i64,
) -> Result<i64> {
    validate_key(key)?;
    ensure!(
        backfill_job_id > 0,
        "coverage recovery job id must be positive"
    );
    load_job_attempt_watermark_with(pool, key, backfill_job_id).await
}

async fn load_job_attempt_watermark_with<'e, E>(
    executor: E,
    key: &CoverageRecoveryFailureKey,
    backfill_job_id: i64,
) -> Result<i64>
where
    E: Executor<'e, Database = Postgres>,
{
    sqlx::query_scalar(
        r#"
        SELECT COALESCE((
            SELECT observed_attempt_count
            FROM normalized_replay_coverage_recovery_job_attempts
            WHERE deployment_profile = $1
              AND chain_id = $2
              AND raw_log_retention_generation = $3
              AND source_family = $4
              AND emitting_address = $5
              AND required_from_block = $6
              AND required_to_block = $7
              AND backfill_job_id = $8
        ), 0)::BIGINT
        "#,
    )
    .bind(&key.deployment_profile)
    .bind(&key.chain_id)
    .bind(key.raw_log_retention_generation)
    .bind(&key.source_family)
    .bind(&key.emitting_address)
    .bind(key.required_from_block)
    .bind(key.required_to_block)
    .bind(backfill_job_id)
    .fetch_one(executor)
    .await
    .context("failed to load coverage recovery job attempt watermark")
}

pub(super) async fn point_failure_at_observed_job(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    key: &CoverageRecoveryFailureKey,
    existing: &CoverageRecoveryFailureRecord,
    backfill_job_id: i64,
    observed_job_attempt_count: i64,
) -> Result<CoverageRecoveryFailureRecord> {
    if existing.last_backfill_job_id == Some(backfill_job_id)
        && existing.last_job_attempt_count >= observed_job_attempt_count
    {
        return Ok(existing.clone());
    }
    let row = sqlx::query(
        r#"
        UPDATE normalized_replay_coverage_recovery_failures
        SET last_backfill_job_id = $8,
            last_job_attempt_count = $9,
            updated_at = now()
        WHERE deployment_profile = $1
          AND chain_id = $2
          AND raw_log_retention_generation = $3
          AND source_family = $4
          AND emitting_address = $5
          AND required_from_block = $6
          AND required_to_block = $7
        RETURNING
            state,
            attempt_count,
            retry_not_before,
            last_backfill_job_id,
            last_job_attempt_count,
            failure_reason,
            failure_metadata
        "#,
    )
    .bind(&key.deployment_profile)
    .bind(&key.chain_id)
    .bind(key.raw_log_retention_generation)
    .bind(&key.source_family)
    .bind(&key.emitting_address)
    .bind(key.required_from_block)
    .bind(key.required_to_block)
    .bind(backfill_job_id)
    .bind(observed_job_attempt_count)
    .fetch_one(&mut **transaction)
    .await
    .context("failed to repoint coverage recovery failure at observed job")?;
    decode_failure(key.clone(), row)
}

pub(super) async fn upsert_job_attempt_watermark(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    key: &CoverageRecoveryFailureKey,
    backfill_job_id: i64,
    observed_attempt_count: i64,
) -> Result<()> {
    sqlx::query(
        r#"
        INSERT INTO normalized_replay_coverage_recovery_job_attempts (
            deployment_profile,
            chain_id,
            raw_log_retention_generation,
            source_family,
            emitting_address,
            required_from_block,
            required_to_block,
            backfill_job_id,
            observed_attempt_count
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
        ON CONFLICT (
            deployment_profile,
            chain_id,
            raw_log_retention_generation,
            source_family,
            emitting_address,
            required_from_block,
            required_to_block,
            backfill_job_id
        ) DO UPDATE
        SET observed_attempt_count = GREATEST(
                normalized_replay_coverage_recovery_job_attempts.observed_attempt_count,
                EXCLUDED.observed_attempt_count
            ),
            updated_at = now()
        "#,
    )
    .bind(&key.deployment_profile)
    .bind(&key.chain_id)
    .bind(key.raw_log_retention_generation)
    .bind(&key.source_family)
    .bind(&key.emitting_address)
    .bind(key.required_from_block)
    .bind(key.required_to_block)
    .bind(backfill_job_id)
    .bind(observed_attempt_count)
    .execute(&mut **transaction)
    .await
    .context("failed to persist coverage recovery job attempt watermark")?;
    Ok(())
}

pub(super) async fn update_bound_job_attempt_count(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    backfill_job_id: i64,
    expected_epoch: i64,
    observed_attempt_count: i64,
) -> Result<()> {
    sqlx::query(
        r#"
        UPDATE backfill_jobs
        SET coverage_recovery_bound_attempt_count = $3,
            updated_at = now()
        WHERE backfill_job_id = $1
          AND coverage_recovery_write_epoch = $2
        "#,
    )
    .bind(backfill_job_id)
    .bind(expected_epoch)
    .bind(observed_attempt_count)
    .execute(&mut **transaction)
    .await
    .context("failed to advance coverage recovery job attempt baseline")?;
    Ok(())
}
