use anyhow::{Context, Result, ensure};
use serde_json::Value;
use sqlx::PgPool;

use super::{
    CoverageRecoveryFailureKey, CoverageRecoveryFailureRecord, decode_failure, fence,
    lock_failure_key, validate_failure, validate_key,
};

pub async fn record_coverage_recovery_terminal_failure(
    pool: &PgPool,
    key: &CoverageRecoveryFailureKey,
    expected_epoch: i64,
    backfill_job_id: Option<i64>,
    job_attempt_count: i64,
    failure_reason: &str,
    failure_metadata: Value,
) -> Result<CoverageRecoveryFailureRecord> {
    validate_key(key)?;
    ensure!(
        backfill_job_id.is_none_or(|job_id| job_id > 0),
        "coverage recovery terminal job id must be positive"
    );
    ensure!(
        job_attempt_count >= 0,
        "coverage recovery terminal job attempt count must not be negative"
    );
    validate_failure(failure_reason, &failure_metadata)?;
    let failure_metadata = serde_json::to_string(&failure_metadata)
        .context("failed to serialize terminal coverage recovery failure metadata")?;
    let mut transaction = pool
        .begin()
        .await
        .context("failed to start terminal coverage recovery failure transaction")?;
    lock_failure_key(&mut transaction, key).await?;
    fence::validate_expected_epoch(&mut transaction, key, expected_epoch).await?;
    if let Some(backfill_job_id) = backfill_job_id {
        fail_job(
            &mut transaction,
            backfill_job_id,
            failure_reason,
            &failure_metadata,
        )
        .await?;
    }
    let row = sqlx::query(
        r#"
        INSERT INTO normalized_replay_coverage_recovery_failures (
            deployment_profile,
            chain_id,
            raw_log_retention_generation,
            source_family,
            emitting_address,
            required_from_block,
            required_to_block,
            state,
            attempt_count,
            retry_not_before,
            last_backfill_job_id,
            last_job_attempt_count,
            failure_reason,
            failure_metadata
        )
        VALUES (
            $1, $2, $3, $4, $5, $6, $7, 'terminal', 0, NULL, $8, $9, $10, $11::jsonb
        )
        ON CONFLICT (
            deployment_profile,
            chain_id,
            raw_log_retention_generation,
            source_family,
            emitting_address,
            required_from_block,
            required_to_block
        ) DO UPDATE
        SET state = 'terminal',
            retry_not_before = NULL,
            last_backfill_job_id = COALESCE(
                EXCLUDED.last_backfill_job_id,
                normalized_replay_coverage_recovery_failures.last_backfill_job_id
            ),
            last_job_attempt_count = GREATEST(
                normalized_replay_coverage_recovery_failures.last_job_attempt_count,
                EXCLUDED.last_job_attempt_count
            ),
            failure_reason = EXCLUDED.failure_reason,
            failure_metadata = EXCLUDED.failure_metadata,
            last_failed_at = now(),
            updated_at = now()
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
    .bind(job_attempt_count)
    .bind(failure_reason)
    .bind(failure_metadata)
    .fetch_one(&mut *transaction)
    .await
    .context("failed to persist terminal coverage recovery failure")?;
    let record = decode_failure(key.clone(), row)?;
    transaction
        .commit()
        .await
        .context("failed to commit terminal coverage recovery failure")?;
    Ok(record)
}

pub(super) async fn fail_job(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    backfill_job_id: i64,
    failure_reason: &str,
    failure_metadata: &str,
) -> Result<()> {
    let status = sqlx::query_scalar::<_, String>(
        r#"
        SELECT status::TEXT
        FROM backfill_jobs
        WHERE backfill_job_id = $1
        FOR UPDATE
        "#,
    )
    .bind(backfill_job_id)
    .fetch_optional(&mut **transaction)
    .await
    .context("failed to lock terminal coverage recovery job")?
    .context("terminal coverage recovery job disappeared")?;
    if status == "completed" {
        return Ok(());
    }

    let unfinished_statuses = sqlx::query_scalar::<_, String>(
        r#"
        SELECT status::TEXT
        FROM backfill_ranges
        WHERE backfill_job_id = $1
          AND status <> 'completed'::backfill_lifecycle_status
        ORDER BY backfill_range_id
        FOR UPDATE
        "#,
    )
    .bind(backfill_job_id)
    .fetch_all(&mut **transaction)
    .await
    .context("failed to lock terminal coverage recovery ranges")?;
    ensure!(
        unfinished_statuses
            .iter()
            .all(|status| status != "reserved" && status != "running"),
        "cannot publish terminal coverage recovery for job {backfill_job_id} while a child range is reserved or running"
    );
    sqlx::query(
        r#"
        UPDATE backfill_ranges
        SET status = 'failed'::backfill_lifecycle_status,
            lease_token = NULL,
            lease_owner = NULL,
            lease_expires_at = NULL,
            failure_reason = $2,
            failure_metadata = $3::jsonb,
            completed_at = NULL,
            updated_at = now()
        WHERE backfill_job_id = $1
          AND status <> 'completed'::backfill_lifecycle_status
        "#,
    )
    .bind(backfill_job_id)
    .bind(failure_reason)
    .bind(failure_metadata)
    .execute(&mut **transaction)
    .await
    .context("failed to mark terminal coverage recovery ranges failed")?;
    sqlx::query(
        r#"
        UPDATE backfill_jobs
        SET status = 'failed'::backfill_lifecycle_status,
            failure_reason = $2,
            failure_metadata = $3::jsonb,
            completed_at = NULL,
            updated_at = now()
        WHERE backfill_job_id = $1
          AND status <> 'completed'::backfill_lifecycle_status
        "#,
    )
    .bind(backfill_job_id)
    .bind(failure_reason)
    .bind(failure_metadata)
    .execute(&mut **transaction)
    .await
    .context("failed to mark terminal coverage recovery job failed")?;
    Ok(())
}
