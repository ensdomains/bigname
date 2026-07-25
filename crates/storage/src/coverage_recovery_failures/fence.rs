use anyhow::{Context, Result, ensure};
use sqlx::PgPool;

use super::{CoverageRecoveryFailureKey, lock_failure_key, validate_key};

pub async fn load_bound_coverage_recovery_job_with_unjournaled_attempt(
    pool: &PgPool,
    key: &CoverageRecoveryFailureKey,
    expected_epoch: i64,
) -> Result<Option<crate::BackfillJobRecord>> {
    validate_key(key)?;
    ensure!(
        expected_epoch >= 0,
        "coverage recovery expected write epoch must not be negative"
    );
    let job_id = sqlx::query_scalar::<_, i64>(
        r#"
        SELECT job.backfill_job_id
        FROM backfill_jobs job
        WHERE job.deployment_profile = $1
          AND job.chain_id = $2
          AND job.raw_log_retention_generation = $3
          AND job.range_start_block_number = $4
          AND job.range_end_block_number = $5
          AND job.status = 'failed'::backfill_lifecycle_status
          AND job.coverage_recovery_write_epoch = $6
          AND job.idempotency_key LIKE
              'indexer-full-closure-coverage-recovery:%'
          AND EXISTS (
                SELECT 1
                FROM jsonb_array_elements(
                    CASE
                        WHEN jsonb_typeof(job.source_identity -> 'selected_targets') = 'array'
                            THEN job.source_identity -> 'selected_targets'
                        ELSE '[]'::jsonb
                    END
                ) selected(target)
                WHERE selected.target ->> 'source_family' = $7
                  AND LOWER(selected.target ->> 'address') = LOWER($8)
          )
          AND EXISTS (
                SELECT 1
                FROM backfill_ranges range
                WHERE range.backfill_job_id = job.backfill_job_id
                  AND range.attempt_count >
                      job.coverage_recovery_bound_attempt_count
          )
        ORDER BY job.backfill_job_id
        LIMIT 1
        "#,
    )
    .bind(&key.deployment_profile)
    .bind(&key.chain_id)
    .bind(key.raw_log_retention_generation)
    .bind(key.required_from_block)
    .bind(key.required_to_block)
    .bind(expected_epoch)
    .bind(&key.source_family)
    .bind(&key.emitting_address)
    .fetch_optional(pool)
    .await
    .context("failed to find an unjournaled bound coverage recovery attempt")?;
    let Some(job_id) = job_id else {
        return Ok(None);
    };
    let job = crate::load_backfill_job(pool, job_id)
        .await?
        .context("bound coverage recovery job disappeared")?;
    let ranges = crate::load_backfill_ranges(pool, job_id).await?;
    Ok(Some(crate::BackfillJobRecord { job, ranges }))
}

pub async fn bind_coverage_recovery_job_write_epoch(
    pool: &PgPool,
    key: &CoverageRecoveryFailureKey,
    expected_epoch: i64,
    backfill_job_id: i64,
) -> Result<()> {
    validate_key(key)?;
    ensure!(
        backfill_job_id > 0,
        "coverage recovery job id must be positive"
    );
    let mut transaction = pool
        .begin()
        .await
        .context("failed to start coverage recovery job epoch transaction")?;
    lock_failure_key(&mut transaction, key).await?;
    validate_expected_epoch(&mut transaction, key, expected_epoch).await?;
    let jobs = sqlx::query_as::<_, (i64, Option<i64>, Option<i64>)>(
        r#"
        SELECT
            job.backfill_job_id,
            job.coverage_recovery_write_epoch,
            job.coverage_recovery_bound_attempt_count
        FROM backfill_jobs job
        WHERE job.deployment_profile = $1
          AND job.chain_id = $2
          AND job.raw_log_retention_generation = $3
          AND job.range_start_block_number = $4
          AND job.range_end_block_number = $5
          AND job.status <> 'completed'::backfill_lifecycle_status
          AND job.idempotency_key LIKE
              'indexer-full-closure-coverage-recovery:%'
          AND EXISTS (
                SELECT 1
                FROM jsonb_array_elements(
                    CASE
                        WHEN jsonb_typeof(job.source_identity -> 'selected_targets') = 'array'
                            THEN job.source_identity -> 'selected_targets'
                        ELSE '[]'::jsonb
                    END
                ) selected(target)
                WHERE selected.target ->> 'source_family' = $6
                  AND LOWER(selected.target ->> 'address') = LOWER($7)
          )
        ORDER BY job.backfill_job_id
        FOR UPDATE OF job
        "#,
    )
    .bind(&key.deployment_profile)
    .bind(&key.chain_id)
    .bind(key.raw_log_retention_generation)
    .bind(key.required_from_block)
    .bind(key.required_to_block)
    .bind(&key.source_family)
    .bind(&key.emitting_address)
    .fetch_all(&mut *transaction)
    .await
    .context("failed to lock exact-window jobs for coverage recovery epoch binding")?;
    ensure!(
        jobs.iter().any(|(job_id, _, _)| *job_id == backfill_job_id),
        "backfill job {backfill_job_id} does not match its exact coverage recovery failure key"
    );
    let mut target_is_bound = false;
    for (job_id, bound_epoch, bound_attempt_count) in jobs {
        if job_id == backfill_job_id && bound_epoch == Some(expected_epoch) {
            ensure!(
                bound_attempt_count.is_some(),
                "coverage recovery job {job_id} has an epoch without an attempt baseline"
            );
            target_is_bound = true;
            continue;
        }
        if bound_epoch == Some(expected_epoch) {
            ensure_job_has_no_unjournaled_attempt(&mut transaction, job_id, bound_attempt_count)
                .await?;
            clear_job_binding(&mut transaction, job_id).await?;
        }
    }
    if !target_is_bound {
        let (current_attempt_count, has_active_lease) =
            load_job_attempt_state(&mut transaction, backfill_job_id).await?;
        ensure!(
            !has_active_lease,
            "cannot bind coverage recovery job {backfill_job_id} while it has an active range lease"
        );
        sqlx::query(
            r#"
            UPDATE backfill_jobs
            SET coverage_recovery_write_epoch = $2,
                coverage_recovery_bound_attempt_count = $3,
                updated_at = now()
            WHERE backfill_job_id = $1
            "#,
        )
        .bind(backfill_job_id)
        .bind(expected_epoch)
        .bind(current_attempt_count)
        .execute(&mut *transaction)
        .await
        .context("failed to bind coverage recovery job write epoch")?;
    }
    transaction
        .commit()
        .await
        .context("failed to commit coverage recovery job epoch binding")?;
    Ok(())
}

async fn ensure_job_has_no_unjournaled_attempt(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    backfill_job_id: i64,
    bound_attempt_count: Option<i64>,
) -> Result<()> {
    let (current_attempt_count, has_active_lease) =
        load_job_attempt_state(transaction, backfill_job_id).await?;
    ensure!(
        !has_active_lease,
        "cannot switch coverage recovery job revisions while job {backfill_job_id} has an active range lease"
    );
    ensure!(
        bound_attempt_count == Some(current_attempt_count),
        "cannot switch coverage recovery job revisions while job {backfill_job_id} has an unjournaled attempt"
    );
    Ok(())
}

async fn load_job_attempt_state(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    backfill_job_id: i64,
) -> Result<(i64, bool)> {
    sqlx::query_as(
        r#"
        SELECT
            COALESCE(MAX(attempt_count), 0)::BIGINT,
            COALESCE(BOOL_OR(
                status IN (
                    'reserved'::backfill_lifecycle_status,
                    'running'::backfill_lifecycle_status
                )
                AND lease_expires_at > now()
            ), FALSE)
        FROM backfill_ranges
        WHERE backfill_job_id = $1
        "#,
    )
    .bind(backfill_job_id)
    .fetch_one(&mut **transaction)
    .await
    .context("failed to inspect coverage recovery job attempts during epoch binding")
}

async fn clear_job_binding(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    backfill_job_id: i64,
) -> Result<()> {
    sqlx::query(
        r#"
        UPDATE backfill_jobs
        SET coverage_recovery_write_epoch = NULL,
            coverage_recovery_bound_attempt_count = NULL,
            updated_at = now()
        WHERE backfill_job_id = $1
        "#,
    )
    .bind(backfill_job_id)
    .execute(&mut **transaction)
    .await
    .context("failed to clear superseded coverage recovery job binding")?;
    Ok(())
}

pub async fn load_coverage_recovery_epoch(
    pool: &PgPool,
    key: &CoverageRecoveryFailureKey,
) -> Result<i64> {
    validate_key(key)?;
    load_epoch(pool, key).await
}

pub(crate) async fn validate_expected_epoch(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    key: &CoverageRecoveryFailureKey,
    expected_epoch: i64,
) -> Result<()> {
    ensure!(
        expected_epoch >= 0,
        "coverage recovery expected write epoch must not be negative"
    );
    let current_epoch = load_epoch(&mut **transaction, key).await?;
    ensure!(
        current_epoch == expected_epoch,
        "coverage recovery write epoch changed from {expected_epoch} to {current_epoch}; replan from current operator and recovery state"
    );
    Ok(())
}

pub(crate) async fn load_epoch_in_transaction(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    key: &CoverageRecoveryFailureKey,
) -> Result<i64> {
    load_epoch(&mut **transaction, key).await
}

pub(super) async fn advance_epoch(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    key: &CoverageRecoveryFailureKey,
) -> Result<i64> {
    sqlx::query_scalar(
        r#"
        INSERT INTO normalized_replay_coverage_recovery_epochs (
            deployment_profile,
            chain_id,
            raw_log_retention_generation,
            source_family,
            emitting_address,
            required_from_block,
            required_to_block,
            write_epoch
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, 1)
        ON CONFLICT (
            deployment_profile,
            chain_id,
            raw_log_retention_generation,
            source_family,
            emitting_address,
            required_from_block,
            required_to_block
        ) DO UPDATE
        SET write_epoch = normalized_replay_coverage_recovery_epochs.write_epoch + 1,
            updated_at = now()
        RETURNING write_epoch
        "#,
    )
    .bind(&key.deployment_profile)
    .bind(&key.chain_id)
    .bind(key.raw_log_retention_generation)
    .bind(&key.source_family)
    .bind(&key.emitting_address)
    .bind(key.required_from_block)
    .bind(key.required_to_block)
    .fetch_one(&mut **transaction)
    .await
    .context("failed to advance coverage recovery write epoch")
}

async fn load_epoch<'e, E>(executor: E, key: &CoverageRecoveryFailureKey) -> Result<i64>
where
    E: sqlx::Executor<'e, Database = sqlx::Postgres>,
{
    sqlx::query_scalar(
        r#"
        SELECT COALESCE((
            SELECT write_epoch
            FROM normalized_replay_coverage_recovery_epochs
            WHERE deployment_profile = $1
              AND chain_id = $2
              AND raw_log_retention_generation = $3
              AND source_family = $4
              AND emitting_address = $5
              AND required_from_block = $6
              AND required_to_block = $7
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
    .fetch_one(executor)
    .await
    .context("failed to load coverage recovery write epoch")
}
