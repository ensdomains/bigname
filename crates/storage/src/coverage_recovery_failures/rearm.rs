use anyhow::{Context, Result, ensure};
use sqlx::PgPool;

use super::{
    CoverageRecoveryFailureKey, CoverageRecoveryFailureState, fence, load_failure_for_update,
    lock_failure_key, validate_key,
};

/// Explicitly reset one operator-remediated terminal interval so automatic
/// recovery may attempt its last incomplete job again. Retry-backoff records
/// are not operator re-arm targets and are left untouched.
pub async fn rearm_terminal_coverage_recovery_failure(
    pool: &PgPool,
    key: &CoverageRecoveryFailureKey,
) -> Result<bool> {
    validate_key(key)?;
    let mut transaction = pool
        .begin()
        .await
        .context("failed to start coverage recovery re-arm transaction")?;
    lock_failure_key(&mut transaction, key).await?;
    let Some(failure) = load_failure_for_update(&mut transaction, key).await? else {
        transaction
            .commit()
            .await
            .context("failed to commit missing coverage recovery re-arm")?;
        return Ok(false);
    };
    if failure.state != CoverageRecoveryFailureState::Terminal {
        transaction
            .commit()
            .await
            .context("failed to commit nonterminal coverage recovery re-arm")?;
        return Ok(false);
    }

    let mut matching_job_ids = load_matching_incomplete_job_ids(&mut transaction, key).await?;
    if let Some(backfill_job_id) = failure.last_backfill_job_id
        && !matching_job_ids.contains(&backfill_job_id)
    {
        matching_job_ids.push(backfill_job_id);
        matching_job_ids.sort_unstable();
    }
    for &backfill_job_id in &matching_job_ids {
        reset_incomplete_job_attempts(&mut transaction, backfill_job_id).await?;
    }
    let write_epoch = fence::advance_epoch(&mut transaction, key).await?;
    for backfill_job_id in matching_job_ids {
        bind_reset_job_to_epoch(&mut transaction, backfill_job_id, write_epoch).await?;
    }
    let removed = delete_failure(&mut transaction, key).await?;
    ensure!(
        removed == 1,
        "terminal coverage recovery record disappeared during re-arm"
    );
    transaction
        .commit()
        .await
        .context("failed to commit coverage recovery re-arm")?;
    Ok(true)
}

async fn load_matching_incomplete_job_ids(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    key: &CoverageRecoveryFailureKey,
) -> Result<Vec<i64>> {
    sqlx::query_scalar(
        r#"
        SELECT job.backfill_job_id
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
    .fetch_all(&mut **transaction)
    .await
    .context("failed to lock exact-window coverage recovery jobs for re-arm")
}

async fn bind_reset_job_to_epoch(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    backfill_job_id: i64,
    write_epoch: i64,
) -> Result<()> {
    sqlx::query(
        r#"
        UPDATE backfill_jobs
        SET coverage_recovery_write_epoch = $2,
            coverage_recovery_bound_attempt_count = 0,
            updated_at = now()
        WHERE backfill_job_id = $1
        "#,
    )
    .bind(backfill_job_id)
    .bind(write_epoch)
    .execute(&mut **transaction)
    .await
    .with_context(|| {
        format!(
            "failed to bind reset coverage recovery job {backfill_job_id} to write epoch {write_epoch}"
        )
    })?;
    Ok(())
}

async fn reset_incomplete_job_attempts(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    backfill_job_id: i64,
) -> Result<()> {
    let job_status = sqlx::query_scalar::<_, String>(
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
    .context("failed to lock terminal coverage recovery job for re-arm")?;
    if job_status
        .as_deref()
        .is_none_or(|status| status == "completed")
    {
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
    .context("failed to lock terminal coverage recovery ranges for re-arm")?;
    ensure!(
        unfinished_statuses
            .iter()
            .all(|status| status != "reserved" && status != "running"),
        "cannot re-arm terminal coverage recovery job {backfill_job_id} while a child range is reserved or running"
    );
    sqlx::query(
        r#"
        UPDATE backfill_ranges
        SET attempt_count = 0,
            updated_at = now()
        WHERE backfill_job_id = $1
          AND status <> 'completed'::backfill_lifecycle_status
        "#,
    )
    .bind(backfill_job_id)
    .execute(&mut **transaction)
    .await
    .context("failed to reset terminal coverage recovery range attempts")?;
    Ok(())
}

async fn delete_failure(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    key: &CoverageRecoveryFailureKey,
) -> Result<u64> {
    sqlx::query(
        r#"
        DELETE FROM normalized_replay_coverage_recovery_failures
        WHERE deployment_profile = $1
          AND chain_id = $2
          AND raw_log_retention_generation = $3
          AND source_family = $4
          AND emitting_address = $5
          AND required_from_block = $6
          AND required_to_block = $7
        "#,
    )
    .bind(&key.deployment_profile)
    .bind(&key.chain_id)
    .bind(key.raw_log_retention_generation)
    .bind(&key.source_family)
    .bind(&key.emitting_address)
    .bind(key.required_from_block)
    .bind(key.required_to_block)
    .execute(&mut **transaction)
    .await
    .context("failed to re-arm terminal coverage recovery failure")
    .map(|result| result.rows_affected())
}
