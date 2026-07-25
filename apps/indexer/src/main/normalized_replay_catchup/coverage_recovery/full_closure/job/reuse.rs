use anyhow::{Context, Result};
use bigname_storage::{BackfillJobRecord, BackfillLifecycleStatus, CoverageRecoveryFailureRecord};
use serde_json::Value;

use super::super::super::super::FullClosureCoverageViolations;
use crate::backfill::BackfillBlockRange;

pub(super) async fn reusable_incomplete_job(
    pool: &sqlx::PgPool,
    persisted_failure: Option<&CoverageRecoveryFailureRecord>,
    deployment_profile: &str,
    requirement: &FullClosureCoverageViolations,
    range: BackfillBlockRange,
    source_identity: &Value,
    expected_epoch: i64,
) -> Result<Option<BackfillJobRecord>> {
    let persisted_job_id = persisted_failure.and_then(|failure| failure.last_backfill_job_id);
    let discovered_job_id = sqlx::query_scalar::<_, i64>(
        r#"
        SELECT backfill_job_id
        FROM backfill_jobs
        WHERE deployment_profile = $1
          AND chain_id = $2
          AND raw_log_retention_generation = $3
          AND range_start_block_number = $4
          AND range_end_block_number = $5
          AND source_identity = $6
          AND coverage_recovery_write_epoch = $7
          AND status <> 'completed'::backfill_lifecycle_status
          AND idempotency_key LIKE
              'indexer-full-closure-coverage-recovery:%'
        ORDER BY backfill_job_id DESC
        LIMIT 1
        "#,
    )
    .bind(deployment_profile)
    .bind(&requirement.chain)
    .bind(requirement.retention_generation)
    .bind(range.from_block)
    .bind(range.to_block)
    .bind(source_identity)
    .bind(expected_epoch)
    .fetch_optional(pool)
    .await
    .context("failed to find an incomplete exact-identity coverage recovery job")?;

    for job_id in [persisted_job_id, discovered_job_id].into_iter().flatten() {
        let Some(job) = bigname_storage::load_backfill_job(pool, job_id).await? else {
            continue;
        };
        if job.status == BackfillLifecycleStatus::Completed
            || job.deployment_profile != deployment_profile
            || job.chain_id != requirement.chain
            || job.raw_log_retention_generation != requirement.retention_generation
            || job.range_start_block_number != range.from_block
            || job.range_end_block_number != range.to_block
            || job.source_identity != *source_identity
        {
            continue;
        }
        let bound_epoch = sqlx::query_scalar::<_, Option<i64>>(
            "SELECT coverage_recovery_write_epoch FROM backfill_jobs WHERE backfill_job_id = $1",
        )
        .bind(job_id)
        .fetch_optional(pool)
        .await
        .context("failed to validate reusable coverage recovery job epoch")?
        .flatten();
        if bound_epoch != Some(expected_epoch) {
            continue;
        }
        let ranges = bigname_storage::load_backfill_ranges(pool, job_id).await?;
        return Ok(Some(BackfillJobRecord { job, ranges }));
    }
    Ok(None)
}
