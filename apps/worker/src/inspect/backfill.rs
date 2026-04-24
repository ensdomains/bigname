use anyhow::{Context, Result};
use bigname_storage::{BackfillJob, BackfillJobRecord, BackfillLifecycleStatus, BackfillRange};
use serde_json::{Value, json};
use sqlx::types::time::OffsetDateTime;

use super::InspectBackfillJobArgs;
use super::formatting::format_timestamp;

pub(in crate::inspect) async fn inspect_backfill_job(args: InspectBackfillJobArgs) -> Result<()> {
    let pool = bigname_storage::connect(&args.database).await?;
    let inspection = load_backfill_job_inspection(&pool, args.backfill_job_id).await?;

    println!("{}", render_backfill_job_inspection(&inspection));
    Ok(())
}

pub(in crate::inspect) async fn load_backfill_job_inspection(
    pool: &sqlx::PgPool,
    backfill_job_id: i64,
) -> Result<BackfillJobRecord> {
    let job = bigname_storage::load_backfill_job(pool, backfill_job_id)
        .await?
        .with_context(|| format!("missing backfill job {backfill_job_id}"))?;
    let mut ranges = bigname_storage::load_backfill_ranges(pool, backfill_job_id).await?;
    ranges.sort_by_key(|range| {
        (
            range.range_start_block_number,
            range.range_end_block_number,
            range.backfill_range_id,
        )
    });

    Ok(BackfillJobRecord { job, ranges })
}

pub(in crate::inspect) fn render_backfill_job_inspection(inspection: &BackfillJobRecord) -> Value {
    json!({
        "job": render_backfill_job(&inspection.job),
        "ranges": inspection
            .ranges
            .iter()
            .map(render_backfill_range)
            .collect::<Vec<_>>(),
    })
}

fn render_backfill_job(job: &BackfillJob) -> Value {
    json!({
        "backfill_job_id": job.backfill_job_id,
        "deployment_profile": job.deployment_profile.as_str(),
        "chain_id": job.chain_id.as_str(),
        "source_identity": job.source_identity.clone(),
        "scan_mode": job.scan_mode.as_str(),
        "status": job.status.as_str(),
        "lifecycle": render_lifecycle_state(job.status),
        "declared_range": render_declared_range(
            job.range_start_block_number,
            job.range_end_block_number,
        ),
        "idempotency_key": job.idempotency_key.as_str(),
        "timestamps": render_timestamps(job.created_at, job.updated_at, job.completed_at),
        "failure": render_failure(job.failure_reason.as_deref(), &job.failure_metadata),
    })
}

fn render_backfill_range(range: &BackfillRange) -> Value {
    json!({
        "backfill_range_id": range.backfill_range_id,
        "backfill_job_id": range.backfill_job_id,
        "status": range.status.as_str(),
        "lifecycle": render_lifecycle_state(range.status),
        "declared_range": render_declared_range(
            range.range_start_block_number,
            range.range_end_block_number,
        ),
        "checkpoint": {
            "block_number": range.checkpoint_block_number,
        },
        "lease": {
            "owner": range.lease_owner.as_deref(),
            "token": range.lease_token.as_deref(),
            "expires_at": range.lease_expires_at.map(format_timestamp),
        },
        "attempt_count": range.attempt_count,
        "timestamps": render_timestamps(range.created_at, range.updated_at, range.completed_at),
        "failure": render_failure(range.failure_reason.as_deref(), &range.failure_metadata),
    })
}

fn render_lifecycle_state(status: BackfillLifecycleStatus) -> Value {
    json!({
        "status": status.as_str(),
        "pending": status == BackfillLifecycleStatus::Pending,
        "reserved": status == BackfillLifecycleStatus::Reserved,
        "running": status == BackfillLifecycleStatus::Running,
        "completed": status == BackfillLifecycleStatus::Completed,
        "failed": status == BackfillLifecycleStatus::Failed,
    })
}

fn render_declared_range(start_block_number: i64, end_block_number: i64) -> Value {
    json!({
        "start_block_number": start_block_number,
        "end_block_number": end_block_number,
    })
}

fn render_timestamps(
    created_at: OffsetDateTime,
    updated_at: OffsetDateTime,
    completed_at: Option<OffsetDateTime>,
) -> Value {
    json!({
        "created_at": format_timestamp(created_at),
        "updated_at": format_timestamp(updated_at),
        "completed_at": completed_at.map(format_timestamp),
    })
}

fn render_failure(reason: Option<&str>, metadata: &Value) -> Value {
    json!({
        "reason": reason,
        "metadata": metadata.clone(),
    })
}
