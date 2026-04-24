use anyhow::{Context, Result};
use sqlx::{Row, postgres::PgRow};

use super::types::{BackfillJob, BackfillLifecycleStatus, BackfillRange};

pub(super) fn decode_backfill_job(row: PgRow) -> Result<BackfillJob> {
    Ok(BackfillJob {
        backfill_job_id: row
            .try_get("backfill_job_id")
            .context("missing backfill_job_id")?,
        deployment_profile: row
            .try_get("deployment_profile")
            .context("missing deployment_profile")?,
        chain_id: row.try_get("chain_id").context("missing chain_id")?,
        source_identity: row
            .try_get("source_identity")
            .context("missing source_identity")?,
        scan_mode: row.try_get("scan_mode").context("missing scan_mode")?,
        range_start_block_number: row
            .try_get("range_start_block_number")
            .context("missing range_start_block_number")?,
        range_end_block_number: row
            .try_get("range_end_block_number")
            .context("missing range_end_block_number")?,
        idempotency_key: row
            .try_get("idempotency_key")
            .context("missing idempotency_key")?,
        status: BackfillLifecycleStatus::parse(
            &row.try_get::<String, _>("status")
                .context("missing status")?,
        )?,
        failure_reason: row
            .try_get("failure_reason")
            .context("missing failure_reason")?,
        failure_metadata: row
            .try_get("failure_metadata")
            .context("missing failure_metadata")?,
        created_at: row.try_get("created_at").context("missing created_at")?,
        updated_at: row.try_get("updated_at").context("missing updated_at")?,
        completed_at: row
            .try_get("completed_at")
            .context("missing completed_at")?,
    })
}

pub(super) fn decode_backfill_range(row: PgRow) -> Result<BackfillRange> {
    Ok(BackfillRange {
        backfill_range_id: row
            .try_get("backfill_range_id")
            .context("missing backfill_range_id")?,
        backfill_job_id: row
            .try_get("backfill_job_id")
            .context("missing backfill_job_id")?,
        range_start_block_number: row
            .try_get("range_start_block_number")
            .context("missing range_start_block_number")?,
        range_end_block_number: row
            .try_get("range_end_block_number")
            .context("missing range_end_block_number")?,
        checkpoint_block_number: row
            .try_get("checkpoint_block_number")
            .context("missing checkpoint_block_number")?,
        status: BackfillLifecycleStatus::parse(
            &row.try_get::<String, _>("status")
                .context("missing status")?,
        )?,
        lease_token: row.try_get("lease_token").context("missing lease_token")?,
        lease_owner: row.try_get("lease_owner").context("missing lease_owner")?,
        lease_expires_at: row
            .try_get("lease_expires_at")
            .context("missing lease_expires_at")?,
        attempt_count: row
            .try_get("attempt_count")
            .context("missing attempt_count")?,
        failure_reason: row
            .try_get("failure_reason")
            .context("missing failure_reason")?,
        failure_metadata: row
            .try_get("failure_metadata")
            .context("missing failure_metadata")?,
        created_at: row.try_get("created_at").context("missing created_at")?,
        updated_at: row.try_get("updated_at").context("missing updated_at")?,
        completed_at: row
            .try_get("completed_at")
            .context("missing completed_at")?,
    })
}
