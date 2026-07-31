use anyhow::{Result, bail};
use bigname_manifests::WatchedSourceSelectorPlan;
use bigname_storage::{BackfillJobRecord, BackfillLifecycleStatus, fail_backfill_job};
use tracing::error;

use crate::{
    backfill::{
        BackfillAdapterSyncMode, BackfillBlockRange, BackfillJobRunConfig,
        DEFAULT_HASH_PINNED_BACKFILL_CHUNK_BLOCKS, create_hash_pinned_backfill_job,
        run_precreated_hash_pinned_backfill_job,
    },
    backfill_lease_expires_at, default_backfill_lease_owner, generated_backfill_lease_token,
    provider::ChainProvider,
};

use super::super::{
    capacity::{CAPACITY_FAILURE_REASON, capacity_metadata, check_capacity},
    config::{OpsCatchupConfig, OpsCatchupOutcome},
    planning::CatchupChunk,
};

// Chunk execution keeps its provider, finalized head, configuration, and outcome explicit.
#[expect(clippy::too_many_arguments)]
pub(super) async fn run_ops_finalized_catchup_chunk(
    pool: &sqlx::PgPool,
    chain: &str,
    provider: &ChainProvider,
    config: &OpsCatchupConfig,
    chunk: &CatchupChunk,
    finalized_head_block_number: i64,
    finalized_head_block_hash: &str,
    outcome: &mut OpsCatchupOutcome,
) -> Result<bool> {
    let source_plan = chunk.source_plan(chain)?;
    let run_config = ops_catchup_run_config(config, chain, &source_plan, chunk.range)?;
    let record = create_hash_pinned_backfill_job(pool, &source_plan, &run_config).await?;
    if record.job.status == BackfillLifecycleStatus::Completed {
        return Ok(true);
    }
    run_ops_finalized_catchup_record(
        pool,
        chain,
        provider,
        config,
        source_plan,
        run_config,
        record,
        finalized_head_block_number,
        finalized_head_block_hash,
        outcome,
    )
    .await?;
    Ok(false)
}

#[expect(clippy::too_many_arguments)]
async fn run_ops_finalized_catchup_record(
    pool: &sqlx::PgPool,
    chain: &str,
    provider: &ChainProvider,
    config: &OpsCatchupConfig,
    source_plan: WatchedSourceSelectorPlan,
    run_config: BackfillJobRunConfig,
    record: BackfillJobRecord,
    finalized_head_block_number: i64,
    finalized_head_block_hash: &str,
    outcome: &mut OpsCatchupOutcome,
) -> Result<()> {
    let range = run_config.range;
    let capacity_snapshot = match check_capacity(pool, &config.capacity, range).await {
        Ok(snapshot) => snapshot,
        Err(error) => {
            outcome.capacity_check_count += 1;
            let metadata = capacity_metadata(
                "check_failed",
                &run_config,
                range,
                finalized_head_block_number,
                finalized_head_block_hash,
                &config.capacity,
                None,
                Some(&error),
            );
            fail_backfill_job(
                pool,
                record.job.backfill_job_id,
                CAPACITY_FAILURE_REASON,
                metadata,
            )
            .await?;
            return Err(error.context("recorded ops catch-up capacity check failure"));
        }
    };
    outcome.capacity_check_count += 1;

    if !capacity_snapshot.breach_reasons.is_empty() {
        let metadata = capacity_metadata(
            "breached",
            &run_config,
            range,
            finalized_head_block_number,
            finalized_head_block_hash,
            &config.capacity,
            Some(&capacity_snapshot),
            None,
        );
        fail_backfill_job(
            pool,
            record.job.backfill_job_id,
            CAPACITY_FAILURE_REASON,
            metadata,
        )
        .await?;
        error!(
            service = "indexer",
            command = "ops-catchup",
            catchup_status = "capacity_breached",
            backfill_job_id = record.job.backfill_job_id,
            chain,
            from_block = range.from_block,
            to_block = range.to_block,
            postgres_database_size_bytes = capacity_snapshot.postgres_database_size_bytes,
            postgres_max_bytes = config.capacity.postgres_max_bytes,
            writable_free_disk_path = %config.capacity.writable_free_disk_path.display(),
            writable_free_disk_bytes = capacity_snapshot.writable_free_disk_bytes,
            min_writable_free_disk_bytes = config.capacity.min_writable_free_disk_bytes,
            estimated_chunk_write_bytes = capacity_snapshot.estimated_chunk_write_bytes,
            capacity_breach_reasons = ?capacity_snapshot.breach_reasons,
            "ops catch-up chunk failed before range work because capacity is insufficient"
        );
        bail!(
            "ops catch-up capacity guard breached for {chain} range {}..={}",
            range.from_block,
            range.to_block
        );
    }

    let job_outcome =
        run_precreated_hash_pinned_backfill_job(pool, &source_plan, provider, run_config, record)
            .await?;
    outcome.add_job(&job_outcome);
    Ok(())
}

fn ops_catchup_run_config(
    config: &OpsCatchupConfig,
    chain: &str,
    source_plan: &WatchedSourceSelectorPlan,
    range: BackfillBlockRange,
) -> Result<BackfillJobRunConfig> {
    Ok(BackfillJobRunConfig {
        deployment_profile: config.deployment_profile.clone(),
        idempotency_key: ops_catchup_idempotency_key(
            &config.deployment_profile,
            chain,
            &source_plan.source_identity_hash(),
            range,
        ),
        scope_idempotency_to_raw_log_retention_generation: true,
        coverage_recovery_reservation_fence: None,
        range,
        lease_owner: format!("{}:ops-finalized-catchup", default_backfill_lease_owner()),
        lease_token: generated_backfill_lease_token()?,
        lease_expires_at: backfill_lease_expires_at(config.lease_duration_secs)?,
        hash_pinned_chunk_blocks: DEFAULT_HASH_PINNED_BACKFILL_CHUNK_BLOCKS,
        adapter_sync_mode: BackfillAdapterSyncMode::Inline,
        header_audit_mode: config.header_audit_mode,
    })
}

pub(crate) fn ops_catchup_idempotency_key(
    deployment_profile: &str,
    chain: &str,
    source_identity_hash: &str,
    range: BackfillBlockRange,
) -> String {
    format!(
        "indexer-ops-finalized-catchup:v2:deployment_profile={deployment_profile}:chain={chain}:source_identity_hash={source_identity_hash}:from={}:to={}",
        range.from_block, range.to_block
    )
}
