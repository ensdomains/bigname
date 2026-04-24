use anyhow::{Context, Result, bail};
use bigname_manifests::WatchedSourceSelectorPlan;
use bigname_storage::{
    BackfillJobCreate, BackfillLifecycleStatus, BackfillRange, BackfillRangeSpec,
    advance_backfill_range, complete_backfill_range, create_backfill_job, load_backfill_job,
    reserve_backfill_range,
};
use tracing::info;

use crate::provider::JsonRpcProvider;

use super::{
    BackfillBlockRange, BackfillJobRunConfig, BackfillJobRunOutcome,
    failure_recording::{ReservedRangeFailure, record_reserved_range_failure},
    fetching::run_hash_pinned_backfill_range,
};

const HASH_PINNED_BACKFILL_SCAN_MODE: &str = "hash_pinned_block";
const HASH_PINNED_BACKFILL_CHUNK_BLOCKS: i64 = 32;

pub(crate) async fn run_resumable_hash_pinned_backfill_job(
    pool: &sqlx::PgPool,
    source_plan: &WatchedSourceSelectorPlan,
    provider: &JsonRpcProvider,
    config: BackfillJobRunConfig,
) -> Result<BackfillJobRunOutcome> {
    let watched_chain = &source_plan.watched_chain_plan;
    let request = BackfillJobCreate {
        deployment_profile: config.deployment_profile.clone(),
        chain_id: watched_chain.chain.clone(),
        source_identity: source_plan.source_identity_payload(),
        scan_mode: HASH_PINNED_BACKFILL_SCAN_MODE.to_owned(),
        range_start_block_number: config.range.from_block,
        range_end_block_number: config.range.to_block,
        idempotency_key: config.idempotency_key.clone(),
        ranges: vec![BackfillRangeSpec {
            range_start_block_number: config.range.from_block,
            range_end_block_number: config.range.to_block,
        }],
    };
    let record = create_backfill_job(pool, &request).await?;
    let mut outcome = BackfillJobRunOutcome::new(record.job.backfill_job_id, source_plan, &config);

    info!(
        service = "indexer",
        command = "backfill",
        backfill_job_id = record.job.backfill_job_id,
        backfill_job_status = record.job.status.as_str(),
        chain = %watched_chain.chain,
        selector_kind = source_plan.selector_kind.as_str(),
        selected_target_count = source_plan.selected_targets.len(),
        deployment_profile = %config.deployment_profile,
        from_block = config.range.from_block,
        to_block = config.range.to_block,
        idempotency_key = %config.idempotency_key,
        range_count = record.ranges.len(),
        "resumable backfill job loaded"
    );

    loop {
        let Some(reserved_range) = reserve_backfill_range(
            pool,
            record.job.backfill_job_id,
            &config.lease_owner,
            &config.lease_token,
            config.lease_expires_at,
        )
        .await?
        else {
            break;
        };

        outcome.reserved_range_count += 1;
        run_reserved_hash_pinned_backfill_range(
            pool,
            source_plan,
            provider,
            &config,
            &reserved_range,
            &mut outcome,
        )
        .await?;
        outcome.completed_range_count += 1;
    }

    let job = load_backfill_job(pool, record.job.backfill_job_id)
        .await?
        .with_context(|| format!("missing backfill job {}", record.job.backfill_job_id))?;
    if job.status == BackfillLifecycleStatus::Completed {
        info!(
            service = "indexer",
            command = "backfill",
            backfill_job_id = outcome.backfill_job_id,
            chain = %outcome.chain,
            from_block = outcome.from_block,
            to_block = outcome.to_block,
            idempotency_key = %outcome.idempotency_key,
            reserved_range_count = outcome.reserved_range_count,
            completed_range_count = outcome.completed_range_count,
            resolved_block_count = outcome.resolved_block_count,
            raw_block_count = outcome.raw_block_count,
            raw_transaction_count = outcome.raw_transaction_count,
            raw_receipt_count = outcome.raw_receipt_count,
            raw_log_count = outcome.raw_log_count,
            raw_code_hash_count = outcome.raw_code_hash_count,
            "resumable hash-pinned backfill job completed"
        );
        return Ok(outcome);
    }

    bail!(
        "backfill job {} has no reservable ranges but is {}; another active lease may still own work",
        record.job.backfill_job_id,
        job.status.as_str()
    );
}

async fn run_reserved_hash_pinned_backfill_range(
    pool: &sqlx::PgPool,
    source_plan: &WatchedSourceSelectorPlan,
    provider: &JsonRpcProvider,
    config: &BackfillJobRunConfig,
    reserved_range: &BackfillRange,
    aggregate: &mut BackfillJobRunOutcome,
) -> Result<()> {
    let mut active_range = reserved_range.clone();
    let mut block_number = active_range.checkpoint_block_number;
    while block_number <= active_range.range_end_block_number {
        let chunk_end = block_number
            .checked_add(HASH_PINNED_BACKFILL_CHUNK_BLOCKS - 1)
            .unwrap_or(active_range.range_end_block_number)
            .min(active_range.range_end_block_number);
        let chunk_range = BackfillBlockRange::new(block_number, chunk_end)?;
        let chunk_outcome =
            match run_hash_pinned_backfill_range(pool, source_plan, provider, chunk_range).await {
                Ok(outcome) => outcome,
                Err(error) => {
                    return Err(record_reserved_range_failure(ReservedRangeFailure {
                        pool,
                        reserved_range: &active_range,
                        config,
                        failure_reason: "hash-pinned backfill failed",
                        block_number: Some(block_number),
                        attempted_range: Some(chunk_range),
                        phase: "hash_pinned_intake",
                        error,
                    })
                    .await);
                }
            };
        aggregate.add_range_outcome(&chunk_outcome);

        active_range = match advance_backfill_range(
            pool,
            active_range.backfill_range_id,
            &config.lease_token,
            chunk_end,
        )
        .await
        {
            Ok(range) => range,
            Err(error) => {
                return Err(record_reserved_range_failure(ReservedRangeFailure {
                    pool,
                    reserved_range: &active_range,
                    config,
                    failure_reason: "backfill checkpoint advance failed",
                    block_number: Some(block_number),
                    attempted_range: Some(chunk_range),
                    phase: "checkpoint_advance",
                    error,
                })
                .await);
            }
        };

        if chunk_end == active_range.range_end_block_number {
            break;
        }
        block_number = chunk_end
            .checked_add(1)
            .context("backfill block number overflowed while advancing range")?;
    }

    if let Err(error) =
        complete_backfill_range(pool, active_range.backfill_range_id, &config.lease_token).await
    {
        return Err(record_reserved_range_failure(ReservedRangeFailure {
            pool,
            reserved_range: &active_range,
            config,
            failure_reason: "backfill range completion failed",
            block_number: None,
            attempted_range: None,
            phase: "range_completion",
            error,
        })
        .await);
    }

    Ok(())
}
