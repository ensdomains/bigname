#[path = "reservation_execution/coinbase_sql.rs"]
mod coinbase_sql_execution;
#[path = "reservation_execution/creation.rs"]
mod creation;
#[path = "reservation_execution/digest.rs"]
mod digest;
#[path = "reservation_execution/generic_topic_identity.rs"]
mod generic_topic_identity;
#[path = "reservation_execution/identity.rs"]
mod identity;
#[path = "reservation_execution/lease_heartbeat.rs"]
mod lease_heartbeat;
#[path = "reservation_execution/progress.rs"]
mod progress;
#[path = "reservation_execution/scan_all.rs"]
mod scan_all;
#[path = "reservation_execution/startup_progress.rs"]
mod startup_progress;
#[cfg(test)]
#[path = "reservation_execution/tests.rs"]
mod tests;

use anyhow::{Context, Result, bail};
use bigname_adapters::StartupAdapterProgress;
use bigname_manifests::WatchedSourceSelectorPlan;
use bigname_storage::{
    BackfillJobCreate, BackfillJobRecord, BackfillLifecycleStatus, BackfillRange,
    BackfillRangeSpec, advance_backfill_range, create_backfill_job,
    create_generation_scoped_backfill_job, load_backfill_job, reserve_backfill_range,
};
use serde_json::{Value, json};
use tracing::info;

use crate::{
    provider::ChainProviderOps,
    source_scope::{
        watched_source_plan_uses_basenames_registry_scan_all,
        watched_source_plan_uses_generic_resolver_scope,
    },
};

use super::{
    BackfillBlockRange, BackfillJobRunConfig, BackfillJobRunOutcome, BackfillTopicPlan,
    CoinbaseSqlBackfillConfig,
    coverage_facts::complete_reserved_range_recording_plan_coverage,
    failure_recording::{ReservedRangeFailure, record_reserved_range_failure},
    fetching::{load_backfill_canonicality_evidence, run_hash_pinned_backfill_range},
    selection::{SelectedTargetIntervalIndex, SelectedTargetRangeCursor},
};
use digest::keccak256_json_digest;
use generic_topic_identity::{generic_topic_scan_source_identity_payload, selected_targets_sample};
use identity::requested_watched_targets_value_with_progress;
pub(crate) use identity::{
    backfill_job_source_identity_payload, backfill_job_source_identity_payload_with_progress,
};

pub(crate) use coinbase_sql_execution::{
    effective_coinbase_sql_adapter_sync_mode,
    ensure_coinbase_sql_registry_range_start_is_replay_safe,
    run_reserved_coinbase_sql_backfill_range, run_resumable_coinbase_sql_backfill_job,
};
pub(super) use lease_heartbeat::{
    backfill_lease_duration_secs, refreshed_backfill_lease_expires_at,
    run_with_backfill_lease_heartbeat, validate_hash_pinned_chunk_blocks,
};
pub(super) use progress::run_reserved_hash_pinned_backfill_range_with_progress;
pub(crate) use progress::run_resumable_hash_pinned_backfill_job_with_progress;
pub(crate) use scan_all::effective_hash_pinned_adapter_sync_mode;
use scan_all::{
    basenames_registry_scan_all_topics_source_identity_payload,
    coinbase_sql_basenames_registry_scan_all_source_identity_payload,
    coinbase_sql_uses_basenames_registry_scan_all,
};

const HASH_PINNED_BACKFILL_SCAN_MODE: &str = "hash_pinned_block";
pub(crate) const COINBASE_SQL_BACKFILL_SCAN_MODE: &str = "coinbase_sql_hash_pinned_logs_v1";
pub(crate) const DEFAULT_HASH_PINNED_BACKFILL_CHUNK_BLOCKS: i64 = 1_024;
pub(crate) const COMPACT_SOURCE_IDENTITY_SELECTED_TARGET_THRESHOLD: usize = 10_000;

#[cfg(test)]
pub(crate) use creation::coinbase_sql_backfill_job_source_identity_payload;
pub(crate) use creation::{
    create_coinbase_sql_backfill_job, create_coinbase_sql_backfill_job_with_ranges,
    create_hash_pinned_backfill_job, create_hash_pinned_backfill_job_with_progress,
    create_hash_pinned_backfill_job_with_ranges,
    create_hash_pinned_backfill_job_with_ranges_with_progress, hash_pinned_backfill_range_specs,
};
pub(crate) async fn run_resumable_hash_pinned_backfill_job(
    pool: &sqlx::PgPool,
    source_plan: &WatchedSourceSelectorPlan,
    provider: &(impl ChainProviderOps + ?Sized),
    mut config: BackfillJobRunConfig,
) -> Result<BackfillJobRunOutcome> {
    config.adapter_sync_mode =
        effective_hash_pinned_adapter_sync_mode(source_plan, config.adapter_sync_mode);
    validate_hash_pinned_chunk_blocks(config.hash_pinned_chunk_blocks)?;
    let record = create_hash_pinned_backfill_job(pool, source_plan, &config).await?;
    run_precreated_hash_pinned_backfill_job_inner(
        pool,
        source_plan,
        provider,
        config,
        record,
        &mut None,
    )
    .await
}

pub(crate) async fn run_precreated_hash_pinned_backfill_job(
    pool: &sqlx::PgPool,
    source_plan: &WatchedSourceSelectorPlan,
    provider: &(impl ChainProviderOps + ?Sized),
    mut config: BackfillJobRunConfig,
    record: BackfillJobRecord,
) -> Result<BackfillJobRunOutcome> {
    config.adapter_sync_mode =
        effective_hash_pinned_adapter_sync_mode(source_plan, config.adapter_sync_mode);
    validate_hash_pinned_chunk_blocks(config.hash_pinned_chunk_blocks)?;
    run_precreated_hash_pinned_backfill_job_inner(
        pool,
        source_plan,
        provider,
        config,
        record,
        &mut None,
    )
    .await
}

async fn run_precreated_hash_pinned_backfill_job_inner(
    pool: &sqlx::PgPool,
    source_plan: &WatchedSourceSelectorPlan,
    provider: &(impl ChainProviderOps + ?Sized),
    mut config: BackfillJobRunConfig,
    record: BackfillJobRecord,
    progress: &mut Option<&mut dyn StartupAdapterProgress>,
) -> Result<BackfillJobRunOutcome> {
    let watched_chain = &source_plan.watched_chain_plan;
    config
        .idempotency_key
        .clone_from(&record.job.idempotency_key);
    let mut outcome = BackfillJobRunOutcome::new(record.job.backfill_job_id, source_plan, &config);
    let lease_duration_secs = backfill_lease_duration_secs(config.lease_expires_at)?;

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
        hash_pinned_chunk_blocks = config.hash_pinned_chunk_blocks,
        adapter_sync_mode = config.adapter_sync_mode.as_str(),
        header_audit_mode = config.header_audit_mode.as_str(),
        range_count = record.ranges.len(),
        "resumable backfill job loaded"
    );

    loop {
        let Some(reserved_range) = reserve_backfill_range(
            pool,
            record.job.backfill_job_id,
            &config.lease_owner,
            &config.lease_token,
            refreshed_backfill_lease_expires_at(lease_duration_secs)?,
        )
        .await?
        else {
            break;
        };

        outcome.reserved_range_count += 1;
        run_reserved_hash_pinned_backfill_range_inner(
            pool,
            source_plan,
            provider,
            &config,
            &reserved_range,
            &mut outcome,
            None,
            progress,
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
            hash_pinned_chunk_blocks = config.hash_pinned_chunk_blocks,
            adapter_sync_mode = config.adapter_sync_mode.as_str(),
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

#[allow(dead_code)]
pub(super) async fn run_reserved_hash_pinned_backfill_range(
    pool: &sqlx::PgPool,
    source_plan: &WatchedSourceSelectorPlan,
    provider: &(impl ChainProviderOps + ?Sized),
    config: &BackfillJobRunConfig,
    reserved_range: &BackfillRange,
    aggregate: &mut BackfillJobRunOutcome,
) -> Result<()> {
    run_reserved_hash_pinned_backfill_range_inner(
        pool,
        source_plan,
        provider,
        config,
        reserved_range,
        aggregate,
        None,
        &mut None,
    )
    .await
}

#[expect(clippy::too_many_arguments)]
async fn run_reserved_hash_pinned_backfill_range_inner(
    pool: &sqlx::PgPool,
    source_plan: &WatchedSourceSelectorPlan,
    provider: &(impl ChainProviderOps + ?Sized),
    config: &BackfillJobRunConfig,
    reserved_range: &BackfillRange,
    aggregate: &mut BackfillJobRunOutcome,
    progress_sender: Option<&tokio::sync::mpsc::UnboundedSender<()>>,
    service_progress: &mut Option<&mut dyn StartupAdapterProgress>,
) -> Result<()> {
    let mut active_range = reserved_range.clone();
    let mut block_number = active_range
        .checkpoint_block_number
        .checked_add(1)
        .context("backfill checkpoint overflowed while computing resume block")?;
    let selected_target_index = SelectedTargetIntervalIndex::from_source_plan(source_plan);
    let mut selected_target_range_cursor = SelectedTargetRangeCursor::from_source_plan(source_plan);
    let canonicality_evidence = match run_with_backfill_lease_heartbeat(
        pool,
        &active_range,
        config,
        load_backfill_canonicality_evidence(pool, &source_plan.watched_chain_plan.chain, provider),
    )
    .await
    {
        Ok(evidence) => evidence,
        Err(error) => {
            return Err(record_reserved_range_failure(ReservedRangeFailure {
                pool,
                reserved_range: &active_range,
                config,
                failure_reason: "backfill canonicality evidence load failed",
                block_number: Some(block_number),
                attempted_range: None,
                phase: "canonicality_evidence",
                error,
            })
            .await);
        }
    };
    while block_number <= active_range.range_end_block_number {
        let chunk_end = block_number
            .checked_add(config.hash_pinned_chunk_blocks - 1)
            .unwrap_or(active_range.range_end_block_number)
            .min(active_range.range_end_block_number);
        let chunk_range = BackfillBlockRange::new(block_number, chunk_end)?;
        let progress_ranges = startup_progress::heartbeat_progress_ranges(
            chunk_range,
            progress_sender.is_some() || service_progress.is_some(),
        )?;
        for progress_range in progress_ranges {
            let selected_target_addresses = scan_all::chunk_addresses_for_plan(
                source_plan,
                &mut selected_target_range_cursor,
                progress_range,
            );
            let outcome = run_with_backfill_lease_heartbeat(
                pool,
                &active_range,
                config,
                run_hash_pinned_backfill_range(
                    pool,
                    source_plan,
                    &selected_target_index,
                    &selected_target_addresses,
                    provider,
                    progress_range,
                    canonicality_evidence.clone(),
                    config.adapter_sync_mode,
                    config.header_audit_mode,
                ),
            )
            .await
            .map_err(|error| ReservedRangeFailure {
                pool,
                reserved_range: &active_range,
                config,
                failure_reason: "hash-pinned backfill failed",
                block_number: Some(progress_range.from_block),
                attempted_range: Some(progress_range),
                phase: "hash_pinned_intake",
                error,
            });
            let outcome = match outcome {
                Ok(outcome) => outcome,
                Err(failure) => return Err(record_reserved_range_failure(failure).await),
            };
            aggregate.add_range_outcome(&outcome);
            if let Some(progress_sender) = progress_sender {
                let _ = progress_sender.send(());
            }
            if let Some(progress) = service_progress.as_deref_mut() {
                progress.record(pool).await?;
            }
        }

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

    complete_reserved_range_recording_plan_coverage(
        pool,
        &active_range,
        config,
        source_plan,
        watched_source_plan_uses_basenames_registry_scan_all(source_plan),
        "backfill range completion failed",
        progress_sender,
        service_progress,
    )
    .await
}
