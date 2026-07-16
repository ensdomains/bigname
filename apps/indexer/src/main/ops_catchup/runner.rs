use std::time::Duration;

use anyhow::{Context, Result, bail};
use bigname_manifests::{
    WatchedSourceSelector, WatchedTargetIdentity, load_ens_v2_retained_history_recovery_targets,
    load_historical_watched_source_selector_plan, load_watched_contracts,
    load_watched_source_selector_plan,
};
use bigname_storage::{BackfillLifecycleStatus, fail_backfill_job};
use tokio::time::sleep;
use tracing::{error, info, warn};

use crate::{
    backfill::{
        BackfillBlockRange, BackfillJobRunConfig, create_hash_pinned_backfill_job,
        run_precreated_hash_pinned_backfill_job,
    },
    backfill_lease_expires_at,
    bootstrap_backfill::{
        automatic_backfill_retention_snapshot_is_stable,
        converge_ens_v2_retained_history_through_block, load_bootstrap_retention_snapshot,
    },
    default_backfill_lease_owner, generated_backfill_lease_token,
    provider::{ChainProvider, ChainProviderOps, ProviderRegistry},
    runtime::{IntakeChainTask, validate_provider_registry_for_intake_tasks},
};

use super::{
    capacity::{CAPACITY_FAILURE_REASON, capacity_metadata, check_capacity},
    config::{OpsCatchupConfig, OpsCatchupOutcome},
    planning::{
        CatchupChunk, catchup_targets_for_chain, merge_retained_history_recovery_targets,
        plan_catchup_chunks,
    },
};

const MAX_OPS_CATCHUP_RETENTION_AUTHORITY_RETRIES: usize = 4;
const MAX_OPS_CATCHUP_DISCOVERY_EXPANSION_PASSES: usize = 1_024;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum OpsCatchupRetryReason {
    DiscoveryExpanded,
    RetentionAuthorityChanged,
}

impl OpsCatchupRetryReason {
    const fn as_str(self) -> &'static str {
        match self {
            Self::DiscoveryExpanded => "discovery_expanded",
            Self::RetentionAuthorityChanged => "retention_authority_changed",
        }
    }
}

#[derive(Default)]
struct OpsCatchupConvergenceTracker {
    consecutive_retention_authority_retries: usize,
    discovery_expansion_passes: usize,
}

impl OpsCatchupConvergenceTracker {
    fn record_retry(&mut self, chain: &str, reason: OpsCatchupRetryReason) -> Result<()> {
        match reason {
            OpsCatchupRetryReason::DiscoveryExpanded => {
                self.consecutive_retention_authority_retries = 0;
                self.discovery_expansion_passes += 1;
                if self.discovery_expansion_passes > MAX_OPS_CATCHUP_DISCOVERY_EXPANSION_PASSES {
                    bail!(
                        "ENSv2 discovery on chain {chain} did not reach a fixed point within {MAX_OPS_CATCHUP_DISCOVERY_EXPANSION_PASSES} ops catch-up passes"
                    );
                }
            }
            OpsCatchupRetryReason::RetentionAuthorityChanged => {
                self.consecutive_retention_authority_retries += 1;
                if self.consecutive_retention_authority_retries
                    >= MAX_OPS_CATCHUP_RETENTION_AUTHORITY_RETRIES
                {
                    bail!(
                        "raw-log retention authority for chain {chain} changed during {} consecutive ops catch-up planning passes",
                        self.consecutive_retention_authority_retries
                    );
                }
            }
        }
        Ok(())
    }
}

pub(crate) async fn run_ops_finalized_catchup(
    pool: &sqlx::PgPool,
    intake_chain_tasks: &[IntakeChainTask],
    provider_registry: &ProviderRegistry,
    config: OpsCatchupConfig,
) -> Result<OpsCatchupOutcome> {
    validate_provider_registry_for_intake_tasks(intake_chain_tasks, provider_registry)?;
    let mut outcome = OpsCatchupOutcome {
        active_chain_count: intake_chain_tasks.len(),
        ..OpsCatchupOutcome::default()
    };
    let mut iteration = 0_u64;

    loop {
        iteration += 1;
        outcome.follow_iteration_count += 1;
        run_ops_finalized_catchup_iteration(
            pool,
            intake_chain_tasks,
            provider_registry,
            &config,
            &mut outcome,
        )
        .await?;

        if !config.follow {
            break;
        }
        if config
            .follow_iterations
            .is_some_and(|limit| iteration >= limit)
        {
            break;
        }
        sleep(Duration::from_secs(config.follow_poll_interval_secs)).await;
    }

    Ok(outcome)
}

async fn run_ops_finalized_catchup_iteration(
    pool: &sqlx::PgPool,
    intake_chain_tasks: &[IntakeChainTask],
    provider_registry: &ProviderRegistry,
    config: &OpsCatchupConfig,
    outcome: &mut OpsCatchupOutcome,
) -> Result<()> {
    let watched_contracts = load_watched_contracts(pool).await?;

    for task in intake_chain_tasks {
        let (_, skipped_unknown_start_targets) =
            catchup_targets_for_chain(&watched_contracts, &task.chain);
        let skipped_unknown_start_target_count = skipped_unknown_start_targets.len();
        outcome.skipped_unknown_start_target_count += skipped_unknown_start_target_count;
        if skipped_unknown_start_target_count > 0 {
            info!(
                service = "indexer",
                command = "ops-catchup",
                catchup_status = "skipped_unknown_start_target",
                chain = %task.chain,
                skipped_unknown_start_target_count,
                skip_reason = "unknown_start",
                "watched targets skipped because they have no admitted start block"
            );
        }

        let Some(provider) = provider_registry.provider_for(&task.chain) else {
            outcome.missing_provider_chain_count += 1;
            warn!(
                service = "indexer",
                command = "ops-catchup",
                catchup_status = "idle_missing_provider",
                chain = %task.chain,
                intake_address_count = task.addresses.len(),
                "no provider source is configured for an active catch-up chain"
            );
            continue;
        };
        outcome.provider_configured_chain_count += 1;

        let heads = provider
            .fetch_chain_heads()
            .await
            .with_context(|| format!("failed to fetch finalized head for {}", task.chain))?;
        let Some(finalized_head) = heads.finalized else {
            outcome.no_finalized_head_chain_count += 1;
            warn!(
                service = "indexer",
                command = "ops-catchup",
                catchup_status = "idle_no_finalized_head",
                chain = %task.chain,
                latest_block_number = heads.canonical.block_number,
                "provider did not return a finalized head for catch-up"
            );
            continue;
        };

        let mut convergence_tracker = OpsCatchupConvergenceTracker::default();
        loop {
            let retention_snapshot =
                load_bootstrap_retention_snapshot(pool, &task.chain, finalized_head.block_number)
                    .await?;
            // Load the watched set after capturing the authority snapshot on
            // every pass. If adapter sync admits a target while a pass is
            // running, the epoch check below forces a retry whose plan now
            // includes that target through the same finalized head.
            let watched_contracts = load_watched_contracts(pool).await?;
            let (targets, _) = catchup_targets_for_chain(&watched_contracts, &task.chain);
            let mut planned_targets = targets.clone();
            if retention_snapshot.requires_ens_v2_history_recovery {
                let recovery_targets = load_ens_v2_retained_history_recovery_targets(
                    pool,
                    &task.chain,
                    finalized_head.block_number,
                )
                .await?;
                merge_retained_history_recovery_targets(&mut planned_targets, &recovery_targets);
            }
            let (chunks, skipped_future_target_count) = plan_catchup_chunks(
                &planned_targets,
                finalized_head.block_number,
                config.chunk_blocks,
            )?;
            let planned_chunk_count = chunks.len();
            for chunk in chunks {
                run_ops_finalized_catchup_chunk(
                    pool,
                    &task.chain,
                    provider,
                    config,
                    &chunk,
                    finalized_head.block_number,
                    &finalized_head.block_hash,
                    retention_snapshot.requires_ens_v2_history_recovery,
                    outcome,
                )
                .await?;
            }
            let newly_required_coverage = if retention_snapshot.requires_ens_v2_history_recovery {
                converge_ens_v2_retained_history_through_block(
                    pool,
                    &task.chain,
                    finalized_head.block_number,
                    retention_snapshot.has_ens_v2_history_requirements,
                )
                .await?
            } else {
                false
            };
            if !newly_required_coverage
                && automatic_backfill_retention_snapshot_is_stable(
                    pool,
                    &task.chain,
                    finalized_head.block_number,
                    retention_snapshot,
                )
                .await?
            {
                outcome.skipped_future_target_count += skipped_future_target_count;
                outcome.planned_chunk_count += planned_chunk_count;
                break;
            }
            let retry_reason = if newly_required_coverage {
                OpsCatchupRetryReason::DiscoveryExpanded
            } else {
                OpsCatchupRetryReason::RetentionAuthorityChanged
            };
            convergence_tracker.record_retry(&task.chain, retry_reason)?;
            warn!(
                service = "indexer",
                command = "ops-catchup",
                catchup_status = retry_reason.as_str(),
                chain = %task.chain,
                planned_raw_log_retention_generation = retention_snapshot.generation,
                planned_discovery_admission_epoch = retention_snapshot.discovery_admission_epoch,
                consecutive_retention_authority_retries =
                    convergence_tracker.consecutive_retention_authority_retries,
                discovery_expansion_passes = convergence_tracker.discovery_expansion_passes,
                "raw-log retention or ENSv2 discovery authority changed during ops catch-up; retrying the complete chain planning pass"
            );
        }
    }

    let profile_convergence =
        crate::resolver_profile_convergence::drain_resolver_profile_input_changes(pool).await?;
    for task in intake_chain_tasks {
        profile_convergence
            .ensure_chain_completion_allowed(&task.chain, "ops catch-up completion")?;
    }
    Ok(())
}

async fn run_ops_finalized_catchup_chunk(
    pool: &sqlx::PgPool,
    chain: &str,
    provider: &ChainProvider,
    config: &OpsCatchupConfig,
    chunk: &CatchupChunk,
    finalized_head_block_number: i64,
    finalized_head_block_hash: &str,
    include_historical_recovery_targets: bool,
    outcome: &mut OpsCatchupOutcome,
) -> Result<()> {
    let selector = WatchedSourceSelector::WatchedTargetSet(
        chunk
            .target_contract_instance_ids()
            .into_iter()
            .map(|contract_instance_id| WatchedTargetIdentity {
                contract_instance_id,
            })
            .collect(),
    );
    let mut source_plan = if include_historical_recovery_targets {
        load_historical_watched_source_selector_plan(
            pool,
            chain,
            selector,
            chunk.range.from_block,
            chunk.range.to_block,
        )
        .await?
    } else {
        load_watched_source_selector_plan(
            pool,
            chain,
            selector,
            chunk.range.from_block,
            chunk.range.to_block,
        )
        .await?
    };
    if include_historical_recovery_targets {
        chunk.narrow_historical_source_plan(&mut source_plan)?;
    }
    let idempotency_key = ops_catchup_idempotency_key(
        &config.deployment_profile,
        chain,
        &source_plan.source_identity_hash(),
        chunk.range,
    );
    let run_config = BackfillJobRunConfig {
        deployment_profile: config.deployment_profile.clone(),
        idempotency_key,
        scope_idempotency_to_raw_log_retention_generation: true,
        range: chunk.range,
        lease_owner: format!("{}:ops-finalized-catchup", default_backfill_lease_owner()),
        lease_token: generated_backfill_lease_token()?,
        lease_expires_at: backfill_lease_expires_at(config.lease_duration_secs)?,
        hash_pinned_chunk_blocks: crate::backfill::DEFAULT_HASH_PINNED_BACKFILL_CHUNK_BLOCKS,
        adapter_sync_mode: crate::backfill::BackfillAdapterSyncMode::Inline,
        header_audit_mode: config.header_audit_mode,
    };
    let record = create_hash_pinned_backfill_job(pool, &source_plan, &run_config).await?;
    if record.job.status == BackfillLifecycleStatus::Completed {
        outcome.reused_completed_chunk_count += 1;
        return Ok(());
    }

    let capacity_snapshot = match check_capacity(pool, &config.capacity, chunk.range).await {
        Ok(snapshot) => snapshot,
        Err(error) => {
            outcome.capacity_check_count += 1;
            let metadata = capacity_metadata(
                "check_failed",
                &run_config,
                chunk.range,
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
            chunk.range,
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
            from_block = chunk.range.from_block,
            to_block = chunk.range.to_block,
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
            chunk.range.from_block,
            chunk.range.to_block
        );
    }

    let job_outcome =
        run_precreated_hash_pinned_backfill_job(pool, &source_plan, provider, run_config, record)
            .await?;
    outcome.add_job(&job_outcome);
    Ok(())
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn discovery_progress_does_not_consume_retention_authority_retries() -> Result<()> {
        let mut tracker = OpsCatchupConvergenceTracker::default();

        for _ in 0..(MAX_OPS_CATCHUP_RETENTION_AUTHORITY_RETRIES + 1) {
            tracker.record_retry("ethereum-sepolia", OpsCatchupRetryReason::DiscoveryExpanded)?;
        }

        assert_eq!(tracker.consecutive_retention_authority_retries, 0);
        assert_eq!(
            tracker.discovery_expansion_passes,
            MAX_OPS_CATCHUP_RETENTION_AUTHORITY_RETRIES + 1
        );
        Ok(())
    }

    #[test]
    fn discovery_progress_resets_consecutive_retention_authority_retries() -> Result<()> {
        let mut tracker = OpsCatchupConvergenceTracker::default();
        tracker.record_retry(
            "ethereum-sepolia",
            OpsCatchupRetryReason::RetentionAuthorityChanged,
        )?;
        tracker.record_retry("ethereum-sepolia", OpsCatchupRetryReason::DiscoveryExpanded)?;

        assert_eq!(tracker.consecutive_retention_authority_retries, 0);
        Ok(())
    }

    #[test]
    fn consecutive_retention_authority_changes_keep_the_small_retry_cap() -> Result<()> {
        let mut tracker = OpsCatchupConvergenceTracker::default();
        for _ in 1..MAX_OPS_CATCHUP_RETENTION_AUTHORITY_RETRIES {
            tracker.record_retry(
                "ethereum-sepolia",
                OpsCatchupRetryReason::RetentionAuthorityChanged,
            )?;
        }

        let error = tracker
            .record_retry(
                "ethereum-sepolia",
                OpsCatchupRetryReason::RetentionAuthorityChanged,
            )
            .expect_err("the fourth consecutive retention change must fail");
        assert!(
            error.to_string().contains("retention authority")
                && error.to_string().contains("4 consecutive")
        );
        Ok(())
    }
}
