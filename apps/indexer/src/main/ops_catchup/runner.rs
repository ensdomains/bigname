use std::time::Duration;

use anyhow::{Context, Result, bail};
use bigname_manifests::{
    load_ens_v2_retained_history_recovery_targets, load_watched_contracts_by_chain,
};
use tokio::time::sleep;
use tracing::{info, warn};

use crate::{
    bootstrap_backfill::{
        automatic_backfill_retention_snapshot_is_stable,
        converge_ens_v2_retained_history_through_block, load_bootstrap_retention_snapshot,
    },
    provider::{ChainProviderOps, ProviderRegistry},
    runtime::{IntakeChainTask, validate_provider_registry_for_intake_tasks},
};

use super::{
    config::{OpsCatchupConfig, OpsCatchupOutcome, OpsCatchupPlanSnapshotOutcome},
    planning::{
        CompletedCatchupPass, catchup_targets_for_chain, merge_retained_history_recovery_targets,
        plan_catchup_chunks_reusing_completed, retry_required_ranges,
    },
};

#[path = "runner/jobs.rs"]
mod jobs;
#[cfg(test)]
pub(crate) use jobs::install_after_ens_v2_proof_publication_failure;
pub(crate) use jobs::ops_catchup_idempotency_key;
use jobs::{
    OpsCatchupAdapterPhase, has_pending_ens_v2_finalization_jobs,
    maybe_fail_after_ens_v2_proof_publication, precreate_ens_v2_finalization_jobs,
    resume_pending_ens_v2_finalization_jobs, run_ops_finalized_catchup_chunk,
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
        Box::pin(run_ops_finalized_catchup_iteration(
            pool,
            intake_chain_tasks,
            provider_registry,
            &config,
            &mut outcome,
        ))
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
    for task in intake_chain_tasks {
        let watched_contracts = load_watched_contracts_by_chain(pool, &task.chain).await?;
        let (_, initially_skipped_unknown_start_targets) =
            catchup_targets_for_chain(&watched_contracts, &task.chain);
        let initial_plan_snapshot = OpsCatchupPlanSnapshotOutcome {
            skipped_unknown_start_target_count: initially_skipped_unknown_start_targets.len(),
            ..OpsCatchupPlanSnapshotOutcome::default()
        };
        let Some(provider) = provider_registry.provider_for(&task.chain) else {
            outcome.missing_provider_chain_count += 1;
            outcome.add_plan_snapshot(initial_plan_snapshot);
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
            outcome.add_plan_snapshot(initial_plan_snapshot);
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
        let mut completed_pass = None::<CompletedCatchupPass>;
        loop {
            let retention_snapshot =
                load_bootstrap_retention_snapshot(pool, &task.chain, finalized_head.block_number)
                    .await?;
            if !retention_snapshot.requires_ens_v2_history_recovery
                && has_pending_ens_v2_finalization_jobs(
                    pool,
                    &config.deployment_profile,
                    &task.chain,
                    retention_snapshot.generation,
                )
                .await?
            {
                // A proof can commit before the full-source registry call
                // returns. Re-run that idempotent reconciliation before
                // consuming durable finalization work so a process death
                // anywhere after proof publication cannot skip absence-aware
                // discovery cleanup.
                if converge_ens_v2_retained_history_through_block(
                    pool,
                    &task.chain,
                    finalized_head.block_number,
                    retention_snapshot.has_ens_v2_history_requirements,
                )
                .await?
                {
                    convergence_tracker
                        .record_retry(&task.chain, OpsCatchupRetryReason::DiscoveryExpanded)?;
                    continue;
                }
                resume_pending_ens_v2_finalization_jobs(
                    pool,
                    &task.chain,
                    provider,
                    config,
                    retention_snapshot.generation,
                    finalized_head.block_number,
                    &finalized_head.block_hash,
                    outcome,
                )
                .await?;
            }
            // Load the watched set after capturing the authority snapshot on
            // every pass. If adapter sync admits a target while a pass is
            // running, the epoch check below forces a retry whose plan now
            // includes that target through the same finalized head.
            let watched_contracts = load_watched_contracts_by_chain(pool, &task.chain).await?;
            let (targets, skipped_unknown_start_targets) =
                catchup_targets_for_chain(&watched_contracts, &task.chain);
            let skipped_unknown_start_target_count = skipped_unknown_start_targets.len();
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
            let required_ranges = retry_required_ranges(
                completed_pass.as_ref(),
                retention_snapshot.generation,
                retention_snapshot.requires_ens_v2_history_recovery,
                &planned_targets,
                finalized_head.block_number,
            )?;
            let chunk_plan = plan_catchup_chunks_reusing_completed(
                &planned_targets,
                finalized_head.block_number,
                config.chunk_blocks,
                required_ranges.as_deref(),
            )?;
            let mut plan_snapshot = OpsCatchupPlanSnapshotOutcome {
                skipped_unknown_start_target_count,
                skipped_future_target_count: chunk_plan.skipped_future_target_count,
                planned_chunk_count: chunk_plan.planned_chunk_count,
                reused_completed_chunk_count: chunk_plan.reused_completed_chunk_count,
            };
            let adapter_phase = if retention_snapshot.requires_ens_v2_history_recovery {
                OpsCatchupAdapterPhase::EnsV2HistoryCollection
            } else {
                OpsCatchupAdapterPhase::Ordinary
            };
            for chunk in chunk_plan.chunks_to_run {
                if run_ops_finalized_catchup_chunk(
                    pool,
                    &task.chain,
                    provider,
                    config,
                    &chunk,
                    finalized_head.block_number,
                    &finalized_head.block_hash,
                    adapter_phase,
                    outcome,
                )
                .await?
                {
                    plan_snapshot.reused_completed_chunk_count += 1;
                }
            }
            if retention_snapshot.requires_ens_v2_history_recovery
                && retention_snapshot.has_ens_v2_history_requirements
            {
                // Persist the complete finalization job set before the
                // full-source registry pass can make the retained-history
                // proof current. A later process can therefore resume the
                // exact deferred scopes after any crash or error.
                let finalization_plan = plan_catchup_chunks_reusing_completed(
                    &planned_targets,
                    finalized_head.block_number,
                    config.chunk_blocks,
                    None,
                )?;
                precreate_ens_v2_finalization_jobs(
                    pool,
                    &task.chain,
                    config,
                    &finalization_plan.chunks_to_run,
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
            if retention_snapshot.requires_ens_v2_history_recovery
                && retention_snapshot.has_ens_v2_history_requirements
                && !newly_required_coverage
            {
                maybe_fail_after_ens_v2_proof_publication(pool, &task.chain).await?;
            }
            let mut retention_snapshot_is_stable = !newly_required_coverage
                && automatic_backfill_retention_snapshot_is_stable(
                    pool,
                    &task.chain,
                    finalized_head.block_number,
                    retention_snapshot,
                )
                .await?;
            if retention_snapshot_is_stable
                && retention_snapshot.requires_ens_v2_history_recovery
                && retention_snapshot.has_ens_v2_history_requirements
            {
                resume_pending_ens_v2_finalization_jobs(
                    pool,
                    &task.chain,
                    provider,
                    config,
                    retention_snapshot.generation,
                    finalized_head.block_number,
                    &finalized_head.block_hash,
                    outcome,
                )
                .await?;
                retention_snapshot_is_stable = automatic_backfill_retention_snapshot_is_stable(
                    pool,
                    &task.chain,
                    finalized_head.block_number,
                    retention_snapshot,
                )
                .await?;
            }
            if retention_snapshot_is_stable {
                outcome.add_plan_snapshot(plan_snapshot);
                break;
            }
            let retry_reason = if newly_required_coverage {
                OpsCatchupRetryReason::DiscoveryExpanded
            } else {
                OpsCatchupRetryReason::RetentionAuthorityChanged
            };
            completed_pass = Some(CompletedCatchupPass::new(
                retention_snapshot.generation,
                retention_snapshot.requires_ens_v2_history_recovery,
                planned_targets,
            ));
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
