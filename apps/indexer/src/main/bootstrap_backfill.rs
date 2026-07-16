use std::{collections::BTreeMap, path::Path};

use anyhow::{Context, Result};
use bigname_manifests::{
    ManifestBootstrapSkippedTarget, load_ens_v2_authoritative_discovery_bootstrap_targets,
    load_ens_v2_retained_history_recovery_targets, load_manifest_declared_bootstrap_targets,
    load_manifest_skipped_bootstrap_targets,
};
use tracing::{info, warn};

use crate::{
    backfill::{
        BackfillAdapterSyncMode, BackfillBlockRange, backfill_job_source_identity_payload,
        hash_pinned_backfill_range_specs, run_resumable_hash_pinned_backfill_job_concurrently,
    },
    backfill_lease_expires_at, default_backfill_lease_owner, deployment_profile_from_manifest_root,
    generated_backfill_lease_token,
    provider::{ChainProviderOps, ProviderBlock, ProviderRegistry},
    reconciliation::{
        HeaderAuditMode, RawFactNormalizedEventReplayRequest,
        RawFactNormalizedEventReplaySelection, log_raw_fact_normalized_event_replay_outcome,
        replay_raw_fact_normalized_events,
    },
    runtime::{IntakeChainTask, validate_provider_registry_for_intake_tasks},
};

#[path = "bootstrap_backfill/checkpoints.rs"]
mod checkpoints;
#[path = "bootstrap_backfill/identity.rs"]
mod identity;
#[path = "bootstrap_backfill/planning.rs"]
mod planning;
#[path = "bootstrap_backfill/recovery.rs"]
mod recovery;

use checkpoints::{
    bootstrap_segment_target_ids, load_bootstrap_segment_checkpoint,
    load_bootstrap_target_checkpoint,
};
pub(crate) use identity::bootstrap_backfill_idempotency_key;
use identity::{
    partitioned_bootstrap_backfill_idempotency_key, replay_source_scope_from_source_plan,
    source_identity_hash_for_backfill,
};
use planning::{
    BootstrapBackfillTargetRange, bootstrap_target_range,
    effective_bootstrap_backfill_worker_count, narrow_manifest_bootstrap_source_plan,
    plan_bootstrap_backfill_segments,
};
pub(crate) use planning::{
    bootstrap_finalized_head_block, resolve_bootstrap_backfill_worker_count,
};
#[cfg(test)]
pub(crate) use recovery::install_forced_retention_rotation;
use recovery::load_bootstrap_source_plan;
use recovery::{
    BootstrapConvergenceTracker, BootstrapPassStatus, finish_bootstrap_convergence_pass,
};
pub(crate) use recovery::{
    automatic_backfill_retention_snapshot_is_stable,
    converge_ens_v2_retained_history_through_block, load_bootstrap_retention_snapshot,
};
const BOOTSTRAP_BACKFILL_LEASE_DURATION_SECS: u64 = 300;
pub(crate) const DEFAULT_BOOTSTRAP_BACKFILL_WORKERS: usize = 0;
pub(crate) const DEFAULT_BOOTSTRAP_BACKFILL_RANGE_BLOCKS: i64 = 50_000;
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct BootstrapBackfillOutcome {
    pub(crate) latched_finalized_heads: BTreeMap<String, ProviderBlock>,
    pub(crate) active_chain_count: usize,
    pub(crate) provider_configured_chain_count: usize,
    pub(crate) missing_provider_chain_count: usize,
    pub(crate) eligible_target_count: usize,
    pub(crate) skipped_unknown_start_target_count: usize,
    pub(crate) skipped_unknown_start_targets: Vec<ManifestBootstrapSkippedTarget>,
    pub(crate) drained_job_count: usize,
    pub(crate) skipped_future_target_count: usize,
    pub(crate) reserved_range_count: usize,
    pub(crate) completed_range_count: usize,
    pub(crate) resolved_block_count: usize,
    pub(crate) raw_block_count: usize,
    pub(crate) raw_transaction_count: usize,
    pub(crate) raw_receipt_count: usize,
    pub(crate) raw_log_count: usize,
    pub(crate) raw_code_hash_count: usize,
    pub(crate) normalized_replay_job_count: usize,
    pub(crate) normalized_replay_synced_count: usize,
    pub(crate) normalized_replay_inserted_count: usize,
    pub(crate) requested_worker_count: usize,
    pub(crate) effective_worker_count: usize,
    pub(crate) range_partition_block_count: i64,
}

impl BootstrapBackfillOutcome {
    fn add_job(&mut self, outcome: &crate::backfill::BackfillJobRunOutcome) {
        self.drained_job_count += 1;
        self.reserved_range_count += outcome.reserved_range_count;
        self.completed_range_count += outcome.completed_range_count;
        self.resolved_block_count += outcome.resolved_block_count;
        self.raw_block_count += outcome.raw_block_count;
        self.raw_transaction_count += outcome.raw_transaction_count;
        self.raw_receipt_count += outcome.raw_receipt_count;
        self.raw_log_count += outcome.raw_log_count;
        self.raw_code_hash_count += outcome.raw_code_hash_count;
    }
}

// Startup orchestration keeps provider, replay, audit, and worker settings explicit.
#[expect(clippy::too_many_arguments)]
pub(crate) async fn run_startup_bootstrap_backfills(
    pool: &sqlx::PgPool,
    manifests_root: &Path,
    intake_chain_tasks: &[IntakeChainTask],
    provider_registry: &ProviderRegistry,
    hash_pinned_chunk_blocks: i64,
    adapter_sync_mode: BackfillAdapterSyncMode,
    replay_completed_raw_ranges: bool,
    header_audit_mode: HeaderAuditMode,
    bootstrap_backfill_workers: usize,
    bootstrap_backfill_range_blocks: i64,
) -> Result<BootstrapBackfillOutcome> {
    validate_provider_registry_for_intake_tasks(intake_chain_tasks, provider_registry)?;
    let backfill_adapter_sync_mode = adapter_sync_mode.startup_hash_pinned_backfill_mode();
    let deployment_profile = deployment_profile_from_manifest_root(manifests_root);
    let lease_owner = format!("{}:bootstrap-backfill", default_backfill_lease_owner());
    let requested_worker_count =
        resolve_bootstrap_backfill_worker_count(bootstrap_backfill_workers);
    let effective_worker_count = effective_bootstrap_backfill_worker_count(
        requested_worker_count,
        backfill_adapter_sync_mode,
    );
    let mut outcome = BootstrapBackfillOutcome {
        active_chain_count: intake_chain_tasks.len(),
        requested_worker_count,
        effective_worker_count,
        range_partition_block_count: bootstrap_backfill_range_blocks,
        ..BootstrapBackfillOutcome::default()
    };

    for task in intake_chain_tasks {
        let skipped_unknown_start_targets =
            load_manifest_skipped_bootstrap_targets(pool, &task.chain).await?;
        outcome.skipped_unknown_start_target_count += skipped_unknown_start_targets.len();
        for skipped_target in &skipped_unknown_start_targets {
            info!(
                service = "indexer",
                command = "run",
                bootstrap_backfill_status = "skipped_unknown_start_target",
                chain = %task.chain,
                source_family = %skipped_target.source_family,
                contract_instance_id = %skipped_target.contract_instance_id,
                address = %skipped_target.address,
                skip_reason = %skipped_target.skip_reason,
                "manifest-declared bootstrap target is skipped because it has no declared start block"
            );
        }
        outcome
            .skipped_unknown_start_targets
            .extend(skipped_unknown_start_targets);

        let Some(provider) = provider_registry.provider_for(&task.chain) else {
            outcome.missing_provider_chain_count += 1;
            warn!(
                service = "indexer",
                command = "run",
                bootstrap_backfill_status = "idle_missing_provider",
                chain = %task.chain,
                intake_address_count = task.addresses.len(),
                "no provider source is configured for an active bootstrap chain; automatic bootstrap backfill will stay idle for this chain"
            );
            continue;
        };
        outcome.provider_configured_chain_count += 1;

        let heads = provider.fetch_chain_heads().await.with_context(|| {
            format!(
                "failed to fetch provider heads for bootstrap backfill on chain {}",
                task.chain
            )
        })?;
        let provider_finalized_head_block = bootstrap_finalized_head_block(&task.chain, &heads)?;
        outcome.latched_finalized_heads.insert(
            task.chain.clone(),
            heads
                .finalized
                .clone()
                .expect("validated bootstrap heads must include finalized"),
        );
        let mut convergence_tracker = BootstrapConvergenceTracker::default();
        loop {
            let retention_snapshot =
                load_bootstrap_retention_snapshot(pool, &task.chain, provider_finalized_head_block)
                    .await?;
            let mut bootstrap_targets =
                load_manifest_declared_bootstrap_targets(pool, &task.chain).await?;
            let discovery_targets = load_ens_v2_authoritative_discovery_bootstrap_targets(
                pool,
                &task.chain,
                provider_finalized_head_block,
            )
            .await?;
            let include_historical_bootstrap_targets = !discovery_targets.is_empty()
                || retention_snapshot.requires_ens_v2_history_recovery;
            bootstrap_targets.extend(discovery_targets);
            if retention_snapshot.requires_ens_v2_history_recovery {
                bootstrap_targets.extend(
                    load_ens_v2_retained_history_recovery_targets(
                        pool,
                        &task.chain,
                        provider_finalized_head_block,
                    )
                    .await?,
                );
                bootstrap_targets.sort();
                bootstrap_targets.dedup();
            }
            let eligible_target_count = bootstrap_targets.len();
            let mut skipped_future_target_count = 0_usize;

            info!(
                service = "indexer",
                command = "run",
                bootstrap_backfill_status = "planning",
                chain = %task.chain,
                provider_finalized_head_block,
                bootstrap_backfill_range_policy = "authoritative_known_start_to_provider_finalized_head",
                hash_pinned_chunk_blocks,
                bootstrap_backfill_workers = requested_worker_count,
                effective_bootstrap_backfill_workers = effective_worker_count,
                bootstrap_backfill_range_blocks,
                eligible_bootstrap_target_count = eligible_target_count,
                raw_log_retention_generation = retention_snapshot.generation,
                ens_v2_retained_history_recovery = retention_snapshot
                    .requires_ens_v2_history_recovery,
                skipped_unknown_start_target_count = outcome.skipped_unknown_start_target_count,
                "automatic bootstrap targets loaded"
            );

            let mut target_ranges = Vec::new();
            for target in bootstrap_targets {
                let Some(range) = bootstrap_target_range(&target, provider_finalized_head_block)?
                else {
                    skipped_future_target_count += 1;
                    info!(
                        service = "indexer",
                        command = "run",
                        bootstrap_backfill_status = "skipped_future_target",
                        chain = %task.chain,
                        source_family = %target.source_family,
                        contract_instance_id = %target.contract_instance_id,
                        address = %target.address,
                        effective_from_block = target.effective_from_block,
                        effective_to_block = target.effective_to_block,
                        provider_finalized_head_block,
                        bootstrap_backfill_range_policy = "authoritative_known_start_to_provider_finalized_head",
                        "automatic bootstrap target starts after the provider finalized bootstrap head"
                    );
                    continue;
                };

                let mut range = range;
                let mut checkpoint_source_plan = load_bootstrap_source_plan(
                pool,
                &task.chain,
                std::slice::from_ref(&target),
                    range,
                    include_historical_bootstrap_targets,
            )
            .await
            .with_context(|| {
                format!(
                    "failed to build bootstrap watched-target checkpoint source plan for chain {} target {} range {}..={}",
                    task.chain,
                    target.contract_instance_id,
                    range.from_block,
                    range.to_block
                )
            })?;
                narrow_manifest_bootstrap_source_plan(
                    &mut checkpoint_source_plan,
                    std::slice::from_ref(&target),
                    range,
                )?;
                let checkpoint_source_identity =
                    backfill_job_source_identity_payload(&checkpoint_source_plan)?;
                if let Some(stored_checkpoint) = load_bootstrap_target_checkpoint(
                    pool,
                    &deployment_profile,
                    &task.chain,
                    &checkpoint_source_identity,
                    range,
                    &target.contract_instance_id.to_string(),
                    retention_snapshot.generation,
                )
                .await?
                {
                    if stored_checkpoint >= range.to_block {
                        info!(
                            service = "indexer",
                            command = "run",
                            bootstrap_backfill_status = "skipped_target_stored_checkpoint",
                            chain = %task.chain,
                            source_family = %target.source_family,
                            contract_instance_id = %target.contract_instance_id,
                            address = %target.address,
                            from_block = range.from_block,
                            to_block = range.to_block,
                            stored_checkpoint_block = stored_checkpoint,
                            "automatic bootstrap target already has stored backfill checkpoint coverage"
                        );
                        continue;
                    }
                    if stored_checkpoint >= range.from_block {
                        let resumed_from_block = stored_checkpoint.checked_add(1).with_context(|| {
                        format!(
                            "stored bootstrap checkpoint {stored_checkpoint} overflowed while resuming target"
                        )
                    })?;
                        info!(
                            service = "indexer",
                            command = "run",
                            bootstrap_backfill_status = "resuming_target_after_stored_checkpoint",
                            chain = %task.chain,
                            source_family = %target.source_family,
                            contract_instance_id = %target.contract_instance_id,
                            address = %target.address,
                            from_block = range.from_block,
                            resumed_from_block,
                            to_block = range.to_block,
                            stored_checkpoint_block = stored_checkpoint,
                            "automatic bootstrap target resumes after stored backfill checkpoint"
                        );
                        range = BackfillBlockRange::new(resumed_from_block, range.to_block)?;
                    }
                }

                target_ranges.push(BootstrapBackfillTargetRange { target, range });
            }

            target_ranges.sort_by(|left, right| {
                (
                    &left.target.source_family,
                    left.target.contract_instance_id,
                    &left.target.address,
                    left.range.from_block,
                    left.range.to_block,
                )
                    .cmp(&(
                        &right.target.source_family,
                        right.target.contract_instance_id,
                        &right.target.address,
                        right.range.from_block,
                        right.range.to_block,
                    ))
            });
            target_ranges.dedup_by(|left, right| {
                left.target.source_family == right.target.source_family
                    && left.target.contract_instance_id == right.target.contract_instance_id
                    && left.target.address == right.target.address
                    && left.range == right.range
            });

            for segment in plan_bootstrap_backfill_segments(target_ranges)? {
                let segment_target_ids = bootstrap_segment_target_ids(&segment.targets);
                let mut checkpoint_source_plan = load_bootstrap_source_plan(
                pool,
                &task.chain,
                &segment.targets,
                segment.range,
                include_historical_bootstrap_targets,
            )
            .await
            .with_context(|| {
                format!(
                    "failed to build bootstrap segment checkpoint source plan for chain {} range {}..={}",
                    task.chain, segment.range.from_block, segment.range.to_block
                )
            })?;
                narrow_manifest_bootstrap_source_plan(
                    &mut checkpoint_source_plan,
                    &segment.targets,
                    segment.range,
                )?;
                let checkpoint_source_identity =
                    backfill_job_source_identity_payload(&checkpoint_source_plan)?;
                let mut segment_range = segment.range;
                if let Some(stored_checkpoint) = load_bootstrap_segment_checkpoint(
                    pool,
                    &deployment_profile,
                    &task.chain,
                    &checkpoint_source_identity,
                    segment.range,
                    &segment_target_ids,
                    retention_snapshot.generation,
                )
                .await?
                {
                    if stored_checkpoint >= segment_range.to_block {
                        info!(
                            service = "indexer",
                            command = "run",
                            bootstrap_backfill_status = "skipped_stored_checkpoint",
                            chain = %task.chain,
                            from_block = segment.range.from_block,
                            to_block = segment.range.to_block,
                            stored_checkpoint_block = stored_checkpoint,
                            selected_target_count = segment.targets.len(),
                            "automatic bootstrap segment already has stored backfill checkpoint coverage"
                        );
                        continue;
                    }
                    if stored_checkpoint >= segment_range.from_block {
                        let resumed_from_block = stored_checkpoint.checked_add(1).with_context(|| {
                        format!(
                            "stored bootstrap checkpoint {stored_checkpoint} overflowed while resuming"
                        )
                    })?;
                        info!(
                            service = "indexer",
                            command = "run",
                            bootstrap_backfill_status = "resuming_after_stored_checkpoint",
                            chain = %task.chain,
                            from_block = segment.range.from_block,
                            resumed_from_block,
                            to_block = segment.range.to_block,
                            stored_checkpoint_block = stored_checkpoint,
                            selected_target_count = segment.targets.len(),
                            "automatic bootstrap segment resumes after stored backfill checkpoint"
                        );
                        segment_range =
                            BackfillBlockRange::new(resumed_from_block, segment_range.to_block)?;
                    }
                }

                let mut source_plan = load_bootstrap_source_plan(
                pool,
                &task.chain,
                &segment.targets,
                segment_range,
                include_historical_bootstrap_targets,
            )
            .await
            .with_context(|| {
                format!(
                    "failed to build bootstrap watched-target source plan for chain {} range {}..={}",
                    task.chain, segment_range.from_block, segment_range.to_block
                )
            })?;
                narrow_manifest_bootstrap_source_plan(
                    &mut source_plan,
                    &segment.targets,
                    segment_range,
                )?;

                let source_identity_hash = source_identity_hash_for_backfill(&source_plan)?;
                let range_specs = hash_pinned_backfill_range_specs(
                    segment_range,
                    bootstrap_backfill_range_blocks,
                )?;
                let idempotency_key = if range_specs.len() == 1 {
                    bootstrap_backfill_idempotency_key(
                        &deployment_profile,
                        &task.chain,
                        &source_identity_hash,
                        segment_range,
                    )
                } else {
                    partitioned_bootstrap_backfill_idempotency_key(
                        &deployment_profile,
                        &task.chain,
                        &source_identity_hash,
                        segment_range,
                        bootstrap_backfill_range_blocks,
                    )
                };
                let config = crate::backfill::BackfillJobRunConfig {
                    deployment_profile: deployment_profile.clone(),
                    idempotency_key,
                    scope_idempotency_to_raw_log_retention_generation: true,
                    range: segment_range,
                    lease_owner: lease_owner.clone(),
                    lease_token: generated_backfill_lease_token()?,
                    lease_expires_at: backfill_lease_expires_at(
                        BOOTSTRAP_BACKFILL_LEASE_DURATION_SECS,
                    )?,
                    hash_pinned_chunk_blocks,
                    adapter_sync_mode: backfill_adapter_sync_mode,
                    header_audit_mode,
                };

                let job_outcome = run_resumable_hash_pinned_backfill_job_concurrently(
                    pool,
                    &source_plan,
                    provider,
                    config,
                    range_specs,
                    effective_worker_count,
                )
                .await?;
                outcome.add_job(&job_outcome);
                if replay_completed_raw_ranges && job_outcome.raw_log_count > 0 {
                    let replay_outcome = replay_raw_fact_normalized_events(
                    pool,
                    RawFactNormalizedEventReplayRequest {
                        deployment_profile: deployment_profile.clone(),
                        chain: task.chain.clone(),
                        selection: RawFactNormalizedEventReplaySelection::ScopedBlockRange {
                            from_block: job_outcome.from_block,
                            to_block: job_outcome.to_block,
                            source_scope: replay_source_scope_from_source_plan(
                                &source_plan,
                                job_outcome.from_block,
                                job_outcome.to_block,
                            ),
                        },
                    },
                )
                .await
                .with_context(|| {
                    format!(
                        "failed to replay normalized events after bootstrap raw backfill for chain {} range {}..={}",
                        task.chain, job_outcome.from_block, job_outcome.to_block
                    )
                })?;
                    log_raw_fact_normalized_event_replay_outcome(&replay_outcome);
                    outcome.normalized_replay_job_count += 1;
                    outcome.normalized_replay_synced_count +=
                        replay_outcome.normalized_event_synced_count;
                    outcome.normalized_replay_inserted_count +=
                        replay_outcome.normalized_event_inserted_count;
                }
            }

            let pass_status = finish_bootstrap_convergence_pass(
                pool,
                &task.chain,
                provider_finalized_head_block,
                retention_snapshot,
                adapter_sync_mode,
            )
            .await?;
            if pass_status == BootstrapPassStatus::Stable {
                outcome.eligible_target_count += eligible_target_count;
                outcome.skipped_future_target_count += skipped_future_target_count;
                break;
            }
            convergence_tracker
                .record_retry(
                    pool,
                    &task.chain,
                    provider_finalized_head_block,
                    pass_status,
                )
                .await?;
            warn!(
                service = "indexer",
                command = "run",
                bootstrap_backfill_status = "retry_retention_authority_changed",
                chain = %task.chain,
                planned_raw_log_retention_generation = retention_snapshot.generation,
                planned_discovery_admission_epoch = retention_snapshot.discovery_admission_epoch,
                pass_status = ?pass_status,
                "raw-log retention or ENSv2 discovery authority changed during automatic bootstrap; retrying the complete chain planning pass"
            );
        }
    }

    info!(
        service = "indexer",
        command = "run",
        bootstrap_backfill_status = "drained",
        active_chain_count = outcome.active_chain_count,
        provider_configured_chain_count = outcome.provider_configured_chain_count,
        missing_provider_chain_count = outcome.missing_provider_chain_count,
        eligible_bootstrap_target_count = outcome.eligible_target_count,
        skipped_unknown_start_target_count = outcome.skipped_unknown_start_target_count,
        drained_bootstrap_job_count = outcome.drained_job_count,
        skipped_future_target_count = outcome.skipped_future_target_count,
        bootstrap_backfill_range_policy = "authoritative_known_start_to_provider_finalized_head",
        hash_pinned_chunk_blocks,
        bootstrap_backfill_workers = outcome.requested_worker_count,
        effective_bootstrap_backfill_workers = outcome.effective_worker_count,
        bootstrap_backfill_range_blocks = outcome.range_partition_block_count,
        reserved_range_count = outcome.reserved_range_count,
        completed_range_count = outcome.completed_range_count,
        resolved_block_count = outcome.resolved_block_count,
        raw_block_count = outcome.raw_block_count,
        raw_transaction_count = outcome.raw_transaction_count,
        raw_receipt_count = outcome.raw_receipt_count,
        raw_log_count = outcome.raw_log_count,
        raw_code_hash_count = outcome.raw_code_hash_count,
        normalized_replay_job_count = outcome.normalized_replay_job_count,
        normalized_replay_synced_count = outcome.normalized_replay_synced_count,
        normalized_replay_inserted_count = outcome.normalized_replay_inserted_count,
        "startup bootstrap backfill jobs drained before live polling"
    );

    Ok(outcome)
}
