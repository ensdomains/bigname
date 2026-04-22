use std::path::Path;

use anyhow::{Context, Result, bail};
use bigname_manifests::{
    ManifestBootstrapSkippedTarget, ManifestBootstrapTarget, WatchedSourceSelector,
    WatchedSourceSelectorKind, WatchedSourceSelectorPlan, WatchedTargetIdentity,
    load_manifest_declared_bootstrap_targets, load_manifest_skipped_bootstrap_targets,
    load_watched_source_selector_plan,
};
use tracing::{info, warn};

use crate::{
    backfill::{BackfillBlockRange, run_resumable_hash_pinned_backfill_job},
    backfill_lease_expires_at, default_backfill_lease_owner, deployment_profile_from_manifest_root,
    generated_backfill_lease_token,
    provider::ProviderRegistry,
    runtime::IntakeChainTask,
};

const BOOTSTRAP_BACKFILL_LEASE_DURATION_SECS: u64 = 300;

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct BootstrapBackfillOutcome {
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

pub(crate) async fn run_startup_bootstrap_backfills(
    pool: &sqlx::PgPool,
    manifests_root: &Path,
    intake_chain_tasks: &[IntakeChainTask],
    provider_registry: &ProviderRegistry,
) -> Result<BootstrapBackfillOutcome> {
    let deployment_profile = deployment_profile_from_manifest_root(manifests_root);
    let lease_owner = format!("{}:bootstrap-backfill", default_backfill_lease_owner());
    let mut outcome = BootstrapBackfillOutcome {
        active_chain_count: intake_chain_tasks.len(),
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
                "no RPC provider is configured for an active bootstrap chain; automatic bootstrap backfill will stay idle for this chain"
            );
            continue;
        };
        outcome.provider_configured_chain_count += 1;

        let heads = provider.fetch_chain_heads().await.with_context(|| {
            format!(
                "failed to fetch finite provider head for bootstrap backfill on chain {}",
                task.chain
            )
        })?;
        let provider_head_block = heads.canonical.block_number;
        let bootstrap_targets = load_manifest_declared_bootstrap_targets(pool, &task.chain).await?;
        outcome.eligible_target_count += bootstrap_targets.len();

        info!(
            service = "indexer",
            command = "run",
            bootstrap_backfill_status = "planning",
            chain = %task.chain,
            provider_head_block,
            eligible_bootstrap_target_count = bootstrap_targets.len(),
            skipped_unknown_start_target_count = outcome.skipped_unknown_start_target_count,
            "manifest-declared bootstrap targets loaded"
        );

        for target in bootstrap_targets {
            let Some(range) = bootstrap_target_range(&target, provider_head_block)? else {
                outcome.skipped_future_target_count += 1;
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
                    provider_head_block,
                    "manifest-declared bootstrap target is outside the finite provider head"
                );
                continue;
            };

            let source_plan = load_watched_source_selector_plan(
                pool,
                &task.chain,
                WatchedSourceSelector::WatchedTargetSet(vec![WatchedTargetIdentity {
                    contract_instance_id: target.contract_instance_id,
                }]),
                range.from_block,
                range.to_block,
            )
            .await
            .with_context(|| {
                format!(
                    "failed to build bootstrap watched-target source plan for chain {} contract_instance_id {}",
                    task.chain, target.contract_instance_id
                )
            })?;
            ensure_manifest_bootstrap_source_plan(&source_plan, &target, range)?;

            let source_identity_hash = source_plan.source_identity_hash();
            let idempotency_key = bootstrap_backfill_idempotency_key(
                &deployment_profile,
                manifests_root,
                &task.chain,
                &source_identity_hash,
                range,
            );
            let config = crate::backfill::BackfillJobRunConfig {
                deployment_profile: deployment_profile.clone(),
                idempotency_key,
                range,
                lease_owner: lease_owner.clone(),
                lease_token: generated_backfill_lease_token()?,
                lease_expires_at: backfill_lease_expires_at(
                    BOOTSTRAP_BACKFILL_LEASE_DURATION_SECS,
                )?,
            };

            let job_outcome =
                run_resumable_hash_pinned_backfill_job(pool, &source_plan, provider, config)
                    .await?;
            outcome.add_job(&job_outcome);
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
        reserved_range_count = outcome.reserved_range_count,
        completed_range_count = outcome.completed_range_count,
        resolved_block_count = outcome.resolved_block_count,
        raw_block_count = outcome.raw_block_count,
        raw_transaction_count = outcome.raw_transaction_count,
        raw_receipt_count = outcome.raw_receipt_count,
        raw_log_count = outcome.raw_log_count,
        raw_code_hash_count = outcome.raw_code_hash_count,
        "startup bootstrap backfill jobs drained before live polling"
    );

    Ok(outcome)
}

pub(crate) fn bootstrap_backfill_idempotency_key(
    deployment_profile: &str,
    manifests_root: &Path,
    chain: &str,
    source_identity_hash: &str,
    range: BackfillBlockRange,
) -> String {
    format!(
        "indexer-bootstrap-backfill:v1:deployment_profile={deployment_profile}:manifest_root={}:chain={chain}:source_identity_hash={source_identity_hash}:from={}:to={}",
        manifests_root.display(),
        range.from_block,
        range.to_block
    )
}

fn bootstrap_target_range(
    target: &ManifestBootstrapTarget,
    provider_head_block: i64,
) -> Result<Option<BackfillBlockRange>> {
    let finite_end_block = target
        .effective_to_block
        .map(|effective_to_block| effective_to_block.min(provider_head_block))
        .unwrap_or(provider_head_block);
    if target.effective_from_block > finite_end_block {
        return Ok(None);
    }

    BackfillBlockRange::new(target.effective_from_block, finite_end_block).map(Some)
}

fn ensure_manifest_bootstrap_source_plan(
    source_plan: &WatchedSourceSelectorPlan,
    target: &ManifestBootstrapTarget,
    range: BackfillBlockRange,
) -> Result<()> {
    if source_plan.selector_kind != WatchedSourceSelectorKind::WatchedTargetSet {
        bail!(
            "bootstrap source plan for contract_instance_id {} used selector kind {} instead of watched_target_set",
            target.contract_instance_id,
            source_plan.selector_kind.as_str()
        );
    }

    if source_plan.selected_targets.len() != 1 {
        bail!(
            "bootstrap source plan for contract_instance_id {} selected {} targets instead of one",
            target.contract_instance_id,
            source_plan.selected_targets.len()
        );
    }

    let selected_target = &source_plan.selected_targets[0];
    if selected_target.source_family != target.source_family
        || selected_target.contract_instance_id != target.contract_instance_id
        || selected_target.address != target.address
        || selected_target.effective_from_block != range.from_block
        || selected_target.effective_to_block != range.to_block
    {
        bail!(
            "bootstrap source plan for contract_instance_id {} does not match the manifest-declared effective range",
            target.contract_instance_id
        );
    }

    Ok(())
}
