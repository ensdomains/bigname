use std::{collections::BTreeMap, path::Path};

use anyhow::Result;
use bigname_manifests::ManifestBootstrapSkippedTarget;

use crate::{
    backfill::BackfillAdapterSyncMode,
    provider::{ProviderBlock, ProviderRegistry},
    reconciliation::HeaderAuditMode,
    run::startup_heartbeat::StartupHeartbeat,
    runtime::IntakeChainTask,
};

use super::run_startup_bootstrap_backfills_inner;

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
    pub(super) fn add_job(&mut self, outcome: &crate::backfill::BackfillJobRunOutcome) {
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
#[cfg(test)]
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
    run_startup_bootstrap_backfills_inner(
        pool,
        manifests_root,
        intake_chain_tasks,
        provider_registry,
        hash_pinned_chunk_blocks,
        adapter_sync_mode,
        replay_completed_raw_ranges,
        header_audit_mode,
        bootstrap_backfill_workers,
        bootstrap_backfill_range_blocks,
        None,
    )
    .await
}

#[expect(clippy::too_many_arguments)]
pub(crate) async fn run_startup_bootstrap_backfills_with_heartbeat(
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
    heartbeat: &mut StartupHeartbeat,
) -> Result<BootstrapBackfillOutcome> {
    run_startup_bootstrap_backfills_inner(
        pool,
        manifests_root,
        intake_chain_tasks,
        provider_registry,
        hash_pinned_chunk_blocks,
        adapter_sync_mode,
        replay_completed_raw_ranges,
        header_audit_mode,
        bootstrap_backfill_workers,
        bootstrap_backfill_range_blocks,
        Some(heartbeat),
    )
    .await
}
