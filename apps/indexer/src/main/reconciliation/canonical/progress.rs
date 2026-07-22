use anyhow::Result;
use bigname_adapters::StartupAdapterProgress;
use bigname_storage::ChainCheckpoint;

use crate::{
    provider::{ChainProviderOps, ProviderBlock},
    runtime::IntakeChainTask,
};

use super::{
    CanonicalReconciliation, ChainCoverageFrontiers, ChainReconciliationOutcome, HeaderAuditMode,
    reconcile_canonical_head_with_progress, reconcile_intake_chain_task_with_adapter_sync_inner,
};

#[allow(dead_code)]
pub(crate) async fn reconcile_intake_chain_task(
    pool: &sqlx::PgPool,
    task: &IntakeChainTask,
    provider: &(impl ChainProviderOps + ?Sized),
) -> Result<Option<(IntakeChainTask, ChainReconciliationOutcome)>> {
    reconcile_intake_chain_task_with_adapter_sync(
        pool,
        "test",
        task,
        provider,
        0,
        true,
        HeaderAuditMode::Minimal,
        &[],
        &ChainCoverageFrontiers::default(),
        None,
    )
    .await
}

#[expect(clippy::too_many_arguments)]
pub(crate) async fn reconcile_intake_chain_task_with_adapter_sync(
    pool: &sqlx::PgPool,
    deployment_profile: &str,
    task: &IntakeChainTask,
    provider: &(impl ChainProviderOps + ?Sized),
    loaded_plan_admission_epoch: i64,
    adapter_sync_enabled: bool,
    header_audit_mode: HeaderAuditMode,
    event_silent_reverse_resolver_addresses: &[String],
    coverage_frontiers: &ChainCoverageFrontiers,
    latched_bootstrap_finalized_head: Option<&ProviderBlock>,
) -> Result<Option<(IntakeChainTask, ChainReconciliationOutcome)>> {
    reconcile_intake_chain_task_with_adapter_sync_inner(
        pool,
        deployment_profile,
        task,
        provider,
        loaded_plan_admission_epoch,
        adapter_sync_enabled,
        header_audit_mode,
        event_silent_reverse_resolver_addresses,
        coverage_frontiers,
        latched_bootstrap_finalized_head,
        &mut None,
    )
    .await
}

#[expect(clippy::too_many_arguments)]
pub(crate) async fn reconcile_intake_chain_task_with_adapter_sync_and_progress(
    pool: &sqlx::PgPool,
    deployment_profile: &str,
    task: &IntakeChainTask,
    provider: &(impl ChainProviderOps + ?Sized),
    loaded_plan_admission_epoch: i64,
    adapter_sync_enabled: bool,
    header_audit_mode: HeaderAuditMode,
    event_silent_reverse_resolver_addresses: &[String],
    coverage_frontiers: &ChainCoverageFrontiers,
    latched_bootstrap_finalized_head: Option<&ProviderBlock>,
    progress: &mut Option<&mut dyn StartupAdapterProgress>,
) -> Result<Option<(IntakeChainTask, ChainReconciliationOutcome)>> {
    reconcile_intake_chain_task_with_adapter_sync_inner(
        pool,
        deployment_profile,
        task,
        provider,
        loaded_plan_admission_epoch,
        adapter_sync_enabled,
        header_audit_mode,
        event_silent_reverse_resolver_addresses,
        coverage_frontiers,
        latched_bootstrap_finalized_head,
        progress,
    )
    .await
}

#[expect(clippy::too_many_arguments)]
#[allow(dead_code)]
pub(crate) async fn reconcile_canonical_head(
    pool: &sqlx::PgPool,
    provider: &(impl ChainProviderOps + ?Sized),
    chain: &str,
    checkpoint: &ChainCheckpoint,
    latest_head: &ProviderBlock,
    header_audit_mode: HeaderAuditMode,
    stored_lineage_promotion_anchors: &[ProviderBlock],
    coverage_frontiers: &ChainCoverageFrontiers,
) -> Result<CanonicalReconciliation> {
    reconcile_canonical_head_with_progress(
        pool,
        provider,
        chain,
        checkpoint,
        latest_head,
        header_audit_mode,
        stored_lineage_promotion_anchors,
        coverage_frontiers,
        &mut None,
    )
    .await
}

pub(super) async fn record_live_progress(
    pool: &sqlx::PgPool,
    progress: &mut Option<&mut dyn StartupAdapterProgress>,
) -> Result<()> {
    if let Some(progress) = progress.as_deref_mut() {
        progress.record(pool).await?;
    }
    Ok(())
}
