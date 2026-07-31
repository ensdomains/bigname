use std::{collections::BTreeMap, time::Instant};

use crate::StartupAdapterProgress;
use crate::{provider::ProviderRegistry, runtime::IntakeChainTask};
use anyhow::Result;
use tracing::warn;

use super::{
    reconcile_intake_chain_task_with_adapter_sync_and_progress,
    stored_lineage::ChainCoverageFrontiers,
};
use crate::{
    provider::ProviderBlock,
    reconciliation::{logging::log_chain_reconciliation_outcome, types::HeaderAuditMode},
    runtime::checkpoint_mode,
};

#[allow(dead_code)]
pub(crate) async fn poll_provider_heads(
    pool: &sqlx::PgPool,
    tasks: &mut [IntakeChainTask],
    provider_registry: &ProviderRegistry,
) -> Result<()> {
    poll_provider_heads_with_adapter_sync(
        pool,
        tasks,
        provider_registry,
        "test",
        &BTreeMap::new(),
        true,
        HeaderAuditMode::Minimal,
        &[],
        &ChainCoverageFrontiers::default(),
        &BTreeMap::new(),
    )
    .await
}

#[expect(clippy::too_many_arguments)]
pub(crate) async fn poll_provider_heads_with_adapter_sync(
    pool: &sqlx::PgPool,
    tasks: &mut [IntakeChainTask],
    provider_registry: &ProviderRegistry,
    deployment_profile: &str,
    watched_plan_admission_epochs: &BTreeMap<String, i64>,
    adapter_sync_enabled: bool,
    header_audit_mode: HeaderAuditMode,
    event_silent_reverse_resolver_addresses: &[String],
    coverage_frontiers: &ChainCoverageFrontiers,
    latched_bootstrap_finalized_heads: &BTreeMap<String, ProviderBlock>,
) -> Result<()> {
    poll_provider_heads_with_adapter_sync_inner(
        pool,
        tasks,
        provider_registry,
        deployment_profile,
        watched_plan_admission_epochs,
        adapter_sync_enabled,
        header_audit_mode,
        event_silent_reverse_resolver_addresses,
        coverage_frontiers,
        latched_bootstrap_finalized_heads,
        &mut None,
    )
    .await
}

#[expect(clippy::too_many_arguments)]
pub(crate) async fn poll_provider_heads_with_adapter_sync_and_progress(
    pool: &sqlx::PgPool,
    tasks: &mut [IntakeChainTask],
    provider_registry: &ProviderRegistry,
    deployment_profile: &str,
    watched_plan_admission_epochs: &BTreeMap<String, i64>,
    adapter_sync_enabled: bool,
    header_audit_mode: HeaderAuditMode,
    event_silent_reverse_resolver_addresses: &[String],
    coverage_frontiers: &ChainCoverageFrontiers,
    latched_bootstrap_finalized_heads: &BTreeMap<String, ProviderBlock>,
    progress: &mut dyn StartupAdapterProgress,
) -> Result<()> {
    poll_provider_heads_with_adapter_sync_inner(
        pool,
        tasks,
        provider_registry,
        deployment_profile,
        watched_plan_admission_epochs,
        adapter_sync_enabled,
        header_audit_mode,
        event_silent_reverse_resolver_addresses,
        coverage_frontiers,
        latched_bootstrap_finalized_heads,
        &mut Some(progress),
    )
    .await
}

#[expect(clippy::too_many_arguments)]
async fn poll_provider_heads_with_adapter_sync_inner(
    pool: &sqlx::PgPool,
    tasks: &mut [IntakeChainTask],
    provider_registry: &ProviderRegistry,
    deployment_profile: &str,
    watched_plan_admission_epochs: &BTreeMap<String, i64>,
    adapter_sync_enabled: bool,
    header_audit_mode: HeaderAuditMode,
    event_silent_reverse_resolver_addresses: &[String],
    coverage_frontiers: &ChainCoverageFrontiers,
    latched_bootstrap_finalized_heads: &BTreeMap<String, ProviderBlock>,
    progress: &mut Option<&mut dyn StartupAdapterProgress>,
) -> Result<()> {
    let mut changed_tasks = Vec::<(usize, IntakeChainTask)>::new();

    for (index, task) in tasks.iter().enumerate() {
        let Some(provider) = provider_registry.provider_for(&task.chain) else {
            continue;
        };
        // Keep replay-admission collisions out of the live-poll failure path. Each retry starts
        // fresh storage transactions and retains the unchanged intake task.
        let mut replay_admission_attempt = 1_usize;
        loop {
            let reconciliation = crate::metrics::with_provider_metrics(
                &task.chain,
                provider.kind(),
                reconcile_intake_chain_task_with_adapter_sync_and_progress(
                    pool,
                    deployment_profile,
                    task,
                    provider,
                    watched_plan_admission_epochs
                        .get(&task.chain)
                        .copied()
                        .unwrap_or(0),
                    adapter_sync_enabled,
                    header_audit_mode,
                    event_silent_reverse_resolver_addresses,
                    coverage_frontiers,
                    latched_bootstrap_finalized_heads.get(&task.chain),
                    progress,
                ),
            )
            .await;
            match reconciliation {
                Ok(Some((next_task, outcome))) => {
                    log_chain_reconciliation_outcome(&outcome);
                    changed_tasks.push((index, next_task));
                    break;
                }
                Ok(None) => break,
                Err(error) => {
                    if wait_for_live_replay_admission_retry(
                        &task.chain,
                        &error,
                        replay_admission_attempt,
                    )
                    .await
                    {
                        replay_admission_attempt += 1;
                        continue;
                    }
                    if bigname_storage::projection_staging::is_fatal_projection_replay_version_fence_error(
                        &error,
                    ) {
                        return Err(error);
                    }
                    warn!(
                        service = "indexer",
                        chain = %task.chain,
                        error = ?error,
                        intake_checkpoint_mode = checkpoint_mode(&task.checkpoint),
                        "failed to fetch and reconcile provider heads for intake chain"
                    );
                    break;
                }
            }
        }
    }
    for (index, next_task) in changed_tasks {
        tasks[index] = next_task;
    }
    Ok(())
}

async fn wait_for_live_replay_admission_retry(
    chain: &str,
    error: &anyhow::Error,
    failed_attempt: usize,
) -> bool {
    let wait_started = Instant::now();
    let should_retry =
        bigname_storage::projection_staging::wait_for_projection_replay_admission_retry(
            error,
            failed_attempt,
        )
        .await;
    if should_retry {
        crate::metrics::record_admission_retry(chain, wait_started.elapsed());
    }
    should_retry
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn live_replay_admission_wait_records_retry_and_fence_duration() {
        let chain = "live-admission-metrics-test";
        let retries_before = crate::metrics::admission_retries(chain);
        let waits_before = crate::metrics::fence_wait_observations(chain);
        let error =
            anyhow::anyhow!("projection replay admission is in progress; retry protected write");

        assert!(wait_for_live_replay_admission_retry(chain, &error, 1).await);
        assert_eq!(crate::metrics::admission_retries(chain), retries_before + 1);
        assert_eq!(
            crate::metrics::fence_wait_observations(chain),
            waits_before + 1
        );
    }
}
