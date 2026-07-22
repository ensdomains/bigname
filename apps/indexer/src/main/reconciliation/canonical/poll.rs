use std::collections::BTreeMap;

use crate::{provider::ProviderRegistry, runtime::IntakeChainTask};
use anyhow::Result;
use bigname_adapters::StartupAdapterProgress;
use tracing::{info, warn};

use super::{
    EnsV2LiveCoverageRecoveryStatus, reconcile_intake_chain_task_with_adapter_sync_and_progress,
    recover_ens_v2_live_coverage_requirement, stored_lineage::ChainCoverageFrontiers,
};
use crate::{
    provider::ProviderBlock,
    reconciliation::{
        logging::log_chain_reconciliation_outcome, replay::LegacyRegistryNewlyRequiredCoverage,
        types::HeaderAuditMode,
    },
    runtime::checkpoint_mode,
};

const MAX_LIVE_COVERAGE_RECOVERY_ATTEMPTS: usize = 32;

#[allow(dead_code)]
pub(crate) async fn poll_provider_heads(
    pool: &sqlx::PgPool,
    tasks: &mut Vec<IntakeChainTask>,
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
    tasks: &mut Vec<IntakeChainTask>,
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
    tasks: &mut Vec<IntakeChainTask>,
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
    tasks: &mut Vec<IntakeChainTask>,
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
    let mut next_tasks = tasks.clone();
    let mut any_change = false;

    for (index, task) in tasks.iter().enumerate() {
        let Some(provider) = provider_registry.provider_for(&task.chain) else {
            continue;
        };
        let mut coverage_recovery_attempt = 0_usize;
        loop {
            match reconcile_intake_chain_task_with_adapter_sync_and_progress(
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
            )
            .await
            {
                Ok(Some((next_task, outcome))) => {
                    log_chain_reconciliation_outcome(&outcome);
                    next_tasks[index] = next_task;
                    any_change = true;
                    break;
                }
                Ok(None) => break,
                Err(error) => {
                    let Some(requirement) = live_coverage_requirement(&error) else {
                        warn!(
                            service = "indexer",
                            chain = %task.chain,
                            error = ?error,
                            intake_checkpoint_mode = checkpoint_mode(&task.checkpoint),
                            "failed to fetch and reconcile provider heads for intake chain"
                        );
                        break;
                    };
                    if coverage_recovery_attempt >= MAX_LIVE_COVERAGE_RECOVERY_ATTEMPTS {
                        warn!(
                            service = "indexer",
                            chain = %task.chain,
                            error = ?error,
                            coverage_recovery_attempt,
                            intake_checkpoint_mode = checkpoint_mode(&task.checkpoint),
                            "live generation-bound coverage recovery did not converge within its bounded retry limit"
                        );
                        break;
                    }
                    coverage_recovery_attempt += 1;
                    let recovery_requirement = requirement.as_recovery_requirement();
                    match recover_ens_v2_live_coverage_requirement(
                        pool,
                        deployment_profile,
                        provider,
                        header_audit_mode,
                        &recovery_requirement,
                    )
                    .await
                    {
                        Ok(status) => {
                            record_progress(pool, progress).await?;
                            info!(
                                service = "indexer",
                                command = "poll",
                                chain = %task.chain,
                                source_family = %requirement.source_family,
                                address = %requirement.address,
                                from_block = requirement.required_from_block,
                                to_block = requirement.required_to_block,
                                retention_generation = requirement.retention_generation,
                                coverage_recovery_attempt,
                                coverage_recovery_status = match status {
                                    EnsV2LiveCoverageRecoveryStatus::Recovered => "recovered",
                                    EnsV2LiveCoverageRecoveryStatus::AuthorityChanged => "authority_changed_replan",
                                },
                                "retrying unchanged live poll after exact generation-bound coverage recovery"
                            );
                        }
                        Err(recovery_error) => {
                            warn!(
                                service = "indexer",
                                command = "poll",
                                chain = %task.chain,
                                source_family = %requirement.source_family,
                                address = %requirement.address,
                                from_block = requirement.required_from_block,
                                to_block = requirement.required_to_block,
                                retention_generation = requirement.retention_generation,
                                coverage_recovery_attempt,
                                error = ?recovery_error,
                                reconciliation_error = ?error,
                                intake_checkpoint_mode = checkpoint_mode(&task.checkpoint),
                                "provider-backed live coverage recovery failed"
                            );
                            break;
                        }
                    }
                }
            }
        }
    }
    if any_change {
        *tasks = next_tasks;
    }
    Ok(())
}

async fn record_progress(
    pool: &sqlx::PgPool,
    progress: &mut Option<&mut dyn StartupAdapterProgress>,
) -> Result<()> {
    if let Some(progress) = progress.as_deref_mut() {
        progress.record(pool).await?;
    }
    Ok(())
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct LiveCoverageRequirement {
    chain: String,
    retention_generation: i64,
    source_family: String,
    address: String,
    required_from_block: i64,
    required_to_block: i64,
}

impl LiveCoverageRequirement {
    fn as_recovery_requirement(&self) -> bigname_adapters::EnsV2MissingCoverage {
        bigname_adapters::EnsV2MissingCoverage {
            chain: self.chain.clone(),
            retention_generation: self.retention_generation,
            source_family: self.source_family.clone(),
            address: self.address.clone(),
            required_from_block: self.required_from_block,
            required_to_block: self.required_to_block,
        }
    }
}

fn live_coverage_requirement(error: &anyhow::Error) -> Option<LiveCoverageRequirement> {
    error.chain().find_map(|cause| {
        if let Some(requirement) = cause.downcast_ref::<bigname_adapters::EnsV2MissingCoverage>() {
            return Some(LiveCoverageRequirement {
                chain: requirement.chain.clone(),
                retention_generation: requirement.retention_generation,
                source_family: requirement.source_family.clone(),
                address: requirement.address.clone(),
                required_from_block: requirement.required_from_block,
                required_to_block: requirement.required_to_block,
            });
        }
        cause
            .downcast_ref::<LegacyRegistryNewlyRequiredCoverage>()
            .map(|requirement| LiveCoverageRequirement {
                chain: requirement.chain.clone(),
                retention_generation: requirement.retention_generation,
                source_family: requirement.source_family.clone(),
                address: requirement.address.clone(),
                required_from_block: requirement.required_from_block,
                required_to_block: requirement.required_to_block,
            })
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn coverage_requirement_survives_nested_reconciliation_context() {
        let requirement = bigname_adapters::EnsV2MissingCoverage {
            chain: "ethereum-sepolia".to_owned(),
            retention_generation: 3,
            source_family: "ens_v2_resolver_l1".to_owned(),
            address: "0x0000000000000000000000000000000000000001".to_owned(),
            required_from_block: 10,
            required_to_block: 20,
        };
        let error = (0..7).fold(anyhow::Error::new(requirement.clone()), |error, layer| {
            error.context(format!("reconciliation layer {layer}"))
        });

        assert_eq!(
            live_coverage_requirement(&error),
            Some(LiveCoverageRequirement {
                chain: requirement.chain,
                retention_generation: requirement.retention_generation,
                source_family: requirement.source_family,
                address: requirement.address,
                required_from_block: requirement.required_from_block,
                required_to_block: requirement.required_to_block,
            })
        );
    }

    #[test]
    fn admitted_legacy_registry_coverage_requirements_preserve_exact_recovery_bounds() {
        for (chain, source_family) in [
            ("ethereum-mainnet", "ens_v1_registry_l1"),
            ("base-mainnet", "basenames_base_registry"),
        ] {
            let requirement = LegacyRegistryNewlyRequiredCoverage {
                chain: chain.to_owned(),
                retention_generation: 3,
                source_family: source_family.to_owned(),
                address: "0x0000000000000000000000000000000000000001".to_owned(),
                required_from_block: 10,
                required_to_block: 20,
            };
            let error = (0..7).fold(anyhow::Error::new(requirement.clone()), |error, layer| {
                error.context(format!("reconciliation layer {layer}"))
            });
            let recovered = live_coverage_requirement(&error)
                .expect("typed legacy registry coverage must be recoverable by live polling");

            assert_eq!(
                recovered,
                LiveCoverageRequirement {
                    chain: requirement.chain,
                    retention_generation: requirement.retention_generation,
                    source_family: requirement.source_family,
                    address: requirement.address,
                    required_from_block: requirement.required_from_block,
                    required_to_block: requirement.required_to_block,
                }
            );
            assert_eq!(
                recovered.as_recovery_requirement(),
                bigname_adapters::EnsV2MissingCoverage {
                    chain: chain.to_owned(),
                    retention_generation: 3,
                    source_family: source_family.to_owned(),
                    address: "0x0000000000000000000000000000000000000001".to_owned(),
                    required_from_block: 10,
                    required_to_block: 20,
                },
                "the provider recovery request must not widen the typed tuple or its inclusive bounds"
            );
        }
    }

    #[test]
    fn unrelated_reconciliation_error_is_not_recoverable_coverage() {
        let error = anyhow::anyhow!("provider failed").context("adapter sync failed");

        assert_eq!(live_coverage_requirement(&error), None);
    }
}
