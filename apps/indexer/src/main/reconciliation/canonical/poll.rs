use std::collections::BTreeMap;

use crate::{provider::ProviderRegistry, runtime::IntakeChainTask};
use anyhow::Result;
use tracing::{info, warn};

use super::{
    EnsV2LiveCoverageRecoveryStatus, reconcile_intake_chain_task_with_adapter_sync,
    recover_ens_v2_live_coverage_requirement, stored_lineage::ChainCoverageFrontiers,
};
use crate::{
    provider::ProviderBlock,
    reconciliation::{logging::log_chain_reconciliation_outcome, types::HeaderAuditMode},
    runtime::checkpoint_mode,
};

const MAX_ENS_V2_LIVE_COVERAGE_RECOVERY_ATTEMPTS: usize = 32;

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
    adapter_sync_enabled: bool,
    header_audit_mode: HeaderAuditMode,
    event_silent_reverse_resolver_addresses: &[String],
    coverage_frontiers: &ChainCoverageFrontiers,
    latched_bootstrap_finalized_heads: &BTreeMap<String, ProviderBlock>,
) -> Result<()> {
    let mut next_tasks = tasks.clone();
    let mut any_change = false;

    for (index, task) in tasks.iter().enumerate() {
        let Some(provider) = provider_registry.provider_for(&task.chain) else {
            continue;
        };
        let mut coverage_recovery_attempt = 0_usize;
        loop {
            match reconcile_intake_chain_task_with_adapter_sync(
                pool,
                deployment_profile,
                task,
                provider,
                adapter_sync_enabled,
                header_audit_mode,
                event_silent_reverse_resolver_addresses,
                coverage_frontiers,
                latched_bootstrap_finalized_heads.get(&task.chain),
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
                    let Some(requirement) = ens_v2_coverage_requirement(&error) else {
                        warn!(
                            service = "indexer",
                            chain = %task.chain,
                            error = ?error,
                            intake_checkpoint_mode = checkpoint_mode(&task.checkpoint),
                            "failed to fetch and reconcile provider heads for intake chain"
                        );
                        break;
                    };
                    if coverage_recovery_attempt >= MAX_ENS_V2_LIVE_COVERAGE_RECOVERY_ATTEMPTS {
                        warn!(
                            service = "indexer",
                            chain = %task.chain,
                            error = ?error,
                            coverage_recovery_attempt,
                            intake_checkpoint_mode = checkpoint_mode(&task.checkpoint),
                            "ENSv2 live coverage recovery did not converge within its bounded retry limit"
                        );
                        break;
                    }
                    coverage_recovery_attempt += 1;
                    match recover_ens_v2_live_coverage_requirement(
                        pool,
                        deployment_profile,
                        provider,
                        header_audit_mode,
                        &requirement,
                    )
                    .await
                    {
                        Ok(status) => {
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
                                "retrying unchanged live poll after exact ENSv2 coverage recovery"
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
                                "provider-backed ENSv2 live coverage recovery failed"
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

fn ens_v2_coverage_requirement(
    error: &anyhow::Error,
) -> Option<bigname_adapters::EnsV2NewlyRequiredCoverage> {
    error.chain().find_map(|cause| {
        cause
            .downcast_ref::<bigname_adapters::EnsV2NewlyRequiredCoverage>()
            .cloned()
    })
}

#[cfg(test)]
mod tests {
    use super::ens_v2_coverage_requirement;

    #[test]
    fn coverage_requirement_survives_nested_reconciliation_context() {
        let requirement = bigname_adapters::EnsV2NewlyRequiredCoverage {
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

        assert_eq!(ens_v2_coverage_requirement(&error), Some(requirement));
    }

    #[test]
    fn unrelated_reconciliation_error_is_not_recoverable_coverage() {
        let error = anyhow::anyhow!("provider failed").context("adapter sync failed");

        assert_eq!(ens_v2_coverage_requirement(&error), None);
    }
}
