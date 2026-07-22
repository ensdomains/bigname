use std::collections::{BTreeMap, BTreeSet};

use anyhow::{Context, Result};
use bigname_storage::acquire_raw_log_staging_read_set_guard;
use tracing::{info, warn};

use crate::normalized_replay_catchup::normalized_replay_cursors_complete;
use crate::provider::{ProviderBlock, ProviderRegistry};
use crate::reconciliation::{
    BacklogHandoffStatus, ChainCoverageFrontiers, HeaderAuditMode,
    poll_provider_heads_with_adapter_sync_and_progress,
    sync_live_adapter_backlog_after_normalized_replay_with_progress,
    validate_chain_handoff_while_guarded,
};
use crate::run::startup_heartbeat::{StartupAdapterHeartbeat, StartupHeartbeat};

use super::super::intake::IntakeChainTask;
use super::super::manifest::ManifestRuntimeState;
use super::discovery_refresh::refresh_discovery_watch_state_with_heartbeat;

fn resolver_profile_convergence_before_handoff() -> bool {
    false
}

#[cfg(test)]
#[path = "replay_handoff/test_hook.rs"]
mod test_hook;
#[cfg(test)]
pub(crate) use test_hook::install_before_latch as install_replay_handoff_before_latch_test_hook;

#[derive(Debug, Eq, PartialEq)]
pub(super) enum ReplayHandoffReadiness {
    AllChainsComplete,
    AwaitingChains { raw_poll_chains: BTreeSet<String> },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ReplayHandoffLatchStatus {
    Latched,
    AwaitingReplay,
    AwaitingBacklog,
}

pub(super) async fn replay_handoff_readiness(
    pool: &sqlx::PgPool,
    deployment_profile: &str,
    provider_configured_chains: &[String],
) -> ReplayHandoffReadiness {
    let mut cursor_results = Vec::with_capacity(provider_configured_chains.len());
    for chain in provider_configured_chains {
        let result = normalized_replay_cursors_complete(
            pool,
            deployment_profile,
            std::slice::from_ref(chain),
        )
        .await;
        cursor_results.push((chain.clone(), result));
    }
    let readiness = classify_replay_handoff_readiness(cursor_results);
    if readiness != ReplayHandoffReadiness::AllChainsComplete {
        return readiness;
    }

    // A final all-chain snapshot remains the authority for switching adapter ownership.
    match normalized_replay_cursors_complete(pool, deployment_profile, provider_configured_chains)
        .await
    {
        Ok(true) => ReplayHandoffReadiness::AllChainsComplete,
        Ok(false) => ReplayHandoffReadiness::AwaitingChains {
            raw_poll_chains: BTreeSet::new(),
        },
        Err(error) => {
            warn!(
                service = "indexer",
                command = "poll",
                error = ?error,
                "failed to confirm all-chain normalized replay completion; global adapter handoff remains disabled"
            );
            ReplayHandoffReadiness::AwaitingChains {
                raw_poll_chains: BTreeSet::new(),
            }
        }
    }
}

fn classify_replay_handoff_readiness(
    cursor_results: Vec<(String, Result<bool>)>,
) -> ReplayHandoffReadiness {
    let configured_chain_count = cursor_results.len();
    let raw_poll_chains = cursor_results
        .into_iter()
        .filter_map(|(chain, result)| match result {
            Ok(true) => Some(chain),
            Ok(false) => None,
            Err(error) => {
                warn!(
                    service = "indexer",
                    command = "poll",
                    chain,
                    error = ?error,
                    "failed to check normalized replay cursor completion; provider polling remains disabled for this chain"
                );
                None
            }
        })
        .collect::<BTreeSet<_>>();
    if raw_poll_chains.len() == configured_chain_count {
        ReplayHandoffReadiness::AllChainsComplete
    } else {
        ReplayHandoffReadiness::AwaitingChains { raw_poll_chains }
    }
}

#[expect(clippy::too_many_arguments)]
pub(super) async fn poll_replay_ready_chains_raw_only(
    pool: &sqlx::PgPool,
    provider_registry: &ProviderRegistry,
    manifest_runtime_state: &mut ManifestRuntimeState,
    intake_chain_tasks: &mut Vec<IntakeChainTask>,
    deployment_profile: &str,
    raw_poll_chains: &BTreeSet<String>,
    watched_plan_admission_epochs: &mut Option<BTreeMap<String, i64>>,
    header_audit_mode: HeaderAuditMode,
    event_silent_reverse_resolver_addresses: &[String],
    coverage_frontiers: &ChainCoverageFrontiers,
    latched_bootstrap_finalized_heads: &BTreeMap<String, ProviderBlock>,
    adapter_sync_page_logs: usize,
    heartbeat: &mut StartupHeartbeat,
    heartbeat_chain_ids: &[String],
) -> Result<()> {
    if raw_poll_chains.is_empty() {
        return Ok(());
    }

    if !refresh_discovery_watch_state_with_heartbeat(
        pool,
        provider_registry,
        manifest_runtime_state,
        intake_chain_tasks,
        false,
        resolver_profile_convergence_before_handoff(),
        watched_plan_admission_epochs,
        deployment_profile,
        adapter_sync_page_logs,
        heartbeat,
        heartbeat_chain_ids,
    )
    .await?
    {
        return Ok(());
    }

    let mut scoped_tasks = intake_chain_tasks
        .iter()
        .filter(|task| raw_poll_chains.contains(&task.chain))
        .cloned()
        .collect::<Vec<_>>();
    let loaded_plan_admission_epochs = watched_plan_admission_epochs
        .as_ref()
        .context("replay-ready watch plan is missing its loaded admission-epoch snapshot")?;
    {
        let mut progress = StartupAdapterHeartbeat::new(heartbeat, heartbeat_chain_ids);
        poll_provider_heads_with_adapter_sync_and_progress(
            pool,
            &mut scoped_tasks,
            provider_registry,
            deployment_profile,
            loaded_plan_admission_epochs,
            false,
            header_audit_mode,
            event_silent_reverse_resolver_addresses,
            coverage_frontiers,
            latched_bootstrap_finalized_heads,
            &mut progress,
        )
        .await?;
    }

    let mut updated_tasks = scoped_tasks
        .into_iter()
        .map(|task| (task.chain.clone(), task))
        .collect::<BTreeMap<_, _>>();
    for task in intake_chain_tasks {
        if let Some(updated_task) = updated_tasks.remove(&task.chain) {
            *task = updated_task;
        }
    }
    Ok(())
}

#[expect(clippy::too_many_arguments)]
pub(super) async fn renew_live_poll_adapter_sync_permit(
    pool: &sqlx::PgPool,
    provider_registry: &ProviderRegistry,
    manifest_runtime_state: &mut ManifestRuntimeState,
    intake_chain_tasks: &mut Vec<IntakeChainTask>,
    deployment_profile: &str,
    provider_configured_chains: &[String],
    live_adapter_sync_latched: &mut bool,
    forced_handoff_plan_reload_complete: &mut bool,
    watched_plan_admission_epochs: &mut Option<BTreeMap<String, i64>>,
    header_audit_mode: HeaderAuditMode,
    event_silent_reverse_resolver_addresses: &[String],
    coverage_frontiers: &ChainCoverageFrontiers,
    latched_bootstrap_finalized_heads: &BTreeMap<String, ProviderBlock>,
    adapter_sync_page_logs: usize,
    heartbeat: &mut StartupHeartbeat,
    heartbeat_chain_ids: &[String],
) -> Result<bool> {
    let handoff_was_previously_latched = *live_adapter_sync_latched;
    *live_adapter_sync_latched = false;
    if let ReplayHandoffReadiness::AwaitingChains { raw_poll_chains } =
        replay_handoff_readiness(pool, deployment_profile, provider_configured_chains).await
    {
        poll_replay_ready_chains_raw_only(
            pool,
            provider_registry,
            manifest_runtime_state,
            intake_chain_tasks,
            deployment_profile,
            &raw_poll_chains,
            watched_plan_admission_epochs,
            header_audit_mode,
            event_silent_reverse_resolver_addresses,
            coverage_frontiers,
            latched_bootstrap_finalized_heads,
            adapter_sync_page_logs,
            heartbeat,
            heartbeat_chain_ids,
        )
        .await?;
        return Ok(false);
    }

    let backlog_result = {
        let mut progress = StartupAdapterHeartbeat::new(heartbeat, heartbeat_chain_ids);
        sync_live_adapter_backlog_after_normalized_replay_with_progress(
            pool,
            deployment_profile,
            provider_configured_chains,
            &mut progress,
        )
        .await
    };
    let backlog_summary = match backlog_result {
        Ok(summary) if summary.awaiting_replay_chain_count == 0 => summary,
        Ok(summary) => {
            warn!(
                service = "indexer",
                command = "poll",
                deployment_profile,
                awaiting_replay_chain_count = summary.awaiting_replay_chain_count,
                "post-replay live adapter backlog observed stale replay input; live adapter handoff remains disabled"
            );
            return Ok(false);
        }
        Err(error) => {
            warn!(
                service = "indexer",
                command = "poll",
                deployment_profile,
                provider_configured_chain_count = provider_configured_chains.len(),
                error = ?error,
                "failed to sync live adapter backlog after normalized replay catch-up; live poll loop will retry"
            );
            return Ok(false);
        }
    };

    // Force one reload at the catch-up-to-live transition, then retain the epoch gate.
    prepare_handoff_plan_reload(
        *forced_handoff_plan_reload_complete,
        watched_plan_admission_epochs,
    );
    if !refresh_discovery_watch_state_with_heartbeat(
        pool,
        provider_registry,
        manifest_runtime_state,
        intake_chain_tasks,
        false,
        resolver_profile_convergence_before_handoff(),
        watched_plan_admission_epochs,
        deployment_profile,
        adapter_sync_page_logs,
        heartbeat,
        heartbeat_chain_ids,
    )
    .await?
    {
        return Ok(false);
    }
    *forced_handoff_plan_reload_complete = true;
    let refreshed_provider_configured_chains = intake_chain_tasks
        .iter()
        .filter(|task| provider_registry.provider_for(&task.chain).is_some())
        .map(|task| task.chain.clone())
        .collect::<Vec<_>>();
    if refreshed_provider_configured_chains.is_empty() {
        return Ok(false);
    }

    match latch_replay_handoff_if_stable(
        pool,
        deployment_profile,
        &refreshed_provider_configured_chains,
        live_adapter_sync_latched,
    )
    .await
    {
        Ok(ReplayHandoffLatchStatus::Latched) => {
            if !handoff_was_previously_latched || backlog_summary.selected_block_count > 0 {
                info!(
                    service = "indexer",
                    command = "poll",
                    deployment_profile,
                    post_replay_backlog_chain_count = backlog_summary.chain_count,
                    post_replay_backlog_selected_block_count = backlog_summary.selected_block_count,
                    post_replay_backlog_scanned_log_count = backlog_summary.scanned_log_count,
                    post_replay_backlog_matched_log_count = backlog_summary.matched_log_count,
                    post_replay_backlog_normalized_event_synced_count =
                        backlog_summary.normalized_event_synced_count,
                    post_replay_backlog_normalized_event_inserted_count =
                        backlog_summary.normalized_event_inserted_count,
                    "renewed live raw payload adapter sync permit after fenced normalized replay handoff"
                );
            }
            Ok(true)
        }
        Ok(ReplayHandoffLatchStatus::AwaitingReplay) => {
            warn!(
                service = "indexer",
                command = "poll",
                deployment_profile,
                "raw input changed through a normalized replay target before handoff; waiting for replay catch-up"
            );
            Ok(false)
        }
        Ok(ReplayHandoffLatchStatus::AwaitingBacklog) => {
            info!(
                service = "indexer",
                command = "poll",
                deployment_profile,
                "raw input changed in the post-replay range before handoff; retrying adapter backlog"
            );
            Ok(false)
        }
        Err(error) => Err(error).context("failed to commit fenced normalized replay handoff"),
    }
}

pub(crate) async fn latch_replay_handoff_if_stable(
    pool: &sqlx::PgPool,
    deployment_profile: &str,
    provider_configured_chains: &[String],
    live_adapter_sync_latched: &mut bool,
) -> Result<ReplayHandoffLatchStatus> {
    *live_adapter_sync_latched = false;
    #[cfg(test)]
    let test_database = bigname_test_support::current_test_database(pool)
        .await
        .expect("replay handoff test hook must identify its database before fencing");
    let mut guard =
        acquire_raw_log_staging_read_set_guard(pool, provider_configured_chains).await?;
    let validation = async {
        let mut status = ReplayHandoffLatchStatus::Latched;
        for chain in provider_configured_chains {
            status =
                match validate_chain_handoff_while_guarded(&mut guard, deployment_profile, chain)
                    .await?
                {
                    BacklogHandoffStatus::Ready => status,
                    BacklogHandoffStatus::AwaitingReplay => {
                        ReplayHandoffLatchStatus::AwaitingReplay
                    }
                    BacklogHandoffStatus::AwaitingBacklog
                        if status != ReplayHandoffLatchStatus::AwaitingReplay =>
                    {
                        ReplayHandoffLatchStatus::AwaitingBacklog
                    }
                    BacklogHandoffStatus::AwaitingBacklog => status,
                };
        }
        if status == ReplayHandoffLatchStatus::Latched {
            #[cfg(test)]
            test_hook::pause_before_latch(&test_database, deployment_profile).await;
            *live_adapter_sync_latched = true;
        }
        Ok(status)
    }
    .await;
    let release = guard.release().await;
    finish_replay_handoff_latch(live_adapter_sync_latched, validation, release)
}

fn finish_replay_handoff_latch(
    live_adapter_sync_latched: &mut bool,
    validation: Result<ReplayHandoffLatchStatus>,
    release: Result<()>,
) -> Result<ReplayHandoffLatchStatus> {
    if validation.is_err() || release.is_err() {
        *live_adapter_sync_latched = false;
    }
    crate::reconciliation::guard_release::prioritize_operation_error(validation, release)
}

fn prepare_handoff_plan_reload(
    forced_handoff_plan_reload_complete: bool,
    watched_plan_admission_epochs: &mut Option<BTreeMap<String, i64>>,
) {
    if !forced_handoff_plan_reload_complete {
        *watched_plan_admission_epochs = None;
    }
}

#[cfg(test)]
pub(super) fn live_poll_adapter_sync_after_handoff_attempt(
    adapter_sync_enabled: bool,
    backlog_sync_failed: bool,
    discovery_refresh_failed: bool,
) -> bool {
    adapter_sync_enabled && !backlog_sync_failed && !discovery_refresh_failed
}

pub(super) fn manifest_refresh_adapter_sync_before_handoff_readiness(
    adapter_sync_on_live_poll: bool,
    adapter_sync_on_manifest_refresh: bool,
    _previous_replay_handoff_permit: bool,
) -> bool {
    adapter_sync_on_live_poll || adapter_sync_on_manifest_refresh
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};

    use anyhow::anyhow;

    use super::{
        ReplayHandoffLatchStatus, ReplayHandoffReadiness, classify_replay_handoff_readiness,
        finish_replay_handoff_latch, live_poll_adapter_sync_after_handoff_attempt,
        manifest_refresh_adapter_sync_before_handoff_readiness, prepare_handoff_plan_reload,
        resolver_profile_convergence_before_handoff,
    };

    #[test]
    fn replay_handoff_scopes_raw_polling_to_complete_chains() {
        let readiness = classify_replay_handoff_readiness(vec![
            ("ethereum-mainnet".to_owned(), Ok(true)),
            ("base-mainnet".to_owned(), Ok(false)),
            ("testnet".to_owned(), Err(anyhow!("database timeout"))),
        ]);

        assert_eq!(
            readiness,
            ReplayHandoffReadiness::AwaitingChains {
                raw_poll_chains: BTreeSet::from(["ethereum-mainnet".to_owned()]),
            }
        );
    }

    #[test]
    fn replay_handoff_becomes_global_after_every_chain_completes() {
        let readiness = classify_replay_handoff_readiness(vec![
            ("ethereum-mainnet".to_owned(), Ok(true)),
            ("base-mainnet".to_owned(), Ok(true)),
            ("testnet".to_owned(), Ok(true)),
        ]);

        assert_eq!(readiness, ReplayHandoffReadiness::AllChainsComplete);
    }

    #[test]
    fn live_adapter_sync_requires_backlog_and_discovery_handoff() {
        assert!(!live_poll_adapter_sync_after_handoff_attempt(
            true, true, false
        ));
        assert!(!live_poll_adapter_sync_after_handoff_attempt(
            true, false, true
        ));
        assert!(live_poll_adapter_sync_after_handoff_attempt(
            true, false, false
        ));
    }

    #[test]
    fn prior_handoff_permit_cannot_authorize_manifest_refresh_adapter_work() {
        assert!(!manifest_refresh_adapter_sync_before_handoff_readiness(
            false, false, true,
        ));
        assert!(manifest_refresh_adapter_sync_before_handoff_readiness(
            true, false, false,
        ));
        assert!(manifest_refresh_adapter_sync_before_handoff_readiness(
            false, true, false,
        ));
    }

    #[test]
    fn handoff_forces_one_plan_reload_preserves_epoch_gate_and_defers_convergence() {
        assert!(!resolver_profile_convergence_before_handoff());
        let epochs = BTreeMap::from([("base-mainnet".to_owned(), 7)]);
        let mut sentinel = Some(epochs.clone());
        prepare_handoff_plan_reload(false, &mut sentinel);
        assert!(sentinel.is_none(), "the transition must force one reload");
        sentinel = Some(epochs.clone());
        prepare_handoff_plan_reload(true, &mut sentinel);
        assert_eq!(
            sentinel,
            Some(epochs),
            "steady-state permit renewal must retain the shared epoch gate"
        );
    }

    #[test]
    fn handoff_release_failure_clears_the_in_memory_latch() {
        let mut latched = true;
        let error = finish_replay_handoff_latch(
            &mut latched,
            Ok(ReplayHandoffLatchStatus::Latched),
            Err(anyhow!("commit failed")),
        )
        .expect_err("a guard release failure must fail the handoff");

        assert!(!latched);
        assert!(error.to_string().contains("commit failed"));
    }
}
