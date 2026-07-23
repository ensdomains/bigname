use std::{collections::BTreeMap, path::PathBuf, time::Duration};

use anyhow::{Context, Result};
use tracing::{info, warn};

use crate::provider::{ProviderBlock, ProviderRegistry};
use crate::reconciliation::{
    ChainCoverageFrontiers, HeaderAuditMode, poll_provider_heads_with_adapter_sync_and_progress,
};
use crate::replay::deployment_profile_from_manifest_root;
use crate::resolver_profile_convergence::drain_resolver_profile_input_changes_with_progress;
use crate::run::startup_heartbeat::{
    RequiredSubtaskActivity, StartupAdapterHeartbeat, StartupHeartbeat,
};

use super::adapter_sync::sync_adapter_owned_raw_log_state_with_heartbeat;
use super::intake::{
    IntakeChainTask, intake_runtime_state, sync_intake_chain_tasks_with_progress,
    validate_provider_registry_for_intake_tasks, watched_chain_plan_state,
    watched_chain_plans_equal_with_progress,
};
use super::logging::{
    log_intake_chain_tasks, log_manifest_normalized_event_summary, log_manifest_runtime_state,
    log_manifest_summary, log_provider_registry, log_watched_chain_plan,
};
use super::manifest::{
    ManifestRuntimeState, RuntimeWatchScope,
    build_manifest_runtime_state_for_repository_refresh_with_progress, ensure_manifest_root_ready,
    load_manifest_repository,
};
use super::refresh::{
    refresh_intake_chain_tasks_with_progress,
    refresh_manifest_normalized_events_from_storage_with_progress,
};

#[path = "poll_loop/discovery_refresh.rs"]
mod discovery_refresh;
#[path = "poll_loop/replay_handoff.rs"]
mod replay_handoff;

#[cfg(test)]
pub(crate) use discovery_refresh::refresh_discovery_watch_state;
use discovery_refresh::refresh_discovery_watch_state_with_heartbeat;
#[cfg(test)]
pub(crate) use replay_handoff::{
    ReplayHandoffLatchStatus, install_replay_handoff_before_latch_test_hook,
    latch_replay_handoff_if_stable,
};
use replay_handoff::{
    manifest_refresh_adapter_sync_before_handoff_readiness, renew_live_poll_adapter_sync_permit,
};
#[expect(clippy::too_many_arguments)]
pub(crate) async fn run_poll_loop(
    pool: &sqlx::PgPool,
    heartbeat: &mut StartupHeartbeat,
    manifests_root: PathBuf,
    mut manifest_runtime_state: ManifestRuntimeState,
    mut intake_chain_tasks: Vec<IntakeChainTask>,
    initial_watched_plan_admission_epochs: BTreeMap<String, i64>,
    provider_registry: &ProviderRegistry,
    poll_interval_secs: u64,
    adapter_sync_page_logs: usize,
    runtime_watch_scope: RuntimeWatchScope,
    adapter_sync_on_manifest_refresh: bool,
    adapter_sync_on_live_poll: bool,
    adapter_sync_on_live_poll_after_normalized_replay_catchup: bool,
    manifest_observation_refresh_enabled: bool,
    discovery_refresh_enabled: bool,
    resolver_profile_convergence_enabled: bool,
    resync_adapter_owned_state_on_discovery_refresh: bool,
    header_audit_mode: HeaderAuditMode,
    event_silent_reverse_resolver_addresses: Vec<String>,
    latched_bootstrap_finalized_heads: BTreeMap<String, ProviderBlock>,
    coverage_frontiers: &ChainCoverageFrontiers,
    required_subtask_activity: &RequiredSubtaskActivity,
) -> Result<()> {
    let deployment_profile = deployment_profile_from_manifest_root(&manifests_root);
    let mut live_poll_adapter_sync_restored_after_replay = false;
    let mut forced_handoff_plan_reload_complete = false;
    // Change-detection sentinel for the per-tick stored watch-plan reload:
    // holds the discovery admission epochs the current plan was loaded under.
    let mut watched_plan_admission_epochs = Some(initial_watched_plan_admission_epochs);
    let mut interval = tokio::time::interval(Duration::from_secs(poll_interval_secs));
    interval.tick().await;

    loop {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                info!(service = "indexer", "shutdown signal received");
                return Ok(());
            }
            _ = interval.tick() => {
                let Some(_required_subtask_exclusion) = required_subtask_activity
                    .exclude_required_subtask_or_shutdown(tokio::signal::ctrl_c())
                    .await
                else {
                    info!(service = "indexer", "shutdown signal received");
                    return Ok(());
                };
                let heartbeat_chains = intake_chain_tasks
                    .iter()
                    .map(|task| task.chain.clone())
                    .collect::<Vec<_>>();
                heartbeat.record(pool, &heartbeat_chains).await?;

                match load_manifest_repository(&manifests_root) {
                    Ok(manifest_repository) => {
                        let manifest_summary = manifest_repository.summary().clone();
                        if manifest_summary != manifest_runtime_state.manifest_summary {
                            log_manifest_summary(&manifest_summary);
                        }

                        if !manifest_runtime_state.repository_refresh_needed(&manifest_repository) {
                        } else if let Err(error) = ensure_manifest_root_ready(&manifest_summary) {
                            let current_watch_state =
                                watched_chain_plan_state(&manifest_runtime_state.watched_chain_plan);
                            let current_intake_state = intake_runtime_state(&intake_chain_tasks);
                            warn!(
                                service = "indexer",
                                refresh_reason = "timer",
                                plan_source = "repository_manifest_reload",
                                error = ?error,
                                manifests_root = %manifest_summary.root.display(),
                                manifests_status = manifest_summary.status.as_str(),
                                watched_chain_count = current_watch_state.chain_count,
                                watched_address_count = current_watch_state.address_count,
                                watched_entry_count_total = current_watch_state.entry_count,
                                intake_chain_count = current_intake_state.chain_count,
                                intake_address_count = current_intake_state.address_count,
                                intake_entry_count_total = current_intake_state.entry_count,
                                "failed to reload repository manifests; keeping last successful runtime state"
                            );
                        } else {
                            let refresh_result = {
                                let mut progress =
                                    StartupAdapterHeartbeat::new(heartbeat, &heartbeat_chains);
                                build_manifest_runtime_state_for_repository_refresh_with_progress(
                                    pool,
                                    &manifest_repository,
                                    runtime_watch_scope,
                                    adapter_sync_on_manifest_refresh,
                                    &mut progress,
                                )
                                .await
                            };
                            match refresh_result {
                                Ok(next_manifest_runtime_state) => {
                                    let watched_plan_changed = {
                                        let mut progress = StartupAdapterHeartbeat::new(
                                            heartbeat,
                                            &heartbeat_chains,
                                        );
                                        !watched_chain_plans_equal_with_progress(
                                            pool,
                                            &next_manifest_runtime_state.watched_chain_plan,
                                            &manifest_runtime_state.watched_chain_plan,
                                            &mut progress,
                                        )
                                        .await?
                                    };
                                    let manifest_state_changed = watched_plan_changed
                                        || manifest_runtime_metadata_changed(
                                            &next_manifest_runtime_state,
                                            &manifest_runtime_state,
                                        );

                                    if adapter_sync_on_manifest_refresh
                                        && (manifest_state_changed || watched_plan_changed)
                                        && let Err(error) = sync_adapter_owned_raw_log_state_with_heartbeat(
                                            pool,
                                            &deployment_profile,
                                            &next_manifest_runtime_state.watched_chain_plan,
                                            adapter_sync_page_logs,
                                            heartbeat,
                                            &heartbeat_chains,
                                        )
                                        .await
                                    {
                                        let current_watch_state = watched_chain_plan_state(
                                            &manifest_runtime_state.watched_chain_plan,
                                        );
                                        let current_intake_state =
                                            intake_runtime_state(&intake_chain_tasks);
                                        warn!(
                                            service = "indexer",
                                            refresh_reason = "timer",
                                            plan_source = "repository_manifest_reload",
                                            error = ?error,
                                            watched_chain_count = current_watch_state.chain_count,
                                            watched_address_count = current_watch_state.address_count,
                                            watched_entry_count_total = current_watch_state.entry_count,
                                            intake_chain_count = current_intake_state.chain_count,
                                            intake_address_count = current_intake_state.address_count,
                                            intake_entry_count_total = current_intake_state.entry_count,
                                            "failed to sync adapter-owned raw-log state after repository manifest refresh; keeping last successful runtime state"
                                        );
                                        continue;
                                    }
                                    if manifest_refresh_adapter_sync_before_handoff_readiness(
                                        adapter_sync_on_live_poll,
                                        adapter_sync_on_manifest_refresh,
                                        live_poll_adapter_sync_restored_after_replay,
                                    )
                                        && !{
                                            let mut progress = StartupAdapterHeartbeat::new(
                                                heartbeat,
                                                &heartbeat_chains,
                                            );
                                            discovery_refresh::resolver_profile_drain_succeeded(
                                                drain_resolver_profile_input_changes_with_progress(
                                                    pool,
                                                    &mut progress,
                                                )
                                                .await,
                                                "timer",
                                                "repository_manifest_reload",
                                            )
                                        }
                                    {
                                        continue;
                                    }

                                    if manifest_state_changed {
                                        let previous_watch_state = watched_chain_plan_state(
                                            &manifest_runtime_state.watched_chain_plan,
                                        );
                                        let next_watch_state = watched_chain_plan_state(
                                            &next_manifest_runtime_state.watched_chain_plan,
                                        );
                                        info!(
                                            service = "indexer",
                                            refresh_reason = "timer",
                                            plan_source = "repository_manifest_reload",
                                            manifest_state_changed = true,
                                            watched_plan_changed,
                                            previous_manifest_count = manifest_runtime_state.manifest_summary.manifest_count,
                                            manifest_count = next_manifest_runtime_state.manifest_summary.manifest_count,
                                            previous_active_manifest_count = manifest_runtime_state.discovery_admission.active_manifest_count,
                                            stored_active_manifest_count = next_manifest_runtime_state.discovery_admission.active_manifest_count,
                                            previous_watched_chain_count = previous_watch_state.chain_count,
                                            previous_watched_address_count = previous_watch_state.address_count,
                                            previous_watched_entry_count_total = previous_watch_state.entry_count,
                                            watched_chain_count = next_watch_state.chain_count,
                                            watched_address_count = next_watch_state.address_count,
                                            watched_entry_count_total = next_watch_state.entry_count,
                                            "repository manifest refresh changed stored runtime state"
                                        );
                                        log_manifest_runtime_state(&next_manifest_runtime_state);
                                    }

                                    if watched_plan_changed {
                                        let next_tasks_result = {
                                            let mut progress = StartupAdapterHeartbeat::new(
                                                heartbeat,
                                                &heartbeat_chains,
                                            );
                                            sync_intake_chain_tasks_with_progress(
                                                pool,
                                                &next_manifest_runtime_state.watched_chain_plan,
                                                &mut progress,
                                            )
                                            .await
                                        };
                                        match next_tasks_result {
                                            Ok(next_tasks) => {
                                                validate_provider_registry_for_intake_tasks(
                                                    &next_tasks,
                                                    provider_registry,
                                                )
                                                .context(
                                                    "refreshed repository manifest state no longer matches configured provider sources",
                                                )?;
                                                let previous_watch_state = watched_chain_plan_state(
                                                    &manifest_runtime_state.watched_chain_plan,
                                                );
                                                let next_watch_state = watched_chain_plan_state(
                                                    &next_manifest_runtime_state.watched_chain_plan,
                                                );
                                                let previous_intake_state =
                                                    intake_runtime_state(&intake_chain_tasks);
                                                let next_intake_state =
                                                    intake_runtime_state(&next_tasks);

                                                info!(
                                                    service = "indexer",
                                                    refresh_reason = "timer",
                                                    watched_plan_changed = true,
                                                    plan_source = "repository_manifest_reload",
                                                    previous_watched_chain_count = previous_watch_state.chain_count,
                                                    previous_watched_address_count = previous_watch_state.address_count,
                                                    previous_watched_entry_count_total = previous_watch_state.entry_count,
                                                    watched_chain_count = next_watch_state.chain_count,
                                                    watched_address_count = next_watch_state.address_count,
                                                    watched_entry_count_total = next_watch_state.entry_count,
                                                    previous_intake_chain_count = previous_intake_state.chain_count,
                                                    previous_intake_address_count = previous_intake_state.address_count,
                                                    previous_intake_entry_count_total = previous_intake_state.entry_count,
                                                    intake_chain_count = next_intake_state.chain_count,
                                                    intake_address_count = next_intake_state.address_count,
                                                    intake_entry_count_total = next_intake_state.entry_count,
                                                    intake_cold_start_chain_count = next_intake_state.cold_start_chain_count,
                                                    intake_resumable_chain_count = next_intake_state.resumable_chain_count,
                                                    "runtime watched chain plan changed after repository manifest refresh"
                                                );
                                                log_watched_chain_plan(
                                                    "refresh",
                                                    &next_manifest_runtime_state.watched_chain_plan,
                                                );
                                                log_intake_chain_tasks("refresh", &next_tasks);
                                                log_provider_registry(
                                                    "refresh",
                                                    &next_tasks,
                                                    provider_registry,
                                                );
                                                manifest_runtime_state = next_manifest_runtime_state;
                                                intake_chain_tasks = next_tasks;
                                            }
                                            Err(error) => {
                                                let current_watch_state = watched_chain_plan_state(
                                                    &manifest_runtime_state.watched_chain_plan,
                                                );
                                                let current_intake_state =
                                                    intake_runtime_state(&intake_chain_tasks);
                                                warn!(
                                                    service = "indexer",
                                                    refresh_reason = "timer",
                                                    plan_source = "repository_manifest_reload",
                                                    error = ?error,
                                                    watched_chain_count = current_watch_state.chain_count,
                                                    watched_address_count = current_watch_state.address_count,
                                                    watched_entry_count_total = current_watch_state.entry_count,
                                                    intake_chain_count = current_intake_state.chain_count,
                                                    intake_address_count = current_intake_state.address_count,
                                                    intake_entry_count_total = current_intake_state.entry_count,
                                                    "failed to sync intake chain tasks for a changed watch plan after repository manifest refresh; keeping last successful runtime state"
                                                );
                                            }
                                        }
                                    } else {
                                        manifest_runtime_state = next_manifest_runtime_state;
                                    }
                                }
                                Err(error) => {
                                    let current_watch_state = watched_chain_plan_state(
                                        &manifest_runtime_state.watched_chain_plan,
                                    );
                                    let current_intake_state = intake_runtime_state(&intake_chain_tasks);
                                    warn!(
                                        service = "indexer",
                                        refresh_reason = "timer",
                                        plan_source = "repository_manifest_reload",
                                        error = ?error,
                                        watched_chain_count = current_watch_state.chain_count,
                                        watched_address_count = current_watch_state.address_count,
                                        watched_entry_count_total = current_watch_state.entry_count,
                                        intake_chain_count = current_intake_state.chain_count,
                                        intake_address_count = current_intake_state.address_count,
                                        intake_entry_count_total = current_intake_state.entry_count,
                                        "failed to sync repository manifests into storage during refresh; keeping last successful runtime state"
                                    );
                                }
                            }
                        }
                    }
                    Err(error) => {
                        let current_watch_state =
                            watched_chain_plan_state(&manifest_runtime_state.watched_chain_plan);
                        let current_intake_state = intake_runtime_state(&intake_chain_tasks);
                        warn!(
                            service = "indexer",
                            refresh_reason = "timer",
                            plan_source = "repository_manifest_reload",
                            error = ?error,
                            manifests_root = %manifests_root.display(),
                            watched_chain_count = current_watch_state.chain_count,
                            watched_address_count = current_watch_state.address_count,
                            watched_entry_count_total = current_watch_state.entry_count,
                            intake_chain_count = current_intake_state.chain_count,
                            intake_address_count = current_intake_state.address_count,
                            intake_entry_count_total = current_intake_state.entry_count,
                            "failed to load repository manifests during refresh; keeping last successful runtime state"
                        );
                    }
                }

                let intake_refresh_result = {
                    let mut progress =
                        StartupAdapterHeartbeat::new(heartbeat, &heartbeat_chains);
                    refresh_intake_chain_tasks_with_progress(
                        pool,
                        &intake_chain_tasks,
                        &manifest_runtime_state.watched_chain_plan,
                        &mut progress,
                    )
                    .await
                };
                match intake_refresh_result {
                    Ok(Some(next_tasks)) => {
                        let previous_state = intake_runtime_state(&intake_chain_tasks);
                        let next_state = intake_runtime_state(&next_tasks);
                        info!(
                            service = "indexer",
                            refresh_reason = "timer",
                            watched_plan_changed = false,
                            checkpoint_state_changed = true,
                            plan_source = "stored_manifest_state",
                            previous_intake_chain_count = previous_state.chain_count,
                            previous_intake_address_count = previous_state.address_count,
                            previous_intake_entry_count_total = previous_state.entry_count,
                            previous_intake_cold_start_chain_count = previous_state.cold_start_chain_count,
                            previous_intake_resumable_chain_count = previous_state.resumable_chain_count,
                            intake_chain_count = next_state.chain_count,
                            intake_address_count = next_state.address_count,
                            intake_entry_count_total = next_state.entry_count,
                            intake_cold_start_chain_count = next_state.cold_start_chain_count,
                            intake_resumable_chain_count = next_state.resumable_chain_count,
                            intake_safe_checkpoint_chain_count = next_state.safe_checkpoint_chain_count,
                            intake_finalized_checkpoint_chain_count = next_state.finalized_checkpoint_chain_count,
                            "persisted checkpoint state changed for active intake chains"
                        );
                        log_intake_chain_tasks("checkpoint-refresh", &next_tasks);
                        intake_chain_tasks = next_tasks;
                    }
                    Ok(None) => {}
                    Err(error) => {
                        let current_watch_state =
                            watched_chain_plan_state(&manifest_runtime_state.watched_chain_plan);
                        let current_intake_state = intake_runtime_state(&intake_chain_tasks);
                        warn!(
                            service = "indexer",
                            refresh_reason = "timer",
                            plan_source = "stored_manifest_state",
                            error = ?error,
                            watched_chain_count = current_watch_state.chain_count,
                            watched_address_count = current_watch_state.address_count,
                            watched_entry_count_total = current_watch_state.entry_count,
                            intake_chain_count = current_intake_state.chain_count,
                            intake_address_count = current_intake_state.address_count,
                            intake_entry_count_total = current_intake_state.entry_count,
                            "failed to refresh runtime intake chain tasks; keeping last successful state"
                        );
                    }
                }

                let provider_configured_chains =
                    if adapter_sync_on_live_poll_after_normalized_replay_catchup {
                        intake_chain_tasks
                            .iter()
                            .filter(|task| provider_registry.provider_for(&task.chain).is_some())
                            .map(|task| task.chain.clone())
                            .collect::<Vec<_>>()
                    } else {
                        Vec::new()
                    };
                let replay_handoff_required =
                    adapter_sync_on_live_poll_after_normalized_replay_catchup
                        && !adapter_sync_on_live_poll
                        && !provider_configured_chains.is_empty();
                let mut effective_adapter_sync_on_live_poll = adapter_sync_on_live_poll;
                if replay_handoff_required {
                    effective_adapter_sync_on_live_poll = renew_live_poll_adapter_sync_permit(
                        pool,
                        provider_registry,
                        &mut manifest_runtime_state,
                        &mut intake_chain_tasks,
                        &deployment_profile,
                        &provider_configured_chains,
                        &mut live_poll_adapter_sync_restored_after_replay,
                        &mut forced_handoff_plan_reload_complete,
                        &mut watched_plan_admission_epochs,
                        header_audit_mode,
                        &event_silent_reverse_resolver_addresses,
                        coverage_frontiers,
                        &latched_bootstrap_finalized_heads,
                        adapter_sync_page_logs,
                        heartbeat,
                        &heartbeat_chains,
                    )
                    .await?;
                    if !effective_adapter_sync_on_live_poll {
                        continue;
                    }
                }

                let loaded_plan_admission_epochs = watched_plan_admission_epochs
                    .as_ref()
                    .context("live watch plan is missing its loaded admission-epoch snapshot")?;
                {
                    let mut progress =
                        StartupAdapterHeartbeat::new(heartbeat, &heartbeat_chains);
                    poll_provider_heads_with_adapter_sync_and_progress(
                        pool,
                        &mut intake_chain_tasks,
                        provider_registry,
                        &deployment_profile,
                        loaded_plan_admission_epochs,
                        effective_adapter_sync_on_live_poll,
                        header_audit_mode,
                        &event_silent_reverse_resolver_addresses,
                        coverage_frontiers,
                        &latched_bootstrap_finalized_heads,
                        &mut progress,
                    )
                    .await?;
                }

                if manifest_observation_refresh_enabled {
                    let refresh_result = {
                        let mut progress =
                            StartupAdapterHeartbeat::new(heartbeat, &heartbeat_chains);
                        refresh_manifest_normalized_events_from_storage_with_progress(
                            pool,
                            &manifest_runtime_state,
                            &mut progress,
                        )
                        .await
                    };
                    match refresh_result {
                        Ok(Some(next_manifest_runtime_state)) => {
                            info!(
                                service = "indexer",
                                refresh_reason = "timer",
                                plan_source = "stored_manifest_observations",
                                normalized_event_inserted_total_count = next_manifest_runtime_state
                                    .manifest_normalized_event_summary
                                    .total_inserted_count,
                                normalized_event_sync_total_count = next_manifest_runtime_state
                                    .manifest_normalized_event_summary
                                    .total_synced_count,
                                normalized_event_kind_count = next_manifest_runtime_state
                                    .manifest_normalized_event_summary
                                    .by_kind
                                    .len(),
                                "manifest observation alert events changed after provider polling"
                            );
                            log_manifest_normalized_event_summary(
                                &next_manifest_runtime_state.manifest_normalized_event_summary,
                            );
                            manifest_runtime_state = next_manifest_runtime_state;
                        }
                        Ok(None) => {}
                        Err(error) => {
                            let current_watch_state =
                                watched_chain_plan_state(&manifest_runtime_state.watched_chain_plan);
                            let current_intake_state = intake_runtime_state(&intake_chain_tasks);
                            warn!(
                                service = "indexer",
                                refresh_reason = "timer",
                                plan_source = "stored_manifest_observations",
                                error = ?error,
                                watched_chain_count = current_watch_state.chain_count,
                                watched_address_count = current_watch_state.address_count,
                                watched_entry_count_total = current_watch_state.entry_count,
                                intake_chain_count = current_intake_state.chain_count,
                                intake_address_count = current_intake_state.address_count,
                                intake_entry_count_total = current_intake_state.entry_count,
                                "failed to refresh manifest observation alert events after provider polling; keeping last successful state"
                            );
                        }
                    }
                }

                if discovery_refresh_enabled || effective_adapter_sync_on_live_poll {
                    let heartbeat_chains = intake_chain_tasks
                        .iter()
                        .map(|task| task.chain.clone())
                        .collect::<Vec<_>>();
                    refresh_discovery_watch_state_with_heartbeat(
                        pool,
                        provider_registry,
                        &mut manifest_runtime_state,
                        &mut intake_chain_tasks,
                        resync_adapter_owned_state_on_discovery_refresh,
                        resolver_profile_convergence_enabled,
                        &mut watched_plan_admission_epochs,
                        &deployment_profile,
                        adapter_sync_page_logs,
                        heartbeat,
                        &heartbeat_chains,
                    )
                    .await?;
                }
            }
        }
    }
}

fn manifest_runtime_metadata_changed(
    left: &ManifestRuntimeState,
    right: &ManifestRuntimeState,
) -> bool {
    left.manifest_repository != right.manifest_repository
        || left.manifest_summary != right.manifest_summary
        || left.sync_summary != right.sync_summary
        || left.discovery_admission != right.discovery_admission
        || left.manifest_normalized_event_summary != right.manifest_normalized_event_summary
        || left.watched_contract_summary != right.watched_contract_summary
}
