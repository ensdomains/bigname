use std::collections::BTreeMap;

use anyhow::{Context, Result};
use tracing::{info, warn};

use crate::provider::ProviderRegistry;
use crate::resolver_profile_convergence::{
    ResolverProfileConvergenceSummary, drain_resolver_profile_input_changes,
};

use super::super::adapter_sync::sync_adapter_owned_raw_log_state;
use super::super::intake::{
    IntakeChainTask, intake_runtime_state, validate_provider_registry_for_intake_tasks,
    watched_chain_plan_state,
};
use super::super::logging::{
    log_intake_chain_tasks, log_provider_registry, log_watched_chain_plan,
    log_watched_contract_summary,
};
use super::super::manifest::ManifestRuntimeState;
use super::super::refresh::refresh_runtime_state_from_stored_discovery_when_epochs_move;

/// Timer-driven stored-discovery refresh of the runtime watch state. A
/// successful refresh replaces the manifest runtime state and intake tasks in
/// place. Storage refresh failures warn, keep the last successful state, and
/// return `false` so callers that must not poll with stale tasks can retry on a
/// later tick. A provider-registry mismatch aborts the poll loop.
///
/// `sync_adapter_state_before_refresh` re-derives discovery edges from the
/// whole stored raw-log corpus before reloading the plan. Live poll writes
/// discovery edges per block, so the tailer only needs the reload; the full
/// re-derivation stays opt-in for broad runtime refresh.
///
/// `last_admission_epochs` is the change-detection sentinel held across ticks:
/// the plan reload only runs when a chain's discovery admission epoch has
/// moved since the last successful reload, so a quiet watched surface costs
/// one tiny sentinel read per tick instead of a full plan scan.
///
/// `resolver_profile_convergence_enabled` is false for raw-only operation, so
/// reloading a stored plan cannot drain adapter-owned resolver-profile work in
/// a mode that explicitly defers those writes.
pub(crate) async fn refresh_discovery_watch_state(
    pool: &sqlx::PgPool,
    provider_registry: &ProviderRegistry,
    manifest_runtime_state: &mut ManifestRuntimeState,
    intake_chain_tasks: &mut Vec<IntakeChainTask>,
    sync_adapter_state_before_refresh: bool,
    resolver_profile_convergence_enabled: bool,
    last_admission_epochs: &mut Option<BTreeMap<String, i64>>,
) -> Result<bool> {
    // The whole-corpus re-derivation must run before the sentinel read: it is
    // what materializes new edges (and bumps epochs) on the broad-refresh path.
    let adapter_sync_result: Result<()> = if sync_adapter_state_before_refresh {
        sync_adapter_owned_raw_log_state(pool, &manifest_runtime_state.watched_chain_plan).await
    } else {
        Ok(())
    };
    let refreshed_state = match adapter_sync_result {
        Ok(()) => {
            refresh_runtime_state_from_stored_discovery_when_epochs_move(
                pool,
                manifest_runtime_state,
                last_admission_epochs.as_ref(),
            )
            .await
        }
        Err(error) => Err(error),
    };
    if refreshed_state.is_ok()
        && resolver_profile_convergence_enabled
        && !resolver_profile_drain_succeeded(
            drain_resolver_profile_input_changes(pool).await,
            "timer",
            "stored_discovery_state",
        )
    {
        return Ok(false);
    }
    match refreshed_state {
        Ok(Some(refresh)) => {
            let Some((next_manifest_runtime_state, next_tasks)) = refresh.refreshed_state else {
                *last_admission_epochs = Some(refresh.admission_epochs);
                return Ok(true);
            };
            validate_provider_registry_for_intake_tasks(&next_tasks, provider_registry).context(
                "refreshed stored discovery state no longer matches configured provider sources",
            )?;
            let previous_watch_state =
                watched_chain_plan_state(&manifest_runtime_state.watched_chain_plan);
            let next_watch_state =
                watched_chain_plan_state(&next_manifest_runtime_state.watched_chain_plan);
            let watched_plan_changed = manifest_runtime_state.watched_chain_plan
                != next_manifest_runtime_state.watched_chain_plan;
            let previous_intake_state = intake_runtime_state(intake_chain_tasks);
            let next_intake_state = intake_runtime_state(&next_tasks);

            info!(
                service = "indexer",
                refresh_reason = "timer",
                watched_plan_changed,
                checkpoint_state_changed = false,
                plan_source = "stored_discovery_state",
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
                intake_safe_checkpoint_chain_count = next_intake_state.safe_checkpoint_chain_count,
                intake_finalized_checkpoint_chain_count =
                    next_intake_state.finalized_checkpoint_chain_count,
                "runtime watched state changed after stored discovery sync"
            );
            log_watched_contract_summary(&next_manifest_runtime_state.watched_contract_summary);
            log_watched_chain_plan(
                "discovery-refresh",
                &next_manifest_runtime_state.watched_chain_plan,
            );
            log_intake_chain_tasks("discovery-refresh", &next_tasks);
            log_provider_registry("discovery-refresh", &next_tasks, provider_registry);
            *manifest_runtime_state = next_manifest_runtime_state;
            *intake_chain_tasks = next_tasks;
            *last_admission_epochs = Some(refresh.admission_epochs);
            Ok(true)
        }
        Ok(None) => Ok(true),
        Err(error) => {
            let current_watch_state =
                watched_chain_plan_state(&manifest_runtime_state.watched_chain_plan);
            let current_intake_state = intake_runtime_state(intake_chain_tasks);
            warn!(
                service = "indexer",
                refresh_reason = "timer",
                plan_source = "stored_discovery_state",
                error = ?error,
                watched_chain_count = current_watch_state.chain_count,
                watched_address_count = current_watch_state.address_count,
                watched_entry_count_total = current_watch_state.entry_count,
                intake_chain_count = current_intake_state.chain_count,
                intake_address_count = current_intake_state.address_count,
                intake_entry_count_total = current_intake_state.entry_count,
                "failed to refresh runtime watch state from stored discovery edges; keeping last successful state"
            );
            Ok(false)
        }
    }
}

pub(super) fn resolver_profile_drain_succeeded(
    result: Result<ResolverProfileConvergenceSummary>,
    refresh_reason: &'static str,
    plan_source: &'static str,
) -> bool {
    match result {
        Ok(_) => true,
        Err(error) => {
            warn!(
                service = "indexer",
                command = "resolver-profile-convergence",
                refresh_reason,
                plan_source,
                error = ?error,
                "failed to drain resolver-profile input changes; durable pending work will be retried on a later poll tick"
            );
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use anyhow::anyhow;

    use super::resolver_profile_drain_succeeded;
    use crate::resolver_profile_convergence::ResolverProfileConvergenceSummary;

    #[test]
    fn poll_loop_retries_transient_resolver_profile_drain_errors() {
        assert!(!resolver_profile_drain_succeeded(
            Err(anyhow!("transient database timeout")),
            "timer",
            "test",
        ));
    }

    #[test]
    fn poll_loop_accepts_successful_resolver_profile_drain() {
        assert!(resolver_profile_drain_succeeded(
            Ok(ResolverProfileConvergenceSummary::default()),
            "timer",
            "test",
        ));
    }
}
