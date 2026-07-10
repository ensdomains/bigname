use anyhow::{Context, Result};
use tracing::{info, warn};

use crate::provider::ProviderRegistry;

use super::super::intake::{
    IntakeChainTask, intake_runtime_state, validate_provider_registry_for_intake_tasks,
    watched_chain_plan_state,
};
use super::super::logging::{
    log_intake_chain_tasks, log_provider_registry, log_watched_chain_plan,
    log_watched_contract_summary,
};
use super::super::manifest::ManifestRuntimeState;
use super::super::refresh::refresh_runtime_state_from_storage_discovery;

/// Timer-driven stored-discovery refresh of the runtime watch state. A
/// successful refresh replaces the manifest runtime state and intake tasks in
/// place; refresh failures only warn and keep the last successful state, while
/// a provider-registry mismatch aborts the poll loop.
pub(super) async fn refresh_discovery_watch_state(
    pool: &sqlx::PgPool,
    provider_registry: &ProviderRegistry,
    manifest_runtime_state: &mut ManifestRuntimeState,
    intake_chain_tasks: &mut Vec<IntakeChainTask>,
) -> Result<()> {
    match refresh_runtime_state_from_storage_discovery(pool, manifest_runtime_state).await {
        Ok(Some((next_manifest_runtime_state, next_tasks))) => {
            validate_provider_registry_for_intake_tasks(&next_tasks, provider_registry).context(
                "refreshed stored discovery state no longer matches configured provider sources",
            )?;
            let previous_watch_state =
                watched_chain_plan_state(&manifest_runtime_state.watched_chain_plan);
            let next_watch_state =
                watched_chain_plan_state(&next_manifest_runtime_state.watched_chain_plan);
            let previous_intake_state = intake_runtime_state(intake_chain_tasks);
            let next_intake_state = intake_runtime_state(&next_tasks);

            info!(
                service = "indexer",
                refresh_reason = "timer",
                watched_plan_changed = true,
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
                "runtime watched chain plan changed after stored discovery sync"
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
        }
        Ok(None) => {}
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
        }
    }

    Ok(())
}
