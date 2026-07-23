use std::time::Instant;

use anyhow::{Result, ensure};
use bigname_adapters::StartupAdapterProgress;
use tracing::info;

use crate::runtime::{
    log_ens_v1_subregistry_discovery_sync_summary, log_ens_v1_unwrapped_authority_sync_summary,
    log_ens_v2_registrar_sync_summary, log_ens_v2_registry_resource_surface_sync_summary,
};

use super::replay::NormalizedEventReplayAdapter;
use super::types::PersistedRawPayloadAdapterSyncSummary;
#[path = "adapter_sync/backlog.rs"]
mod backlog;
#[path = "adapter_sync/block_scoped.rs"]
mod block_scoped;
#[path = "adapter_sync/ens_v1_subregistry.rs"]
mod ens_v1_subregistry;
#[path = "adapter_sync/ens_v2_registry.rs"]
mod ens_v2_registry;
#[path = "adapter_sync/ens_v2_tail.rs"]
mod ens_v2_tail;
#[path = "adapter_sync/entrypoints.rs"]
mod entrypoints;
#[path = "adapter_sync/full_closure.rs"]
mod full_closure;
#[path = "adapter_sync/mode.rs"]
mod mode;
#[path = "adapter_sync/progress.rs"]
mod progress;
#[path = "adapter_sync/scope.rs"]
mod scope;
#[path = "adapter_sync/stateless.rs"]
mod stateless;
#[path = "adapter_sync/logging.rs"]
mod sync_logging;
#[cfg(test)]
#[path = "adapter_sync/test_hooks.rs"]
mod test_hooks;
#[cfg(test)]
pub(crate) use backlog::install_backlog_after_adapter_sync_test_hook;
#[cfg(test)]
pub(crate) use backlog::sync_live_adapter_backlog_after_normalized_replay;
pub(crate) use backlog::{
    BacklogHandoffStatus, sync_live_adapter_backlog_after_normalized_replay_with_progress,
    validate_chain_handoff_while_guarded,
};
use block_scoped::{sync_ens_v1_unwrapped_authority_for_scope, sync_ens_v2_registrar_for_scope};
use ens_v1_subregistry::{ens_v1_subregistry_sync_operation, sync_ens_v1_subregistry_for_mode};
#[cfg(test)]
pub(crate) use ens_v2_registry::sync_ens_v2_registry_for_mode as sync_ens_v2_registry_for_mode_for_test;
use ens_v2_registry::{ens_v2_registry_sync_operation, sync_ens_v2_registry_for_mode};
use ens_v2_tail::sync_ens_v2_tail_adapters;
#[allow(unused_imports)]
pub(crate) use entrypoints::{
    sync_adapter_state_from_persisted_raw_payloads,
    sync_adapter_state_from_scoped_persisted_raw_payloads,
    sync_live_adapter_state_from_persisted_raw_payloads,
    sync_live_adapter_state_from_persisted_raw_payloads_after_reorg,
    sync_live_adapter_state_from_persisted_raw_payloads_after_reorg_with_progress,
    sync_live_adapter_state_from_persisted_raw_payloads_with_progress,
    sync_replay_normalized_events_from_persisted_raw_payloads_with_progress,
};
pub(crate) use full_closure::{
    AutomaticTwoPhaseFullClosureSyncResult, automatic_stateless_replay_completed,
    sync_automatic_two_phase_full_closure_normalized_events,
    sync_manual_full_closure_normalized_events_from_persisted_raw_payloads,
};
#[cfg(test)]
pub(crate) use full_closure::{
    install_after_stateless_failure, install_ownership_release_test_hook,
    install_stateless_page_observer,
    sync_full_closure_normalized_events_from_persisted_raw_payloads,
};
#[cfg(test)]
pub(crate) use mode::PersistedRawPayloadAdapterSyncMode as PersistedRawPayloadAdapterSyncModeForTest;
use mode::{PersistedRawPayloadAdapterSyncMode, ensure_raw_fact_adapter_allowed};
use progress::{journal_authority_epoch_with_progress, record_adapter_progress};
use scope::load_live_adapter_source_scope;
use stateless::{sync_block_derived_for_mode, sync_reverse_claim_for_mode};
use sync_logging::{log_adapter_call_timing, log_live_poll_adapter_sync_completion};
#[cfg(test)]
use test_hooks::fail_after_discovery_mutation as fail_after_discovery_mutation_for_test;
#[cfg(test)]
pub(crate) use test_hooks::install_post_discovery_mutation_failure as install_post_discovery_mutation_failure_for_test;
// Adapter synchronization keeps deployment, scope, mode, and reconciliation inputs explicit.
#[expect(clippy::too_many_arguments)]
async fn sync_adapter_state_from_persisted_raw_payloads_with_mode(
    pool: &sqlx::PgPool,
    live_deployment_profile: Option<&str>,
    chain: &str,
    block_hashes: &[String],
    source_scope: Option<&[(String, String, i64, i64)]>,
    mode: PersistedRawPayloadAdapterSyncMode,
    reload_live_source_scope: bool,
    full_source_reconciliation: entrypoints::FullSourceReconciliationScope,
    progress: &mut Option<&mut dyn StartupAdapterProgress>,
) -> Result<PersistedRawPayloadAdapterSyncSummary> {
    if matches!(mode, PersistedRawPayloadAdapterSyncMode::LivePoll) {
        ensure!(
            live_deployment_profile.is_some_and(|profile| !profile.trim().is_empty()),
            "live adapter sync requires a deployment profile"
        );
    } else {
        ensure!(
            live_deployment_profile.is_none(),
            "only live-poll adapter sync may carry a deployment profile"
        );
    }
    let legacy_full_source = full_source_reconciliation.reconciles_legacy_registry();
    let ens_v2_full_source = full_source_reconciliation.reconciles_ens_v2_registry();
    let mut aggregate = PersistedRawPayloadAdapterSyncSummary::default();
    if !mode.uses_stateless_replay_authority() {
        let epoch_guard = journal_authority_epoch_with_progress(pool, chain, progress).await?;
        aggregate.resolver_profile_authority_epoch_guard_count += epoch_guard.epoch_guard_count;
        aggregate.resolver_profile_authority_scan_count += epoch_guard.authority_scan_count;
    }
    let mut active_source_scope = source_scope.map(<[_]>::to_vec);
    sync_block_derived_for_mode(
        pool,
        chain,
        block_hashes,
        active_source_scope.as_deref(),
        mode,
        &mut aggregate,
        progress,
    )
    .await?;
    if legacy_full_source
        || mode.selects_adapter(
            active_source_scope.as_deref(),
            NormalizedEventReplayAdapter::EnsV1SubregistryDiscovery,
        )
    {
        ensure_raw_fact_adapter_allowed(
            mode,
            NormalizedEventReplayAdapter::EnsV1SubregistryDiscovery,
        )?;
        let adapter_started = Instant::now();
        let source_scope_target_count = active_source_scope.as_deref().map_or(0, <[_]>::len);
        info!(
            service = "indexer",
            command = "adapter-sync",
            chain,
            adapter = "ens_v1_subregistry_discovery",
            block_hash_count = block_hashes.len(),
            source_scope_target_count,
            adapter_sync_mode = ?mode,
            "adapter sync call started"
        );
        let (subregistry_discovery_summary, stateless_replay_authority) =
            sync_ens_v1_subregistry_for_mode(
                pool,
                chain,
                block_hashes,
                active_source_scope.as_deref(),
                mode,
                legacy_full_source,
                progress,
            )
            .await?;
        log_adapter_call_timing(
            chain,
            "ens_v1_subregistry_discovery",
            ens_v1_subregistry_sync_operation(legacy_full_source),
            block_hashes.len(),
            source_scope_target_count,
            subregistry_discovery_summary.scanned_log_count,
            subregistry_discovery_summary.matched_log_count,
            subregistry_discovery_summary.total_normalized_event_count,
            subregistry_discovery_summary.total_normalized_event_inserted_count,
            adapter_started.elapsed().as_millis(),
        );
        log_ens_v1_subregistry_discovery_sync_summary(chain, &subregistry_discovery_summary);
        aggregate.add_counts(
            subregistry_discovery_summary.scanned_log_count,
            subregistry_discovery_summary.matched_log_count,
            subregistry_discovery_summary.total_normalized_event_count,
            subregistry_discovery_summary.total_normalized_event_inserted_count,
        );
        aggregate.add_stateless_replay_authority(&stateless_replay_authority);
        record_adapter_progress(pool, progress).await?;
        let discovery_mutated = subregistry_discovery_summary.inserted_edge_count > 0
            || subregistry_discovery_summary.deactivated_edge_count > 0;
        #[cfg(test)]
        if discovery_mutated {
            fail_after_discovery_mutation_for_test(pool).await?;
        }
        if !mode.uses_stateless_replay_authority() {
            let epoch_guard = journal_authority_epoch_with_progress(pool, chain, progress).await?;
            aggregate.resolver_profile_authority_epoch_guard_count += epoch_guard.epoch_guard_count;
            aggregate.resolver_profile_authority_scan_count += epoch_guard.authority_scan_count;
        }
        if reload_live_source_scope && discovery_mutated {
            active_source_scope =
                Some(load_live_adapter_source_scope(pool, chain, block_hashes).await?);
        }
    }
    let source_scope = active_source_scope.as_deref();
    sync_reverse_claim_for_mode(
        pool,
        chain,
        block_hashes,
        source_scope,
        mode,
        &mut aggregate,
        progress,
    )
    .await?;
    if mode.selects_adapter(
        source_scope,
        NormalizedEventReplayAdapter::EnsV1UnwrappedAuthority,
    ) {
        ensure_raw_fact_adapter_allowed(
            mode,
            NormalizedEventReplayAdapter::EnsV1UnwrappedAuthority,
        )?;
        let adapter_started = Instant::now();
        let source_scope_target_count = source_scope.map_or(0, <[_]>::len);
        info!(
            service = "indexer",
            command = "adapter-sync",
            chain,
            adapter = "ens_v1_unwrapped_authority",
            block_hash_count = block_hashes.len(),
            source_scope_target_count,
            adapter_sync_mode = ?mode,
            "adapter sync call started"
        );
        let unwrapped_authority_summary = sync_ens_v1_unwrapped_authority_for_scope(
            pool,
            chain,
            block_hashes,
            source_scope,
            progress,
        )
        .await?;
        log_adapter_call_timing(
            chain,
            "ens_v1_unwrapped_authority",
            "sync_for_block_hashes",
            block_hashes.len(),
            source_scope_target_count,
            unwrapped_authority_summary.scanned_log_count,
            unwrapped_authority_summary.matched_log_count,
            unwrapped_authority_summary.total_normalized_event_count,
            unwrapped_authority_summary.total_normalized_event_inserted_count,
            adapter_started.elapsed().as_millis(),
        );
        log_ens_v1_unwrapped_authority_sync_summary(chain, &unwrapped_authority_summary);
        aggregate.add_counts(
            unwrapped_authority_summary.scanned_log_count,
            unwrapped_authority_summary.matched_log_count,
            unwrapped_authority_summary.total_normalized_event_count,
            unwrapped_authority_summary.total_normalized_event_inserted_count,
        );
        record_adapter_progress(pool, progress).await?;
    }
    if !mode.selects_adapter(
        source_scope,
        NormalizedEventReplayAdapter::EnsV1UnwrappedAuthority,
    ) {
        info!(
            service = "indexer",
            chain, "ENSv1 unwrapped-authority adapter sync skipped outside selected source scope"
        );
    }
    if ens_v2_full_source
        || mode.selects_adapter(
            source_scope,
            NormalizedEventReplayAdapter::EnsV2RegistryResourceSurface,
        )
    {
        ensure_raw_fact_adapter_allowed(
            mode,
            NormalizedEventReplayAdapter::EnsV2RegistryResourceSurface,
        )?;
        let adapter_started = Instant::now();
        let source_scope_target_count = source_scope.map_or(0, <[_]>::len);
        info!(
            service = "indexer",
            command = "adapter-sync",
            chain,
            adapter = "ens_v2_registry_resource_surface",
            block_hash_count = block_hashes.len(),
            source_scope_target_count,
            adapter_sync_mode = ?mode,
            "adapter sync call started"
        );
        let ens_v2_registry_summary = sync_ens_v2_registry_for_mode(
            pool,
            live_deployment_profile,
            chain,
            block_hashes,
            source_scope,
            mode,
            ens_v2_full_source,
            progress,
        )
        .await?;
        log_adapter_call_timing(
            chain,
            "ens_v2_registry_resource_surface",
            ens_v2_registry_sync_operation(mode, ens_v2_full_source),
            block_hashes.len(),
            source_scope_target_count,
            ens_v2_registry_summary.scanned_log_count,
            ens_v2_registry_summary.matched_log_count,
            ens_v2_registry_summary.total_normalized_event_count,
            ens_v2_registry_summary.total_normalized_event_inserted_count,
            adapter_started.elapsed().as_millis(),
        );
        log_ens_v2_registry_resource_surface_sync_summary(chain, &ens_v2_registry_summary);
        aggregate.add_counts(
            ens_v2_registry_summary.scanned_log_count,
            ens_v2_registry_summary.matched_log_count,
            ens_v2_registry_summary.total_normalized_event_count,
            ens_v2_registry_summary.total_normalized_event_inserted_count,
        );
        record_adapter_progress(pool, progress).await?;
        if reload_live_source_scope
            && (ens_v2_registry_summary.inserted_edge_count > 0
                || ens_v2_registry_summary.deactivated_edge_count > 0)
        {
            active_source_scope =
                Some(load_live_adapter_source_scope(pool, chain, block_hashes).await?);
        }
    }
    let source_scope = active_source_scope.as_deref();
    let source_scope_target_count = source_scope.map_or(0, <[_]>::len);
    if let Some(source_scope) = source_scope.filter(|scope| {
        mode.selects_adapter(Some(*scope), NormalizedEventReplayAdapter::EnsV2Registrar)
    }) {
        ensure_raw_fact_adapter_allowed(mode, NormalizedEventReplayAdapter::EnsV2Registrar)?;
        let adapter_started = Instant::now();
        info!(
            service = "indexer",
            command = "adapter-sync",
            chain,
            adapter = "ens_v2_registrar",
            block_hash_count = block_hashes.len(),
            source_scope_target_count,
            adapter_sync_mode = ?mode,
            "adapter sync call started"
        );
        let ens_v2_registrar_summary = sync_ens_v2_registrar_for_scope(
            pool,
            chain,
            block_hashes,
            Some(source_scope),
            progress,
        )
        .await?;
        log_adapter_call_timing(
            chain,
            "ens_v2_registrar",
            "sync_for_block_hashes",
            block_hashes.len(),
            source_scope_target_count,
            ens_v2_registrar_summary.scanned_log_count,
            ens_v2_registrar_summary.matched_log_count,
            ens_v2_registrar_summary.total_synced_count,
            ens_v2_registrar_summary.total_inserted_count,
            adapter_started.elapsed().as_millis(),
        );
        log_ens_v2_registrar_sync_summary(chain, &ens_v2_registrar_summary);
        aggregate.add_counts(
            ens_v2_registrar_summary.scanned_log_count,
            ens_v2_registrar_summary.matched_log_count,
            ens_v2_registrar_summary.total_synced_count,
            ens_v2_registrar_summary.total_inserted_count,
        );
        record_adapter_progress(pool, progress).await?;
    } else if source_scope.is_none() {
        ensure_raw_fact_adapter_allowed(mode, NormalizedEventReplayAdapter::EnsV2Registrar)?;
        let adapter_started = Instant::now();
        info!(
            service = "indexer",
            command = "adapter-sync",
            chain,
            adapter = "ens_v2_registrar",
            block_hash_count = block_hashes.len(),
            source_scope_target_count = 0usize,
            adapter_sync_mode = ?mode,
            "adapter sync call started"
        );
        let ens_v2_registrar_summary =
            sync_ens_v2_registrar_for_scope(pool, chain, block_hashes, None, progress).await?;
        log_adapter_call_timing(
            chain,
            "ens_v2_registrar",
            "sync_for_block_hashes",
            block_hashes.len(),
            0,
            ens_v2_registrar_summary.scanned_log_count,
            ens_v2_registrar_summary.matched_log_count,
            ens_v2_registrar_summary.total_synced_count,
            ens_v2_registrar_summary.total_inserted_count,
            adapter_started.elapsed().as_millis(),
        );
        log_ens_v2_registrar_sync_summary(chain, &ens_v2_registrar_summary);
        aggregate.add_counts(
            ens_v2_registrar_summary.scanned_log_count,
            ens_v2_registrar_summary.matched_log_count,
            ens_v2_registrar_summary.total_synced_count,
            ens_v2_registrar_summary.total_inserted_count,
        );
        record_adapter_progress(pool, progress).await?;
    }
    sync_ens_v2_tail_adapters(
        pool,
        chain,
        block_hashes,
        source_scope,
        mode,
        &mut aggregate,
        progress,
    )
    .await?;
    if mode == PersistedRawPayloadAdapterSyncMode::LivePoll {
        log_live_poll_adapter_sync_completion(
            chain,
            block_hashes.len(),
            source_scope.map_or(0, <[_]>::len),
            &aggregate,
        );
    }
    Ok(aggregate)
}
