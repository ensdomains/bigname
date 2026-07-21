use std::time::Instant;

use anyhow::{Result, ensure};
use tracing::info;

use crate::resolver_profile_convergence::journal_resolver_profile_authority_if_epoch_changed;
use crate::runtime::{
    log_block_derived_normalized_event_summary, log_ens_v1_reverse_claim_sync_summary,
    log_ens_v1_subregistry_discovery_sync_summary, log_ens_v1_unwrapped_authority_sync_summary,
    log_ens_v2_permissions_sync_summary, log_ens_v2_registrar_sync_summary,
    log_ens_v2_registry_resource_surface_sync_summary, log_ens_v2_resolver_sync_summary,
};

use super::replay::NormalizedEventReplayAdapter;
use super::types::PersistedRawPayloadAdapterSyncSummary;
#[path = "adapter_sync/backlog.rs"]
mod backlog;
#[path = "adapter_sync/ens_v1_subregistry.rs"]
mod ens_v1_subregistry;
#[path = "adapter_sync/ens_v2_registry.rs"]
mod ens_v2_registry;
#[path = "adapter_sync/entrypoints.rs"]
mod entrypoints;
#[path = "adapter_sync/full_closure.rs"]
mod full_closure;
#[path = "adapter_sync/mode.rs"]
mod mode;
#[path = "adapter_sync/scope.rs"]
mod scope;
#[path = "adapter_sync/logging.rs"]
mod sync_logging;
#[cfg(test)]
#[path = "adapter_sync/test_hooks.rs"]
mod test_hooks;
#[cfg(test)]
pub(crate) use backlog::install_backlog_after_adapter_sync_test_hook;
pub(crate) use backlog::{
    BacklogHandoffStatus, sync_live_adapter_backlog_after_normalized_replay,
    validate_chain_handoff_while_guarded,
};
use ens_v1_subregistry::{ens_v1_subregistry_sync_operation, sync_ens_v1_subregistry_for_mode};
use ens_v2_registry::{ens_v2_registry_sync_operation, sync_ens_v2_registry_for_mode};
pub(crate) use entrypoints::{
    sync_adapter_state_from_persisted_raw_payloads,
    sync_adapter_state_from_scoped_persisted_raw_payloads,
    sync_live_adapter_state_from_persisted_raw_payloads,
    sync_live_adapter_state_from_persisted_raw_payloads_after_reorg,
    sync_replay_normalized_events_from_persisted_raw_payloads,
};
pub(crate) use full_closure::{
    AutomaticTwoPhaseFullClosureSyncResult,
    sync_automatic_two_phase_full_closure_normalized_events,
    sync_manual_full_closure_normalized_events_from_persisted_raw_payloads,
};
#[cfg(test)]
pub(crate) use full_closure::{
    install_after_stateless_failure, install_ownership_release_test_hook,
    install_stateless_page_observer,
    sync_full_closure_normalized_events_from_persisted_raw_payloads,
};
use mode::{PersistedRawPayloadAdapterSyncMode, ensure_raw_fact_adapter_allowed};
use scope::load_live_adapter_source_scope;
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
    let epoch_guard = journal_resolver_profile_authority_if_epoch_changed(pool, chain).await?;
    aggregate.resolver_profile_authority_epoch_guard_count += epoch_guard.epoch_guard_count;
    aggregate.resolver_profile_authority_scan_count += epoch_guard.authority_scan_count;
    let mut active_source_scope = source_scope.map(<[_]>::to_vec);
    let adapter_started = Instant::now();
    let source_scope_target_count = active_source_scope.as_deref().map_or(0, <[_]>::len);
    ensure_raw_fact_adapter_allowed(
        mode,
        NormalizedEventReplayAdapter::BlockDerivedNormalizedEvents,
    )?;
    info!(
        service = "indexer",
        command = "adapter-sync",
        chain,
        adapter = "block_derived_normalized_events",
        block_hash_count = block_hashes.len(),
        source_scope_target_count,
        adapter_sync_mode = ?mode,
        "adapter sync call started"
    );
    let normalized_event_summary = match mode {
        PersistedRawPayloadAdapterSyncMode::RawFactReplay {
            canonical_raw_log_count,
            ..
        } => {
            bigname_adapters::sync_block_derived_normalized_events_with_scanned_log_count(
                pool,
                chain,
                block_hashes,
                active_source_scope.as_deref(),
                canonical_raw_log_count,
            )
            .await?
        }
        PersistedRawPayloadAdapterSyncMode::LivePoll
        | PersistedRawPayloadAdapterSyncMode::LiveOrBackfill => {
            bigname_adapters::sync_block_derived_normalized_events(
                pool,
                chain,
                block_hashes,
                active_source_scope.as_deref(),
            )
            .await?
        }
    };
    log_adapter_call_timing(
        chain,
        "block_derived_normalized_events",
        "sync_block_derived_normalized_events",
        block_hashes.len(),
        source_scope_target_count,
        normalized_event_summary.scanned_log_count,
        normalized_event_summary.matched_log_count,
        normalized_event_summary.total_synced_count,
        normalized_event_summary.total_inserted_count,
        adapter_started.elapsed().as_millis(),
    );
    log_block_derived_normalized_event_summary(chain, &normalized_event_summary);
    aggregate.add_counts(
        normalized_event_summary.scanned_log_count,
        normalized_event_summary.matched_log_count,
        normalized_event_summary.total_synced_count,
        normalized_event_summary.total_inserted_count,
    );
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
        let subregistry_discovery_summary = sync_ens_v1_subregistry_for_mode(
            pool,
            chain,
            block_hashes,
            active_source_scope.as_deref(),
            mode,
            legacy_full_source,
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
        let discovery_mutated = subregistry_discovery_summary.inserted_edge_count > 0
            || subregistry_discovery_summary.deactivated_edge_count > 0;
        #[cfg(test)]
        if discovery_mutated {
            fail_after_discovery_mutation_for_test(pool).await?;
        }
        let epoch_guard = journal_resolver_profile_authority_if_epoch_changed(pool, chain).await?;
        aggregate.resolver_profile_authority_epoch_guard_count += epoch_guard.epoch_guard_count;
        aggregate.resolver_profile_authority_scan_count += epoch_guard.authority_scan_count;
        if reload_live_source_scope && discovery_mutated {
            active_source_scope =
                Some(load_live_adapter_source_scope(pool, chain, block_hashes).await?);
        }
    }
    let source_scope = active_source_scope.as_deref();
    if mode.selects_adapter(
        source_scope,
        NormalizedEventReplayAdapter::EnsV1ReverseClaim,
    ) {
        ensure_raw_fact_adapter_allowed(mode, NormalizedEventReplayAdapter::EnsV1ReverseClaim)?;
        let adapter_started = Instant::now();
        let source_scope_target_count = source_scope.map_or(0, <[_]>::len);
        info!(
            service = "indexer",
            command = "adapter-sync",
            chain,
            adapter = "ens_v1_reverse_claim",
            block_hash_count = block_hashes.len(),
            source_scope_target_count,
            adapter_sync_mode = ?mode,
            "adapter sync call started"
        );
        let reverse_claim_summary = if let Some(source_scope) = source_scope {
            bigname_adapters::EnsV1ReverseClaimSyncSummary::sync_for_block_hashes_with_source_scope(
                pool,
                chain,
                block_hashes,
                source_scope,
            )
            .await?
        } else {
            bigname_adapters::EnsV1ReverseClaimSyncSummary::sync_for_block_hashes(
                pool,
                chain,
                block_hashes,
            )
            .await?
        };
        log_adapter_call_timing(
            chain,
            "ens_v1_reverse_claim",
            "sync_for_block_hashes",
            block_hashes.len(),
            source_scope_target_count,
            reverse_claim_summary.scanned_log_count,
            reverse_claim_summary.matched_log_count,
            reverse_claim_summary.total_synced_count,
            reverse_claim_summary.total_inserted_count,
            adapter_started.elapsed().as_millis(),
        );
        log_ens_v1_reverse_claim_sync_summary(chain, &reverse_claim_summary);
        aggregate.add_counts(
            reverse_claim_summary.scanned_log_count,
            reverse_claim_summary.matched_log_count,
            reverse_claim_summary.total_synced_count,
            reverse_claim_summary.total_inserted_count,
        );
    }
    if !mode.selects_adapter(
        source_scope,
        NormalizedEventReplayAdapter::EnsV1ReverseClaim,
    ) {
        info!(
            service = "indexer",
            chain, "ENSv1 reverse-claim adapter sync skipped outside selected source scope"
        );
    }
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
        let unwrapped_authority_summary = if let Some(source_scope) = source_scope {
            bigname_adapters::EnsV1UnwrappedAuthoritySyncSummary::sync_for_block_hashes_with_source_scope(
                pool,
                chain,
                block_hashes,
                source_scope,
            )
            .await?
        } else {
            bigname_adapters::EnsV1UnwrappedAuthoritySyncSummary::sync_for_block_hashes(
                pool,
                chain,
                block_hashes,
            )
            .await?
        };
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
        let ens_v2_registrar_summary =
            bigname_adapters::EnsV2RegistrarSyncSummary::sync_for_block_hashes_with_source_scope(
                pool,
                chain,
                block_hashes,
                source_scope,
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
            bigname_adapters::EnsV2RegistrarSyncSummary::sync_for_block_hashes(
                pool,
                chain,
                block_hashes,
            )
            .await?;
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
    }
    if mode.selects_adapter(source_scope, NormalizedEventReplayAdapter::EnsV2Resolver) {
        ensure_raw_fact_adapter_allowed(mode, NormalizedEventReplayAdapter::EnsV2Resolver)?;
        let adapter_started = Instant::now();
        info!(
            service = "indexer",
            command = "adapter-sync",
            chain,
            adapter = "ens_v2_resolver",
            block_hash_count = block_hashes.len(),
            source_scope_target_count,
            adapter_sync_mode = ?mode,
            "adapter sync call started"
        );
        let ens_v2_resolver_summary = if let Some(source_scope) = source_scope {
            bigname_adapters::EnsV2ResolverSyncSummary::sync_for_block_hashes_with_source_scope(
                pool,
                chain,
                block_hashes,
                source_scope,
            )
            .await?
        } else {
            bigname_adapters::EnsV2ResolverSyncSummary::sync_for_block_hashes(
                pool,
                chain,
                block_hashes,
            )
            .await?
        };
        log_adapter_call_timing(
            chain,
            "ens_v2_resolver",
            "sync_for_block_hashes",
            block_hashes.len(),
            source_scope_target_count,
            ens_v2_resolver_summary.scanned_log_count,
            ens_v2_resolver_summary.matched_log_count,
            ens_v2_resolver_summary.total_synced_count,
            ens_v2_resolver_summary.total_inserted_count,
            adapter_started.elapsed().as_millis(),
        );
        log_ens_v2_resolver_sync_summary(chain, &ens_v2_resolver_summary);
        aggregate.add_counts(
            ens_v2_resolver_summary.scanned_log_count,
            ens_v2_resolver_summary.matched_log_count,
            ens_v2_resolver_summary.total_synced_count,
            ens_v2_resolver_summary.total_inserted_count,
        );
    }
    if mode.selects_adapter(source_scope, NormalizedEventReplayAdapter::EnsV2Permissions) {
        ensure_raw_fact_adapter_allowed(mode, NormalizedEventReplayAdapter::EnsV2Permissions)?;
        let adapter_started = Instant::now();
        info!(
            service = "indexer",
            command = "adapter-sync",
            chain,
            adapter = "ens_v2_permissions",
            block_hash_count = block_hashes.len(),
            source_scope_target_count,
            adapter_sync_mode = ?mode,
            "adapter sync call started"
        );
        let ens_v2_permissions_summary = if let Some(source_scope) = source_scope {
            bigname_adapters::EnsV2PermissionsSyncSummary::sync_for_block_hashes_with_source_scope(
                pool,
                chain,
                block_hashes,
                source_scope,
            )
            .await?
        } else {
            bigname_adapters::EnsV2PermissionsSyncSummary::sync_for_block_hashes(
                pool,
                chain,
                block_hashes,
            )
            .await?
        };
        log_adapter_call_timing(
            chain,
            "ens_v2_permissions",
            "sync_for_block_hashes",
            block_hashes.len(),
            source_scope_target_count,
            ens_v2_permissions_summary.scanned_log_count,
            ens_v2_permissions_summary.matched_log_count,
            ens_v2_permissions_summary.total_synced_count,
            ens_v2_permissions_summary.total_inserted_count,
            adapter_started.elapsed().as_millis(),
        );
        log_ens_v2_permissions_sync_summary(chain, &ens_v2_permissions_summary);
        aggregate.add_counts(
            ens_v2_permissions_summary.scanned_log_count,
            ens_v2_permissions_summary.matched_log_count,
            ens_v2_permissions_summary.total_synced_count,
            ens_v2_permissions_summary.total_inserted_count,
        );
    }
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
