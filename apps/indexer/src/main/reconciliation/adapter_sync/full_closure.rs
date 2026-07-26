use std::{future::Future, time::Instant};

use crate::runtime::{
    log_ens_v1_reverse_claim_sync_summary, log_ens_v1_subregistry_discovery_sync_summary,
    log_ens_v1_unwrapped_authority_sync_summary, log_ens_v2_permissions_sync_summary,
    log_ens_v2_registrar_sync_summary, log_ens_v2_registry_resource_surface_sync_summary,
    log_ens_v2_resolver_sync_summary,
};
use anyhow::{Context, Result, ensure};
use bigname_adapters::StartupAdapterProgress;
use bigname_storage::{
    RawLogStagingInputVersion, acquire_raw_log_staging_read_guard,
    load_raw_log_staging_input_version,
};

use super::sync_logging::log_adapter_call_timing;
use crate::reconciliation::{
    replay::{
        NormalizedEventReplayAdapter, ensure_full_closure_retention_authority_for_adapters,
        replay_contract,
    },
    types::PersistedRawPayloadAdapterSyncSummary,
};

#[path = "full_closure/automatic.rs"]
mod automatic;
#[path = "full_closure/heartbeat.rs"]
mod heartbeat;
#[path = "full_closure/ownership.rs"]
mod ownership;
#[path = "full_closure/reverse_claim.rs"]
mod reverse_claim;

pub(crate) use automatic::{
    AutomaticTwoPhaseFullClosureSyncResult, automatic_stateless_replay_completed,
    sync_automatic_two_phase_full_closure_normalized_events,
};
#[cfg(test)]
pub(crate) use automatic::{install_after_stateless_failure, install_stateless_page_observer};
use heartbeat::{
    journal_full_closure_authority_with_progress, record_full_closure_progress,
    trim_allocator_after_full_closure_adapter,
};
#[cfg(test)]
pub(crate) use ownership::install_ownership_release_test_hook;
use ownership::with_full_closure_replay_lock;
pub(crate) use ownership::{
    FullClosureReplayLockWaitDeadlineExceeded, FullClosureReplayLockWaitHeartbeat,
};
use reverse_claim::sync_ens_v1_reverse_claim_range_in_pages;

#[cfg(test)]
#[expect(clippy::too_many_arguments)]
pub(crate) async fn sync_full_closure_normalized_events_from_persisted_raw_payloads(
    pool: &sqlx::PgPool,
    deployment_profile: &str,
    chain: &str,
    checkpoint_cursor_kind: &str,
    range_start_block_number: i64,
    target_block_number: i64,
    adapters: &[NormalizedEventReplayAdapter],
    max_raw_logs_per_page: usize,
) -> Result<FullClosureSyncResult> {
    sync_full_closure_with_checkpoint_completion(
        pool,
        deployment_profile,
        chain,
        checkpoint_cursor_kind,
        range_start_block_number,
        target_block_number,
        adapters,
        max_raw_logs_per_page,
        FullClosureCheckpointCompletion::Retain,
        &mut None,
        &mut None,
    )
    .await
}

#[expect(clippy::too_many_arguments)]
pub(crate) async fn sync_manual_full_closure_normalized_events_from_persisted_raw_payloads(
    pool: &sqlx::PgPool,
    deployment_profile: &str,
    chain: &str,
    checkpoint_cursor_kind: &str,
    range_start_block_number: i64,
    target_block_number: i64,
    adapters: &[NormalizedEventReplayAdapter],
    max_raw_logs_per_page: usize,
) -> Result<PersistedRawPayloadAdapterSyncSummary> {
    Ok(sync_full_closure_with_checkpoint_completion(
        pool,
        deployment_profile,
        chain,
        checkpoint_cursor_kind,
        range_start_block_number,
        target_block_number,
        adapters,
        max_raw_logs_per_page,
        FullClosureCheckpointCompletion::ClearOnSuccess,
        &mut None,
        &mut None,
    )
    .await?
    .summary)
}

pub(crate) struct FullClosureSyncResult {
    pub(crate) summary: PersistedRawPayloadAdapterSyncSummary,
}

#[derive(Clone, Copy)]
enum FullClosureCheckpointCompletion {
    Retain,
    ClearOnSuccess,
}

#[expect(clippy::too_many_arguments)]
async fn sync_full_closure_with_checkpoint_completion(
    pool: &sqlx::PgPool,
    deployment_profile: &str,
    chain: &str,
    checkpoint_cursor_kind: &str,
    range_start_block_number: i64,
    target_block_number: i64,
    adapters: &[NormalizedEventReplayAdapter],
    max_raw_logs_per_page: usize,
    checkpoint_completion: FullClosureCheckpointCompletion,
    lock_wait_heartbeat: &mut Option<&mut dyn FullClosureReplayLockWaitHeartbeat>,
    progress: &mut Option<&mut dyn StartupAdapterProgress>,
) -> Result<FullClosureSyncResult> {
    let (result, ()) = sync_full_closure_with_checkpoint_completion_and_prelude(
        pool,
        deployment_profile,
        chain,
        checkpoint_cursor_kind,
        range_start_block_number,
        target_block_number,
        adapters,
        max_raw_logs_per_page,
        checkpoint_completion,
        lock_wait_heartbeat,
        progress,
        || async { Ok(()) },
    )
    .await?;
    Ok(result)
}

#[expect(clippy::too_many_arguments)]
async fn sync_full_closure_with_checkpoint_completion_and_prelude<T, Prelude, PreludeFuture>(
    pool: &sqlx::PgPool,
    deployment_profile: &str,
    chain: &str,
    checkpoint_cursor_kind: &str,
    range_start_block_number: i64,
    target_block_number: i64,
    adapters: &[NormalizedEventReplayAdapter],
    max_raw_logs_per_page: usize,
    checkpoint_completion: FullClosureCheckpointCompletion,
    lock_wait_heartbeat: &mut Option<&mut dyn FullClosureReplayLockWaitHeartbeat>,
    progress: &mut Option<&mut dyn StartupAdapterProgress>,
    prelude: Prelude,
) -> Result<(FullClosureSyncResult, T)>
where
    Prelude: FnOnce() -> PreludeFuture,
    PreludeFuture: Future<Output = Result<T>>,
{
    ensure!(
        !checkpoint_cursor_kind.trim().is_empty(),
        "full-closure replay checkpoint cursor kind must not be empty"
    );
    with_full_closure_replay_lock(
        pool,
        deployment_profile,
        chain,
        lock_wait_heartbeat,
        || async {
            let raw_log_input_version = if adapters.is_empty() {
                RawLogStagingInputVersion::default()
            } else {
                load_raw_log_staging_input_version(pool, chain).await?
            };
            if !adapters.is_empty() {
                ensure_full_closure_retention_authority_for_adapters(
                    pool,
                    chain,
                    adapters,
                    target_block_number,
                )
                .await?;
            }
            let prelude_output = prelude().await?;
            if !adapters.is_empty() {
                ensure_full_closure_retention_authority_for_adapters(
                    pool,
                    chain,
                    adapters,
                    target_block_number,
                )
                .await?;
            }
            let summary = sync_full_closure_normalized_events_without_lock(
                pool,
                deployment_profile,
                chain,
                checkpoint_cursor_kind,
                range_start_block_number,
                target_block_number,
                adapters,
                max_raw_logs_per_page,
                progress,
            )
            .await?;
            let raw_log_guard = if adapters.is_empty() {
                None
            } else {
                let mut guard = acquire_raw_log_staging_read_guard(pool, chain).await?;
                guard
                    .accept_newer_revisions_after(raw_log_input_version, target_block_number)
                    .await
                    .with_context(|| {
                        format!(
                            "raw-log staging input changed during full-closure replay for {chain} through block {target_block_number}"
                        )
                    })?;
                Some(guard)
            };
            if matches!(
                checkpoint_completion,
                FullClosureCheckpointCompletion::ClearOnSuccess
            ) {
                bigname_adapters::clear_replay_adapter_checkpoints(
                    pool,
                    deployment_profile,
                    chain,
                    checkpoint_cursor_kind,
                )
                .await
                .with_context(|| {
                    format!(
                        "failed to clear successful manual full-closure replay checkpoints for {deployment_profile}/{chain}/{checkpoint_cursor_kind}"
                    )
                })?;
            }
            if let Some(guard) = raw_log_guard {
                guard.release().await?;
            }
            Ok((FullClosureSyncResult { summary }, prelude_output))
        },
    )
    .await
}

#[expect(clippy::too_many_arguments)]
async fn sync_full_closure_normalized_events_without_lock(
    pool: &sqlx::PgPool,
    deployment_profile: &str,
    chain: &str,
    checkpoint_cursor_kind: &str,
    range_start_block_number: i64,
    target_block_number: i64,
    adapters: &[NormalizedEventReplayAdapter],
    max_raw_logs_per_page: usize,
    progress: &mut Option<&mut dyn StartupAdapterProgress>,
) -> Result<PersistedRawPayloadAdapterSyncSummary> {
    let mut aggregate = PersistedRawPayloadAdapterSyncSummary::default();
    let checkpoint_context = bigname_adapters::ReplayAdapterCheckpointContext {
        deployment_profile: deployment_profile.to_owned(),
        cursor_kind: checkpoint_cursor_kind.to_owned(),
        range_start_block_number,
        target_block_number,
    };

    if adapters.contains(&NormalizedEventReplayAdapter::EnsV1ReverseClaim) {
        let adapter_started = Instant::now();
        let summary = sync_ens_v1_reverse_claim_range_in_pages(
            pool,
            chain,
            range_start_block_number,
            target_block_number,
            replay_contract(NormalizedEventReplayAdapter::EnsV1ReverseClaim).source_families,
            max_raw_logs_per_page,
            progress,
        )
        .await?;
        log_adapter_call_timing(
            chain,
            "ens_v1_reverse_claim",
            "sync_ens_v1_reverse_claim_range",
            0,
            0,
            summary.scanned_log_count,
            summary.matched_log_count,
            summary.total_synced_count,
            summary.total_inserted_count,
            adapter_started.elapsed().as_millis(),
        );
        log_ens_v1_reverse_claim_sync_summary(chain, &summary);
        aggregate.add_counts(
            summary.scanned_log_count,
            summary.matched_log_count,
            summary.total_synced_count,
            summary.total_inserted_count,
        );
        record_full_closure_progress(pool, progress).await?;
    }

    if adapters.contains(&NormalizedEventReplayAdapter::EnsV1SubregistryDiscovery) {
        let epoch_guard =
            journal_full_closure_authority_with_progress(pool, chain, progress).await?;
        aggregate.resolver_profile_authority_epoch_guard_count += epoch_guard.epoch_guard_count;
        aggregate.resolver_profile_authority_scan_count += epoch_guard.authority_scan_count;
        let adapter_started = Instant::now();
        let summary = match progress.as_deref_mut() {
            Some(progress) => {
                bigname_adapters::sync_ens_v1_subregistry_discovery_with_replay_checkpoint_and_log_limit_and_progress(
                    pool,
                    chain,
                    &checkpoint_context,
                    max_raw_logs_per_page,
                    progress,
                )
                .await?
            }
            None => {
                bigname_adapters::sync_ens_v1_subregistry_discovery_with_replay_checkpoint_and_log_limit(
                    pool,
                    chain,
                    &checkpoint_context,
                    max_raw_logs_per_page,
                )
                .await?
            }
        };
        log_adapter_call_timing(
            chain,
            "ens_v1_subregistry_discovery",
            "sync_ens_v1_subregistry_discovery",
            0,
            0,
            summary.scanned_log_count,
            summary.matched_log_count,
            summary.total_normalized_event_count,
            summary.total_normalized_event_inserted_count,
            adapter_started.elapsed().as_millis(),
        );
        log_ens_v1_subregistry_discovery_sync_summary(chain, &summary);
        aggregate.add_counts(
            summary.scanned_log_count,
            summary.matched_log_count,
            summary.total_normalized_event_count,
            summary.total_normalized_event_inserted_count,
        );
        let epoch_guard =
            journal_full_closure_authority_with_progress(pool, chain, progress).await?;
        aggregate.resolver_profile_authority_epoch_guard_count += epoch_guard.epoch_guard_count;
        aggregate.resolver_profile_authority_scan_count += epoch_guard.authority_scan_count;
        trim_allocator_after_full_closure_adapter("ens_v1_subregistry_discovery");
        record_full_closure_progress(pool, progress).await?;
    }

    if adapters.contains(&NormalizedEventReplayAdapter::EnsV1UnwrappedAuthority) {
        let adapter_started = Instant::now();
        let summary = match progress.as_deref_mut() {
            Some(progress) => {
                bigname_adapters::sync_ens_v1_unwrapped_authority_with_replay_checkpoint_and_log_limit_and_progress(
                    pool,
                    chain,
                    &checkpoint_context,
                    max_raw_logs_per_page,
                    progress,
                )
                .await?
            }
            None => {
                bigname_adapters::sync_ens_v1_unwrapped_authority_with_replay_checkpoint_and_log_limit(
                    pool,
                    chain,
                    &checkpoint_context,
                    max_raw_logs_per_page,
                )
                .await?
            }
        };
        log_adapter_call_timing(
            chain,
            "ens_v1_unwrapped_authority",
            "sync_ens_v1_unwrapped_authority",
            0,
            0,
            summary.scanned_log_count,
            summary.matched_log_count,
            summary.total_normalized_event_count,
            summary.total_normalized_event_inserted_count,
            adapter_started.elapsed().as_millis(),
        );
        log_ens_v1_unwrapped_authority_sync_summary(chain, &summary);
        aggregate.add_counts(
            summary.scanned_log_count,
            summary.matched_log_count,
            summary.total_normalized_event_count,
            summary.total_normalized_event_inserted_count,
        );
        trim_allocator_after_full_closure_adapter("ens_v1_unwrapped_authority");
        record_full_closure_progress(pool, progress).await?;
    }

    if adapters.contains(&NormalizedEventReplayAdapter::EnsV2RegistryResourceSurface) {
        let adapter_started = Instant::now();
        let summary = match progress.as_deref_mut() {
            Some(progress) => {
                bigname_adapters::sync_ens_v2_registry_resource_surface_through_block_with_progress(
                    pool,
                    chain,
                    target_block_number,
                    progress,
                )
                .await?
            }
            None => {
                bigname_adapters::sync_ens_v2_registry_resource_surface_through_block(
                    pool,
                    chain,
                    target_block_number,
                )
                .await?
            }
        };
        log_adapter_call_timing(
            chain,
            "ens_v2_registry_resource_surface",
            "sync_ens_v2_registry_resource_surface_through_block",
            0,
            0,
            summary.scanned_log_count,
            summary.matched_log_count,
            summary.total_normalized_event_count,
            summary.total_normalized_event_inserted_count,
            adapter_started.elapsed().as_millis(),
        );
        log_ens_v2_registry_resource_surface_sync_summary(chain, &summary);
        aggregate.add_counts(
            summary.scanned_log_count,
            summary.matched_log_count,
            summary.total_normalized_event_count,
            summary.total_normalized_event_inserted_count,
        );
        trim_allocator_after_full_closure_adapter("ens_v2_registry_resource_surface");
        record_full_closure_progress(pool, progress).await?;
    }

    if adapters.contains(&NormalizedEventReplayAdapter::EnsV2Registrar) {
        let adapter_started = Instant::now();
        let summary = match progress.as_deref_mut() {
            Some(progress) => {
                bigname_adapters::sync_ens_v2_registrar_through_block_with_progress(
                    pool,
                    chain,
                    target_block_number,
                    progress,
                )
                .await?
            }
            None => {
                bigname_adapters::sync_ens_v2_registrar_through_block(
                    pool,
                    chain,
                    target_block_number,
                )
                .await?
            }
        };
        log_adapter_call_timing(
            chain,
            "ens_v2_registrar",
            "sync_ens_v2_registrar_through_block",
            0,
            0,
            summary.scanned_log_count,
            summary.matched_log_count,
            summary.total_synced_count,
            summary.total_inserted_count,
            adapter_started.elapsed().as_millis(),
        );
        log_ens_v2_registrar_sync_summary(chain, &summary);
        aggregate.add_counts(
            summary.scanned_log_count,
            summary.matched_log_count,
            summary.total_synced_count,
            summary.total_inserted_count,
        );
        trim_allocator_after_full_closure_adapter("ens_v2_registrar");
        record_full_closure_progress(pool, progress).await?;
    }

    if adapters.contains(&NormalizedEventReplayAdapter::EnsV2Resolver) {
        let adapter_started = Instant::now();
        let summary = match progress.as_deref_mut() {
            Some(progress) => {
                bigname_adapters::sync_ens_v2_resolver_through_block_with_progress(
                    pool,
                    chain,
                    target_block_number,
                    progress,
                )
                .await?
            }
            None => {
                bigname_adapters::sync_ens_v2_resolver_through_block(
                    pool,
                    chain,
                    target_block_number,
                )
                .await?
            }
        };
        log_adapter_call_timing(
            chain,
            "ens_v2_resolver",
            "sync_ens_v2_resolver_through_block",
            0,
            0,
            summary.scanned_log_count,
            summary.matched_log_count,
            summary.total_synced_count,
            summary.total_inserted_count,
            adapter_started.elapsed().as_millis(),
        );
        log_ens_v2_resolver_sync_summary(chain, &summary);
        aggregate.add_counts(
            summary.scanned_log_count,
            summary.matched_log_count,
            summary.total_synced_count,
            summary.total_inserted_count,
        );
        trim_allocator_after_full_closure_adapter("ens_v2_resolver");
        record_full_closure_progress(pool, progress).await?;
    }

    if adapters.contains(&NormalizedEventReplayAdapter::EnsV2Permissions) {
        let adapter_started = Instant::now();
        let summary = match progress.as_deref_mut() {
            Some(progress) => {
                bigname_adapters::sync_ens_v2_permissions_through_block_with_progress(
                    pool,
                    chain,
                    target_block_number,
                    progress,
                )
                .await?
            }
            None => {
                bigname_adapters::sync_ens_v2_permissions_through_block(
                    pool,
                    chain,
                    target_block_number,
                )
                .await?
            }
        };
        log_adapter_call_timing(
            chain,
            "ens_v2_permissions",
            "sync_ens_v2_permissions_through_block",
            0,
            0,
            summary.scanned_log_count,
            summary.matched_log_count,
            summary.total_synced_count,
            summary.total_inserted_count,
            adapter_started.elapsed().as_millis(),
        );
        log_ens_v2_permissions_sync_summary(chain, &summary);
        aggregate.add_counts(
            summary.scanned_log_count,
            summary.matched_log_count,
            summary.total_synced_count,
            summary.total_inserted_count,
        );
        trim_allocator_after_full_closure_adapter("ens_v2_permissions");
        record_full_closure_progress(pool, progress).await?;
    }

    Ok(aggregate)
}
