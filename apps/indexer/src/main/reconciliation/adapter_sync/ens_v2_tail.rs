use std::time::Instant;

use anyhow::Result;
use bigname_adapters::StartupAdapterProgress;
use tracing::info;

use crate::runtime::{log_ens_v2_permissions_sync_summary, log_ens_v2_resolver_sync_summary};

use super::super::{
    replay::NormalizedEventReplayAdapter, types::PersistedRawPayloadAdapterSyncSummary,
};
use super::{
    mode::{PersistedRawPayloadAdapterSyncMode, ensure_raw_fact_adapter_allowed},
    progress::record_adapter_progress,
    sync_logging::log_adapter_call_timing,
};

pub(super) async fn sync_ens_v2_tail_adapters(
    pool: &sqlx::PgPool,
    chain: &str,
    block_hashes: &[String],
    source_scope: Option<&[(String, String, i64, i64)]>,
    mode: PersistedRawPayloadAdapterSyncMode,
    aggregate: &mut PersistedRawPayloadAdapterSyncSummary,
    progress: &mut Option<&mut dyn StartupAdapterProgress>,
) -> Result<()> {
    let source_scope_target_count = source_scope.map_or(0, <[_]>::len);
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
        let summary = match (source_scope, progress.as_deref_mut()) {
            (Some(source_scope), Some(progress)) => {
                bigname_adapters::EnsV2ResolverSyncSummary::sync_for_block_hashes_with_source_scope_and_progress(
                    pool,
                    chain,
                    block_hashes,
                    source_scope,
                    progress,
                )
                .await?
            }
            (Some(source_scope), None) => {
                bigname_adapters::EnsV2ResolverSyncSummary::sync_for_block_hashes_with_source_scope(
                    pool,
                    chain,
                    block_hashes,
                    source_scope,
                )
                .await?
            }
            (None, Some(progress)) => {
                bigname_adapters::EnsV2ResolverSyncSummary::sync_for_block_hashes_with_progress(
                    pool,
                    chain,
                    block_hashes,
                    progress,
                )
                .await?
            }
            (None, None) => {
                bigname_adapters::EnsV2ResolverSyncSummary::sync_for_block_hashes(
                    pool,
                    chain,
                    block_hashes,
                )
                .await?
            }
        };
        log_adapter_call_timing(
            chain,
            "ens_v2_resolver",
            "sync_for_block_hashes",
            block_hashes.len(),
            source_scope_target_count,
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
        record_adapter_progress(pool, progress).await?;
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
        let summary = match (source_scope, progress.as_deref_mut()) {
            (Some(source_scope), Some(progress)) => {
                bigname_adapters::EnsV2PermissionsSyncSummary::sync_for_block_hashes_with_source_scope_and_progress(
                    pool,
                    chain,
                    block_hashes,
                    source_scope,
                    progress,
                )
                .await?
            }
            (Some(source_scope), None) => {
                bigname_adapters::EnsV2PermissionsSyncSummary::sync_for_block_hashes_with_source_scope(
                    pool,
                    chain,
                    block_hashes,
                    source_scope,
                )
                .await?
            }
            (None, Some(progress)) => {
                bigname_adapters::EnsV2PermissionsSyncSummary::sync_for_block_hashes_with_progress(
                    pool,
                    chain,
                    block_hashes,
                    progress,
                )
                .await?
            }
            (None, None) => {
                bigname_adapters::EnsV2PermissionsSyncSummary::sync_for_block_hashes(
                    pool,
                    chain,
                    block_hashes,
                )
                .await?
            }
        };
        log_adapter_call_timing(
            chain,
            "ens_v2_permissions",
            "sync_for_block_hashes",
            block_hashes.len(),
            source_scope_target_count,
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
        record_adapter_progress(pool, progress).await?;
    }
    Ok(())
}
