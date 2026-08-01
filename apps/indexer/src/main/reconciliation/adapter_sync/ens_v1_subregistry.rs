use anyhow::Result;
use bigname_storage::acquire_raw_log_staging_read_guard;

use crate::{
    StartupAdapterProgress,
    reconciliation::{
        guard_release::prioritize_operation_error,
        replay::{
            NormalizedEventReplayAdapter,
            ensure_legacy_registry_closure_retention_authority_for_adapters,
        },
    },
};

use super::{
    mode::PersistedRawPayloadAdapterSyncMode, scope::load_live_adapter_target_block_number,
};

pub(super) async fn sync_ens_v1_subregistry_for_mode(
    pool: &sqlx::PgPool,
    chain: &str,
    block_hashes: &[String],
    source_scope: Option<&[(String, String, i64, i64)]>,
    mode: PersistedRawPayloadAdapterSyncMode,
    reconcile_full_source: bool,
    progress: &mut Option<&mut dyn StartupAdapterProgress>,
) -> Result<(
    bigname_adapters::EnsV1SubregistryDiscoverySyncSummary,
    bigname_storage::NormalizedEventReplayAuthoritySummary,
)> {
    if reconcile_full_source {
        let target_block_number =
            load_live_adapter_target_block_number(pool, chain, block_hashes).await?;
        record_progress(pool, progress).await?;
        let raw_log_guard = acquire_raw_log_staging_read_guard(pool, chain).await?;
        let sync_result = async {
            let expected_admission_epoch =
                ensure_legacy_registry_closure_retention_authority_for_adapters(
                    pool,
                    chain,
                    &[NormalizedEventReplayAdapter::EnsV1SubregistryDiscovery],
                    target_block_number,
                )
                .await?;
            record_progress(pool, progress).await?;
            let summary = bigname_adapters::sync_ens_v1_subregistry_discovery_through_block_with_expected_admission_epoch(
                pool,
                chain,
                target_block_number,
                expected_admission_epoch,
            )
            .await?;
            record_progress(pool, progress).await?;
            Ok(summary)
        }
        .await;
        let release_result = raw_log_guard.release().await;
        return prioritize_operation_error(sync_result, release_result).map(|summary| {
            (
                summary,
                bigname_storage::NormalizedEventReplayAuthoritySummary::default(),
            )
        });
    }

    let summary = match (mode, source_scope) {
        (
            PersistedRawPayloadAdapterSyncMode::LivePoll
            | PersistedRawPayloadAdapterSyncMode::LiveOrBackfill,
            Some(source_scope),
        ) => {
            bigname_adapters::EnsV1SubregistryDiscoverySyncSummary::sync_for_block_hashes_with_source_scope(
                pool,
                chain,
                block_hashes,
                source_scope,
            )
            .await?
        }
        (
            PersistedRawPayloadAdapterSyncMode::LivePoll
            | PersistedRawPayloadAdapterSyncMode::LiveOrBackfill,
            None,
        ) => {
            bigname_adapters::EnsV1SubregistryDiscoverySyncSummary::sync_for_block_hashes_without_discovery_reconciliation(
                pool,
                chain,
                block_hashes,
            )
            .await?
        }
        (PersistedRawPayloadAdapterSyncMode::RawFactReplay { .. }, Some(source_scope)) => {
            bigname_adapters::EnsV1SubregistryDiscoverySyncSummary::sync_for_block_hashes_with_source_scope_without_discovery_reconciliation(
                pool,
                chain,
                block_hashes,
                source_scope,
            )
            .await?
        }
        (PersistedRawPayloadAdapterSyncMode::RawFactReplay { .. }, None) => {
            bigname_adapters::EnsV1SubregistryDiscoverySyncSummary::sync_for_block_hashes_without_discovery_reconciliation(
                pool,
                chain,
                block_hashes,
            )
            .await?
        }
    };
    record_progress(pool, progress).await?;
    Ok((
        summary,
        bigname_storage::NormalizedEventReplayAuthoritySummary::default(),
    ))
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

pub(super) const fn ens_v1_subregistry_sync_operation(reconcile_full_source: bool) -> &'static str {
    if reconcile_full_source {
        "sync_ens_v1_subregistry_discovery_through_block"
    } else {
        "sync_for_block_hashes"
    }
}
