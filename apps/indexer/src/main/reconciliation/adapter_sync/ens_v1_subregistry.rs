use anyhow::Result;
use bigname_storage::acquire_raw_log_staging_read_guard;

use crate::reconciliation::guard_release::prioritize_operation_error;
use crate::reconciliation::replay::{
    NormalizedEventReplayAdapter, ensure_legacy_registry_closure_retention_authority_for_adapters,
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
) -> Result<(
    bigname_adapters::EnsV1SubregistryDiscoverySyncSummary,
    bigname_storage::NormalizedEventReplayAuthoritySummary,
)> {
    if reconcile_full_source {
        let target_block_number =
            load_live_adapter_target_block_number(pool, chain, block_hashes).await?;
        // The same-chain raw-log mutation fence spans coverage proof, complete
        // loading, absence-aware discovery persistence, and normalized-event
        // persistence. Releasing it earlier would let a late raw fact land
        // between the proof and the winning checkpoint's adapter repair.
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
            bigname_adapters::sync_ens_v1_subregistry_discovery_through_block_with_expected_admission_epoch(
                pool,
                chain,
                target_block_number,
                expected_admission_epoch,
            )
            .await
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

    match mode {
        PersistedRawPayloadAdapterSyncMode::LivePoll
        | PersistedRawPayloadAdapterSyncMode::LiveOrBackfill => {
            if let Some(source_scope) = source_scope {
                bigname_adapters::EnsV1SubregistryDiscoverySyncSummary::sync_for_block_hashes_with_source_scope(
                    pool,
                    chain,
                    block_hashes,
                    source_scope,
                )
                .await
            } else {
                bigname_adapters::EnsV1SubregistryDiscoverySyncSummary::sync_for_block_hashes_without_discovery_reconciliation(
                    pool,
                    chain,
                    block_hashes,
                )
                .await
            }
            .map(|summary| {
                (
                    summary,
                    bigname_storage::NormalizedEventReplayAuthoritySummary::default(),
                )
            })
        }
        PersistedRawPayloadAdapterSyncMode::RawFactReplay { .. }
            if mode.uses_stateless_replay_authority() =>
        {
            if let Some(source_scope) = source_scope {
                bigname_adapters::EnsV1SubregistryDiscoverySyncSummary::sync_for_block_hashes_with_source_scope_and_stateless_replay_authority(
                    pool,
                    chain,
                    block_hashes,
                    source_scope,
                )
                .await
            } else {
                bigname_adapters::EnsV1SubregistryDiscoverySyncSummary::sync_for_block_hashes_with_stateless_replay_authority(
                    pool,
                    chain,
                    block_hashes,
                )
                .await
            }
        }
        PersistedRawPayloadAdapterSyncMode::RawFactReplay { .. } => {
            if let Some(source_scope) = source_scope {
                bigname_adapters::EnsV1SubregistryDiscoverySyncSummary::sync_for_block_hashes_with_source_scope_without_discovery_reconciliation(
                    pool,
                    chain,
                    block_hashes,
                    source_scope,
                )
                .await
            } else {
                bigname_adapters::EnsV1SubregistryDiscoverySyncSummary::sync_for_block_hashes_without_discovery_reconciliation(
                    pool,
                    chain,
                    block_hashes,
                )
                .await
            }
            .map(|summary| {
                (
                    summary,
                    bigname_storage::NormalizedEventReplayAuthoritySummary::default(),
                )
            })
        }
    }
}

pub(super) const fn ens_v1_subregistry_sync_operation(reconcile_full_source: bool) -> &'static str {
    if reconcile_full_source {
        "sync_ens_v1_subregistry_discovery_through_block"
    } else {
        "sync_for_block_hashes"
    }
}
