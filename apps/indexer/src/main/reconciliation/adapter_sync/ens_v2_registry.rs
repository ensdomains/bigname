use anyhow::Result;

use super::{
    mode::PersistedRawPayloadAdapterSyncMode, scope::load_live_adapter_target_block_number,
};

pub(super) async fn sync_ens_v2_registry_for_mode(
    pool: &sqlx::PgPool,
    chain: &str,
    block_hashes: &[String],
    source_scope: Option<&[(String, String, i64, i64)]>,
    mode: PersistedRawPayloadAdapterSyncMode,
) -> Result<bigname_adapters::EnsV2RegistryResourceSurfaceSyncSummary> {
    match (mode, source_scope) {
        (PersistedRawPayloadAdapterSyncMode::LivePoll, _) => {
            let target_block_number =
                load_live_adapter_target_block_number(pool, chain, block_hashes).await?;
            bigname_adapters::sync_ens_v2_registry_resource_surface_through_block(
                pool,
                chain,
                target_block_number,
            )
            .await
        }
        (PersistedRawPayloadAdapterSyncMode::RawFactReplay { .. }, Some(source_scope)) => {
            bigname_adapters::EnsV2RegistryResourceSurfaceSyncSummary::sync_for_block_hashes_with_source_scope_canonical_only(
                pool,
                chain,
                block_hashes,
                source_scope,
            )
            .await
        }
        (PersistedRawPayloadAdapterSyncMode::RawFactReplay { .. }, None) => {
            bigname_adapters::EnsV2RegistryResourceSurfaceSyncSummary::sync_for_block_hashes_canonical_only(
                pool,
                chain,
                block_hashes,
            )
            .await
        }
        (_, Some(source_scope)) => {
            bigname_adapters::EnsV2RegistryResourceSurfaceSyncSummary::sync_for_block_hashes_with_source_scope(
                pool,
                chain,
                block_hashes,
                source_scope,
            )
            .await
        }
        (_, None) => {
            bigname_adapters::EnsV2RegistryResourceSurfaceSyncSummary::sync_for_block_hashes(
                pool,
                chain,
                block_hashes,
            )
            .await
        }
    }
}

pub(super) const fn ens_v2_registry_sync_operation(
    mode: PersistedRawPayloadAdapterSyncMode,
) -> &'static str {
    if matches!(mode, PersistedRawPayloadAdapterSyncMode::LivePoll) {
        "sync_ens_v2_registry_resource_surface_through_block"
    } else {
        "sync_for_block_hashes"
    }
}
