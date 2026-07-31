use anyhow::{Context, Result};
use bigname_storage::acquire_raw_log_staging_read_guard;

use crate::reconciliation::guard_release::prioritize_operation_error;

use super::{
    mode::PersistedRawPayloadAdapterSyncMode, scope::load_live_adapter_target_block_number,
};

#[expect(clippy::too_many_arguments)]
pub(crate) async fn sync_ens_v2_registry_for_mode(
    pool: &sqlx::PgPool,
    live_deployment_profile: Option<&str>,
    chain: &str,
    block_hashes: &[String],
    source_scope: Option<&[(String, String, i64, i64)]>,
    mode: PersistedRawPayloadAdapterSyncMode,
    reconcile_full_source: bool,
    progress: &mut Option<&mut dyn crate::StartupAdapterProgress>,
) -> Result<bigname_adapters::EnsV2RegistryResourceSurfaceSyncSummary> {
    let raw_log_guard = if reconcile_full_source {
        Some(acquire_raw_log_staging_read_guard(pool, chain).await?)
    } else {
        None
    };
    let operation = async {
        let summary = if reconcile_full_source {
            let target_block_number =
                load_live_adapter_target_block_number(pool, chain, block_hashes).await?;
            bigname_adapters::sync_ens_v2_registry_resource_surface_through_block(
                pool,
                chain,
                target_block_number,
            )
            .await?
        } else {
            match (mode, source_scope) {
                (PersistedRawPayloadAdapterSyncMode::LivePoll, _) => {
                    let deployment_profile = live_deployment_profile
                        .context("ENSv2 live-poll adapter sync is missing its deployment profile")?;
                    let target_block_number =
                        load_live_adapter_target_block_number(pool, chain, block_hashes).await?;
                    bigname_adapters::sync_ens_v2_registry_resource_surface_live_poll(
                        pool,
                        deployment_profile,
                        chain,
                        target_block_number,
                        block_hashes,
                    )
                    .await?
                }
                (PersistedRawPayloadAdapterSyncMode::RawFactReplay { .. }, Some(source_scope)) => {
                    bigname_adapters::EnsV2RegistryResourceSurfaceSyncSummary::sync_for_block_hashes_with_source_scope_canonical_only(
                        pool,
                        chain,
                        block_hashes,
                        source_scope,
                    )
                    .await?
                }
                (PersistedRawPayloadAdapterSyncMode::RawFactReplay { .. }, None) => {
                    bigname_adapters::EnsV2RegistryResourceSurfaceSyncSummary::sync_for_block_hashes_canonical_only(
                        pool,
                        chain,
                        block_hashes,
                    )
                    .await?
                }
                (_, Some(source_scope)) => {
                    bigname_adapters::EnsV2RegistryResourceSurfaceSyncSummary::sync_for_block_hashes_with_source_scope(
                        pool,
                        chain,
                        block_hashes,
                        source_scope,
                    )
                    .await?
                }
                (_, None) => {
                    bigname_adapters::EnsV2RegistryResourceSurfaceSyncSummary::sync_for_block_hashes(
                        pool,
                        chain,
                        block_hashes,
                    )
                    .await?
                }
            }
        };
        if let Some(progress) = progress.as_deref_mut() {
            progress.record(pool).await?;
        }
        Ok(summary)
    }
    .await;
    let release = match raw_log_guard {
        Some(guard) => guard.release().await,
        None => Ok(()),
    };
    prioritize_operation_error(operation, release)
}

pub(super) const fn ens_v2_registry_sync_operation(
    mode: PersistedRawPayloadAdapterSyncMode,
    reconcile_full_source: bool,
) -> &'static str {
    if reconcile_full_source {
        "sync_ens_v2_registry_resource_surface_through_block"
    } else if matches!(mode, PersistedRawPayloadAdapterSyncMode::LivePoll) {
        "sync_ens_v2_registry_resource_surface_live_poll"
    } else {
        "sync_for_block_hashes"
    }
}
