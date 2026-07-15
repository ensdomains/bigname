use anyhow::Result;
use tracing::info;

use crate::reconciliation::{
    replay::{
        NormalizedEventReplayAdapter, RawFactReplayContractPlan,
        active_closure_or_dependency_replay_adapters,
    },
    types::PersistedRawPayloadAdapterSyncSummary,
};

use super::{
    mode::PersistedRawPayloadAdapterSyncMode, scope::load_live_adapter_source_scope,
    sync_adapter_state_from_persisted_raw_payloads_with_mode,
};

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct ReorgDiscoveryFullSources {
    legacy_registry: bool,
    ens_v2_registry: bool,
}

fn reorg_discovery_full_sources(
    active_adapters: &[NormalizedEventReplayAdapter],
) -> ReorgDiscoveryFullSources {
    ReorgDiscoveryFullSources {
        legacy_registry: active_adapters
            .contains(&NormalizedEventReplayAdapter::EnsV1SubregistryDiscovery),
        ens_v2_registry: active_adapters
            .contains(&NormalizedEventReplayAdapter::EnsV2RegistryResourceSurface),
    }
}

pub(crate) async fn sync_adapter_state_from_persisted_raw_payloads(
    pool: &sqlx::PgPool,
    chain: &str,
    block_hashes: &[String],
) -> Result<PersistedRawPayloadAdapterSyncSummary> {
    info!(
        service = "indexer",
        command = "adapter-sync",
        chain,
        block_hash_count = block_hashes.len(),
        adapter_sync_mode = "live_or_backfill",
        "loading live adapter source scope"
    );
    let source_scope = load_live_adapter_source_scope(pool, chain, block_hashes).await?;
    info!(
        service = "indexer",
        command = "adapter-sync",
        chain,
        block_hash_count = block_hashes.len(),
        source_scope_target_count = source_scope.len(),
        adapter_sync_mode = "live_or_backfill",
        "loaded live adapter source scope"
    );
    sync_adapter_state_from_persisted_raw_payloads_with_mode(
        pool,
        None,
        chain,
        block_hashes,
        Some(&source_scope),
        PersistedRawPayloadAdapterSyncMode::LiveOrBackfill,
        true,
        false,
        false,
    )
    .await
}

pub(crate) async fn sync_live_adapter_state_from_persisted_raw_payloads(
    pool: &sqlx::PgPool,
    deployment_profile: &str,
    chain: &str,
    block_hashes: &[String],
) -> Result<PersistedRawPayloadAdapterSyncSummary> {
    sync_live_adapter_state_from_persisted_raw_payloads_with_reorg_repair(
        pool,
        deployment_profile,
        chain,
        block_hashes,
        false,
    )
    .await
}

pub(crate) async fn sync_live_adapter_state_from_persisted_raw_payloads_after_reorg(
    pool: &sqlx::PgPool,
    deployment_profile: &str,
    chain: &str,
    block_hashes: &[String],
) -> Result<PersistedRawPayloadAdapterSyncSummary> {
    sync_live_adapter_state_from_persisted_raw_payloads_with_reorg_repair(
        pool,
        deployment_profile,
        chain,
        block_hashes,
        true,
    )
    .await
}

async fn sync_live_adapter_state_from_persisted_raw_payloads_with_reorg_repair(
    pool: &sqlx::PgPool,
    deployment_profile: &str,
    chain: &str,
    block_hashes: &[String],
    reconcile_discovery_full_sources: bool,
) -> Result<PersistedRawPayloadAdapterSyncSummary> {
    info!(
        service = "indexer",
        command = "adapter-sync",
        chain,
        block_hash_count = block_hashes.len(),
        adapter_sync_mode = "live_poll",
        "loading live adapter source scope"
    );
    let source_scope = load_live_adapter_source_scope(pool, chain, block_hashes).await?;
    let active_reorg_adapters = if reconcile_discovery_full_sources {
        active_closure_or_dependency_replay_adapters(pool, chain).await?
    } else {
        Vec::new()
    };
    let reorg_full_sources = reorg_discovery_full_sources(&active_reorg_adapters);
    info!(
        service = "indexer",
        command = "adapter-sync",
        chain,
        block_hash_count = block_hashes.len(),
        source_scope_target_count = source_scope.len(),
        adapter_sync_mode = "live_poll",
        "loaded live adapter source scope"
    );
    sync_adapter_state_from_persisted_raw_payloads_with_mode(
        pool,
        Some(deployment_profile),
        chain,
        block_hashes,
        Some(&source_scope),
        PersistedRawPayloadAdapterSyncMode::LivePoll,
        true,
        reorg_full_sources.legacy_registry,
        reorg_full_sources.ens_v2_registry,
    )
    .await
}

pub(crate) async fn sync_replay_normalized_events_from_persisted_raw_payloads(
    pool: &sqlx::PgPool,
    chain: &str,
    block_hashes: &[String],
    source_scope: Option<&[(String, String, i64, i64)]>,
    canonical_raw_log_count: usize,
    replay_contract_plan: RawFactReplayContractPlan,
) -> Result<PersistedRawPayloadAdapterSyncSummary> {
    sync_adapter_state_from_persisted_raw_payloads_with_mode(
        pool,
        None,
        chain,
        block_hashes,
        source_scope,
        PersistedRawPayloadAdapterSyncMode::RawFactReplay {
            canonical_raw_log_count,
            replay_contract_plan,
        },
        false,
        false,
        false,
    )
    .await
}

pub(crate) async fn sync_adapter_state_from_scoped_persisted_raw_payloads(
    pool: &sqlx::PgPool,
    chain: &str,
    block_hashes: &[String],
    source_scope: &[(String, String, i64, i64)],
) -> Result<()> {
    sync_adapter_state_from_persisted_raw_payloads_with_mode(
        pool,
        None,
        chain,
        block_hashes,
        Some(source_scope),
        PersistedRawPayloadAdapterSyncMode::LiveOrBackfill,
        false,
        false,
        false,
    )
    .await
    .map(|_| ())?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn live_reorg_force_selects_every_active_registry_discovery_family() {
        assert_eq!(
            reorg_discovery_full_sources(&[
                NormalizedEventReplayAdapter::EnsV1SubregistryDiscovery,
                NormalizedEventReplayAdapter::EnsV2RegistryResourceSurface,
            ]),
            ReorgDiscoveryFullSources {
                legacy_registry: true,
                ens_v2_registry: true,
            }
        );
        assert_eq!(
            reorg_discovery_full_sources(&[
                NormalizedEventReplayAdapter::EnsV1SubregistryDiscovery,
            ]),
            ReorgDiscoveryFullSources {
                legacy_registry: true,
                ens_v2_registry: false,
            }
        );
    }
}
