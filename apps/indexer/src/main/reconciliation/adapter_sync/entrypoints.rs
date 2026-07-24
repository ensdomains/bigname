use std::collections::BTreeSet;

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

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(super) struct FullSourceReconciliationScope {
    adapters: BTreeSet<NormalizedEventReplayAdapter>,
}

impl FullSourceReconciliationScope {
    fn from_active_adapters(
        adapters: impl IntoIterator<Item = NormalizedEventReplayAdapter>,
    ) -> Self {
        Self {
            adapters: adapters.into_iter().collect(),
        }
    }

    fn includes(&self, adapter: NormalizedEventReplayAdapter) -> bool {
        self.adapters.contains(&adapter)
    }

    pub(super) fn reconciles_legacy_registry(&self) -> bool {
        self.includes(NormalizedEventReplayAdapter::EnsV1SubregistryDiscovery)
    }

    pub(super) fn reconciles_ens_v2_registry(&self) -> bool {
        self.includes(NormalizedEventReplayAdapter::EnsV2RegistryResourceSurface)
    }
}

pub(crate) async fn sync_adapter_state_from_persisted_raw_payloads(
    pool: &sqlx::PgPool,
    chain: &str,
    block_hashes: &[String],
) -> Result<PersistedRawPayloadAdapterSyncSummary> {
    sync_adapter_state_from_persisted_raw_payloads_for_backfill(pool, chain, block_hashes, false)
        .await
}

pub(crate) async fn sync_adapter_state_from_persisted_raw_payloads_without_ens_v2_adapters(
    pool: &sqlx::PgPool,
    chain: &str,
    block_hashes: &[String],
) -> Result<PersistedRawPayloadAdapterSyncSummary> {
    sync_adapter_state_from_persisted_raw_payloads_for_backfill(pool, chain, block_hashes, true)
        .await
}

async fn sync_adapter_state_from_persisted_raw_payloads_for_backfill(
    pool: &sqlx::PgPool,
    chain: &str,
    block_hashes: &[String],
    defer_ens_v2_adapters: bool,
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
        defer_ens_v2_adapters,
        true,
        FullSourceReconciliationScope::default(),
        &mut None,
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
        &mut None,
    )
    .await
}

#[allow(dead_code)]
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
        &mut None,
    )
    .await
}

pub(crate) async fn sync_live_adapter_state_from_persisted_raw_payloads_with_progress(
    pool: &sqlx::PgPool,
    deployment_profile: &str,
    chain: &str,
    block_hashes: &[String],
    progress: &mut Option<&mut dyn bigname_adapters::StartupAdapterProgress>,
) -> Result<PersistedRawPayloadAdapterSyncSummary> {
    sync_live_adapter_state_from_persisted_raw_payloads_with_reorg_repair(
        pool,
        deployment_profile,
        chain,
        block_hashes,
        false,
        progress,
    )
    .await
}

pub(crate) async fn sync_live_adapter_state_from_persisted_raw_payloads_after_reorg_with_progress(
    pool: &sqlx::PgPool,
    deployment_profile: &str,
    chain: &str,
    block_hashes: &[String],
    progress: &mut Option<&mut dyn bigname_adapters::StartupAdapterProgress>,
) -> Result<PersistedRawPayloadAdapterSyncSummary> {
    sync_live_adapter_state_from_persisted_raw_payloads_with_reorg_repair(
        pool,
        deployment_profile,
        chain,
        block_hashes,
        true,
        progress,
    )
    .await
}

async fn sync_live_adapter_state_from_persisted_raw_payloads_with_reorg_repair(
    pool: &sqlx::PgPool,
    deployment_profile: &str,
    chain: &str,
    block_hashes: &[String],
    reconcile_discovery_full_sources: bool,
    progress: &mut Option<&mut dyn bigname_adapters::StartupAdapterProgress>,
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
    let full_source_reconciliation = if reconcile_discovery_full_sources {
        FullSourceReconciliationScope::from_active_adapters(
            active_closure_or_dependency_replay_adapters(pool, chain).await?,
        )
    } else {
        FullSourceReconciliationScope::default()
    };
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
        false,
        true,
        full_source_reconciliation,
        progress,
    )
    .await
}

pub(crate) async fn sync_replay_normalized_events_from_persisted_raw_payloads_with_progress(
    pool: &sqlx::PgPool,
    chain: &str,
    block_hashes: &[String],
    source_scope: Option<&[(String, String, i64, i64)]>,
    canonical_raw_log_count: usize,
    replay_contract_plan: RawFactReplayContractPlan,
    progress: &mut Option<&mut dyn bigname_adapters::StartupAdapterProgress>,
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
        FullSourceReconciliationScope::default(),
        progress,
    )
    .await
}

pub(crate) async fn sync_adapter_state_from_scoped_persisted_raw_payloads(
    pool: &sqlx::PgPool,
    chain: &str,
    block_hashes: &[String],
    source_scope: &[(String, String, i64, i64)],
) -> Result<()> {
    sync_adapter_state_from_scoped_persisted_raw_payloads_for_backfill(
        pool,
        chain,
        block_hashes,
        source_scope,
        false,
    )
    .await
}

pub(crate) async fn sync_adapter_state_from_scoped_persisted_raw_payloads_without_ens_v2_adapters(
    pool: &sqlx::PgPool,
    chain: &str,
    block_hashes: &[String],
    source_scope: &[(String, String, i64, i64)],
) -> Result<()> {
    sync_adapter_state_from_scoped_persisted_raw_payloads_for_backfill(
        pool,
        chain,
        block_hashes,
        source_scope,
        true,
    )
    .await
}

async fn sync_adapter_state_from_scoped_persisted_raw_payloads_for_backfill(
    pool: &sqlx::PgPool,
    chain: &str,
    block_hashes: &[String],
    source_scope: &[(String, String, i64, i64)],
    defer_ens_v2_adapters: bool,
) -> Result<()> {
    sync_adapter_state_from_persisted_raw_payloads_with_mode(
        pool,
        None,
        chain,
        block_hashes,
        Some(source_scope),
        PersistedRawPayloadAdapterSyncMode::LiveOrBackfill,
        defer_ens_v2_adapters,
        false,
        FullSourceReconciliationScope::default(),
        &mut None,
    )
    .await
    .map(|_| ())?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn live_reorg_full_source_scope_preserves_every_active_adapter() {
        let scope = FullSourceReconciliationScope::from_active_adapters([
            NormalizedEventReplayAdapter::EnsV1SubregistryDiscovery,
            NormalizedEventReplayAdapter::EnsV2RegistryResourceSurface,
            NormalizedEventReplayAdapter::EnsV2Registrar,
        ]);

        assert_eq!(
            scope.adapters,
            BTreeSet::from([
                NormalizedEventReplayAdapter::EnsV1SubregistryDiscovery,
                NormalizedEventReplayAdapter::EnsV2RegistryResourceSurface,
                NormalizedEventReplayAdapter::EnsV2Registrar,
            ])
        );
    }
}
