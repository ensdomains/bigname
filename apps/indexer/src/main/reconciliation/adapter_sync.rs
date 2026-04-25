use anyhow::{Context, Result};

use crate::runtime::{
    log_block_derived_normalized_event_summary, log_ens_v1_reverse_claim_sync_summary,
    log_ens_v1_subregistry_discovery_sync_summary, log_ens_v1_unwrapped_authority_sync_summary,
    log_ens_v2_permissions_sync_summary, log_ens_v2_registrar_sync_summary,
    log_ens_v2_registry_resource_surface_sync_summary, log_ens_v2_resolver_sync_summary,
};

use super::types::PersistedRawPayloadAdapterSyncSummary;

const SOURCE_FAMILY_ENS_V2_ROOT_L1: &str = "ens_v2_root_l1";
const SOURCE_FAMILY_ENS_V2_REGISTRY_L1: &str = "ens_v2_registry_l1";
const SOURCE_FAMILY_ENS_V1_REGISTRY_L1: &str = "ens_v1_registry_l1";
const SOURCE_FAMILY_BASENAMES_BASE_REGISTRY: &str = "basenames_base_registry";

pub(crate) async fn sync_adapter_state_from_persisted_raw_payloads(
    pool: &sqlx::PgPool,
    chain: &str,
    block_hashes: &[String],
) -> Result<PersistedRawPayloadAdapterSyncSummary> {
    sync_adapter_state_from_persisted_raw_payloads_with_mode(
        pool,
        chain,
        block_hashes,
        PersistedRawPayloadAdapterSyncMode::LiveOrBackfill,
    )
    .await
}

pub(crate) async fn sync_replay_normalized_events_from_persisted_raw_payloads(
    pool: &sqlx::PgPool,
    chain: &str,
    block_hashes: &[String],
) -> Result<PersistedRawPayloadAdapterSyncSummary> {
    sync_adapter_state_from_persisted_raw_payloads_with_mode(
        pool,
        chain,
        block_hashes,
        PersistedRawPayloadAdapterSyncMode::RawFactReplay,
    )
    .await
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PersistedRawPayloadAdapterSyncMode {
    LiveOrBackfill,
    RawFactReplay,
}

async fn sync_adapter_state_from_persisted_raw_payloads_with_mode(
    pool: &sqlx::PgPool,
    chain: &str,
    block_hashes: &[String],
    mode: PersistedRawPayloadAdapterSyncMode,
) -> Result<PersistedRawPayloadAdapterSyncSummary> {
    let mut aggregate = PersistedRawPayloadAdapterSyncSummary::default();
    let initial_normalized_event_count = load_normalized_event_count(pool, chain).await?;

    let normalized_event_summary =
        bigname_adapters::sync_block_derived_normalized_events(pool, chain, block_hashes, None)
            .await?;
    log_block_derived_normalized_event_summary(chain, &normalized_event_summary);
    aggregate.add_counts(
        normalized_event_summary.scanned_log_count,
        normalized_event_summary.matched_log_count,
        normalized_event_summary.total_synced_count,
        0,
    );
    let subregistry_discovery_summary = match mode {
        PersistedRawPayloadAdapterSyncMode::LiveOrBackfill => {
            bigname_adapters::sync_ens_v1_subregistry_discovery(pool, chain).await?
        }
        PersistedRawPayloadAdapterSyncMode::RawFactReplay => {
            bigname_adapters::EnsV1SubregistryDiscoverySyncSummary::sync_for_block_hashes_without_discovery_reconciliation(
                pool,
                chain,
                block_hashes,
            )
            .await?
        }
    };
    log_ens_v1_subregistry_discovery_sync_summary(chain, &subregistry_discovery_summary);
    let reverse_claim_summary =
        bigname_adapters::EnsV1ReverseClaimSyncSummary::sync_for_block_hashes(
            pool,
            chain,
            block_hashes,
        )
        .await?;
    log_ens_v1_reverse_claim_sync_summary(chain, &reverse_claim_summary);
    aggregate.add_counts(
        reverse_claim_summary.scanned_log_count,
        reverse_claim_summary.matched_log_count,
        reverse_claim_summary.total_synced_count,
        0,
    );
    let unwrapped_authority_summary =
        bigname_adapters::EnsV1UnwrappedAuthoritySyncSummary::sync_for_block_hashes(
            pool,
            chain,
            block_hashes,
        )
        .await?;
    log_ens_v1_unwrapped_authority_sync_summary(chain, &unwrapped_authority_summary);
    aggregate.add_counts(
        unwrapped_authority_summary.scanned_log_count,
        unwrapped_authority_summary.matched_log_count,
        unwrapped_authority_summary.total_normalized_event_count,
        0,
    );
    let ens_v2_registry_summary =
        bigname_adapters::EnsV2RegistryResourceSurfaceSyncSummary::sync_for_block_hashes(
            pool,
            chain,
            block_hashes,
        )
        .await?;
    log_ens_v2_registry_resource_surface_sync_summary(chain, &ens_v2_registry_summary);
    aggregate.add_counts(
        ens_v2_registry_summary.scanned_log_count,
        ens_v2_registry_summary.matched_log_count,
        ens_v2_registry_summary.total_normalized_event_count,
        0,
    );
    let ens_v2_registrar_summary =
        bigname_adapters::EnsV2RegistrarSyncSummary::sync_for_block_hashes(
            pool,
            chain,
            block_hashes,
        )
        .await?;
    log_ens_v2_registrar_sync_summary(chain, &ens_v2_registrar_summary);
    aggregate.add_counts(
        ens_v2_registrar_summary.scanned_log_count,
        ens_v2_registrar_summary.matched_log_count,
        ens_v2_registrar_summary.total_synced_count,
        0,
    );
    let ens_v2_resolver_summary =
        bigname_adapters::EnsV2ResolverSyncSummary::sync_for_block_hashes(
            pool,
            chain,
            block_hashes,
        )
        .await?;
    log_ens_v2_resolver_sync_summary(chain, &ens_v2_resolver_summary);
    aggregate.add_counts(
        ens_v2_resolver_summary.scanned_log_count,
        ens_v2_resolver_summary.matched_log_count,
        ens_v2_resolver_summary.total_synced_count,
        0,
    );
    let ens_v2_permissions_summary =
        bigname_adapters::EnsV2PermissionsSyncSummary::sync_for_block_hashes(
            pool,
            chain,
            block_hashes,
        )
        .await?;
    log_ens_v2_permissions_sync_summary(chain, &ens_v2_permissions_summary);
    aggregate.add_counts(
        ens_v2_permissions_summary.scanned_log_count,
        ens_v2_permissions_summary.matched_log_count,
        ens_v2_permissions_summary.total_synced_count,
        0,
    );
    let final_normalized_event_count = load_normalized_event_count(pool, chain).await?;
    aggregate.total_inserted_count = normalized_event_insert_delta(
        initial_normalized_event_count,
        final_normalized_event_count,
    )?;

    Ok(aggregate)
}

async fn load_normalized_event_count(pool: &sqlx::PgPool, chain: &str) -> Result<i64> {
    sqlx::query_scalar::<_, i64>(
        r#"
        SELECT COUNT(*)::BIGINT
        FROM normalized_events
        WHERE chain_id = $1
        "#,
    )
    .bind(chain)
    .fetch_one(pool)
    .await
    .with_context(|| format!("failed to count normalized_events for chain {chain}"))
}

fn normalized_event_insert_delta(before: i64, after: i64) -> Result<usize> {
    let inserted = after.saturating_sub(before);
    usize::try_from(inserted).context("normalized event insert count does not fit in usize")
}

pub(crate) async fn sync_adapter_state_from_scoped_persisted_raw_payloads(
    pool: &sqlx::PgPool,
    chain: &str,
    block_hashes: &[String],
    source_scope: &[(String, String, i64, i64)],
) -> Result<()> {
    let normalized_event_summary = bigname_adapters::sync_block_derived_normalized_events(
        pool,
        chain,
        block_hashes,
        Some(source_scope),
    )
    .await?;
    log_block_derived_normalized_event_summary(chain, &normalized_event_summary);

    if source_scope_includes_ens_v1_registry(source_scope) {
        let subregistry_discovery_summary =
            bigname_adapters::EnsV1SubregistryDiscoverySyncSummary::sync_for_block_hashes_with_source_scope(
                pool,
                chain,
                block_hashes,
                source_scope,
            )
            .await?;
        log_ens_v1_subregistry_discovery_sync_summary(chain, &subregistry_discovery_summary);

        let unwrapped_authority_summary =
            bigname_adapters::EnsV1UnwrappedAuthoritySyncSummary::sync_for_block_hashes_with_source_scope(
                pool,
                chain,
                block_hashes,
                source_scope,
            )
            .await?;
        log_ens_v1_unwrapped_authority_sync_summary(chain, &unwrapped_authority_summary);
    }

    if source_scope_includes_ens_v2_registry(source_scope) {
        let ens_v2_registry_summary =
            bigname_adapters::EnsV2RegistryResourceSurfaceSyncSummary::sync_for_block_hashes_with_source_scope(
                pool,
                chain,
                block_hashes,
                source_scope,
            )
            .await?;
        log_ens_v2_registry_resource_surface_sync_summary(chain, &ens_v2_registry_summary);
    }

    Ok(())
}

fn source_scope_includes_ens_v1_registry(source_scope: &[(String, String, i64, i64)]) -> bool {
    source_scope.iter().any(|(source_family, _, _, _)| {
        source_family == SOURCE_FAMILY_ENS_V1_REGISTRY_L1
            || source_family == SOURCE_FAMILY_BASENAMES_BASE_REGISTRY
    })
}

fn source_scope_includes_ens_v2_registry(source_scope: &[(String, String, i64, i64)]) -> bool {
    source_scope.iter().any(|(source_family, _, _, _)| {
        source_family == SOURCE_FAMILY_ENS_V2_ROOT_L1
            || source_family == SOURCE_FAMILY_ENS_V2_REGISTRY_L1
    })
}
