use anyhow::{Context, Result};
use bigname_manifests::WatchedChainPlan;

use super::logging::{
    log_ens_v1_reverse_claim_sync_summary, log_ens_v1_subregistry_discovery_sync_summary,
    log_ens_v1_unwrapped_authority_sync_summary, log_ens_v2_permissions_sync_summary,
    log_ens_v2_registrar_sync_summary, log_ens_v2_registry_resource_surface_sync_summary,
    log_ens_v2_resolver_sync_summary,
};

pub(crate) async fn sync_adapter_owned_raw_log_state(
    pool: &sqlx::PgPool,
    watched_chain_plan: &[WatchedChainPlan],
) -> Result<()> {
    for chain in watched_chain_plan {
        let summary = bigname_adapters::sync_ens_v1_reverse_claim(pool, &chain.chain)
            .await
            .with_context(|| {
                format!(
                    "failed to sync ENSv1 reverse claim from stored raw logs for chain {}",
                    chain.chain
                )
            })?;
        log_ens_v1_reverse_claim_sync_summary(&chain.chain, &summary);

        let summary = bigname_adapters::sync_ens_v1_subregistry_discovery(pool, &chain.chain)
            .await
            .with_context(|| {
                format!(
                    "failed to sync ENSv1 registry discovery from stored raw logs for chain {}",
                    chain.chain
                )
            })?;
        log_ens_v1_subregistry_discovery_sync_summary(&chain.chain, &summary);

        let summary = bigname_adapters::sync_ens_v1_unwrapped_authority(pool, &chain.chain)
            .await
            .with_context(|| {
                format!(
                    "failed to sync ENSv1 unwrapped authority from stored raw logs for chain {}",
                    chain.chain
                )
            })?;
        log_ens_v1_unwrapped_authority_sync_summary(&chain.chain, &summary);

        let summary = bigname_adapters::sync_ens_v2_registry_resource_surface(pool, &chain.chain)
            .await
            .with_context(|| {
                format!(
                    "failed to sync ENSv2 registry resource/surface state from stored raw logs for chain {}",
                    chain.chain
                )
            })?;
        log_ens_v2_registry_resource_surface_sync_summary(&chain.chain, &summary);

        let summary = bigname_adapters::sync_ens_v2_registrar(pool, &chain.chain)
            .await
            .with_context(|| {
                format!(
                    "failed to sync ENSv2 registrar state from stored raw logs for chain {}",
                    chain.chain
                )
            })?;
        log_ens_v2_registrar_sync_summary(&chain.chain, &summary);

        let summary = bigname_adapters::sync_ens_v2_resolver(pool, &chain.chain)
            .await
            .with_context(|| {
                format!(
                    "failed to sync ENSv2 resolver state from stored raw logs for chain {}",
                    chain.chain
                )
            })?;
        log_ens_v2_resolver_sync_summary(&chain.chain, &summary);

        let summary = bigname_adapters::sync_ens_v2_permissions(pool, &chain.chain)
            .await
            .with_context(|| {
                format!(
                    "failed to sync ENSv2 permissions state from stored raw logs for chain {}",
                    chain.chain
                )
            })?;
        log_ens_v2_permissions_sync_summary(&chain.chain, &summary);
    }

    Ok(())
}
