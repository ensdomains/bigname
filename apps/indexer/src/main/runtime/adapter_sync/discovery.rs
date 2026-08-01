use anyhow::{Context, Result};
use bigname_manifests::WatchedChainPlan;

use crate::run::startup_heartbeat::StartupHeartbeat;

use super::super::logging::{
    log_ens_v1_subregistry_discovery_sync_summary,
    log_ens_v2_registry_resource_surface_sync_summary,
};
use super::{await_full_source_adapter_with_optional_heartbeat, record_startup_sync_progress};

/// Materialize only the discovery edges needed by the post-bootstrap live-plan widen. Auto
/// bootstrap stores raw facts without adapter work; replay catch-up owns the remaining historical
/// adapter families.
#[cfg(test)]
pub(crate) async fn sync_discovery_adapter_owned_raw_log_state(
    pool: &sqlx::PgPool,
    watched_chain_plan: &[WatchedChainPlan],
) -> Result<()> {
    sync_discovery_adapter_owned_raw_log_state_inner(pool, watched_chain_plan, None).await
}

pub(crate) async fn sync_discovery_adapter_owned_raw_log_state_with_heartbeat(
    pool: &sqlx::PgPool,
    watched_chain_plan: &[WatchedChainPlan],
    heartbeat: &mut StartupHeartbeat,
    heartbeat_chain_ids: &[String],
) -> Result<()> {
    heartbeat.record(pool, heartbeat_chain_ids).await?;
    sync_discovery_adapter_owned_raw_log_state_inner(
        pool,
        watched_chain_plan,
        Some((heartbeat, heartbeat_chain_ids)),
    )
    .await
}

async fn sync_discovery_adapter_owned_raw_log_state_inner(
    pool: &sqlx::PgPool,
    watched_chain_plan: &[WatchedChainPlan],
    mut startup_heartbeat: Option<(&mut StartupHeartbeat, &[String])>,
) -> Result<()> {
    record_startup_sync_progress(pool, &mut startup_heartbeat).await?;
    for chain in watched_chain_plan {
        let summary = await_full_source_adapter_with_optional_heartbeat(
            pool,
            &chain.chain,
            &mut startup_heartbeat,
            false,
            bigname_adapters::sync_ens_v1_subregistry_discovery(pool, &chain.chain),
        )
        .await
        .with_context(|| {
            format!(
                "failed to sync ENSv1 registry discovery from stored raw logs for chain {}",
                chain.chain
            )
        })?;
        log_ens_v1_subregistry_discovery_sync_summary(&chain.chain, &summary);
        record_startup_sync_progress(pool, &mut startup_heartbeat).await?;

        let summary = await_full_source_adapter_with_optional_heartbeat(
            pool,
            &chain.chain,
            &mut startup_heartbeat,
            false,
            bigname_adapters::sync_ens_v2_registry_resource_surface(pool, &chain.chain),
        )
        .await
        .with_context(|| {
            format!(
                "failed to sync ENSv2 registry resource/surface state and discovery from stored raw logs for chain {}",
                chain.chain
            )
        })?;
        log_ens_v2_registry_resource_surface_sync_summary(&chain.chain, &summary);
        record_startup_sync_progress(pool, &mut startup_heartbeat).await?;
    }
    Ok(())
}
