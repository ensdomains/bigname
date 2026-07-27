use anyhow::{Context, Result};
use bigname_adapters::ENS_V1_SUBREGISTRY_DISCOVERY_STARTUP_VERSION;
use bigname_manifests::WatchedChainPlan;

use crate::run::startup_heartbeat::{StartupAdapterHeartbeat, StartupHeartbeat};

use super::super::logging::log_ens_v1_subregistry_discovery_sync_summary;
use super::{
    MAX_NON_LOOPING_STARTUP_FAMILY_PASSES, checkpoint::prepare_startup_family_sync,
    complete_non_looping_startup_family, journal_authority_with_startup_progress,
    load_startup_adapter_checkpoint_context, record_startup_sync_progress,
    sync_ens_v2_registry_to_startup_fixed_point,
};

/// Materialize only the discovery edges needed by the post-bootstrap live-plan
/// widen. Auto bootstrap stores raw facts without adapter work; replay catch-up
/// owns the remaining historical adapter families.
#[cfg(test)]
pub(crate) async fn sync_discovery_adapter_owned_raw_log_state(
    pool: &sqlx::PgPool,
    deployment_profile: &str,
    watched_chain_plan: &[WatchedChainPlan],
    startup_discovery_page_logs: usize,
) -> Result<()> {
    sync_discovery_adapter_owned_raw_log_state_inner(
        pool,
        deployment_profile,
        watched_chain_plan,
        startup_discovery_page_logs,
        None,
    )
    .await
}

pub(crate) async fn sync_discovery_adapter_owned_raw_log_state_with_heartbeat(
    pool: &sqlx::PgPool,
    deployment_profile: &str,
    watched_chain_plan: &[WatchedChainPlan],
    startup_discovery_page_logs: usize,
    heartbeat: &mut StartupHeartbeat,
    heartbeat_chain_ids: &[String],
) -> Result<()> {
    heartbeat.record(pool, heartbeat_chain_ids).await?;
    sync_discovery_adapter_owned_raw_log_state_inner(
        pool,
        deployment_profile,
        watched_chain_plan,
        startup_discovery_page_logs,
        Some((heartbeat, heartbeat_chain_ids)),
    )
    .await
}

async fn sync_discovery_adapter_owned_raw_log_state_inner(
    pool: &sqlx::PgPool,
    deployment_profile: &str,
    watched_chain_plan: &[WatchedChainPlan],
    startup_discovery_page_logs: usize,
    mut startup_heartbeat: Option<(&mut StartupHeartbeat, &[String])>,
) -> Result<()> {
    record_startup_sync_progress(pool, &mut startup_heartbeat).await?;
    journal_authority_with_startup_progress(pool, &mut startup_heartbeat).await?;
    for chain in watched_chain_plan {
        for pass in 1..=MAX_NON_LOOPING_STARTUP_FAMILY_PASSES {
            let Some(attempt) = prepare_startup_family_sync(
                pool,
                Some(deployment_profile),
                &chain.chain,
                ENS_V1_SUBREGISTRY_DISCOVERY_STARTUP_VERSION,
            )
            .await?
            else {
                break;
            };
            let startup_checkpoint =
                load_startup_adapter_checkpoint_context(pool, deployment_profile, &chain.chain)
                    .await?;
            let summary = match startup_heartbeat.as_mut() {
                Some((heartbeat, chain_ids)) => {
                    let mut progress = StartupAdapterHeartbeat::new(heartbeat, chain_ids);
                    bigname_adapters::sync_ens_v1_subregistry_discovery_with_startup_checkpoint_and_log_limit_and_progress(
                        pool,
                        &chain.chain,
                        &startup_checkpoint,
                        startup_discovery_page_logs,
                        &mut progress,
                    )
                    .await
                }
                None => {
                    bigname_adapters::sync_ens_v1_subregistry_discovery_with_startup_checkpoint_and_log_limit(
                        pool,
                        &chain.chain,
                        &startup_checkpoint,
                        startup_discovery_page_logs,
                    )
                    .await
                }
            }
            .with_context(|| {
                format!(
                    "failed to sync ENSv1 registry discovery from stored raw logs for chain {}",
                    chain.chain
                )
            })?;
            log_ens_v1_subregistry_discovery_sync_summary(&chain.chain, &summary);
            if complete_non_looping_startup_family(
                attempt,
                pool,
                &chain.chain,
                ENS_V1_SUBREGISTRY_DISCOVERY_STARTUP_VERSION,
                pass,
            )
            .await?
            {
                break;
            }
        }
        record_startup_sync_progress(pool, &mut startup_heartbeat).await?;

        sync_ens_v2_registry_to_startup_fixed_point(
            pool,
            Some(deployment_profile),
            &chain.chain,
            &mut startup_heartbeat,
        )
        .await?;
        record_startup_sync_progress(pool, &mut startup_heartbeat).await?;
    }
    journal_authority_with_startup_progress(pool, &mut startup_heartbeat).await?;
    record_startup_sync_progress(pool, &mut startup_heartbeat).await?;
    Ok(())
}
