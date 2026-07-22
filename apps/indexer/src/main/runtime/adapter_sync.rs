use anyhow::{Context, Result};
use bigname_manifests::WatchedChainPlan;

use crate::{
    resolver_profile_convergence::{
        journal_resolver_profile_authority, journal_resolver_profile_authority_with_progress,
    },
    run::startup_heartbeat::{StartupAdapterHeartbeat, StartupHeartbeat},
};

use super::logging::{
    log_ens_v1_reverse_claim_sync_summary, log_ens_v1_subregistry_discovery_sync_summary,
    log_ens_v1_unwrapped_authority_sync_summary, log_ens_v2_permissions_sync_summary,
    log_ens_v2_registrar_sync_summary, log_ens_v2_registry_resource_surface_sync_summary,
    log_ens_v2_resolver_sync_summary,
};

// At roughly 2 KiB per log, the 100,000-log target is about 200 MB page-resident:
// about 10x smaller than normalized replay's resident set and 400x smaller than
// the #218 OOM class. Ownership/control pages preserve whole blocks, so an
// unusually dense block can exceed this target.
pub(crate) const DEFAULT_STARTUP_DISCOVERY_PAGE_LOGS: usize = 100_000;

pub(crate) async fn sync_adapter_owned_raw_log_state(
    pool: &sqlx::PgPool,
    watched_chain_plan: &[WatchedChainPlan],
) -> Result<()> {
    sync_adapter_owned_raw_log_state_with_startup_context(pool, watched_chain_plan, None, None)
        .await
}

pub(crate) async fn sync_adapter_owned_raw_log_state_with_heartbeat(
    pool: &sqlx::PgPool,
    deployment_profile: &str,
    watched_chain_plan: &[WatchedChainPlan],
    startup_discovery_page_logs: usize,
    heartbeat: &mut StartupHeartbeat,
    heartbeat_chain_ids: &[String],
) -> Result<()> {
    heartbeat.record(pool, heartbeat_chain_ids).await?;
    sync_adapter_owned_raw_log_state_with_startup_context(
        pool,
        watched_chain_plan,
        Some((deployment_profile, startup_discovery_page_logs)),
        Some((heartbeat, heartbeat_chain_ids)),
    )
    .await
}

async fn sync_adapter_owned_raw_log_state_with_startup_context(
    pool: &sqlx::PgPool,
    watched_chain_plan: &[WatchedChainPlan],
    startup_context: Option<(&str, usize)>,
    mut startup_heartbeat: Option<(&mut StartupHeartbeat, &[String])>,
) -> Result<()> {
    record_startup_sync_progress(pool, &mut startup_heartbeat).await?;
    // Broad startup/timer passes also recover any prior discovery transaction
    // that committed before its caller could journal the epoch change.
    journal_resolver_profile_authority_with_optional_progress(pool, &mut startup_heartbeat).await?;
    let mut completed_startup_checkpoints = Vec::new();
    for chain in watched_chain_plan {
        let startup_checkpoint = match startup_context {
            Some((deployment_profile, page_logs)) => Some((
                load_startup_adapter_checkpoint_context(pool, deployment_profile, &chain.chain)
                    .await?,
                page_logs,
            )),
            None => None,
        };
        let summary = match startup_heartbeat.as_mut() {
            Some((heartbeat, chain_ids)) => {
                let mut progress = StartupAdapterHeartbeat::new(heartbeat, chain_ids);
                bigname_adapters::sync_ens_v1_reverse_claim_with_progress(
                    pool,
                    &chain.chain,
                    &mut progress,
                )
                .await
            }
            None => bigname_adapters::sync_ens_v1_reverse_claim(pool, &chain.chain).await,
        }
        .with_context(|| {
            format!(
                "failed to sync ENSv1 reverse claim from stored raw logs for chain {}",
                chain.chain
            )
        })?;
        log_ens_v1_reverse_claim_sync_summary(&chain.chain, &summary);
        record_startup_sync_progress(pool, &mut startup_heartbeat).await?;

        let summary = match startup_checkpoint.as_ref() {
            Some((checkpoint, page_logs)) => match startup_heartbeat.as_mut() {
                Some((heartbeat, chain_ids)) => {
                    let mut progress = StartupAdapterHeartbeat::new(heartbeat, chain_ids);
                    bigname_adapters::sync_ens_v1_subregistry_discovery_with_startup_checkpoint_and_log_limit_and_progress(
                        pool,
                        &chain.chain,
                        checkpoint,
                        *page_logs,
                        &mut progress,
                    )
                    .await
                }
                None => {
                    bigname_adapters::sync_ens_v1_subregistry_discovery_with_startup_checkpoint_and_log_limit(
                        pool,
                        &chain.chain,
                        checkpoint,
                        *page_logs,
                    )
                    .await
                }
            },
            None => bigname_adapters::sync_ens_v1_subregistry_discovery(pool, &chain.chain).await,
        }
        .with_context(|| {
            format!(
                "failed to sync ENSv1 registry discovery from stored raw logs for chain {}",
                chain.chain
            )
        })?;
        log_ens_v1_subregistry_discovery_sync_summary(&chain.chain, &summary);
        record_startup_sync_progress(pool, &mut startup_heartbeat).await?;

        let summary = match startup_checkpoint.as_ref() {
            Some((checkpoint, page_logs)) => match startup_heartbeat.as_mut() {
                Some((heartbeat, chain_ids)) => {
                    let mut progress = StartupAdapterHeartbeat::new(heartbeat, chain_ids);
                    bigname_adapters::sync_ens_v1_unwrapped_authority_with_startup_checkpoint_and_log_limit_and_progress(
                        pool,
                        &chain.chain,
                        checkpoint,
                        *page_logs,
                        &mut progress,
                    )
                    .await
                }
                None => {
                    bigname_adapters::sync_ens_v1_unwrapped_authority_with_startup_checkpoint_and_log_limit(
                        pool,
                        &chain.chain,
                        checkpoint,
                        *page_logs,
                    )
                    .await
                }
            },
            None => bigname_adapters::sync_ens_v1_unwrapped_authority(pool, &chain.chain).await,
        }
        .with_context(|| {
            format!(
                "failed to sync ENSv1 unwrapped authority from stored raw logs for chain {}",
                chain.chain
            )
        })?;
        log_ens_v1_unwrapped_authority_sync_summary(&chain.chain, &summary);
        record_startup_sync_progress(pool, &mut startup_heartbeat).await?;

        let summary = bigname_adapters::sync_ens_v2_registry_resource_surface(pool, &chain.chain)
            .await
            .with_context(|| {
                format!(
                    "failed to sync ENSv2 registry resource/surface state from stored raw logs for chain {}",
                    chain.chain
                )
            })?;
        log_ens_v2_registry_resource_surface_sync_summary(&chain.chain, &summary);
        record_startup_sync_progress(pool, &mut startup_heartbeat).await?;

        let summary = bigname_adapters::sync_ens_v2_registrar(pool, &chain.chain)
            .await
            .with_context(|| {
                format!(
                    "failed to sync ENSv2 registrar state from stored raw logs for chain {}",
                    chain.chain
                )
            })?;
        log_ens_v2_registrar_sync_summary(&chain.chain, &summary);
        record_startup_sync_progress(pool, &mut startup_heartbeat).await?;

        let summary = bigname_adapters::sync_ens_v2_resolver(pool, &chain.chain)
            .await
            .with_context(|| {
                format!(
                    "failed to sync ENSv2 resolver state from stored raw logs for chain {}",
                    chain.chain
                )
            })?;
        log_ens_v2_resolver_sync_summary(&chain.chain, &summary);
        record_startup_sync_progress(pool, &mut startup_heartbeat).await?;

        let summary = bigname_adapters::sync_ens_v2_permissions(pool, &chain.chain)
            .await
            .with_context(|| {
                format!(
                    "failed to sync ENSv2 permissions state from stored raw logs for chain {}",
                    chain.chain
                )
            })?;
        log_ens_v2_permissions_sync_summary(&chain.chain, &summary);
        record_startup_sync_progress(pool, &mut startup_heartbeat).await?;

        if let Some((checkpoint, _)) = startup_checkpoint {
            completed_startup_checkpoints.push((chain.chain.clone(), checkpoint));
        }
    }

    journal_resolver_profile_authority_with_optional_progress(pool, &mut startup_heartbeat).await?;
    clear_completed_startup_adapter_checkpoints(pool, &completed_startup_checkpoints).await?;
    record_startup_sync_progress(pool, &mut startup_heartbeat).await?;
    Ok(())
}

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
    journal_resolver_profile_authority_with_optional_progress(pool, &mut startup_heartbeat).await?;
    let mut completed_startup_checkpoints = Vec::new();
    for chain in watched_chain_plan {
        let startup_checkpoint =
            load_startup_adapter_checkpoint_context(pool, deployment_profile, &chain.chain).await?;
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
        record_startup_sync_progress(pool, &mut startup_heartbeat).await?;

        let summary = bigname_adapters::sync_ens_v2_registry_resource_surface(pool, &chain.chain)
            .await
            .with_context(|| {
                format!(
                    "failed to sync ENSv2 registry discovery from stored raw logs for chain {}",
                    chain.chain
                )
            })?;
        log_ens_v2_registry_resource_surface_sync_summary(&chain.chain, &summary);
        record_startup_sync_progress(pool, &mut startup_heartbeat).await?;
        completed_startup_checkpoints.push((chain.chain.clone(), startup_checkpoint));
    }
    journal_resolver_profile_authority_with_optional_progress(pool, &mut startup_heartbeat).await?;
    clear_completed_startup_adapter_checkpoints(pool, &completed_startup_checkpoints).await?;
    record_startup_sync_progress(pool, &mut startup_heartbeat).await?;
    Ok(())
}

async fn record_startup_sync_progress(
    pool: &sqlx::PgPool,
    startup_heartbeat: &mut Option<(&mut StartupHeartbeat, &[String])>,
) -> Result<()> {
    if let Some((heartbeat, chain_ids)) = startup_heartbeat.as_mut() {
        heartbeat.record_if_due(pool, chain_ids).await?;
    }
    Ok(())
}

async fn journal_resolver_profile_authority_with_optional_progress(
    pool: &sqlx::PgPool,
    startup_heartbeat: &mut Option<(&mut StartupHeartbeat, &[String])>,
) -> Result<()> {
    match startup_heartbeat.as_mut() {
        Some((heartbeat, chain_ids)) => {
            let mut progress = StartupAdapterHeartbeat::new(heartbeat, chain_ids);
            journal_resolver_profile_authority_with_progress(pool, &mut progress).await?;
        }
        None => {
            journal_resolver_profile_authority(pool).await?;
        }
    }
    Ok(())
}

async fn load_startup_adapter_checkpoint_context(
    pool: &sqlx::PgPool,
    deployment_profile: &str,
    chain: &str,
) -> Result<bigname_adapters::StartupAdapterCheckpointContext> {
    let target_block_number = sqlx::query_scalar::<_, Option<i64>>(
        r#"
        SELECT GREATEST(
            (
                SELECT MAX(block_number)::BIGINT
                FROM chain_lineage
                WHERE chain_id = $1
                  AND canonicality_state IN (
                      'canonical'::canonicality_state,
                      'safe'::canonicality_state,
                      'finalized'::canonicality_state
                  )
            ),
            (
                SELECT MAX(block_number)::BIGINT
                FROM raw_logs
                WHERE chain_id = $1
                  AND canonicality_state IN (
                      'canonical'::canonicality_state,
                      'safe'::canonicality_state,
                      'finalized'::canonicality_state
                  )
            )
        )
        "#,
    )
    .bind(chain)
    .fetch_one(pool)
    .await
    .with_context(|| format!("failed to load startup adapter checkpoint target for {chain}"))?
    .unwrap_or(0);
    bigname_adapters::StartupAdapterCheckpointContext::new(deployment_profile, target_block_number)
}

async fn clear_completed_startup_adapter_checkpoints(
    pool: &sqlx::PgPool,
    completed: &[(String, bigname_adapters::StartupAdapterCheckpointContext)],
) -> Result<()> {
    for (chain, checkpoint) in completed {
        bigname_adapters::clear_startup_adapter_checkpoints(pool, chain, checkpoint).await?;
    }
    Ok(())
}
