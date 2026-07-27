use anyhow::{Context, Result, bail};
use bigname_adapters::{
    ENS_V1_REVERSE_CLAIM_STARTUP_VERSION, ENS_V1_SUBREGISTRY_DISCOVERY_STARTUP_VERSION,
    ENS_V1_UNWRAPPED_AUTHORITY_STARTUP_VERSION, ENS_V2_PERMISSIONS_STARTUP_VERSION,
    ENS_V2_REGISTRAR_STARTUP_VERSION, ENS_V2_REGISTRY_RESOURCE_SURFACE_STARTUP_VERSION,
    ENS_V2_RESOLVER_STARTUP_VERSION, StartupAdapterVersion,
};
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
#[path = "adapter_sync/checkpoint.rs"]
pub(crate) mod checkpoint;
use checkpoint::{
    StartupFamilySyncAttempt, StartupFamilySyncCompletion, prepare_startup_family_sync,
};
pub(crate) use checkpoint::{
    await_live_adapter_sync_with_heartbeat, await_live_full_source_adapter_with_heartbeat,
};
#[path = "adapter_sync/discovery.rs"]
mod discovery;
#[cfg(test)]
pub(crate) use discovery::sync_discovery_adapter_owned_raw_log_state;
pub(crate) use discovery::sync_discovery_adapter_owned_raw_log_state_with_heartbeat;

// Startup heartbeat replay uses small pages so long ownership/control scans
// report liveness between pages. These pages preserve whole blocks, so an
// unusually dense block can exceed this target.
pub(crate) const DEFAULT_STARTUP_DISCOVERY_PAGE_LOGS: usize = 1_000;
const MAX_ENS_V2_STARTUP_DISCOVERY_EXPANSION_PASSES: usize = 1_024;
const MAX_NON_LOOPING_STARTUP_FAMILY_PASSES: usize = 2;

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

pub(crate) async fn sync_adapter_owned_raw_log_state_live_with_heartbeat(
    pool: &sqlx::PgPool,
    watched_chain_plan: &[WatchedChainPlan],
    heartbeat: &mut StartupHeartbeat,
    heartbeat_chain_ids: &[String],
) -> Result<()> {
    heartbeat.record(pool, heartbeat_chain_ids).await?;
    // Intake stays live during timer and discovery refreshes. These passes are
    // intentionally tolerant of concurrent input and never verify or publish a
    // boot-only startup checkpoint.
    sync_adapter_owned_raw_log_state_with_startup_context(
        pool,
        watched_chain_plan,
        None,
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
    journal_authority_with_startup_progress(pool, &mut startup_heartbeat).await?;
    let startup_deployment_profile = startup_context.map(|(profile, _)| profile);
    for chain in watched_chain_plan {
        for pass in 1..=MAX_NON_LOOPING_STARTUP_FAMILY_PASSES {
            let Some(attempt) = prepare_startup_family_sync(
                pool,
                startup_deployment_profile,
                &chain.chain,
                ENS_V1_REVERSE_CLAIM_STARTUP_VERSION,
            )
            .await?
            else {
                break;
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
            if complete_non_looping_startup_family(
                attempt,
                pool,
                &chain.chain,
                ENS_V1_REVERSE_CLAIM_STARTUP_VERSION,
                pass,
            )
            .await?
            {
                break;
            }
        }
        record_startup_sync_progress(pool, &mut startup_heartbeat).await?;

        for pass in 1..=MAX_NON_LOOPING_STARTUP_FAMILY_PASSES {
            let Some(attempt) = prepare_startup_family_sync(
                pool,
                startup_deployment_profile,
                &chain.chain,
                ENS_V1_SUBREGISTRY_DISCOVERY_STARTUP_VERSION,
            )
            .await?
            else {
                break;
            };
            let startup_checkpoint = match startup_context {
                Some((deployment_profile, page_logs)) => Some((
                    load_startup_adapter_checkpoint_context(pool, deployment_profile, &chain.chain)
                        .await?,
                    page_logs,
                )),
                None => None,
            };
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
                None => match startup_heartbeat.as_mut() {
                    Some((heartbeat, chain_ids)) => {
                        await_live_full_source_adapter_with_heartbeat(
                            pool,
                            &chain.chain,
                            heartbeat,
                            chain_ids,
                            bigname_adapters::sync_ens_v1_subregistry_discovery(
                                pool,
                                &chain.chain,
                            ),
                        )
                        .await
                    }
                    None => {
                        bigname_adapters::sync_ens_v1_subregistry_discovery(pool, &chain.chain).await
                    }
                },
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

        for pass in 1..=MAX_NON_LOOPING_STARTUP_FAMILY_PASSES {
            let Some(attempt) = prepare_startup_family_sync(
                pool,
                startup_deployment_profile,
                &chain.chain,
                ENS_V1_UNWRAPPED_AUTHORITY_STARTUP_VERSION,
            )
            .await?
            else {
                break;
            };
            let startup_checkpoint = match startup_context {
                Some((deployment_profile, page_logs)) => Some((
                    load_startup_adapter_checkpoint_context(pool, deployment_profile, &chain.chain)
                        .await?,
                    page_logs,
                )),
                None => None,
            };
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
                None => match startup_heartbeat.as_mut() {
                    Some((heartbeat, chain_ids)) => {
                        await_live_adapter_sync_with_heartbeat(
                            pool,
                            heartbeat,
                            chain_ids,
                            bigname_adapters::sync_ens_v1_unwrapped_authority(pool, &chain.chain),
                        )
                        .await
                    }
                    None => {
                        bigname_adapters::sync_ens_v1_unwrapped_authority(pool, &chain.chain).await
                    }
                },
            }
            .with_context(|| {
                format!(
                    "failed to sync ENSv1 unwrapped authority from stored raw logs for chain {}",
                    chain.chain
                )
            })?;
            log_ens_v1_unwrapped_authority_sync_summary(&chain.chain, &summary);
            if complete_non_looping_startup_family(
                attempt,
                pool,
                &chain.chain,
                ENS_V1_UNWRAPPED_AUTHORITY_STARTUP_VERSION,
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
            startup_deployment_profile,
            &chain.chain,
            &mut startup_heartbeat,
        )
        .await?;
        record_startup_sync_progress(pool, &mut startup_heartbeat).await?;

        for pass in 1..=MAX_NON_LOOPING_STARTUP_FAMILY_PASSES {
            let Some(attempt) = prepare_startup_family_sync(
                pool,
                startup_deployment_profile,
                &chain.chain,
                ENS_V2_REGISTRAR_STARTUP_VERSION,
            )
            .await?
            else {
                break;
            };
            let summary = match startup_heartbeat.as_mut() {
                Some((heartbeat, chain_ids)) => {
                    let mut progress = StartupAdapterHeartbeat::new(heartbeat, chain_ids);
                    bigname_adapters::sync_ens_v2_registrar_with_progress(
                        pool,
                        &chain.chain,
                        &mut progress,
                    )
                    .await
                }
                None => bigname_adapters::sync_ens_v2_registrar(pool, &chain.chain).await,
            }
            .with_context(|| {
                format!(
                    "failed to sync ENSv2 registrar state from stored raw logs for chain {}",
                    chain.chain
                )
            })?;
            log_ens_v2_registrar_sync_summary(&chain.chain, &summary);
            if complete_non_looping_startup_family(
                attempt,
                pool,
                &chain.chain,
                ENS_V2_REGISTRAR_STARTUP_VERSION,
                pass,
            )
            .await?
            {
                break;
            }
        }
        record_startup_sync_progress(pool, &mut startup_heartbeat).await?;

        for pass in 1..=MAX_NON_LOOPING_STARTUP_FAMILY_PASSES {
            let Some(attempt) = prepare_startup_family_sync(
                pool,
                startup_deployment_profile,
                &chain.chain,
                ENS_V2_RESOLVER_STARTUP_VERSION,
            )
            .await?
            else {
                break;
            };
            let summary = match startup_heartbeat.as_mut() {
                Some((heartbeat, chain_ids)) => {
                    let mut progress = StartupAdapterHeartbeat::new(heartbeat, chain_ids);
                    bigname_adapters::sync_ens_v2_resolver_with_progress(
                        pool,
                        &chain.chain,
                        &mut progress,
                    )
                    .await
                }
                None => bigname_adapters::sync_ens_v2_resolver(pool, &chain.chain).await,
            }
            .with_context(|| {
                format!(
                    "failed to sync ENSv2 resolver state from stored raw logs for chain {}",
                    chain.chain
                )
            })?;
            log_ens_v2_resolver_sync_summary(&chain.chain, &summary);
            if complete_non_looping_startup_family(
                attempt,
                pool,
                &chain.chain,
                ENS_V2_RESOLVER_STARTUP_VERSION,
                pass,
            )
            .await?
            {
                break;
            }
        }
        record_startup_sync_progress(pool, &mut startup_heartbeat).await?;

        for pass in 1..=MAX_NON_LOOPING_STARTUP_FAMILY_PASSES {
            let Some(attempt) = prepare_startup_family_sync(
                pool,
                startup_deployment_profile,
                &chain.chain,
                ENS_V2_PERMISSIONS_STARTUP_VERSION,
            )
            .await?
            else {
                break;
            };
            let summary = match startup_heartbeat.as_mut() {
                Some((heartbeat, chain_ids)) => {
                    let mut progress = StartupAdapterHeartbeat::new(heartbeat, chain_ids);
                    bigname_adapters::sync_ens_v2_permissions_with_progress(
                        pool,
                        &chain.chain,
                        &mut progress,
                    )
                    .await
                }
                None => bigname_adapters::sync_ens_v2_permissions(pool, &chain.chain).await,
            }
            .with_context(|| {
                format!(
                    "failed to sync ENSv2 permissions state from stored raw logs for chain {}",
                    chain.chain
                )
            })?;
            log_ens_v2_permissions_sync_summary(&chain.chain, &summary);
            if complete_non_looping_startup_family(
                attempt,
                pool,
                &chain.chain,
                ENS_V2_PERMISSIONS_STARTUP_VERSION,
                pass,
            )
            .await?
            {
                break;
            }
        }
        record_startup_sync_progress(pool, &mut startup_heartbeat).await?;
    }

    journal_authority_with_startup_progress(pool, &mut startup_heartbeat).await?;
    record_startup_sync_progress(pool, &mut startup_heartbeat).await?;
    Ok(())
}

pub(crate) async fn complete_non_looping_startup_family(
    attempt: StartupFamilySyncAttempt,
    pool: &sqlx::PgPool,
    chain: &str,
    adapter: StartupAdapterVersion,
    pass: usize,
) -> Result<bool> {
    if attempt.complete_or_retry(pool, chain, adapter).await? == StartupFamilySyncCompletion::Stable
    {
        return Ok(true);
    }
    if pass == MAX_NON_LOOPING_STARTUP_FAMILY_PASSES {
        bail!(
            "startup adapter input changed while syncing {adapter_name} for {chain} on both \
             bounded passes; refusing to publish completion",
            adapter_name = adapter.adapter,
        );
    }
    Ok(false)
}

pub(super) async fn sync_ens_v2_registry_to_startup_fixed_point(
    pool: &sqlx::PgPool,
    deployment_profile: Option<&str>,
    chain: &str,
    startup_heartbeat: &mut Option<(&mut StartupHeartbeat, &[String])>,
) -> Result<()> {
    for pass in 1..=MAX_ENS_V2_STARTUP_DISCOVERY_EXPANSION_PASSES {
        let Some(attempt) = prepare_startup_family_sync(
            pool,
            deployment_profile,
            chain,
            ENS_V2_REGISTRY_RESOURCE_SURFACE_STARTUP_VERSION,
        )
        .await?
        else {
            return Ok(());
        };
        let summary = match startup_heartbeat.as_mut() {
            Some((heartbeat, chain_ids)) => {
                let mut progress = StartupAdapterHeartbeat::new(heartbeat, chain_ids);
                bigname_adapters::sync_ens_v2_registry_resource_surface_with_progress(
                    pool,
                    chain,
                    &mut progress,
                )
                .await
            }
            None => bigname_adapters::sync_ens_v2_registry_resource_surface(pool, chain).await,
        }
        .with_context(|| {
            format!(
                "failed to sync ENSv2 registry resource/surface state and discovery from stored \
                 raw logs for chain {chain}"
            )
        })?;
        log_ens_v2_registry_resource_surface_sync_summary(chain, &summary);
        if attempt
            .complete_or_retry(
                pool,
                chain,
                ENS_V2_REGISTRY_RESOURCE_SURFACE_STARTUP_VERSION,
            )
            .await?
            == StartupFamilySyncCompletion::Stable
        {
            return Ok(());
        }
        if pass == MAX_ENS_V2_STARTUP_DISCOVERY_EXPANSION_PASSES {
            bail!(
                "ENSv2 registry discovery on chain {chain} did not reach a stable startup \
                 checkpoint within {MAX_ENS_V2_STARTUP_DISCOVERY_EXPANSION_PASSES} passes"
            );
        }
    }
    unreachable!("the bounded ENSv2 startup discovery loop returns on its final pass")
}

pub(super) async fn record_startup_sync_progress(
    pool: &sqlx::PgPool,
    startup_heartbeat: &mut Option<(&mut StartupHeartbeat, &[String])>,
) -> Result<()> {
    if let Some((heartbeat, chain_ids)) = startup_heartbeat.as_mut() {
        heartbeat.record_if_due(pool, chain_ids).await?;
    }
    Ok(())
}

pub(super) async fn journal_authority_with_startup_progress(
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

pub(super) async fn load_startup_adapter_checkpoint_context(
    pool: &sqlx::PgPool,
    deployment_profile: &str,
    chain: &str,
) -> Result<bigname_adapters::StartupAdapterCheckpointContext> {
    let target_block_number = sqlx::query_scalar::<_, Option<i64>>(
        r#"
        SELECT MAX(block_number)::BIGINT
        FROM (
            SELECT block_number
            FROM chain_lineage
            WHERE chain_id = $1
              AND canonicality_state IN (
                  'canonical'::canonicality_state,
                  'safe'::canonicality_state,
                  'finalized'::canonicality_state
              )
            UNION ALL
            SELECT block_number
            FROM raw_logs
            WHERE chain_id = $1
              AND canonicality_state IN (
                  'canonical'::canonicality_state,
                  'safe'::canonicality_state,
                  'finalized'::canonicality_state
              )
        ) AS startup_adapter_inputs
        "#,
    )
    .bind(chain)
    .fetch_one(pool)
    .await
    .with_context(|| format!("failed to load startup adapter checkpoint target for {chain}"))?
    .unwrap_or(0);
    bigname_adapters::StartupAdapterCheckpointContext::new(deployment_profile, target_block_number)
}
