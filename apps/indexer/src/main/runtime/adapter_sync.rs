use std::{future::Future, time::Duration};

use anyhow::{Context, Result, bail};
use bigname_manifests::WatchedChainPlan;
use bigname_storage::acquire_raw_log_staging_read_guard;
use tracing::info;

use crate::{
    reconciliation::{
        RawFactNormalizedEventReplayRequest, RawFactNormalizedEventReplaySelection,
        replay_startup_stateless_only_raw_fact_normalized_events,
        replay_startup_stateless_only_raw_fact_normalized_events_with_progress,
        select_log_bounded_replay_to_block,
    },
    run::startup_heartbeat::{StartupAdapterHeartbeat, StartupHeartbeat},
};

use super::logging::{
    log_ens_v1_reverse_claim_sync_summary, log_ens_v1_unwrapped_authority_sync_summary,
    log_ens_v2_permissions_sync_summary, log_ens_v2_registrar_sync_summary,
    log_ens_v2_registry_resource_surface_sync_summary, log_ens_v2_resolver_sync_summary,
};

#[path = "adapter_sync/discovery.rs"]
mod discovery;
#[cfg(test)]
pub(crate) use discovery::sync_discovery_adapter_owned_raw_log_state;
pub(crate) use discovery::sync_discovery_adapter_owned_raw_log_state_with_heartbeat;

// This existing startup page limit now applies only to the permissions producer replay. Adapter
// interpretation itself uses the plain full-source entry points.
pub(crate) const DEFAULT_STARTUP_DISCOVERY_PAGE_LOGS: usize = 1_000;

#[cfg(not(test))]
const LIVE_ADAPTER_HEARTBEAT_TICK: Duration = Duration::from_secs(1);
#[cfg(test)]
const LIVE_ADAPTER_HEARTBEAT_TICK: Duration = Duration::from_millis(10);

pub(crate) async fn sync_adapter_owned_raw_log_state(
    pool: &sqlx::PgPool,
    watched_chain_plan: &[WatchedChainPlan],
) -> Result<()> {
    sync_adapter_owned_raw_log_state_inner(pool, watched_chain_plan, None, None).await
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
    sync_adapter_owned_raw_log_state_inner(
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
    sync_adapter_owned_raw_log_state_inner(
        pool,
        watched_chain_plan,
        None,
        Some((heartbeat, heartbeat_chain_ids)),
    )
    .await
}

async fn sync_adapter_owned_raw_log_state_inner(
    pool: &sqlx::PgPool,
    watched_chain_plan: &[WatchedChainPlan],
    startup_context: Option<(&str, usize)>,
    mut startup_heartbeat: Option<(&mut StartupHeartbeat, &[String])>,
) -> Result<()> {
    record_startup_sync_progress(pool, &mut startup_heartbeat).await?;
    let heartbeat_while_waiting = startup_context.is_none();
    for chain in watched_chain_plan {
        let summary = await_adapter_with_optional_heartbeat(
            pool,
            &mut startup_heartbeat,
            heartbeat_while_waiting,
            bigname_adapters::sync_ens_v1_reverse_claim(pool, &chain.chain),
        )
        .await
        .with_context(|| {
            format!(
                "failed to sync ENSv1 reverse claim from stored raw logs for chain {}",
                chain.chain
            )
        })?;
        log_ens_v1_reverse_claim_sync_summary(&chain.chain, &summary);
        record_startup_sync_progress(pool, &mut startup_heartbeat).await?;

        let summary = await_full_source_adapter_with_optional_heartbeat(
            pool,
            &chain.chain,
            &mut startup_heartbeat,
            heartbeat_while_waiting,
            bigname_adapters::sync_ens_v1_unwrapped_authority(pool, &chain.chain),
        )
        .await
        .with_context(|| {
            format!(
                "failed to sync ENSv1 unwrapped authority from stored raw logs for chain {}",
                chain.chain
            )
        })?;
        log_ens_v1_unwrapped_authority_sync_summary(&chain.chain, &summary);
        record_startup_sync_progress(pool, &mut startup_heartbeat).await?;

        let summary = await_full_source_adapter_with_optional_heartbeat(
            pool,
            &chain.chain,
            &mut startup_heartbeat,
            heartbeat_while_waiting,
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

        let summary = await_adapter_with_optional_heartbeat(
            pool,
            &mut startup_heartbeat,
            heartbeat_while_waiting,
            bigname_adapters::sync_ens_v2_registrar(pool, &chain.chain),
        )
        .await
        .with_context(|| {
            format!(
                "failed to sync ENSv2 registrar state from stored raw logs for chain {}",
                chain.chain
            )
        })?;
        log_ens_v2_registrar_sync_summary(&chain.chain, &summary);
        record_startup_sync_progress(pool, &mut startup_heartbeat).await?;

        let summary = await_adapter_with_optional_heartbeat(
            pool,
            &mut startup_heartbeat,
            heartbeat_while_waiting,
            bigname_adapters::sync_ens_v2_resolver(pool, &chain.chain),
        )
        .await
        .with_context(|| {
            format!(
                "failed to sync ENSv2 resolver state from stored raw logs for chain {}",
                chain.chain
            )
        })?;
        log_ens_v2_resolver_sync_summary(&chain.chain, &summary);
        record_startup_sync_progress(pool, &mut startup_heartbeat).await?;

        let summary = if let Some((deployment_profile, max_raw_logs_per_page)) = startup_context {
            let target_block_number = load_current_adapter_input_extent(pool, &chain.chain).await?;
            sync_startup_ens_v2_permissions(
                pool,
                deployment_profile,
                &chain.chain,
                target_block_number,
                max_raw_logs_per_page,
                &mut startup_heartbeat,
            )
            .await
        } else {
            await_adapter_with_optional_heartbeat(
                pool,
                &mut startup_heartbeat,
                heartbeat_while_waiting,
                bigname_adapters::sync_ens_v2_permissions(pool, &chain.chain),
            )
            .await
        }
        .with_context(|| {
            format!(
                "failed to sync ENSv2 permissions state from stored raw logs for chain {}",
                chain.chain
            )
        })?;
        log_ens_v2_permissions_sync_summary(&chain.chain, &summary);
        record_startup_sync_progress(pool, &mut startup_heartbeat).await?;
    }
    Ok(())
}

pub(crate) async fn sync_startup_ens_v2_permissions(
    pool: &sqlx::PgPool,
    deployment_profile: &str,
    chain: &str,
    target_block_number: i64,
    max_raw_logs_per_page: usize,
    startup_heartbeat: &mut Option<(&mut StartupHeartbeat, &[String])>,
) -> Result<bigname_adapters::EnsV2PermissionsSyncSummary> {
    replay_startup_permissions_producers(
        pool,
        deployment_profile,
        chain,
        target_block_number,
        max_raw_logs_per_page,
        startup_heartbeat,
    )
    .await
    .with_context(|| {
        format!(
            "failed to rederive normalized-event producer inputs before ENSv2 permissions startup sync for chain {chain}"
        )
    })?;
    await_adapter_with_optional_heartbeat(
        pool,
        startup_heartbeat,
        false,
        bigname_adapters::sync_ens_v2_permissions(pool, chain),
    )
    .await
}

pub(crate) async fn await_live_adapter_sync_with_heartbeat<T, F>(
    pool: &sqlx::PgPool,
    heartbeat: &mut StartupHeartbeat,
    heartbeat_chain_ids: &[String],
    sync: F,
) -> Result<T>
where
    F: Future<Output = Result<T>>,
{
    tokio::pin!(sync);
    let first_tick = tokio::time::Instant::now() + LIVE_ADAPTER_HEARTBEAT_TICK;
    let mut interval = tokio::time::interval_at(first_tick, LIVE_ADAPTER_HEARTBEAT_TICK);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            result = &mut sync => return result,
            _ = interval.tick() => heartbeat.record_if_due(pool, heartbeat_chain_ids).await?,
        }
    }
}

#[cfg(test)]
pub(crate) async fn await_live_full_source_adapter_with_heartbeat<T, F>(
    pool: &sqlx::PgPool,
    chain: &str,
    heartbeat: &mut StartupHeartbeat,
    heartbeat_chain_ids: &[String],
    sync: F,
) -> Result<T>
where
    F: Future<Output = Result<T>>,
{
    let guard = acquire_raw_log_staging_read_guard(pool, chain).await?;
    let sync_result =
        await_live_adapter_sync_with_heartbeat(pool, heartbeat, heartbeat_chain_ids, sync).await;
    let release_result = guard.release().await;
    match (sync_result, release_result) {
        (Err(error), _) | (Ok(_), Err(error)) => Err(error),
        (Ok(value), Ok(())) => Ok(value),
    }
}

pub(crate) async fn await_adapter_with_optional_heartbeat<T, F>(
    pool: &sqlx::PgPool,
    startup_heartbeat: &mut Option<(&mut StartupHeartbeat, &[String])>,
    heartbeat_while_waiting: bool,
    sync: F,
) -> Result<T>
where
    F: Future<Output = Result<T>>,
{
    match (heartbeat_while_waiting, startup_heartbeat.as_mut()) {
        (true, Some((heartbeat, chain_ids))) => {
            await_live_adapter_sync_with_heartbeat(pool, heartbeat, chain_ids, sync).await
        }
        _ => sync.await,
    }
}

pub(crate) async fn await_full_source_adapter_with_optional_heartbeat<T, F>(
    pool: &sqlx::PgPool,
    chain: &str,
    startup_heartbeat: &mut Option<(&mut StartupHeartbeat, &[String])>,
    heartbeat_while_waiting: bool,
    sync: F,
) -> Result<T>
where
    F: Future<Output = Result<T>>,
{
    let guard = acquire_raw_log_staging_read_guard(pool, chain).await?;
    let sync_result = await_adapter_with_optional_heartbeat(
        pool,
        startup_heartbeat,
        heartbeat_while_waiting,
        sync,
    )
    .await;
    let release_result = guard.release().await;
    match (sync_result, release_result) {
        (Err(error), _) | (Ok(_), Err(error)) => Err(error),
        (Ok(value), Ok(())) => Ok(value),
    }
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

async fn replay_startup_permissions_producers(
    pool: &sqlx::PgPool,
    deployment_profile: &str,
    chain: &str,
    target_block_number: i64,
    max_raw_logs_per_page: usize,
    startup_heartbeat: &mut Option<(&mut StartupHeartbeat, &[String])>,
) -> Result<()> {
    if max_raw_logs_per_page == 0 {
        bail!("startup permissions producer replay max logs per page must be positive");
    }
    info!(
        service = "indexer",
        command = "startup-adapter-sync",
        deployment_profile,
        chain,
        target_block_number,
        "ENSv2 permissions startup rerun requires stateless producer replay first"
    );

    let mut from_block = 0;
    loop {
        let to_block = select_log_bounded_replay_to_block(
            pool,
            chain,
            from_block,
            target_block_number,
            max_raw_logs_per_page,
        )
        .await?;
        let request = RawFactNormalizedEventReplayRequest {
            deployment_profile: deployment_profile.to_owned(),
            chain: chain.to_owned(),
            selection: RawFactNormalizedEventReplaySelection::BlockRange {
                from_block,
                to_block,
            },
        };
        match startup_heartbeat.as_mut() {
            Some((heartbeat, chain_ids)) => {
                let mut progress = StartupAdapterHeartbeat::new(heartbeat, chain_ids);
                replay_startup_stateless_only_raw_fact_normalized_events_with_progress(
                    pool,
                    request,
                    &mut progress,
                )
                .await?;
            }
            None => {
                replay_startup_stateless_only_raw_fact_normalized_events(pool, request).await?;
            }
        }
        if to_block == target_block_number {
            return Ok(());
        }
        from_block = to_block
            .checked_add(1)
            .context("startup permissions producer replay page boundary overflowed")?;
    }
}

async fn load_current_adapter_input_extent(pool: &sqlx::PgPool, chain: &str) -> Result<i64> {
    sqlx::query_scalar::<_, Option<i64>>(
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
    .with_context(|| format!("failed to load startup adapter input extent for {chain}"))
    .map(|extent| extent.unwrap_or(0))
}
