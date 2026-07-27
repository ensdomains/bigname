use anyhow::{Context, Result, bail};
use bigname_adapters::{
    ENS_V2_PERMISSIONS_STARTUP_VERSION, EnsV1ReverseClaimSyncSummary, EnsV2PermissionsSyncSummary,
    EnsV2RegistrarSyncSummary, EnsV2RegistryResourceSurfaceSyncSummary, EnsV2ResolverSyncSummary,
};
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

use super::await_live_adapter_sync_with_heartbeat;

pub(super) async fn sync_ens_v1_reverse_claim(
    pool: &sqlx::PgPool,
    chain: &str,
    scanned_through_block: Option<i64>,
    startup_heartbeat: &mut Option<(&mut StartupHeartbeat, &[String])>,
) -> Result<EnsV1ReverseClaimSyncSummary> {
    match (scanned_through_block, startup_heartbeat.as_mut()) {
        (Some(through_block), Some((heartbeat, chain_ids))) => {
            await_live_adapter_sync_with_heartbeat(
                pool,
                heartbeat,
                chain_ids,
                bigname_adapters::sync_ens_v1_reverse_claim_range(pool, chain, 0, through_block),
            )
            .await
        }
        (Some(through_block), None) => {
            bigname_adapters::sync_ens_v1_reverse_claim_range(pool, chain, 0, through_block).await
        }
        (None, Some((heartbeat, chain_ids))) => {
            let mut progress = StartupAdapterHeartbeat::new(heartbeat, chain_ids);
            bigname_adapters::sync_ens_v1_reverse_claim_with_progress(pool, chain, &mut progress)
                .await
        }
        (None, None) => bigname_adapters::sync_ens_v1_reverse_claim(pool, chain).await,
    }
}

pub(super) async fn sync_ens_v2_registrar(
    pool: &sqlx::PgPool,
    chain: &str,
    scanned_through_block: Option<i64>,
    startup_heartbeat: &mut Option<(&mut StartupHeartbeat, &[String])>,
) -> Result<EnsV2RegistrarSyncSummary> {
    match (scanned_through_block, startup_heartbeat.as_mut()) {
        (Some(through_block), Some((heartbeat, chain_ids))) => {
            let mut progress = StartupAdapterHeartbeat::new(heartbeat, chain_ids);
            bigname_adapters::sync_ens_v2_registrar_through_block_with_progress(
                pool,
                chain,
                through_block,
                &mut progress,
            )
            .await
        }
        (Some(through_block), None) => {
            bigname_adapters::sync_ens_v2_registrar_through_block(pool, chain, through_block).await
        }
        (None, Some((heartbeat, chain_ids))) => {
            let mut progress = StartupAdapterHeartbeat::new(heartbeat, chain_ids);
            bigname_adapters::sync_ens_v2_registrar_with_progress(pool, chain, &mut progress).await
        }
        (None, None) => bigname_adapters::sync_ens_v2_registrar(pool, chain).await,
    }
}

pub(super) async fn sync_ens_v2_resolver(
    pool: &sqlx::PgPool,
    chain: &str,
    scanned_through_block: Option<i64>,
    startup_heartbeat: &mut Option<(&mut StartupHeartbeat, &[String])>,
) -> Result<EnsV2ResolverSyncSummary> {
    match (scanned_through_block, startup_heartbeat.as_mut()) {
        (Some(through_block), Some((heartbeat, chain_ids))) => {
            let mut progress = StartupAdapterHeartbeat::new(heartbeat, chain_ids);
            bigname_adapters::sync_ens_v2_resolver_through_block_with_progress(
                pool,
                chain,
                through_block,
                &mut progress,
            )
            .await
        }
        (Some(through_block), None) => {
            bigname_adapters::sync_ens_v2_resolver_through_block(pool, chain, through_block).await
        }
        (None, Some((heartbeat, chain_ids))) => {
            let mut progress = StartupAdapterHeartbeat::new(heartbeat, chain_ids);
            bigname_adapters::sync_ens_v2_resolver_with_progress(pool, chain, &mut progress).await
        }
        (None, None) => bigname_adapters::sync_ens_v2_resolver(pool, chain).await,
    }
}

pub(super) async fn sync_ens_v2_permissions(
    pool: &sqlx::PgPool,
    chain: &str,
    scanned_through_block: Option<i64>,
    startup_heartbeat: &mut Option<(&mut StartupHeartbeat, &[String])>,
) -> Result<EnsV2PermissionsSyncSummary> {
    match (scanned_through_block, startup_heartbeat.as_mut()) {
        (Some(through_block), Some((heartbeat, chain_ids))) => {
            let mut progress = StartupAdapterHeartbeat::new(heartbeat, chain_ids);
            bigname_adapters::sync_ens_v2_permissions_through_block_with_progress(
                pool,
                chain,
                through_block,
                &mut progress,
            )
            .await
        }
        (Some(through_block), None) => {
            bigname_adapters::sync_ens_v2_permissions_through_block(pool, chain, through_block)
                .await
        }
        (None, Some((heartbeat, chain_ids))) => {
            let mut progress = StartupAdapterHeartbeat::new(heartbeat, chain_ids);
            bigname_adapters::sync_ens_v2_permissions_with_progress(pool, chain, &mut progress)
                .await
        }
        (None, None) => bigname_adapters::sync_ens_v2_permissions(pool, chain).await,
    }
}

pub(crate) async fn sync_startup_ens_v2_permissions(
    pool: &sqlx::PgPool,
    deployment_profile: &str,
    chain: &str,
    scanned_through_block: i64,
    max_raw_logs_per_page: usize,
    startup_heartbeat: &mut Option<(&mut StartupHeartbeat, &[String])>,
) -> Result<EnsV2PermissionsSyncSummary> {
    replay_startup_permissions_producers(
        pool,
        deployment_profile,
        chain,
        scanned_through_block,
        max_raw_logs_per_page,
        startup_heartbeat,
    )
    .await
    .with_context(|| {
        format!(
            "failed to authoritatively rederive normalized-event producer inputs before ENSv2 \
             permissions startup sync for chain {chain}"
        )
    })?;
    sync_ens_v2_permissions(pool, chain, Some(scanned_through_block), startup_heartbeat).await
}

pub(super) async fn sync_ens_v2_registry_resource_surface(
    pool: &sqlx::PgPool,
    chain: &str,
    scanned_through_block: Option<i64>,
    startup_heartbeat: &mut Option<(&mut StartupHeartbeat, &[String])>,
) -> Result<EnsV2RegistryResourceSurfaceSyncSummary> {
    match (scanned_through_block, startup_heartbeat.as_mut()) {
        (Some(through_block), Some((heartbeat, chain_ids))) => {
            let mut progress = StartupAdapterHeartbeat::new(heartbeat, chain_ids);
            bigname_adapters::sync_ens_v2_registry_resource_surface_through_block_with_progress(
                pool,
                chain,
                through_block,
                &mut progress,
            )
            .await
        }
        (Some(through_block), None) => {
            bigname_adapters::sync_ens_v2_registry_resource_surface_through_block(
                pool,
                chain,
                through_block,
            )
            .await
        }
        (None, Some((heartbeat, chain_ids))) => {
            let mut progress = StartupAdapterHeartbeat::new(heartbeat, chain_ids);
            bigname_adapters::sync_ens_v2_registry_resource_surface_with_progress(
                pool,
                chain,
                &mut progress,
            )
            .await
        }
        (None, None) => bigname_adapters::sync_ens_v2_registry_resource_surface(pool, chain).await,
    }
}

pub(super) async fn replay_startup_permissions_producers(
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
        adapter = ENS_V2_PERMISSIONS_STARTUP_VERSION.adapter,
        adapter_semantic_version = ENS_V2_PERMISSIONS_STARTUP_VERSION.semantic_version,
        target_block_number,
        "ENSv2 permissions startup rerun requires authoritative stateless producer replay first"
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

pub(super) async fn load_startup_adapter_checkpoint_context(
    pool: &sqlx::PgPool,
    deployment_profile: &str,
    chain: &str,
    scanned_through_block: Option<i64>,
) -> Result<bigname_adapters::StartupAdapterCheckpointContext> {
    let target_block_number = match scanned_through_block {
        Some(target_block_number) => target_block_number,
        None => sqlx::query_scalar::<_, Option<i64>>(
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
        .unwrap_or(0),
    };
    bigname_adapters::StartupAdapterCheckpointContext::new(deployment_profile, target_block_number)
}
