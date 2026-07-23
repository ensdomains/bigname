use anyhow::{Context, Result, ensure};
use bigname_storage::acquire_raw_log_staging_read_guard;
use sqlx::PgPool;

use crate::checkpoint_context::{AdapterCheckpointContext, StartupAdapterProgress};

use super::{
    DiscoveryEdgeMutation, EnsV1SubregistryDiscoverySyncSummary, ReplayAdapterCheckpointContext,
    checkpoint::PAGE_LIMIT, sync_ens_v1_subregistry_discovery_with_scope,
};

/// The checkpointed replay holds the raw-log staging guard and the streamed
/// reconcile transaction while paging staged assignments over a third pooled
/// connection; a smaller pool deadlocks on connection acquisition instead of
/// failing fast. The minimum assumes one checkpointed replay per process
/// sharing the pool (the deployment model — the replay runner is a single
/// supervised task); concurrent replays on one pool would each need their
/// own three connections.
const MIN_CHECKPOINT_POOL_CONNECTIONS: u32 = 3;

pub async fn sync_ens_v1_subregistry_discovery_with_replay_checkpoint(
    pool: &PgPool,
    chain: &str,
    checkpoint: &ReplayAdapterCheckpointContext,
) -> Result<EnsV1SubregistryDiscoverySyncSummary> {
    sync_ens_v1_subregistry_discovery_with_replay_checkpoint_and_log_limit(
        pool,
        chain,
        checkpoint,
        usize::try_from(PAGE_LIMIT)?,
    )
    .await
}

pub async fn sync_ens_v1_subregistry_discovery_with_replay_checkpoint_and_log_limit(
    pool: &PgPool,
    chain: &str,
    checkpoint: &ReplayAdapterCheckpointContext,
    max_raw_logs_per_page: usize,
) -> Result<EnsV1SubregistryDiscoverySyncSummary> {
    let checkpoint = AdapterCheckpointContext::for_replay(checkpoint);
    sync_ens_v1_subregistry_discovery_with_checkpoint_context(
        pool,
        chain,
        &checkpoint,
        max_raw_logs_per_page,
        None,
        None,
    )
    .await
}

pub async fn sync_ens_v1_subregistry_discovery_with_replay_checkpoint_and_log_limit_and_progress(
    pool: &PgPool,
    chain: &str,
    checkpoint: &ReplayAdapterCheckpointContext,
    max_raw_logs_per_page: usize,
    progress: &mut dyn StartupAdapterProgress,
) -> Result<EnsV1SubregistryDiscoverySyncSummary> {
    let checkpoint = AdapterCheckpointContext::for_replay(checkpoint);
    sync_ens_v1_subregistry_discovery_with_checkpoint_context(
        pool,
        chain,
        &checkpoint,
        max_raw_logs_per_page,
        None,
        Some(progress),
    )
    .await
}

pub(super) async fn sync_ens_v1_subregistry_discovery_with_checkpoint_context(
    pool: &PgPool,
    chain: &str,
    checkpoint: &AdapterCheckpointContext,
    max_raw_logs_per_page: usize,
    progress_log_every_pages: Option<usize>,
    mut startup_progress: Option<&mut dyn StartupAdapterProgress>,
) -> Result<EnsV1SubregistryDiscoverySyncSummary> {
    ensure!(
        max_raw_logs_per_page > 0,
        "ENSv1 subregistry checkpoint max logs per page must be positive"
    );
    let checkpoint_page_limit = i64::try_from(max_raw_logs_per_page)
        .context("subregistry checkpoint page limit overflowed i64")?;
    ensure!(
        pool.options().get_max_connections() >= MIN_CHECKPOINT_POOL_CONNECTIONS,
        "checkpointed ENSv1 subregistry sync needs at least {MIN_CHECKPOINT_POOL_CONNECTIONS} \
         pooled connections (raw-log staging guard, streamed reconcile transaction, and staged \
         assignment page reads), but the pool allows only {}",
        pool.options().get_max_connections()
    );
    let raw_log_guard = acquire_raw_log_staging_read_guard(pool, chain).await?;
    let mut checkpoint = checkpoint.clone();
    let summary = loop {
        let (summary, repeat_checkpoint_replay, _) = sync_ens_v1_subregistry_discovery_with_scope(
            pool,
            chain,
            false,
            &[],
            None,
            DiscoveryEdgeMutation::Reconcile,
            None,
            None,
            Some(&checkpoint),
            checkpoint_page_limit,
            progress_log_every_pages,
            &mut startup_progress,
        )
        .await?;
        if !repeat_checkpoint_replay {
            break summary;
        }
        checkpoint.refresh_startup_authority(pool, chain).await?;
    };
    raw_log_guard.release().await?;
    Ok(summary)
}
