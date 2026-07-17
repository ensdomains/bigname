use anyhow::{Context, Result, ensure};
use bigname_storage::acquire_raw_log_staging_read_guard;
use sqlx::PgPool;

use super::{
    DiscoveryEdgeMutation, EnsV1SubregistryDiscoverySyncSummary, ReplayAdapterCheckpointContext,
    checkpoint::PAGE_LIMIT, sync_ens_v1_subregistry_discovery_with_scope,
};

/// The checkpointed replay holds the raw-log staging guard and the streamed
/// reconcile transaction while paging staged assignments over a third pooled
/// connection; a smaller pool deadlocks on connection acquisition instead of
/// failing fast.
const MIN_REPLAY_POOL_CONNECTIONS: u32 = 3;

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
    let checkpoint_page_limit = i64::try_from(max_raw_logs_per_page.max(1))
        .context("subregistry checkpoint page limit overflowed i64")?;
    ensure!(
        pool.options().get_max_connections() >= MIN_REPLAY_POOL_CONNECTIONS,
        "checkpointed ENSv1 subregistry replay needs at least {MIN_REPLAY_POOL_CONNECTIONS} \
         pooled connections (raw-log staging guard, streamed reconcile transaction, and staged \
         assignment page reads), but the pool allows only {}",
        pool.options().get_max_connections()
    );
    let raw_log_guard = acquire_raw_log_staging_read_guard(pool, chain).await?;
    let summary = loop {
        let (summary, repeat_checkpoint_replay) = sync_ens_v1_subregistry_discovery_with_scope(
            pool,
            chain,
            false,
            &[],
            None,
            DiscoveryEdgeMutation::Reconcile,
            None,
            None,
            Some(checkpoint),
            checkpoint_page_limit,
        )
        .await?;
        if !repeat_checkpoint_replay {
            break summary;
        }
    };
    raw_log_guard.release().await?;
    Ok(summary)
}
