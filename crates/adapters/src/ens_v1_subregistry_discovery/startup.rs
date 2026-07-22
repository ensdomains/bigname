use anyhow::Result;
use sqlx::PgPool;

use super::{
    EnsV1SubregistryDiscoverySyncSummary, StartupAdapterCheckpointContext, StartupAdapterProgress,
    checkpoint::SubregistryReplayCheckpoint, loader::RegistryRawLogPosition,
    replay::sync_ens_v1_subregistry_discovery_with_checkpoint_context,
};

const STARTUP_PROGRESS_LOG_EVERY_PAGES: usize = 25;

pub async fn sync_ens_v1_subregistry_discovery_with_startup_checkpoint_and_log_limit(
    pool: &PgPool,
    chain: &str,
    checkpoint: &StartupAdapterCheckpointContext,
    max_raw_logs_per_page: usize,
) -> Result<EnsV1SubregistryDiscoverySyncSummary> {
    let checkpoint = checkpoint.adapter_context(pool, chain).await?;
    sync_ens_v1_subregistry_discovery_with_checkpoint_context(
        pool,
        chain,
        &checkpoint,
        max_raw_logs_per_page,
        Some(STARTUP_PROGRESS_LOG_EVERY_PAGES),
        None,
    )
    .await
}

pub async fn sync_ens_v1_subregistry_discovery_with_startup_checkpoint_and_log_limit_and_progress(
    pool: &PgPool,
    chain: &str,
    checkpoint: &StartupAdapterCheckpointContext,
    max_raw_logs_per_page: usize,
    progress: &mut dyn StartupAdapterProgress,
) -> Result<EnsV1SubregistryDiscoverySyncSummary> {
    let checkpoint = checkpoint.adapter_context(pool, chain).await?;
    sync_ens_v1_subregistry_discovery_with_checkpoint_context(
        pool,
        chain,
        &checkpoint,
        max_raw_logs_per_page,
        Some(STARTUP_PROGRESS_LOG_EVERY_PAGES),
        Some(progress),
    )
    .await
}

pub(super) fn log_checkpoint_stream_progress(
    cadence: Option<usize>,
    checkpoint: &SubregistryReplayCheckpoint,
    page_count: usize,
    page_limit: i64,
    last_position: &RegistryRawLogPosition,
    scanned_log_count: usize,
    matched_log_count: usize,
) {
    if !cadence.is_some_and(|cadence| {
        page_count == 1 || (cadence > 0 && page_count.is_multiple_of(cadence))
    }) {
        return;
    }
    tracing::info!(
        service = "adapters",
        adapter = "ens_v1_subregistry_discovery",
        chain = checkpoint.chain.as_str(),
        checkpoint_page_count = page_count,
        checkpoint_page_limit = page_limit,
        checkpoint_block_number = last_position.block_number,
        checkpoint_target_block_number = checkpoint.target_block_number(),
        scanned_log_count,
        matched_log_count,
        "ENSv1 subregistry startup checkpoint stream progress"
    );
}
