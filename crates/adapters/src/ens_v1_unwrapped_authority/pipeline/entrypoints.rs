use super::*;
use crate::checkpoint_context::{
    AdapterCheckpointContext, ReplayAdapterCheckpointContext, StartupAdapterCheckpointContext,
    StartupAdapterProgress,
};

pub async fn sync_ens_v1_unwrapped_authority(
    pool: &PgPool,
    chain: &str,
) -> Result<EnsV1UnwrappedAuthoritySyncSummary> {
    sync_ens_v1_unwrapped_authority_with_scope(
        pool,
        chain,
        false,
        &[],
        None,
        None,
        None,
        None,
        None,
        None,
    )
    .await
}

pub async fn sync_ens_v1_unwrapped_authority_with_replay_checkpoint_and_log_limit(
    pool: &PgPool,
    chain: &str,
    checkpoint: &ReplayAdapterCheckpointContext,
    max_raw_logs_per_page: usize,
) -> Result<EnsV1UnwrappedAuthoritySyncSummary> {
    sync_ens_v1_unwrapped_authority_with_checkpoint_context(
        pool,
        chain,
        &AdapterCheckpointContext::for_replay(checkpoint),
        max_raw_logs_per_page,
        None,
    )
    .await
}

pub async fn sync_ens_v1_unwrapped_authority_with_startup_checkpoint_and_log_limit(
    pool: &PgPool,
    chain: &str,
    checkpoint: &StartupAdapterCheckpointContext,
    max_raw_logs_per_page: usize,
) -> Result<EnsV1UnwrappedAuthoritySyncSummary> {
    let checkpoint = checkpoint.adapter_context(pool, chain).await?;
    sync_ens_v1_unwrapped_authority_with_checkpoint_context(
        pool,
        chain,
        &checkpoint,
        max_raw_logs_per_page,
        None,
    )
    .await
}

pub async fn sync_ens_v1_unwrapped_authority_with_startup_checkpoint_and_log_limit_and_progress(
    pool: &PgPool,
    chain: &str,
    checkpoint: &StartupAdapterCheckpointContext,
    max_raw_logs_per_page: usize,
    progress: &mut dyn StartupAdapterProgress,
) -> Result<EnsV1UnwrappedAuthoritySyncSummary> {
    let checkpoint = checkpoint.adapter_context(pool, chain).await?;
    sync_ens_v1_unwrapped_authority_with_checkpoint_context(
        pool,
        chain,
        &checkpoint,
        max_raw_logs_per_page,
        Some(progress),
    )
    .await
}

async fn sync_ens_v1_unwrapped_authority_with_checkpoint_context(
    pool: &PgPool,
    chain: &str,
    checkpoint: &AdapterCheckpointContext,
    max_raw_logs_per_page: usize,
    startup_progress: Option<&mut dyn StartupAdapterProgress>,
) -> Result<EnsV1UnwrappedAuthoritySyncSummary> {
    let raw_log_guard = acquire_raw_log_staging_read_guard(pool, chain).await?;
    let summary = sync_ens_v1_unwrapped_authority_with_scope(
        pool,
        chain,
        false,
        &[],
        None,
        None,
        Some(checkpoint),
        Some(max_raw_logs_per_page),
        None,
        startup_progress,
    )
    .await?;
    raw_log_guard.release().await?;
    Ok(summary)
}

impl EnsV1UnwrappedAuthoritySyncSummary {
    pub async fn sync_for_block_hashes(
        pool: &PgPool,
        chain: &str,
        block_hashes: &[String],
    ) -> Result<Self> {
        sync_ens_v1_unwrapped_authority_with_scope(
            pool,
            chain,
            true,
            block_hashes,
            None,
            None,
            None,
            None,
            None,
            None,
        )
        .await
    }

    pub async fn sync_for_block_hashes_with_source_scope(
        pool: &PgPool,
        chain: &str,
        block_hashes: &[String],
        source_scope: &[(String, String, i64, i64)],
    ) -> Result<Self> {
        sync_ens_v1_unwrapped_authority_with_scope(
            pool,
            chain,
            true,
            block_hashes,
            None,
            Some(source_scope),
            None,
            None,
            None,
            None,
        )
        .await
    }

    pub async fn sync_for_block_hashes_with_source_scope_and_transactions(
        pool: &PgPool,
        chain: &str,
        block_hashes: &[String],
        source_scope: &[(String, String, i64, i64)],
        transaction_hashes: &[String],
    ) -> Result<Self> {
        sync_ens_v1_unwrapped_authority_with_scope(
            pool,
            chain,
            true,
            block_hashes,
            Some(transaction_hashes),
            Some(source_scope),
            None,
            None,
            None,
            None,
        )
        .await
    }
}
