use super::*;

pub(crate) async fn run_resumable_hash_pinned_backfill_job_with_progress(
    pool: &sqlx::PgPool,
    source_plan: &WatchedSourceSelectorPlan,
    provider: &(impl ChainProviderOps + ?Sized),
    mut config: BackfillJobRunConfig,
    progress: &mut dyn StartupAdapterProgress,
) -> Result<BackfillJobRunOutcome> {
    config.adapter_sync_mode =
        effective_hash_pinned_adapter_sync_mode(source_plan, config.adapter_sync_mode);
    validate_hash_pinned_chunk_blocks(config.hash_pinned_chunk_blocks)?;
    let record =
        create_hash_pinned_backfill_job_with_progress(pool, source_plan, &config, progress).await?;
    run_precreated_hash_pinned_backfill_job_inner(
        pool,
        source_plan,
        provider,
        config,
        record,
        &mut Some(progress),
    )
    .await
}

pub(crate) async fn run_reserved_hash_pinned_backfill_range_with_progress(
    pool: &sqlx::PgPool,
    source_plan: &WatchedSourceSelectorPlan,
    provider: &(impl ChainProviderOps + ?Sized),
    config: &BackfillJobRunConfig,
    reserved_range: &BackfillRange,
    aggregate: &mut BackfillJobRunOutcome,
    progress: &tokio::sync::mpsc::UnboundedSender<()>,
) -> Result<()> {
    run_reserved_hash_pinned_backfill_range_inner(
        pool,
        source_plan,
        provider,
        config,
        reserved_range,
        aggregate,
        Some(progress),
        &mut None,
    )
    .await
}
