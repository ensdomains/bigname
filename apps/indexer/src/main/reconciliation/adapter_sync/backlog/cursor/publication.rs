use anyhow::{Context, Result, ensure};
use bigname_storage::{
    RawLogStagingBoundaryStatus, RawLogStagingReadGuard, acquire_raw_log_staging_read_guard,
};
use sqlx::PgPool;

use crate::reconciliation::guard_release::prioritize_operation_error;

use super::{
    BacklogCursor, BacklogCursorPublication, PageCounts, advance_backlog_cursor,
    load_backlog_cursor_for_update, load_backlog_target_block, rewind_backlog_cursor,
};

pub(crate) async fn publish_backlog_cursor_page(
    pool: &PgPool,
    deployment_profile: &str,
    chain: &str,
    cursor: BacklogCursor,
    completed_to_block: i64,
    selected_block_count: usize,
    summary: &crate::reconciliation::PersistedRawPayloadAdapterSyncSummary,
) -> Result<BacklogCursorPublication> {
    let counts = PageCounts {
        selected: i64::try_from(selected_block_count)
            .context("post-replay backlog selected block count does not fit in i64")?,
        scanned: i64::try_from(summary.scanned_log_count)
            .context("post-replay backlog scanned log count does not fit in i64")?,
        matched: i64::try_from(summary.matched_log_count)
            .context("post-replay backlog matched log count does not fit in i64")?,
        synced: i64::try_from(summary.total_synced_count)
            .context("post-replay backlog synced count does not fit in i64")?,
        inserted: i64::try_from(summary.total_inserted_count)
            .context("post-replay backlog inserted count does not fit in i64")?,
    };
    publish_cursor_page(
        pool,
        deployment_profile,
        chain,
        cursor,
        completed_to_block,
        counts,
    )
    .await
}

pub(crate) async fn publish_empty_backlog_cursor_page(
    pool: &PgPool,
    deployment_profile: &str,
    chain: &str,
    cursor: BacklogCursor,
) -> Result<BacklogCursorPublication> {
    publish_cursor_page(
        pool,
        deployment_profile,
        chain,
        cursor,
        cursor.target_block_number,
        PageCounts::default(),
    )
    .await
}

async fn publish_cursor_page(
    pool: &PgPool,
    deployment_profile: &str,
    chain: &str,
    cursor: BacklogCursor,
    completed_to_block: i64,
    counts: PageCounts,
) -> Result<BacklogCursorPublication> {
    ensure!(
        completed_to_block >= cursor.next_block_number,
        "post-replay backlog page must not complete before its cursor"
    );
    let mut guard = acquire_raw_log_staging_read_guard(pool, chain).await?;
    let publication = publish_cursor_page_while_guarded(
        &mut guard,
        deployment_profile,
        chain,
        cursor,
        completed_to_block,
        counts,
    )
    .await;
    let release = guard.release().await;
    prioritize_operation_error(publication, release)
}

async fn publish_cursor_page_while_guarded(
    guard: &mut RawLogStagingReadGuard,
    deployment_profile: &str,
    chain: &str,
    cursor: BacklogCursor,
    completed_to_block: i64,
    counts: PageCounts,
) -> Result<BacklogCursorPublication> {
    let stored = load_backlog_cursor_for_update(guard.connection_mut(), deployment_profile, chain)
        .await?
        .context("post-replay backlog cursor disappeared before page publication")?;
    if stored != cursor {
        return Ok(BacklogCursorPublication::Retry(stored));
    }
    let replay_target = cursor
        .range_start_block_number
        .checked_sub(1)
        .context("post-replay backlog replay target underflowed")?;
    match guard
        .classify_newer_revisions_after(cursor.input_version, completed_to_block)
        .await?
    {
        RawLogStagingBoundaryStatus::RetentionGenerationChanged { .. } => {
            Ok(BacklogCursorPublication::AwaitingReplay)
        }
        RawLogStagingBoundaryStatus::ChangedAtOrBefore { earliest_block, .. }
            if earliest_block <= replay_target =>
        {
            Ok(BacklogCursorPublication::AwaitingReplay)
        }
        RawLogStagingBoundaryStatus::ChangedAtOrBefore {
            observed,
            earliest_block,
        } => {
            let target = load_backlog_target_block(
                guard.connection_mut(),
                chain,
                cursor.range_start_block_number,
            )
            .await?
            .unwrap_or(cursor.range_start_block_number);
            Ok(BacklogCursorPublication::Retry(
                rewind_backlog_cursor(
                    guard.connection_mut(),
                    deployment_profile,
                    chain,
                    cursor,
                    earliest_block,
                    target,
                    observed,
                )
                .await?,
            ))
        }
        RawLogStagingBoundaryStatus::Accepted(observed) => {
            let target = load_backlog_target_block(
                guard.connection_mut(),
                chain,
                cursor.range_start_block_number,
            )
            .await?
            .unwrap_or(cursor.range_start_block_number)
            .max(cursor.target_block_number);
            Ok(BacklogCursorPublication::Advanced(
                advance_backlog_cursor(
                    guard.connection_mut(),
                    deployment_profile,
                    chain,
                    completed_to_block,
                    target,
                    counts,
                    observed,
                )
                .await?,
            ))
        }
    }
}
