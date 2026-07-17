use anyhow::{Context, Result};
use bigname_storage::{
    RawLogStagingBoundaryStatus, RawLogStagingInputVersion, RawLogStagingReadGuard,
    acquire_raw_log_staging_read_guard,
};
use sqlx::PgPool;

use crate::reconciliation::guard_release::prioritize_operation_error;

use super::{
    BacklogCursorPreparation, insert_backlog_cursor, load_backlog_cursor_for_update,
    load_backlog_target_block, load_replay_cursor, refresh_backlog_cursor, reset_backlog_cursor,
    rewind_backlog_cursor,
};

pub(crate) async fn prepare_backlog_cursor(
    pool: &PgPool,
    deployment_profile: &str,
    chain: &str,
) -> Result<BacklogCursorPreparation> {
    let mut guard = acquire_raw_log_staging_read_guard(pool, chain).await?;
    let preparation =
        prepare_backlog_cursor_while_guarded(&mut guard, deployment_profile, chain).await;
    let release = guard.release().await;
    prioritize_operation_error(preparation, release)
}

async fn prepare_backlog_cursor_while_guarded(
    guard: &mut RawLogStagingReadGuard,
    deployment_profile: &str,
    chain: &str,
) -> Result<BacklogCursorPreparation> {
    let Some(replay) =
        load_replay_cursor(guard.connection_mut(), deployment_profile, chain).await?
    else {
        let empty_generation_zero_corpus = guard.version().retention_generation == 0
            && load_backlog_target_block(guard.connection_mut(), chain, 0)
                .await?
                .is_none();
        return Ok(if empty_generation_zero_corpus {
            BacklogCursorPreparation::NoWork
        } else {
            BacklogCursorPreparation::AwaitingReplay
        });
    };
    if replay.next_block_number <= replay.target_block_number {
        return Ok(BacklogCursorPreparation::AwaitingReplay);
    }
    let accepted_replay_version = match guard
        .classify_newer_revisions_after(replay.input_version, replay.target_block_number)
        .await?
    {
        RawLogStagingBoundaryStatus::Accepted(observed) => observed,
        RawLogStagingBoundaryStatus::RetentionGenerationChanged { .. }
        | RawLogStagingBoundaryStatus::ChangedAtOrBefore { .. } => {
            return Ok(BacklogCursorPreparation::AwaitingReplay);
        }
    };

    let range_start = replay
        .target_block_number
        .checked_add(1)
        .context("post-replay live adapter backlog start block overflowed")?;
    let target = load_backlog_target_block(guard.connection_mut(), chain, range_start)
        .await?
        .unwrap_or(range_start);
    let existing =
        load_backlog_cursor_for_update(guard.connection_mut(), deployment_profile, chain).await?;
    let Some(cursor) = existing else {
        return Ok(BacklogCursorPreparation::Ready(
            insert_backlog_cursor(
                guard.connection_mut(),
                deployment_profile,
                chain,
                range_start,
                target,
                accepted_replay_version,
            )
            .await?,
        ));
    };

    if cursor.range_start_block_number != range_start
        || backlog_version_predates_replay(cursor.input_version, replay.input_version)
    {
        return Ok(BacklogCursorPreparation::Ready(
            reset_backlog_cursor(
                guard.connection_mut(),
                deployment_profile,
                chain,
                range_start,
                target,
                accepted_replay_version,
            )
            .await?,
        ));
    }

    let consumed_through = cursor
        .next_block_number
        .checked_sub(1)
        .context("post-replay live adapter backlog consumed boundary underflowed")?;
    match guard
        .classify_newer_revisions_after(cursor.input_version, consumed_through)
        .await?
    {
        RawLogStagingBoundaryStatus::Accepted(observed) => Ok(BacklogCursorPreparation::Ready(
            refresh_backlog_cursor(
                guard.connection_mut(),
                deployment_profile,
                chain,
                cursor,
                target,
                observed,
            )
            .await?,
        )),
        // The replay cursor already proved the current prefix. A stale or
        // legacy backlog version is safely upgraded by replaying its whole
        // post-target range from that accepted prefix.
        RawLogStagingBoundaryStatus::RetentionGenerationChanged { .. } => {
            Ok(BacklogCursorPreparation::Ready(
                reset_backlog_cursor(
                    guard.connection_mut(),
                    deployment_profile,
                    chain,
                    range_start,
                    target,
                    accepted_replay_version,
                )
                .await?,
            ))
        }
        RawLogStagingBoundaryStatus::ChangedAtOrBefore { earliest_block, .. }
            if earliest_block <= replay.target_block_number =>
        {
            Ok(BacklogCursorPreparation::Ready(
                reset_backlog_cursor(
                    guard.connection_mut(),
                    deployment_profile,
                    chain,
                    range_start,
                    target,
                    accepted_replay_version,
                )
                .await?,
            ))
        }
        RawLogStagingBoundaryStatus::ChangedAtOrBefore {
            observed,
            earliest_block,
        } => Ok(BacklogCursorPreparation::Ready(
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
        )),
    }
}

fn backlog_version_predates_replay(
    backlog: RawLogStagingInputVersion,
    replay: RawLogStagingInputVersion,
) -> bool {
    backlog.retention_generation != replay.retention_generation
        || backlog.revision < replay.revision
}
