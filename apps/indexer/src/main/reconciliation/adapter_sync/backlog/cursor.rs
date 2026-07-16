use anyhow::{Context, Result};
use bigname_storage::{
    RawLogStagingBoundaryStatus, RawLogStagingInputVersion, RawLogStagingReadSetGuard,
};
use sqlx::{PgConnection, PgPool};

use crate::normalized_replay_catchup::CURSOR_KIND_RAW_FACT_NORMALIZED_EVENTS;

#[path = "cursor/preparation.rs"]
mod preparation;
#[path = "cursor/publication.rs"]
mod publication;

pub(super) use preparation::prepare_backlog_cursor;
pub(super) use publication::{publish_backlog_cursor_page, publish_empty_backlog_cursor_page};

pub(super) const CURSOR_KIND_POST_REPLAY_LIVE_ADAPTER_BACKLOG: &str =
    "post_replay_live_adapter_backlog";
const DEFAULT_LIVE_ADAPTER_BACKLOG_BLOCK_BATCH_SIZE: i64 = 1;
const MAX_LIVE_ADAPTER_BACKLOG_BLOCK_BATCH_SIZE: i64 = 64;
const LIVE_ADAPTER_BACKLOG_BLOCK_BATCH_SIZE_ENV: &str =
    "BIGNAME_INDEXER_LIVE_ADAPTER_BACKLOG_BLOCK_BATCH_SIZE";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct BacklogCursor {
    pub(super) range_start_block_number: i64,
    pub(super) next_block_number: i64,
    pub(super) target_block_number: i64,
    pub(super) input_version: RawLogStagingInputVersion,
}

pub(super) enum BacklogCursorPreparation {
    Ready(BacklogCursor),
    NoWork,
    AwaitingReplay,
}

pub(super) enum BacklogCursorPublication {
    Advanced(BacklogCursor),
    Retry(BacklogCursor),
    AwaitingReplay,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum BacklogHandoffStatus {
    Ready,
    AwaitingReplay,
    AwaitingBacklog,
}

pub(crate) async fn validate_chain_handoff_while_guarded(
    guard: &mut RawLogStagingReadSetGuard,
    deployment_profile: &str,
    chain: &str,
) -> Result<BacklogHandoffStatus> {
    let Some(replay) =
        load_replay_cursor(guard.connection_mut(), deployment_profile, chain).await?
    else {
        let version = guard
            .version(chain)
            .with_context(|| format!("raw-log handoff guard does not include {chain}"))?;
        let has_canonical_logs = load_canonical_raw_max(guard.connection_mut(), chain, 0)
            .await?
            .is_some();
        return Ok(
            if version.retention_generation == 0 && !has_canonical_logs {
                BacklogHandoffStatus::Ready
            } else {
                BacklogHandoffStatus::AwaitingReplay
            },
        );
    };
    if replay.next_block_number <= replay.target_block_number {
        return Ok(BacklogHandoffStatus::AwaitingReplay);
    }
    if !matches!(
        guard
            .classify_newer_revisions_after(
                chain,
                replay.input_version,
                replay.target_block_number,
            )
            .await?,
        RawLogStagingBoundaryStatus::Accepted(_)
    ) {
        return Ok(BacklogHandoffStatus::AwaitingReplay);
    }

    let range_start = replay
        .target_block_number
        .checked_add(1)
        .context("post-replay live adapter backlog final-validation start block overflowed")?;
    let current_max = load_canonical_raw_max(guard.connection_mut(), chain, range_start).await?;
    let Some(backlog) =
        load_backlog_cursor(guard.connection_mut(), deployment_profile, chain).await?
    else {
        return Ok(BacklogHandoffStatus::AwaitingBacklog);
    };
    if backlog.range_start_block_number != range_start
        || backlog.next_block_number <= backlog.target_block_number
        || current_max.is_some_and(|block| backlog.next_block_number <= block)
    {
        return Ok(BacklogHandoffStatus::AwaitingBacklog);
    }
    let consumed_through = backlog
        .next_block_number
        .checked_sub(1)
        .context("post-replay live adapter backlog final-validation boundary underflowed")?;
    Ok(
        match guard
            .classify_newer_revisions_after(chain, backlog.input_version, consumed_through)
            .await?
        {
            RawLogStagingBoundaryStatus::Accepted(_) => BacklogHandoffStatus::Ready,
            RawLogStagingBoundaryStatus::RetentionGenerationChanged { .. } => {
                BacklogHandoffStatus::AwaitingBacklog
            }
            RawLogStagingBoundaryStatus::ChangedAtOrBefore { earliest_block, .. }
                if earliest_block <= replay.target_block_number =>
            {
                BacklogHandoffStatus::AwaitingReplay
            }
            RawLogStagingBoundaryStatus::ChangedAtOrBefore { .. } => {
                BacklogHandoffStatus::AwaitingBacklog
            }
        },
    )
}

#[derive(Clone, Copy)]
struct ReplayCursor {
    next_block_number: i64,
    target_block_number: i64,
    input_version: RawLogStagingInputVersion,
}

async fn load_replay_cursor(
    connection: &mut PgConnection,
    deployment_profile: &str,
    chain: &str,
) -> Result<Option<ReplayCursor>> {
    let row = sqlx::query_as::<_, (i64, i64, i64, i64)>(
        r#"
        SELECT next_block_number, target_block_number,
               raw_log_input_revision, raw_log_retention_generation
        FROM normalized_replay_cursors
        WHERE deployment_profile = $1 AND chain_id = $2 AND cursor_kind = $3
        "#,
    )
    .bind(deployment_profile)
    .bind(chain)
    .bind(CURSOR_KIND_RAW_FACT_NORMALIZED_EVENTS)
    .fetch_optional(connection)
    .await
    .context("failed to load normalized replay cursor for live backlog")?;
    Ok(row.map(|row| ReplayCursor {
        next_block_number: row.0,
        target_block_number: row.1,
        input_version: RawLogStagingInputVersion {
            revision: row.2,
            retention_generation: row.3,
        },
    }))
}

async fn load_backlog_cursor(
    connection: &mut PgConnection,
    deployment_profile: &str,
    chain: &str,
) -> Result<Option<BacklogCursor>> {
    load_backlog_cursor_with_lock(connection, deployment_profile, chain, false).await
}

async fn load_backlog_cursor_for_update(
    connection: &mut PgConnection,
    deployment_profile: &str,
    chain: &str,
) -> Result<Option<BacklogCursor>> {
    load_backlog_cursor_with_lock(connection, deployment_profile, chain, true).await
}

async fn load_backlog_cursor_with_lock(
    connection: &mut PgConnection,
    deployment_profile: &str,
    chain: &str,
    for_update: bool,
) -> Result<Option<BacklogCursor>> {
    let lock = if for_update { " FOR UPDATE" } else { "" };
    let row = sqlx::query_as::<_, (i64, i64, i64, i64, i64)>(&format!(
        "SELECT range_start_block_number, next_block_number, target_block_number, \
         raw_log_input_revision, raw_log_retention_generation \
         FROM normalized_replay_cursors \
         WHERE deployment_profile = $1 AND chain_id = $2 AND cursor_kind = $3{lock}"
    ))
    .bind(deployment_profile)
    .bind(chain)
    .bind(CURSOR_KIND_POST_REPLAY_LIVE_ADAPTER_BACKLOG)
    .fetch_optional(connection)
    .await
    .context("failed to load post-replay live adapter backlog cursor")?;
    Ok(row.map(cursor_from_row))
}

fn cursor_from_row(row: (i64, i64, i64, i64, i64)) -> BacklogCursor {
    BacklogCursor {
        range_start_block_number: row.0,
        next_block_number: row.1,
        target_block_number: row.2,
        input_version: RawLogStagingInputVersion {
            revision: row.3,
            retention_generation: row.4,
        },
    }
}

async fn insert_backlog_cursor(
    connection: &mut PgConnection,
    profile: &str,
    chain: &str,
    start: i64,
    target: i64,
    version: RawLogStagingInputVersion,
) -> Result<BacklogCursor> {
    write_reset_cursor(connection, profile, chain, start, target, version, true).await
}

async fn reset_backlog_cursor(
    connection: &mut PgConnection,
    profile: &str,
    chain: &str,
    start: i64,
    target: i64,
    version: RawLogStagingInputVersion,
) -> Result<BacklogCursor> {
    write_reset_cursor(connection, profile, chain, start, target, version, false).await
}

async fn write_reset_cursor(
    connection: &mut PgConnection,
    profile: &str,
    chain: &str,
    start: i64,
    target: i64,
    version: RawLogStagingInputVersion,
    insert: bool,
) -> Result<BacklogCursor> {
    let sql = if insert {
        r#"INSERT INTO normalized_replay_cursors (
            deployment_profile, chain_id, cursor_kind, range_start_block_number,
            next_block_number, target_block_number, raw_log_input_revision,
            raw_log_retention_generation
        ) VALUES ($1, $2, $3, $4, $4, $5, $6, $7)
        RETURNING range_start_block_number, next_block_number, target_block_number,
                  raw_log_input_revision, raw_log_retention_generation"#
    } else {
        r#"UPDATE normalized_replay_cursors SET
            range_start_block_number = $4, next_block_number = $4,
            target_block_number = $5, last_completed_block_number = NULL,
            raw_log_input_revision = $6, raw_log_retention_generation = $7,
            last_replayed_at = NULL, updated_at = now()
        WHERE deployment_profile = $1 AND chain_id = $2 AND cursor_kind = $3
        RETURNING range_start_block_number, next_block_number, target_block_number,
                  raw_log_input_revision, raw_log_retention_generation"#
    };
    let row = sqlx::query_as::<_, (i64, i64, i64, i64, i64)>(sql)
        .bind(profile)
        .bind(chain)
        .bind(CURSOR_KIND_POST_REPLAY_LIVE_ADAPTER_BACKLOG)
        .bind(start)
        .bind(target)
        .bind(version.revision)
        .bind(version.retention_generation)
        .fetch_one(connection)
        .await
        .context("failed to initialize post-replay live adapter backlog cursor")?;
    Ok(cursor_from_row(row))
}

async fn refresh_backlog_cursor(
    connection: &mut PgConnection,
    profile: &str,
    chain: &str,
    cursor: BacklogCursor,
    target: i64,
    version: RawLogStagingInputVersion,
) -> Result<BacklogCursor> {
    let row = sqlx::query_as::<_, (i64, i64, i64, i64, i64)>(
        r#"UPDATE normalized_replay_cursors SET
            target_block_number = GREATEST(target_block_number, $4),
            raw_log_input_revision = $5, raw_log_retention_generation = $6,
            updated_at = now()
        WHERE deployment_profile = $1 AND chain_id = $2 AND cursor_kind = $3
        RETURNING range_start_block_number, next_block_number, target_block_number,
                  raw_log_input_revision, raw_log_retention_generation"#,
    )
    .bind(profile)
    .bind(chain)
    .bind(CURSOR_KIND_POST_REPLAY_LIVE_ADAPTER_BACKLOG)
    .bind(target.max(cursor.target_block_number))
    .bind(version.revision)
    .bind(version.retention_generation)
    .fetch_one(connection)
    .await
    .context("failed to refresh post-replay live adapter backlog cursor")?;
    Ok(cursor_from_row(row))
}

async fn rewind_backlog_cursor(
    connection: &mut PgConnection,
    profile: &str,
    chain: &str,
    cursor: BacklogCursor,
    rewind_block: i64,
    target: i64,
    version: RawLogStagingInputVersion,
) -> Result<BacklogCursor> {
    let rewind = rewind_block
        .max(cursor.range_start_block_number)
        .min(cursor.next_block_number);
    let row = sqlx::query_as::<_, (i64, i64, i64, i64, i64)>(
        r#"UPDATE normalized_replay_cursors SET
            next_block_number = $4, target_block_number = GREATEST(target_block_number, $5),
            last_completed_block_number = CASE WHEN $4 > range_start_block_number THEN $4 - 1 ELSE NULL END,
            raw_log_input_revision = $6, raw_log_retention_generation = $7,
            updated_at = now()
        WHERE deployment_profile = $1 AND chain_id = $2 AND cursor_kind = $3
        RETURNING range_start_block_number, next_block_number, target_block_number,
                  raw_log_input_revision, raw_log_retention_generation"#,
    )
    .bind(profile)
    .bind(chain)
    .bind(CURSOR_KIND_POST_REPLAY_LIVE_ADAPTER_BACKLOG)
    .bind(rewind)
    .bind(target)
    .bind(version.revision)
    .bind(version.retention_generation)
    .fetch_one(connection)
    .await
    .context("failed to rewind post-replay live adapter backlog cursor")?;
    Ok(cursor_from_row(row))
}

#[derive(Clone, Copy, Default)]
struct PageCounts {
    selected: i64,
    scanned: i64,
    matched: i64,
    synced: i64,
    inserted: i64,
}

async fn advance_backlog_cursor(
    connection: &mut PgConnection,
    profile: &str,
    chain: &str,
    completed: i64,
    target: i64,
    counts: PageCounts,
    version: RawLogStagingInputVersion,
) -> Result<BacklogCursor> {
    let next = completed
        .checked_add(1)
        .context("post-replay live adapter backlog next block overflowed")?;
    let row = sqlx::query_as::<_, (i64, i64, i64, i64, i64)>(
        r#"UPDATE normalized_replay_cursors SET
            next_block_number = $4, target_block_number = GREATEST(target_block_number, $5),
            last_completed_block_number = $6, last_selected_block_count = $7,
            last_scanned_raw_log_count = $8, last_matched_raw_log_count = $9,
            last_normalized_event_synced_count = $10,
            last_normalized_event_inserted_count = $11,
            raw_log_input_revision = $12, raw_log_retention_generation = $13,
            last_replayed_at = now(), last_failure_reason = NULL, last_failure_at = NULL,
            updated_at = now()
        WHERE deployment_profile = $1 AND chain_id = $2 AND cursor_kind = $3
        RETURNING range_start_block_number, next_block_number, target_block_number,
                  raw_log_input_revision, raw_log_retention_generation"#,
    )
    .bind(profile)
    .bind(chain)
    .bind(CURSOR_KIND_POST_REPLAY_LIVE_ADAPTER_BACKLOG)
    .bind(next)
    .bind(target)
    .bind(completed)
    .bind(counts.selected)
    .bind(counts.scanned)
    .bind(counts.matched)
    .bind(counts.synced)
    .bind(counts.inserted)
    .bind(version.revision)
    .bind(version.retention_generation)
    .fetch_one(connection)
    .await
    .context("failed to advance post-replay live adapter backlog cursor")?;
    Ok(cursor_from_row(row))
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct BacklogBlock {
    pub(super) block_number: i64,
    pub(super) block_hash: String,
}

pub(super) async fn load_backlog_block_hash_page(
    pool: &PgPool,
    chain: &str,
    next: i64,
    target: i64,
) -> Result<Vec<BacklogBlock>> {
    let rows = sqlx::query_as::<_, (i64, String)>(
        r#"SELECT DISTINCT lineage.block_number, lineage.block_hash
        FROM raw_logs JOIN chain_lineage AS lineage
          ON lineage.chain_id = raw_logs.chain_id AND lineage.block_hash = raw_logs.block_hash
        WHERE raw_logs.chain_id = $1 AND raw_logs.block_number BETWEEN $2 AND $3
          AND lineage.canonicality_state IN ('canonical', 'safe', 'finalized')
          AND raw_logs.canonicality_state IN ('canonical', 'safe', 'finalized')
        ORDER BY lineage.block_number, lineage.block_hash LIMIT $4"#,
    )
    .bind(chain)
    .bind(next)
    .bind(target)
    .bind(live_adapter_backlog_block_batch_size())
    .fetch_all(pool)
    .await
    .with_context(|| format!("failed to load post-replay backlog page for {chain}"))?;
    Ok(rows
        .into_iter()
        .map(|row| BacklogBlock {
            block_number: row.0,
            block_hash: row.1,
        })
        .collect())
}

async fn load_backlog_target_block(
    connection: &mut PgConnection,
    chain: &str,
    start: i64,
) -> Result<Option<i64>> {
    load_canonical_raw_max(connection, chain, start).await
}

async fn load_canonical_raw_max(
    connection: &mut PgConnection,
    chain: &str,
    start: i64,
) -> Result<Option<i64>> {
    sqlx::query_scalar::<_, Option<i64>>(
        r#"SELECT MAX(raw_logs.block_number)
        FROM raw_logs JOIN chain_lineage AS lineage
          ON lineage.chain_id = raw_logs.chain_id AND lineage.block_hash = raw_logs.block_hash
        WHERE raw_logs.chain_id = $1 AND raw_logs.block_number >= $2
          AND lineage.canonicality_state IN ('canonical', 'safe', 'finalized')
          AND raw_logs.canonicality_state IN ('canonical', 'safe', 'finalized')"#,
    )
    .bind(chain)
    .bind(start)
    .fetch_one(connection)
    .await
    .with_context(|| format!("failed to load canonical raw-log maximum for {chain}"))
}

fn live_adapter_backlog_block_batch_size() -> i64 {
    std::env::var(LIVE_ADAPTER_BACKLOG_BLOCK_BATCH_SIZE_ENV)
        .ok()
        .and_then(|raw| raw.parse::<i64>().ok())
        .filter(|value| *value > 0)
        .map(|value| value.min(MAX_LIVE_ADAPTER_BACKLOG_BLOCK_BATCH_SIZE))
        .unwrap_or(DEFAULT_LIVE_ADAPTER_BACKLOG_BLOCK_BATCH_SIZE)
}
