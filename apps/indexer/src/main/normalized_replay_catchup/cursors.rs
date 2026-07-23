use std::collections::BTreeSet;

use anyhow::{Context, Result};
use bigname_storage::{
    RawLogStagingBoundaryStatus, RawLogStagingInputVersion, acquire_raw_log_staging_read_guard,
    earliest_raw_log_staging_block_changed_since, load_raw_log_staging_input_version,
};
use sqlx::types::time::OffsetDateTime;
use sqlx::{PgConnection, PgPool};

use crate::reconciliation::RawFactNormalizedEventReplayOutcome;

use super::{
    CURSOR_KIND_RAW_FACT_NORMALIZED_EVENTS, NormalizedReplayCursor, RawLogBounds,
    TargetRefreshPolicy,
};

pub(super) async fn ensure_cursor(
    pool: &PgPool,
    deployment_profile: &str,
    chain: &str,
    cursor_kind: &str,
    bounds: RawLogBounds,
    target_refresh_policy: TargetRefreshPolicy,
) -> Result<NormalizedReplayCursor> {
    let row = sqlx::query_as::<_, (i64, i64, i64, Option<OffsetDateTime>, i64, i64)>(
        r#"
        INSERT INTO normalized_replay_cursors (
            deployment_profile,
            chain_id,
            cursor_kind,
            range_start_block_number,
            next_block_number,
            target_block_number
        )
        VALUES ($1, $2, $3, $4, $4, $5)
        ON CONFLICT (deployment_profile, chain_id, cursor_kind) DO UPDATE
        SET
            range_start_block_number = LEAST(
                normalized_replay_cursors.range_start_block_number,
                EXCLUDED.range_start_block_number
            ),
            next_block_number = CASE
                WHEN EXCLUDED.range_start_block_number < normalized_replay_cursors.range_start_block_number
                    THEN LEAST(
                        normalized_replay_cursors.next_block_number,
                        EXCLUDED.range_start_block_number
                    )
                ELSE normalized_replay_cursors.next_block_number
            END,
            target_block_number = CASE
                WHEN $6 THEN GREATEST(
                    normalized_replay_cursors.target_block_number,
                    EXCLUDED.target_block_number
                )
                ELSE normalized_replay_cursors.target_block_number
            END,
            updated_at = now()
        RETURNING
            range_start_block_number,
            next_block_number,
            target_block_number,
            last_replayed_at,
            raw_log_input_revision,
            raw_log_retention_generation
        "#,
    )
    .bind(deployment_profile)
    .bind(chain)
    .bind(cursor_kind)
    .bind(bounds.start_block)
    .bind(bounds.target_block)
    .bind(matches!(
        target_refresh_policy,
        TargetRefreshPolicy::RefreshToLatestRawLog
    ))
    .fetch_one(pool)
    .await
    .with_context(|| {
        format!(
            "failed to create or refresh normalized replay cursor for {deployment_profile}/{chain}"
        )
    })?;

    Ok(NormalizedReplayCursor {
        range_start_block_number: row.0,
        next_block_number: row.1,
        target_block_number: row.2,
        last_replayed_at: row.3,
        raw_log_input_revision: row.4,
        raw_log_retention_generation: row.5,
    })
}

pub(super) async fn rewind_cursor_for_newly_observed_older_logs(
    pool: &PgPool,
    deployment_profile: &str,
    chain: &str,
    cursor: NormalizedReplayCursor,
) -> Result<(NormalizedReplayCursor, RawLogStagingInputVersion)> {
    let current_version = load_raw_log_staging_input_version(pool, chain).await?;
    let Some(last_replayed_at) = cursor.last_replayed_at else {
        return Ok((cursor, current_version));
    };

    let revision_rewind =
        if current_version.retention_generation != cursor.raw_log_retention_generation {
            Some(cursor.range_start_block_number)
        } else if current_version.revision > cursor.raw_log_input_revision
            && cursor.next_block_number > 0
        {
            earliest_raw_log_staging_block_changed_since(
                pool,
                chain,
                cursor.raw_log_input_revision,
                cursor.next_block_number - 1,
            )
            .await?
        } else {
            None
        };
    let rewind_block = if revision_rewind.is_some() {
        revision_rewind
    } else {
        sqlx::query_scalar::<_, Option<i64>>(
            r#"
            SELECT MIN(raw_logs.block_number)
            FROM raw_logs
            JOIN chain_lineage AS lineage
              ON lineage.chain_id = raw_logs.chain_id
             AND lineage.block_hash = raw_logs.block_hash
            WHERE raw_logs.chain_id = $1
              AND raw_logs.block_number < $2
              AND raw_logs.observed_at > $3
              AND lineage.canonicality_state IN (
                  'canonical'::canonicality_state,
                  'safe'::canonicality_state,
                  'finalized'::canonicality_state
              )
              AND raw_logs.canonicality_state IN (
                  'canonical'::canonicality_state,
                  'safe'::canonicality_state,
                  'finalized'::canonicality_state
              )
            "#,
        )
        .bind(chain)
        .bind(cursor.next_block_number)
        .bind(last_replayed_at)
        .fetch_one(pool)
        .await
        .with_context(|| {
            format!(
                "failed to inspect newly observed older normalized replay work for {deployment_profile}/{chain}/{CURSOR_KIND_RAW_FACT_NORMALIZED_EVENTS}"
            )
        })?
    };

    let Some(rewind_block) = rewind_block else {
        return Ok((cursor, current_version));
    };

    let row = sqlx::query_as::<_, (i64, i64, i64, Option<OffsetDateTime>, i64, i64)>(
        r#"
        UPDATE normalized_replay_cursors
        SET
            range_start_block_number = LEAST(range_start_block_number, $4),
            next_block_number = LEAST(next_block_number, $4),
            updated_at = now()
        WHERE deployment_profile = $1
          AND chain_id = $2
          AND cursor_kind = $3
        RETURNING
            range_start_block_number,
            next_block_number,
            target_block_number,
            last_replayed_at,
            raw_log_input_revision,
            raw_log_retention_generation
        "#,
    )
    .bind(deployment_profile)
    .bind(chain)
    .bind(CURSOR_KIND_RAW_FACT_NORMALIZED_EVENTS)
    .bind(rewind_block)
    .fetch_one(pool)
    .await
    .with_context(|| {
        format!(
            "failed to rewind normalized replay cursor for {deployment_profile}/{chain}/{CURSOR_KIND_RAW_FACT_NORMALIZED_EVENTS} to block {rewind_block}"
        )
    })?;

    Ok((
        NormalizedReplayCursor {
            range_start_block_number: row.0,
            next_block_number: row.1,
            target_block_number: row.2,
            last_replayed_at: row.3,
            raw_log_input_revision: row.4,
            raw_log_retention_generation: row.5,
        },
        current_version,
    ))
}

pub(super) async fn all_configured_cursors_complete(
    pool: &PgPool,
    deployment_profile: &str,
    chains: &[String],
) -> Result<bool> {
    for chain in chains.iter().collect::<BTreeSet<_>>() {
        if !configured_cursor_complete(pool, deployment_profile, chain).await? {
            return Ok(false);
        }
    }
    Ok(true)
}

async fn configured_cursor_complete(
    pool: &PgPool,
    deployment_profile: &str,
    chain: &str,
) -> Result<bool> {
    let mut raw_log_guard = acquire_raw_log_staging_read_guard(pool, chain).await?;
    let readiness = async {
        let cursor = sqlx::query_as::<_, (i64, i64, i64, i64)>(
            r#"
            SELECT
                next_block_number,
                target_block_number,
                raw_log_input_revision,
                raw_log_retention_generation
            FROM normalized_replay_cursors
            WHERE deployment_profile = $1
              AND chain_id = $2
              AND cursor_kind = $3
            "#,
        )
        .bind(deployment_profile)
        .bind(chain)
        .bind(CURSOR_KIND_RAW_FACT_NORMALIZED_EVENTS)
        .fetch_optional(raw_log_guard.connection_mut())
        .await
        .with_context(|| {
            format!(
                "failed to inspect normalized replay cursor completion for {deployment_profile}/{chain}"
            )
        })?;

        let Some((next_block, target_block, input_revision, retention_generation)) = cursor else {
            return Ok(raw_log_guard.version().retention_generation == 0
                && !chain_has_canonical_raw_logs(raw_log_guard.connection_mut(), chain).await?);
        };
        if next_block <= target_block {
            return Ok(false);
        }

        let expected_input = RawLogStagingInputVersion {
            retention_generation,
            revision: input_revision,
        };
        Ok(matches!(
            raw_log_guard
                .classify_newer_revisions_after(expected_input, target_block)
                .await?,
            RawLogStagingBoundaryStatus::Accepted(_)
        ))
    }
    .await;
    let release = raw_log_guard.release().await;
    crate::reconciliation::guard_release::prioritize_operation_error(readiness, release)
}

async fn chain_has_canonical_raw_logs(connection: &mut PgConnection, chain: &str) -> Result<bool> {
    sqlx::query_scalar::<_, bool>(
        r#"
        SELECT (
            SELECT raw_log_id
            FROM raw_logs
            WHERE raw_logs.chain_id = $1
              AND raw_logs.canonicality_state IN (
                  'canonical'::canonicality_state,
                  'safe'::canonicality_state,
                  'finalized'::canonicality_state
              )
            ORDER BY raw_logs.block_number DESC
            LIMIT 1
        ) IS NOT NULL
        "#,
    )
    .bind(chain)
    .fetch_one(connection)
    .await
    .with_context(|| format!("failed to inspect canonical raw logs for {chain}"))
}

// Cursor persistence keeps replay identity, bounds, outcome, and input version explicit.
#[expect(clippy::too_many_arguments)]
pub(super) async fn advance_cursor(
    pool: &PgPool,
    deployment_profile: &str,
    chain: &str,
    cursor_kind: &str,
    latest_target_block: i64,
    completed_to_block: i64,
    outcome: &RawFactNormalizedEventReplayOutcome,
    raw_log_input_version: RawLogStagingInputVersion,
) -> Result<()> {
    let next_block = completed_to_block
        .checked_add(1)
        .context("normalized replay cursor next block overflowed")?;
    let selected_block_count = i64::try_from(outcome.selected_block_count)
        .context("selected block count does not fit in i64")?;
    let canonical_raw_log_count = i64::try_from(outcome.canonical_raw_log_count)
        .context("canonical raw log count does not fit in i64")?;
    let scanned_raw_log_count = i64::try_from(outcome.scanned_raw_log_count)
        .context("scanned raw log count does not fit in i64")?;
    let matched_raw_log_count = i64::try_from(outcome.matched_raw_log_count)
        .context("matched raw log count does not fit in i64")?;
    let normalized_event_synced_count = i64::try_from(outcome.normalized_event_synced_count)
        .context("normalized event synced count does not fit in i64")?;
    let normalized_event_inserted_count = i64::try_from(outcome.normalized_event_inserted_count)
        .context("normalized event inserted count does not fit in i64")?;

    let mut raw_log_guard = acquire_raw_log_staging_read_guard(pool, chain).await?;
    let accepted_raw_log_input_version = raw_log_guard
        .accept_newer_revisions_after(raw_log_input_version, completed_to_block)
        .await
        .with_context(|| {
            format!(
                "raw-log staging input changed before normalized replay cursor publication for {chain}: expected generation {} revision {}, observed generation {} revision {}",
                raw_log_input_version.retention_generation,
                raw_log_input_version.revision,
                raw_log_guard.version().retention_generation,
                raw_log_guard.version().revision
            )
        })?;
    sqlx::query(
        r#"
        UPDATE normalized_replay_cursors
        SET
            next_block_number = GREATEST(next_block_number, $4),
            target_block_number = GREATEST(target_block_number, $5),
            last_completed_block_number = CASE
                WHEN last_completed_block_number IS NULL THEN $6
                ELSE GREATEST(last_completed_block_number, $6)
            END,
            last_selected_block_count = $7,
            last_canonical_raw_log_count = $8,
            last_scanned_raw_log_count = $9,
            last_matched_raw_log_count = $10,
            last_normalized_event_synced_count = $11,
            last_normalized_event_inserted_count = $12,
            raw_log_input_revision = $13,
            raw_log_retention_generation = $14,
            last_replayed_at = now(),
            last_failure_reason = NULL,
            last_failure_at = NULL,
            updated_at = now()
        WHERE deployment_profile = $1
          AND chain_id = $2
          AND cursor_kind = $3
        "#,
    )
    .bind(deployment_profile)
    .bind(chain)
    .bind(cursor_kind)
    .bind(next_block)
    .bind(latest_target_block)
    .bind(completed_to_block)
    .bind(selected_block_count)
    .bind(canonical_raw_log_count)
    .bind(scanned_raw_log_count)
    .bind(matched_raw_log_count)
    .bind(normalized_event_synced_count)
    .bind(normalized_event_inserted_count)
    .bind(accepted_raw_log_input_version.revision)
    .bind(accepted_raw_log_input_version.retention_generation)
    .execute(raw_log_guard.connection_mut())
    .await
    .with_context(|| {
        format!(
            "failed to advance normalized replay cursor for {deployment_profile}/{chain} through block {completed_to_block}"
        )
    })?;

    raw_log_guard.release().await
}

pub(super) async fn record_cursor_failure(
    pool: &PgPool,
    deployment_profile: &str,
    chain: &str,
    error: &anyhow::Error,
) -> Result<()> {
    #[cfg(test)]
    super::test_hook::pause_before_cursor_failure_record(pool, deployment_profile, chain).await;
    let failure_reason = postgres_text_safe(&format!("{error:#}"));
    sqlx::query(
        r#"
        UPDATE normalized_replay_cursors
        SET
            last_failure_reason = $4,
            last_failure_at = now(),
            updated_at = now()
        WHERE deployment_profile = $1
          AND chain_id = $2
          AND cursor_kind = $3
        "#,
    )
    .bind(deployment_profile)
    .bind(chain)
    .bind(CURSOR_KIND_RAW_FACT_NORMALIZED_EVENTS)
    .bind(failure_reason)
    .execute(pool)
    .await
    .with_context(|| {
        format!(
            "failed to record normalized replay cursor failure for {deployment_profile}/{chain}"
        )
    })?;

    Ok(())
}

fn postgres_text_safe(text: &str) -> String {
    text.replace('\0', "\\u0000")
}
