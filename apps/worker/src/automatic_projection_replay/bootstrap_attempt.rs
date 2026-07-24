use super::*;
use anyhow::ensure;
use bigname_storage::projection_staging::{
    load_current_projection_full_replay_input_revision_in_transaction,
    lock_current_projection_replay_version_for_replay_write_in_transaction,
};
use sqlx::{Postgres, Row};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct ProjectionReplayAttempt {
    pub(super) normalized_target_block: Option<i64>,
    pub(super) full_replay_input_revision: i64,
    pub(super) apply_baseline_change_id: i64,
}

pub(super) async fn load_projection_replay_attempt(
    pool: &PgPool,
) -> Result<Option<ProjectionReplayAttempt>> {
    let mut transaction = pool
        .begin()
        .await
        .context("failed to open current-projection replay attempt transaction")?;
    lock_current_projection_replay_version_for_replay_write_in_transaction(&mut transaction)
        .await?;
    let current_input_revision =
        load_current_projection_full_replay_input_revision_in_transaction(&mut transaction).await?;
    let stored = load_stored_attempt(&mut transaction).await?;
    let attempt = stored.filter(|(version, attempt)| {
        *version == replay::CURRENT_PROJECTION_REPLAY_VERSION
            && attempt.full_replay_input_revision == current_input_revision
    });
    if stored.is_some() && attempt.is_none() {
        sqlx::query("DELETE FROM current_projection_replay_attempt WHERE singleton")
            .execute(&mut *transaction)
            .await
            .context("failed to discard an incompatible current-projection replay attempt")?;
    }
    transaction
        .commit()
        .await
        .context("failed to commit current-projection replay attempt inspection")?;
    Ok(attempt.map(|(_, attempt)| attempt))
}

pub(super) async fn start_projection_replay_attempt(
    pool: &PgPool,
    candidate_target_block: Option<i64>,
    captured_watermark: projection_apply::NormalizedEventChangeCursor,
) -> Result<ProjectionReplayAttempt> {
    let mut transaction = pool
        .begin()
        .await
        .context("failed to open current-projection replay attempt start transaction")?;
    lock_current_projection_replay_version_for_replay_write_in_transaction(&mut transaction)
        .await?;
    let current_input_revision =
        load_current_projection_full_replay_input_revision_in_transaction(&mut transaction).await?;
    if let Some((version, attempt)) = load_stored_attempt(&mut transaction).await? {
        if version == replay::CURRENT_PROJECTION_REPLAY_VERSION
            && attempt.full_replay_input_revision == current_input_revision
        {
            transaction.commit().await?;
            return Ok(attempt);
        }
        sqlx::query("DELETE FROM current_projection_replay_attempt WHERE singleton")
            .execute(&mut *transaction)
            .await
            .context("failed to replace an incompatible current-projection replay attempt")?;
    }

    let durable_targets =
        load_durable_progress_targets(&mut transaction, current_input_revision).await?;
    let normalized_target_block = match durable_targets.as_slice() {
        [Some(target)] => Some(*target),
        _ => candidate_target_block,
    };
    let apply_baseline_change_id = if durable_targets.is_empty() {
        captured_watermark.change_id
    } else {
        0
    };
    sqlx::query(
        r#"
        INSERT INTO current_projection_replay_attempt (
            singleton,
            replay_version,
            normalized_target_block,
            full_replay_input_revision,
            apply_baseline_change_id
        )
        VALUES (true, $1, $2, $3, $4)
        "#,
    )
    .bind(replay::CURRENT_PROJECTION_REPLAY_VERSION)
    .bind(normalized_target_block)
    .bind(current_input_revision)
    .bind(apply_baseline_change_id)
    .execute(&mut *transaction)
    .await
    .context("failed to persist current-projection replay attempt")?;
    transaction
        .commit()
        .await
        .context("failed to commit current-projection replay attempt")?;

    Ok(ProjectionReplayAttempt {
        normalized_target_block,
        full_replay_input_revision: current_input_revision,
        apply_baseline_change_id,
    })
}

pub(super) async fn clear_projection_replay_attempt(pool: &PgPool) -> Result<()> {
    let mut transaction = pool
        .begin()
        .await
        .context("failed to open current-projection replay attempt clear transaction")?;
    lock_current_projection_replay_version_for_replay_write_in_transaction(&mut transaction)
        .await?;
    sqlx::query("DELETE FROM current_projection_replay_attempt WHERE singleton")
        .execute(&mut *transaction)
        .await
        .context("failed to clear completed current-projection replay attempt")?;
    transaction
        .commit()
        .await
        .context("failed to commit current-projection replay attempt clear")?;
    Ok(())
}

pub(super) async fn finalize_projection_replay_attempt(
    pool: &PgPool,
    attempt: ProjectionReplayAttempt,
    cursor_seed: Option<projection_apply::NormalizedEventChangeCursor>,
) -> Result<()> {
    let mut transaction = pool
        .begin()
        .await
        .context("failed to open current-projection replay handoff transaction")?;
    lock_current_projection_replay_version_for_replay_write_in_transaction(&mut transaction)
        .await?;
    let current_input_revision =
        load_current_projection_full_replay_input_revision_in_transaction(&mut transaction).await?;
    ensure!(
        current_input_revision == attempt.full_replay_input_revision,
        "current-projection replay input revision changed from {} to {current_input_revision} before handoff",
        attempt.full_replay_input_revision
    );
    let projections = replay::ALL_CURRENT_PROJECTION_ORDER.to_vec();
    let marker_count = sqlx::query_scalar::<_, i64>(
        r#"
        SELECT COUNT(DISTINCT projection)::BIGINT
        FROM current_projection_replay_status
        WHERE replay_version = $1
          AND full_replay_input_revision = $2
          AND projection = ANY($3::TEXT[])
          AND (
              $4::BIGINT IS NULL
              OR completed_normalized_target_block >= $4
          )
        "#,
    )
    .bind(replay::CURRENT_PROJECTION_REPLAY_VERSION)
    .bind(attempt.full_replay_input_revision)
    .bind(&projections)
    .bind(attempt.normalized_target_block)
    .fetch_one(&mut *transaction)
    .await
    .context("failed to verify current-projection replay markers before handoff")?;
    ensure!(
        marker_count == replay::ALL_CURRENT_PROJECTION_ORDER.len() as i64,
        "current-projection replay markers changed before handoff"
    );
    if let Some(cursor_seed) = cursor_seed {
        projection_apply::seed_normalized_event_cursor_if_absent_in_transaction(
            &mut transaction,
            cursor_seed,
        )
        .await?;
    }
    let deleted = sqlx::query(
        r#"
        DELETE FROM current_projection_replay_attempt
        WHERE singleton
          AND replay_version = $1
          AND normalized_target_block IS NOT DISTINCT FROM $2
          AND full_replay_input_revision = $3
          AND apply_baseline_change_id = $4
        "#,
    )
    .bind(replay::CURRENT_PROJECTION_REPLAY_VERSION)
    .bind(attempt.normalized_target_block)
    .bind(attempt.full_replay_input_revision)
    .bind(attempt.apply_baseline_change_id)
    .execute(&mut *transaction)
    .await
    .context("failed to consume current-projection replay attempt at handoff")?
    .rows_affected();
    ensure!(
        deleted == 1,
        "current-projection replay attempt changed before handoff"
    );
    transaction
        .commit()
        .await
        .context("failed to commit current-projection replay handoff")?;
    Ok(())
}

async fn load_stored_attempt(
    transaction: &mut sqlx::Transaction<'_, Postgres>,
) -> Result<Option<(i32, ProjectionReplayAttempt)>> {
    let row = sqlx::query(
        r#"
        SELECT
            replay_version,
            normalized_target_block,
            full_replay_input_revision,
            apply_baseline_change_id
        FROM current_projection_replay_attempt
        WHERE singleton
        FOR UPDATE
        "#,
    )
    .fetch_optional(&mut **transaction)
    .await
    .context("failed to load current-projection replay attempt")?;
    row.map(|row| {
        Ok((
            row.try_get("replay_version")?,
            ProjectionReplayAttempt {
                normalized_target_block: row.try_get("normalized_target_block")?,
                full_replay_input_revision: row.try_get("full_replay_input_revision")?,
                apply_baseline_change_id: row.try_get("apply_baseline_change_id")?,
            },
        ))
    })
    .transpose()
}

async fn load_durable_progress_targets(
    transaction: &mut sqlx::Transaction<'_, Postgres>,
    current_input_revision: i64,
) -> Result<Vec<Option<i64>>> {
    let projections = replay::ALL_CURRENT_PROJECTION_ORDER.to_vec();
    sqlx::query_scalar(
        r#"
        SELECT DISTINCT progress.normalized_target_block
        FROM (
            SELECT completed_normalized_target_block AS normalized_target_block
            FROM current_projection_replay_status
            WHERE replay_version = $1
              AND full_replay_input_revision = $2
              AND projection = ANY($3::TEXT[])
            UNION ALL
            SELECT completed_normalized_target_block AS normalized_target_block
            FROM current_projection_staging_checkpoints
            WHERE replay_version = $1
              AND full_replay_input_revision = $2
              AND projection = ANY($3::TEXT[])
        ) AS progress
        ORDER BY progress.normalized_target_block NULLS FIRST
        "#,
    )
    .bind(replay::CURRENT_PROJECTION_REPLAY_VERSION)
    .bind(current_input_revision)
    .bind(&projections)
    .fetch_all(&mut **transaction)
    .await
    .context("failed to load durable current-projection replay progress targets")
}
