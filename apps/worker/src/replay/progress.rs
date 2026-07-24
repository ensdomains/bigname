use anyhow::{Context, Result};
pub use bigname_storage::CURRENT_PROJECTION_REPLAY_VERSION;
use sqlx::PgPool;

use super::CurrentProjectionReplayStepSummary;

pub(super) async fn projection_replay_completed(
    pool: &PgPool,
    projection: &str,
    normalized_target_block: Option<i64>,
) -> Result<bool> {
    sqlx::query_scalar::<_, bool>(
        r#"
        SELECT EXISTS (
            SELECT 1
            FROM current_projection_replay_status AS status
            JOIN current_projection_full_replay_input_revision AS input_revision
              ON input_revision.singleton
             AND input_revision.revision = status.full_replay_input_revision
            WHERE status.projection = $1
              AND status.replay_version = $2
              AND (
                  $3::BIGINT IS NULL
                  OR status.completed_normalized_target_block >= $3
              )
        )
        "#,
    )
    .bind(projection)
    .bind(CURRENT_PROJECTION_REPLAY_VERSION)
    .bind(normalized_target_block)
    .fetch_one(pool)
    .await
    .with_context(|| format!("failed to inspect replay status for {projection}"))
}

pub(super) async fn clear_projection_replay_completed(
    pool: &PgPool,
    projection: &str,
) -> Result<()> {
    let mut transaction = pool
        .begin()
        .await
        .with_context(|| format!("failed to open replay-status clear for {projection}"))?;
    bigname_storage::projection_staging::lock_current_projection_replay_version_for_replay_write_in_transaction(
        &mut transaction,
    )
    .await?;
    sqlx::query(
        r#"
        DELETE FROM current_projection_replay_status
        WHERE projection = $1
        "#,
    )
    .bind(projection)
    .execute(&mut *transaction)
    .await
    .with_context(|| format!("failed to clear replay status for {projection}"))?;
    transaction
        .commit()
        .await
        .with_context(|| format!("failed to commit replay-status clear for {projection}"))?;

    Ok(())
}

pub(super) async fn mark_projection_replay_completed(
    pool: &PgPool,
    step: &CurrentProjectionReplayStepSummary,
    normalized_target_block: Option<i64>,
) -> Result<()> {
    let mut transaction = pool.begin().await.with_context(|| {
        format!(
            "failed to open replay completion transaction for {}",
            step.projection
        )
    })?;
    let full_replay_input_revision =
        super::staging::cleanup::consume_completed_projection_checkpoint(
            &mut transaction,
            step.projection,
            normalized_target_block,
        )
        .await?;
    sqlx::query(
        r#"
        INSERT INTO current_projection_replay_status (
            projection,
            replay_version,
            completed_normalized_target_block,
            full_replay_input_revision,
            requested_key_count,
            upserted_row_count,
            deleted_row_count,
            completed_at
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, now())
        ON CONFLICT (projection)
        DO UPDATE SET
            replay_version = EXCLUDED.replay_version,
            completed_normalized_target_block = EXCLUDED.completed_normalized_target_block,
            full_replay_input_revision = EXCLUDED.full_replay_input_revision,
            requested_key_count = EXCLUDED.requested_key_count,
            upserted_row_count = EXCLUDED.upserted_row_count,
            deleted_row_count = EXCLUDED.deleted_row_count,
            completed_at = EXCLUDED.completed_at
        "#,
    )
    .bind(step.projection)
    .bind(CURRENT_PROJECTION_REPLAY_VERSION)
    .bind(normalized_target_block)
    .bind(full_replay_input_revision)
    .bind(i64::try_from(step.requested_key_count).context("requested key count overflow")?)
    .bind(i64::try_from(step.upserted_row_count).context("upserted row count overflow")?)
    .bind(i64::try_from(step.deleted_row_count).context("deleted row count overflow")?)
    .execute(&mut *transaction)
    .await
    .with_context(|| format!("failed to mark replay status for {}", step.projection))?;
    transaction
        .commit()
        .await
        .with_context(|| format!("failed to commit replay status for {}", step.projection))?;

    Ok(())
}
