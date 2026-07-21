use super::*;
use crate::checkpoint_context::FULL_CLOSURE_CHECKPOINT_SCOPE;

pub async fn clear_replay_adapter_checkpoints(
    pool: &PgPool,
    deployment_profile: &str,
    chain: &str,
    cursor_kind: &str,
) -> Result<()> {
    let result = sqlx::query(
        r#"
        DELETE FROM normalized_replay_adapter_checkpoints
        WHERE deployment_profile = $1
          AND chain_id = $2
          AND cursor_kind = $3
          AND adapter = $4
          AND checkpoint_scope = $5
        "#,
    )
    .bind(deployment_profile)
    .bind(chain)
    .bind(cursor_kind)
    .bind(ADAPTER)
    .bind(FULL_CLOSURE_CHECKPOINT_SCOPE)
    .execute(pool)
    .await;
    if is_undefined_table_error(&result) {
        return Ok(());
    }
    result.with_context(|| {
        format!(
            "failed to clear unwrapped-authority replay adapter checkpoints for {deployment_profile}/{chain}/{cursor_kind}"
        )
    })?;
    Ok(())
}

fn is_undefined_table_error<T>(result: &std::result::Result<T, sqlx::Error>) -> bool {
    matches!(
        result,
        Err(sqlx::Error::Database(error)) if error.code().as_deref() == Some("42P01")
    )
}

pub(super) async fn load_checkpoint_row(
    pool: &PgPool,
    chain: &str,
    context: &AdapterCheckpointContext,
) -> Result<Option<UnwrappedAuthorityReplayCheckpoint>> {
    let row = sqlx::query(
        r#"
        SELECT
            replay_start_block_number,
            replay_target_block_number,
            last_block_number,
            scanned_log_count,
            matched_log_count,
            status,
            state_payload,
            raw_log_retention_generation,
            raw_log_input_revision
        FROM normalized_replay_adapter_checkpoints
        WHERE deployment_profile = $1
          AND chain_id = $2
          AND cursor_kind = $3
          AND adapter = $4
          AND checkpoint_scope = $5
        "#,
    )
    .bind(&context.deployment_profile)
    .bind(chain)
    .bind(&context.cursor_kind)
    .bind(ADAPTER)
    .bind(context.checkpoint_scope)
    .fetch_optional(pool)
    .await
    .with_context(|| {
        format!(
            "failed to load {ADAPTER} replay checkpoint for {}/{}",
            context.deployment_profile, chain
        )
    })?;

    row.map(|row| checkpoint_from_row(chain, context, row))
        .transpose()
}

fn checkpoint_from_row(
    chain: &str,
    context: &AdapterCheckpointContext,
    row: sqlx::postgres::PgRow,
) -> Result<UnwrappedAuthorityReplayCheckpoint> {
    let state_payload: Value = row.try_get("state_payload")?;
    let flushed_events = flushed_events_from_payload(&state_payload)?;
    Ok(UnwrappedAuthorityReplayCheckpoint {
        context: AdapterCheckpointContext {
            deployment_profile: context.deployment_profile.clone(),
            cursor_kind: context.cursor_kind.clone(),
            checkpoint_scope: context.checkpoint_scope,
            range_start_block_number: row.try_get("replay_start_block_number")?,
            target_block_number: row.try_get("replay_target_block_number")?,
            startup_discovery_admission_epoch: context.startup_discovery_admission_epoch,
        },
        chain: chain.to_owned(),
        status: row.try_get("status")?,
        last_block_number: row.try_get("last_block_number")?,
        scanned_log_count: usize::try_from(row.try_get::<i64, _>("scanned_log_count")?)
            .context("checkpoint scanned log count overflowed usize")?,
        matched_log_count: usize::try_from(row.try_get::<i64, _>("matched_log_count")?)
            .context("checkpoint matched log count overflowed usize")?,
        state_payload,
        flushed_events,
        raw_log_input_version: RawLogStagingInputVersion {
            retention_generation: row.try_get("raw_log_retention_generation")?,
            revision: row.try_get("raw_log_input_revision")?,
        },
    })
}

pub(super) async fn delete_checkpoint(
    pool: &PgPool,
    chain: &str,
    context: &AdapterCheckpointContext,
) -> Result<()> {
    sqlx::query(
        r#"
        DELETE FROM normalized_replay_adapter_checkpoints
        WHERE deployment_profile = $1
          AND chain_id = $2
          AND cursor_kind = $3
          AND adapter = $4
          AND checkpoint_scope = $5
        "#,
    )
    .bind(&context.deployment_profile)
    .bind(chain)
    .bind(&context.cursor_kind)
    .bind(ADAPTER)
    .bind(context.checkpoint_scope)
    .execute(pool)
    .await
    .context("failed to reset stale unwrapped-authority replay checkpoint")?;
    Ok(())
}
