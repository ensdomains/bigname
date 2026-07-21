use anyhow::{Context, Result};
use sqlx::PgPool;

use crate::checkpoint_context::{AdapterCheckpointContext, FULL_CLOSURE_CHECKPOINT_SCOPE};

use super::ADAPTER;

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
            "failed to clear replay adapter checkpoints for {deployment_profile}/{chain}/{cursor_kind}"
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

pub(crate) async fn delete_checkpoint(
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
    .context("failed to reset stale subregistry replay checkpoint")?;
    Ok(())
}
