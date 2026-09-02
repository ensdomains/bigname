use anyhow::{Context, Result};
use sqlx::{PgConnection, PgPool};

#[derive(Debug)]
pub struct InterpretRedoInProgress;

impl std::fmt::Display for InterpretRedoInProgress {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("Interpret redo is in progress")
    }
}

impl std::error::Error for InterpretRedoInProgress {}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InterpretRedoFence(Vec<(String, i64)>);

pub async fn capture_interpret_redo_fence(pool: &PgPool) -> Result<InterpretRedoFence> {
    let mut transaction = pool
        .begin()
        .await
        .context("failed to begin Interpret redo fence transaction")?;
    sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
        .execute(&mut *transaction)
        .await
        .context("failed to configure Interpret redo fence transaction")?;
    ensure_interpret_not_redo(&mut transaction).await?;
    let fence = InterpretRedoFence(load_interpret_redo_generations(&mut transaction).await?);
    transaction
        .commit()
        .await
        .context("failed to commit Interpret redo fence transaction")?;
    Ok(fence)
}

pub(super) async fn capture_fence_if(
    pool: &PgPool,
    required: bool,
) -> Result<Option<InterpretRedoFence>> {
    match required {
        true => capture_interpret_redo_fence(pool).await.map(Some),
        false => Ok(None),
    }
}

pub async fn revalidate_interpret_redo_fence(
    pool: &PgPool,
    expected: &InterpretRedoFence,
) -> Result<()> {
    if &capture_interpret_redo_fence(pool).await? != expected {
        return Err(InterpretRedoInProgress.into());
    }
    Ok(())
}

pub(super) async fn ensure_interpret_not_redo(connection: &mut PgConnection) -> Result<()> {
    let redo_in_progress: bool = sqlx::query_scalar(
        "SELECT EXISTS (
             SELECT 1
             FROM bigname_phase.chain_phase_state
             WHERE phase_name = 'interpret'
               AND redo_in_progress IS TRUE
         )",
    )
    .fetch_one(connection)
    .await?;
    if redo_in_progress {
        return Err(InterpretRedoInProgress.into());
    }
    Ok(())
}

pub(super) async fn ensure_interpret_redo_fence(
    connection: &mut PgConnection,
    expected: &InterpretRedoFence,
) -> Result<()> {
    ensure_interpret_not_redo(connection).await?;
    let current = InterpretRedoFence(load_interpret_redo_generations(connection).await?);
    if &current != expected {
        return Err(InterpretRedoInProgress.into());
    }
    Ok(())
}

async fn load_interpret_redo_generations(
    connection: &mut PgConnection,
) -> Result<Vec<(String, i64)>> {
    Ok(sqlx::query_as(
        "SELECT chain_id, redo_attempt_generation
         FROM bigname_phase.chain_phase_state
         WHERE phase_name = 'interpret'
         ORDER BY chain_id",
    )
    .fetch_all(connection)
    .await?)
}
