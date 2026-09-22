//! Test-only execution of literal SQL from the pre-optimization implementation.
//! Selection is transaction-local so parallel integration tests cannot affect each other.
use crate::{ProjectError, Result};
use sqlx::{Postgres, Transaction};

pub(crate) async fn enabled(transaction: &mut Transaction<'_, Postgres>) -> Result<bool> {
    sqlx::query_scalar(
        "SELECT COALESCE(current_setting('bigname.benchmark_reference', true), '') = 'on'",
    )
    .fetch_one(&mut **transaction)
    .await
    .map_err(|error| ProjectError::database("failed to read test reference mode", error))
}

pub(crate) async fn execute(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    target_block: i64,
    target_hash: Option<&str>,
    statements: &str,
) -> Result<()> {
    for statement in statements.split(';').filter(|sql| !sql.trim().is_empty()) {
        let mut query = sqlx::query(statement);
        if statement.contains("$1") {
            query = query.bind(chain_id).bind(target_block);
        }
        if statement.contains("$3") {
            query = query.bind(target_hash);
        }
        query.execute(&mut **transaction).await.map_err(|error| {
            ProjectError::database("failed to execute pre-optimization reference SQL", error)
        })?;
    }
    Ok(())
}
