use sqlx::{Postgres, Transaction};

use crate::{ProjectError, Result};

// Follow only newly scoped names/resources and changed v1 nodes. The seen sets are
// transaction-local; replay starts fresh and never reuses a graph from another head.
pub(super) async fn stage(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    target_block: i64,
) -> Result<()> {
    for statement in include_str!("mirror.sql")
        .split(';')
        .filter(|sql| !sql.trim().is_empty())
    {
        let query = sqlx::query(statement);
        let query = if statement.contains("$1") {
            query.bind(chain_id).bind(target_block)
        } else {
            query
        };
        query
            .execute(&mut **transaction)
            .await
            .map_err(|error| ProjectError::database("failed to stage mirror scope pairs", error))?;
    }
    Ok(())
}

pub(super) async fn include(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    target_block: i64,
) -> Result<()> {
    for statement in [
        "ANALYZE project_scope_names",
        "ANALYZE project_scope_resources",
    ] {
        sqlx::query(statement)
            .execute(&mut **transaction)
            .await
            .map_err(|error| ProjectError::database("failed to analyze mirror frontier", error))?;
    }
    sqlx::query(include_str!("mirror_include.sql"))
        .bind(chain_id)
        .bind(target_block)
        .execute(&mut **transaction)
        .await
        .map_err(|error| ProjectError::database("failed to scope mirror resolver pairs", error))?;
    Ok(())
}

#[cfg(test)]
#[path = "mirror_tests.rs"]
mod tests;
