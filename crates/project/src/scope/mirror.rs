use sqlx::{Postgres, Transaction};

use crate::{ProjectError, Result};

// Pointer history, surfaces, changed events and declarations do not change during inventory
// closure. Build their dependency pairs once; only scope membership changes between passes.
// Start from the mirror's actual suffix walks so unrelated v1 nodes are never aggregated.
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

pub(super) async fn include(transaction: &mut Transaction<'_, Postgres>) -> Result<()> {
    sqlx::query(include_str!("mirror_include.sql"))
        .execute(&mut **transaction)
        .await
        .map_err(|error| ProjectError::database("failed to scope mirror resolver pairs", error))?;
    Ok(())
}

#[cfg(test)]
#[path = "mirror_tests.rs"]
mod tests;
