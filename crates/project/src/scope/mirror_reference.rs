// Test-only literal strategy from 2abf62296b5228e1a14aa101ffed09f106125ac5.
// SQL and control flow retained, only fixture paths are relocated.
use sqlx::{Postgres, Transaction};

use crate::{ProjectError, Result};

// This changes only the execution strategy, never scope membership. Once bulk
// pairs exist, reuse them until this transaction ends, including later closure steps.
#[derive(Default)]
pub(super) struct Strategy {
    bulk: bool,
}

// Follow only newly scoped names/resources and changed v1 nodes. The seen sets are
// transaction-local; replay starts fresh and never reuses a graph from another head.
pub(super) async fn stage(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    target_block: i64,
) -> Result<Strategy> {
    for statement in include_str!("mirror_reference/mirror.sql")
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
    Ok(Strategy::default())
}

pub(super) async fn include(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    target_block: i64,
    strategy: &mut Strategy,
) -> Result<()> {
    if !strategy.bulk {
        let broad: bool = sqlx::query_scalar(include_str!("mirror_reference/mirror_broad.sql"))
            .bind(chain_id)
            .bind(target_block)
            .fetch_one(&mut **transaction)
            .await
            .map_err(|error| ProjectError::database("failed to assess mirror scope size", error))?;
        if broad {
            stage_bulk(transaction, chain_id, target_block).await?;
            strategy.bulk = true;
        }
    }
    if strategy.bulk {
        sqlx::query(include_str!("mirror_reference/mirror_bulk_include.sql"))
            .execute(&mut **transaction)
            .await
            .map_err(|error| ProjectError::database("failed to scope bulk mirror pairs", error))?;
        return Ok(());
    }
    for statement in [
        "ANALYZE project_scope_names",
        "ANALYZE project_scope_resources",
    ] {
        sqlx::query(statement)
            .execute(&mut **transaction)
            .await
            .map_err(|error| ProjectError::database("failed to analyze mirror frontier", error))?;
    }
    sqlx::query(include_str!("mirror_reference/mirror_include.sql"))
        .bind(chain_id)
        .bind(target_block)
        .execute(&mut **transaction)
        .await
        .map_err(|error| ProjectError::database("failed to scope mirror resolver pairs", error))?;
    Ok(())
}

async fn stage_bulk(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    target_block: i64,
) -> Result<()> {
    // Reconstruct from original changed events, not the consumed frontier. This
    // also preserves changed-node dependencies when switching after a small pass.
    for statement in include_str!("mirror_reference/mirror_bulk.sql")
        .split(';')
        .filter(|s| !s.trim().is_empty())
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
            .map_err(|error| ProjectError::database("failed to stage bulk mirror pairs", error))?;
    }
    Ok(())
}

pub(super) async fn finish(
    transaction: &mut Transaction<'_, Postgres>,
    strategy: Strategy,
) -> Result<()> {
    if strategy.bulk {
        sqlx::query("DROP TABLE project_mirror_pairs")
            .execute(&mut **transaction)
            .await
            .map_err(|error| ProjectError::database("failed to drop bulk mirror pairs", error))?;
    }
    Ok(())
}

#[cfg(test)]
#[path = "mirror_reference_tests.rs"]
mod tests;
