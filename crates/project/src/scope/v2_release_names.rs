use sqlx::{Postgres, Transaction};

use crate::{ProjectError, Result};

const V2_RELEASE_NAMES: &str = include_str!("v2_release_names.sql");
const V2_RETRACTED_RELEASE_NAMES: &str = include_str!("v2_release_names_retracted.sql");

/// A changed ENSv2 release without a name brings in every name bound to its resource, closed
/// bindings included, so the batch rebuilds the names a rebuild would give the release to.
pub(super) async fn include_names_bound_to_released_resources(
    transaction: &mut Transaction<'_, Postgres>,
    target_block: i64,
) -> Result<()> {
    sqlx::query(V2_RELEASE_NAMES)
        .bind(target_block)
        .execute(&mut **transaction)
        .await
        .map_err(|error| {
            ProjectError::database(
                "failed to scope names bound to resources with an ENSv2 release without a name",
                error,
            )
        })?;
    Ok(())
}

/// A redo brings in every name bound to the resource of an ENSv2 release without a name that the
/// reorg retracted, closed bindings included, so the name is rebuilt without that release.
pub(super) async fn include_names_bound_to_retracted_releases(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    from_block: i64,
    to_block: i64,
    target_block: i64,
) -> Result<()> {
    sqlx::query(V2_RETRACTED_RELEASE_NAMES)
        .bind(chain_id)
        .bind(from_block)
        .bind(to_block)
        .bind(target_block)
        .execute(&mut **transaction)
        .await
        .map_err(|error| {
            ProjectError::database(
                "failed to scope names bound to resources with a retracted ENSv2 release without a name",
                error,
            )
        })?;
    Ok(())
}

#[cfg(test)]
#[path = "v2_release_names_tests.rs"]
mod tests;
