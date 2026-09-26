use sqlx::{Postgres, Transaction};

use crate::{ProjectError, Result};

const V2_RELEASE_NAMES: &str = include_str!("v2_release_names.sql");

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
