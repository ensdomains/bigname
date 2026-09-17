use crate::{Marker, ProjectError, Result};
use sqlx::{Postgres, Transaction};

pub(in crate::builders) mod query;

pub(super) async fn build(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    target: &Marker,
) -> Result<()> {
    for statement in query::STAGE_V2_LIFECYCLE_EVENTS {
        sqlx::query(statement)
            .execute(&mut **transaction)
            .await
            .map_err(|error| {
                ProjectError::database("failed to stage ENSv2 lifecycle events", error)
            })?;
    }
    sqlx::query(query::BUILD_NAME_CURRENT)
        .bind(chain_id)
        .bind(target.number)
        .bind(&target.hash)
        .execute(&mut **transaction)
        .await
        .map_err(|error| ProjectError::database("failed to build name_current", error))?;
    Ok(())
}
