use crate::{Marker, ProjectError, Result};
use sqlx::{Postgres, Transaction};

mod query;

pub(super) async fn build(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    target: &Marker,
) -> Result<()> {
    sqlx::query(query::BUILD_NAME_CURRENT)
        .bind(chain_id)
        .bind(target.number)
        .bind(&target.hash)
        .execute(&mut **transaction)
        .await
        .map_err(|error| ProjectError::database("failed to build name_current", error))?;
    Ok(())
}
