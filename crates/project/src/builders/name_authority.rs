use crate::{Marker, ProjectError, Result};
use sqlx::{Postgres, Transaction};
mod stage;
pub(super) async fn build(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    target: &Marker,
) -> Result<()> {
    stage::ownerless_registry(transaction).await?;
    sqlx::query(include_str!("name_authority/build.sql"))
        .bind(chain_id)
        .bind(target.number)
        .bind(&target.hash)
        .execute(&mut **transaction)
        .await
        .map_err(|error| ProjectError::database("failed to select name authority", error))?;
    stage::build(transaction).await
}
