use crate::{Marker, ProjectError, Result};
use sqlx::{Postgres, Transaction};
#[cfg(test)]
mod plan_tests;
mod stage;
pub(super) async fn build(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    target: &Marker,
) -> Result<()> {
    stage::prepare(transaction).await?;
    // The pull request 947 registry-record fold runs inside this statement; a profiled benchmark
    // records its plan so the fold's cost can be read from the plan's `registry_records` node.
    #[cfg(test)]
    if crate::profile::execute_bound(
        transaction,
        include_str!("name_authority/build.sql"),
        crate::profile::Stage::NameAuthority,
        &[
            crate::profile::Parameter::Text(chain_id),
            crate::profile::Parameter::I64(target.number),
            crate::profile::Parameter::Text(&target.hash),
        ],
    )
    .await?
    {
        return stage::build(transaction).await;
    }
    sqlx::query(include_str!("name_authority/build.sql"))
        .bind(chain_id)
        .bind(target.number)
        .bind(&target.hash)
        .execute(&mut **transaction)
        .await
        .map_err(|error| ProjectError::database("failed to select name authority", error))?;
    stage::build(transaction).await
}
