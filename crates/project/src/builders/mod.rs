mod address_names;
mod children;
mod name_current;
mod name_topology;
mod permissions;
mod primary_names;
mod record_inventory;
mod resolver;

use sqlx::{Postgres, Transaction};

use crate::Result;

pub(crate) async fn build_all(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    target: &crate::Marker,
    full_rebuild: bool,
) -> Result<()> {
    permissions::build(transaction, chain_id, target).await?;
    name_current::build(transaction, chain_id, target).await?;
    resolver::build(transaction, chain_id, target, full_rebuild).await?;
    record_inventory::build(transaction, chain_id, target).await?;
    name_topology::build(transaction, chain_id, target).await?;
    children::build(transaction, chain_id, target).await?;
    address_names::build(transaction, chain_id, target).await?;
    primary_names::build(transaction, chain_id, target).await?;
    Ok(())
}
