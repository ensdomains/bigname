mod account_permissions;
mod address_names;
mod address_records;
mod children;
mod linked_records;
mod name_authority;
mod name_current;
mod name_topology;
mod permission_resources;
mod permissions;
mod primary_names;
#[cfg(test)]
mod rebuild_tests;
mod record_inventory;
mod resolver;
pub(crate) use resolver::LINK_DIGEST_SQL;
pub(crate) use resolver::SUMMARY_VERSION as RESOLVER_SUMMARY_VERSION;

use sqlx::{Postgres, Transaction};

use crate::Result;

pub(crate) async fn build_all(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    target: &crate::Marker,
    full_rebuild: bool,
) -> Result<()> {
    let started = std::time::Instant::now();
    name_authority::build(transaction, chain_id, target).await?;
    tracing::debug!(
        builder = "name_authority",
        elapsed_ms = started.elapsed().as_millis() as u64,
        "Project builder completed"
    );
    let started = std::time::Instant::now();
    account_permissions::build(transaction, chain_id, target).await?;
    tracing::debug!(
        builder = "account_permissions",
        elapsed_ms = started.elapsed().as_millis() as u64,
        "Project builder completed"
    );
    let started = std::time::Instant::now();
    permissions::build(transaction, chain_id, target, full_rebuild).await?;
    tracing::debug!(
        builder = "permissions",
        elapsed_ms = started.elapsed().as_millis() as u64,
        "Project builder completed"
    );
    let started = std::time::Instant::now();
    name_current::build(transaction, chain_id, target).await?;
    tracing::debug!(
        builder = "name_current",
        elapsed_ms = started.elapsed().as_millis() as u64,
        "Project builder completed"
    );
    let started = std::time::Instant::now();
    permission_resources::build_registry_binding(transaction).await?;
    tracing::debug!(
        builder = "permission_resources",
        elapsed_ms = started.elapsed().as_millis() as u64,
        "Project builder completed"
    );
    let started = std::time::Instant::now();
    resolver::build(transaction, chain_id, target, full_rebuild).await?;
    tracing::debug!(
        builder = "resolver",
        elapsed_ms = started.elapsed().as_millis() as u64,
        "Project builder completed"
    );
    let started = std::time::Instant::now();
    linked_records::build(transaction).await?;
    tracing::debug!(
        builder = "linked_records",
        elapsed_ms = started.elapsed().as_millis() as u64,
        "Project builder completed"
    );
    let started = std::time::Instant::now();
    record_inventory::build(transaction, chain_id, target).await?;
    tracing::debug!(
        builder = "record_inventory",
        elapsed_ms = started.elapsed().as_millis() as u64,
        "Project builder completed"
    );
    let started = std::time::Instant::now();
    name_topology::build(transaction, chain_id, target).await?;
    tracing::debug!(
        builder = "name_topology",
        elapsed_ms = started.elapsed().as_millis() as u64,
        "Project builder completed"
    );
    let started = std::time::Instant::now();
    children::build(transaction, chain_id, target).await?;
    tracing::debug!(
        builder = "children",
        elapsed_ms = started.elapsed().as_millis() as u64,
        "Project builder completed"
    );
    let started = std::time::Instant::now();
    address_names::build(transaction, chain_id, target).await?;
    tracing::debug!(
        builder = "address_names",
        elapsed_ms = started.elapsed().as_millis() as u64,
        "Project builder completed"
    );
    let started = std::time::Instant::now();
    address_records::build(transaction, chain_id, target).await?;
    tracing::debug!(
        builder = "address_records",
        elapsed_ms = started.elapsed().as_millis() as u64,
        "Project builder completed"
    );
    let started = std::time::Instant::now();
    primary_names::build(transaction, chain_id, target).await?;
    tracing::debug!(
        builder = "primary_names",
        elapsed_ms = started.elapsed().as_millis() as u64,
        "Project builder completed"
    );
    Ok(())
}
