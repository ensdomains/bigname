mod account_permissions;
mod address_names;
mod address_records;
pub(crate) mod child_registrations;
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
    steps: &crate::steps::Steps<'_>,
) -> Result<()> {
    steps.enter("name_authority");
    let started = std::time::Instant::now();
    name_authority::build(transaction, chain_id, target).await?;
    tracing::debug!(
        builder = "name_authority",
        elapsed_ms = started.elapsed().as_millis() as u64,
        "Project builder completed"
    );
    steps.enter("account_permissions");
    let started = std::time::Instant::now();
    account_permissions::build(transaction, chain_id, target).await?;
    tracing::debug!(
        builder = "account_permissions",
        elapsed_ms = started.elapsed().as_millis() as u64,
        "Project builder completed"
    );
    steps.enter("permissions");
    let started = std::time::Instant::now();
    permissions::build(transaction, chain_id, target, full_rebuild).await?;
    tracing::debug!(
        builder = "permissions",
        elapsed_ms = started.elapsed().as_millis() as u64,
        "Project builder completed"
    );
    steps.enter("name_current");
    let started = std::time::Instant::now();
    name_current::build(transaction, chain_id, target).await?;
    tracing::debug!(
        builder = "name_current",
        elapsed_ms = started.elapsed().as_millis() as u64,
        "Project builder completed"
    );
    steps.enter("permission_resources");
    let started = std::time::Instant::now();
    permission_resources::build_registry_binding(transaction).await?;
    tracing::debug!(
        builder = "permission_resources",
        elapsed_ms = started.elapsed().as_millis() as u64,
        "Project builder completed"
    );
    steps.enter("resolver");
    let started = std::time::Instant::now();
    resolver::build(transaction, chain_id, target, full_rebuild).await?;
    tracing::debug!(
        builder = "resolver",
        elapsed_ms = started.elapsed().as_millis() as u64,
        "Project builder completed"
    );
    steps.enter("linked_records");
    let started = std::time::Instant::now();
    linked_records::build(transaction).await?;
    tracing::debug!(
        builder = "linked_records",
        elapsed_ms = started.elapsed().as_millis() as u64,
        "Project builder completed"
    );
    steps.enter("record_inventory");
    let started = std::time::Instant::now();
    record_inventory::build(transaction, chain_id, target).await?;
    tracing::debug!(
        builder = "record_inventory",
        elapsed_ms = started.elapsed().as_millis() as u64,
        "Project builder completed"
    );
    steps.enter("name_topology");
    let started = std::time::Instant::now();
    name_topology::build(transaction, chain_id, target).await?;
    tracing::debug!(
        builder = "name_topology",
        elapsed_ms = started.elapsed().as_millis() as u64,
        "Project builder completed"
    );
    steps.enter("children");
    let started = std::time::Instant::now();
    children::build(transaction, chain_id, target).await?;
    tracing::debug!(
        builder = "children",
        elapsed_ms = started.elapsed().as_millis() as u64,
        "Project builder completed"
    );
    steps.enter("address_names");
    let started = std::time::Instant::now();
    address_names::build(transaction, chain_id, target).await?;
    tracing::debug!(
        builder = "address_names",
        elapsed_ms = started.elapsed().as_millis() as u64,
        "Project builder completed"
    );
    steps.enter("address_records");
    let started = std::time::Instant::now();
    address_records::build(transaction, chain_id, target).await?;
    tracing::debug!(
        builder = "address_records",
        elapsed_ms = started.elapsed().as_millis() as u64,
        "Project builder completed"
    );
    steps.enter("primary_names");
    let started = std::time::Instant::now();
    primary_names::build(transaction, chain_id, target).await?;
    tracing::debug!(
        builder = "primary_names",
        elapsed_ms = started.elapsed().as_millis() as u64,
        "Project builder completed"
    );
    steps.enter("child_registrations");
    let started = std::time::Instant::now();
    child_registrations::build(transaction, target, full_rebuild).await?;
    tracing::debug!(
        builder = "child_registrations",
        elapsed_ms = started.elapsed().as_millis() as u64,
        "Project builder completed"
    );
    Ok(())
}
