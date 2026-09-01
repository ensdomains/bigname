use sqlx::{Postgres, Transaction};

use crate::{ProjectError, Result};

pub(super) async fn include(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
) -> Result<()> {
    sqlx::query(
        "INSERT INTO project_scope_resources
         SELECT inventory.resource_id
         FROM record_inventory_current inventory
         JOIN project_scope_resolver_dependents scope
           ON lower(scope.resolver_address) = lower(
               inventory.provenance ->> 'resolver_address'
           )
         WHERE inventory.provenance ->> 'chain_id' = $1
         UNION
         SELECT name.resource_id
         FROM name_current name
         JOIN project_scope_resolver_dependents scope
           ON lower(scope.resolver_address) = lower(
               name.declared_summary #>> '{resolver,address}'
           )
         WHERE name.declared_summary #>> '{resolver,chain_id}' = $1
           AND name.resource_id IS NOT NULL
         ON CONFLICT DO NOTHING",
    )
    .bind(chain_id)
    .execute(&mut **transaction)
    .await
    .map_err(|error| ProjectError::database("failed to scope resolver resources", error))?;

    sqlx::query(
        "INSERT INTO project_scope_names
         SELECT inventory.provenance ->> 'logical_name_id'
         FROM record_inventory_current inventory
         JOIN project_scope_resolver_dependents scope
           ON lower(scope.resolver_address) = lower(
               inventory.provenance ->> 'resolver_address'
           )
         WHERE inventory.provenance ->> 'chain_id' = $1
           AND inventory.provenance ->> 'logical_name_id' IS NOT NULL
         UNION
         SELECT name.logical_name_id
         FROM name_current name
         JOIN project_scope_resolver_dependents scope
           ON lower(scope.resolver_address) = lower(
               name.declared_summary #>> '{resolver,address}'
           )
         WHERE name.declared_summary #>> '{resolver,chain_id}' = $1
         ON CONFLICT DO NOTHING",
    )
    .bind(chain_id)
    .execute(&mut **transaction)
    .await
    .map_err(|error| ProjectError::database("failed to scope resolver names", error))?;
    Ok(())
}
