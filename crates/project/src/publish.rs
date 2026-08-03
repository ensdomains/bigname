use sqlx::{Postgres, Transaction};

use crate::{ProjectError, Result};

pub(crate) async fn swap(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    full_rebuild: bool,
) -> Result<u64> {
    let deletes = if full_rebuild {
        vec![
            "DELETE FROM address_names_current row USING name_surfaces surface WHERE row.logical_name_id = surface.logical_name_id AND surface.chain_id = $1",
            "DELETE FROM children_current row USING name_surfaces surface WHERE row.parent_logical_name_id = surface.logical_name_id AND surface.chain_id = $1",
            "DELETE FROM name_current row USING name_surfaces surface WHERE row.logical_name_id = surface.logical_name_id AND surface.chain_id = $1",
            "DELETE FROM permissions_current row USING resources resource WHERE row.resource_id = resource.resource_id AND resource.chain_id = $1",
            "DELETE FROM permissions_current_resource_summary row USING resources resource WHERE row.resource_id = resource.resource_id AND resource.chain_id = $1",
            "DELETE FROM record_inventory_current row USING resources resource WHERE row.resource_id = resource.resource_id AND resource.chain_id = $1",
            "DELETE FROM resolver_current WHERE chain_id = $1",
            "DELETE FROM primary_names_current WHERE claim_provenance ->> 'chain_id' = $1",
        ]
    } else {
        vec![
            "DELETE FROM address_names_current row WHERE EXISTS (SELECT 1 FROM project_scope_names scope WHERE scope.logical_name_id = row.logical_name_id) OR EXISTS (SELECT 1 FROM project_scope_resources scope WHERE scope.resource_id = row.resource_id)",
            "DELETE FROM children_current row WHERE EXISTS (SELECT 1 FROM project_scope_children scope WHERE scope.logical_name_id IN (row.parent_logical_name_id, row.child_logical_name_id))",
            "DELETE FROM name_current row USING project_scope_names scope WHERE row.logical_name_id = scope.logical_name_id",
            "DELETE FROM permissions_current row USING project_scope_resources scope WHERE row.resource_id = scope.resource_id",
            "DELETE FROM permissions_current_resource_summary row USING project_scope_resources scope WHERE row.resource_id = scope.resource_id",
            "DELETE FROM record_inventory_current row USING project_scope_resources scope WHERE row.resource_id = scope.resource_id",
            "DELETE FROM resolver_current row USING project_scope_resolvers scope WHERE row.chain_id = $1 AND lower(row.resolver_address) = lower(scope.resolver_address)",
            "DELETE FROM primary_names_current row USING project_scope_primary scope WHERE row.address = scope.address AND row.coin_type = scope.coin_type AND row.namespace = scope.namespace",
        ]
    };
    for statement in deletes {
        let query = sqlx::query(statement);
        let result = if statement.contains("$1") {
            query.bind(chain_id).execute(&mut **transaction).await
        } else {
            query.execute(&mut **transaction).await
        };
        result.map_err(|error| {
            ProjectError::database("failed to clear projection swap scope", error)
        })?;
    }

    let mut inserted = 0u64;
    let scoped = [
        (
            "name_current",
            "EXISTS (SELECT 1 FROM project_scope_names scope WHERE scope.logical_name_id = project_stage_name_current.logical_name_id)",
        ),
        (
            "children_current",
            "EXISTS (SELECT 1 FROM project_scope_children scope WHERE scope.logical_name_id IN (project_stage_children_current.parent_logical_name_id, project_stage_children_current.child_logical_name_id))",
        ),
        (
            "permissions_current",
            "EXISTS (SELECT 1 FROM project_scope_resources scope WHERE scope.resource_id = project_stage_permissions_current.resource_id)",
        ),
        (
            "permissions_current_resource_summary",
            "EXISTS (SELECT 1 FROM project_scope_resources scope WHERE scope.resource_id = project_stage_permissions_current_resource_summary.resource_id)",
        ),
        (
            "record_inventory_current",
            "EXISTS (SELECT 1 FROM project_scope_resources scope WHERE scope.resource_id = project_stage_record_inventory_current.resource_id)",
        ),
        (
            "resolver_current",
            "EXISTS (SELECT 1 FROM project_scope_resolvers scope WHERE lower(scope.resolver_address) = lower(project_stage_resolver_current.resolver_address))",
        ),
        (
            "address_names_current",
            "EXISTS (SELECT 1 FROM project_scope_names scope WHERE scope.logical_name_id = project_stage_address_names_current.logical_name_id) OR EXISTS (SELECT 1 FROM project_scope_resources scope WHERE scope.resource_id = project_stage_address_names_current.resource_id)",
        ),
        (
            "primary_names_current",
            "EXISTS (SELECT 1 FROM project_scope_primary scope WHERE scope.address = project_stage_primary_names_current.address AND scope.coin_type = project_stage_primary_names_current.coin_type AND scope.namespace = project_stage_primary_names_current.namespace)",
        ),
    ];
    for (table, scoped_predicate) in scoped {
        let predicate = if full_rebuild {
            "TRUE"
        } else {
            scoped_predicate
        };
        let statement =
            format!("INSERT INTO {table} SELECT * FROM project_stage_{table} WHERE {predicate}");
        inserted = inserted.saturating_add(
            sqlx::query(&statement)
                .execute(&mut **transaction)
                .await
                .map_err(|error| {
                    ProjectError::database(format!("failed to publish {table}"), error)
                })?
                .rows_affected(),
        );
    }
    Ok(inserted)
}
