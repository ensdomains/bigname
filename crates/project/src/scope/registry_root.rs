use sqlx::{Postgres, Transaction};

use crate::{ProjectError, Result};

/// A changed ENSv2 root permission re-scopes the root resource and every registration of that
/// registry: a registration's `locked_roles` reads the admin roles held on its registry root, so
/// the root's own change must rebuild each registration's summary even though none of their
/// events lie in the window. Resource identities carry the registry instance and the upstream
/// resource word (zero for the root).
/// (upstream: .refs/ens_v2/contracts/src/access-control/EnhancedAccessControl.sol:L418-L424 @ ens_v2@a971bd64)
/// (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L560-L572 @ ens_v2@a971bd64)
pub(super) async fn include_registry_registrations(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    target_block: i64,
) -> Result<()> {
    sqlx::query(
        r#"/* project:scope.registry_root */
        WITH changed_roots AS (
            SELECT DISTINCT event.after_state ->> 'registry_contract_instance_id' AS registry
            FROM project_changed_events event
            WHERE event.event_kind = 'RootPermissionChanged'
              AND event.after_state ->> 'registry_contract_instance_id' IS NOT NULL
        )
        INSERT INTO project_scope_permission_effect_resources
        SELECT DISTINCT resource.resource_id
        FROM changed_roots root
        JOIN resources resource
          ON resource.provenance ->> 'registry_contract_instance_id' = root.registry
        JOIN chain_lineage lineage
          ON lineage.chain_id = resource.chain_id
         AND lineage.block_hash = resource.block_hash
         AND lineage.block_number = resource.block_number
        WHERE resource.chain_id = $1
          AND resource.block_number <= $2
          AND resource.canonicality_state IN ('canonical', 'safe', 'finalized')
          AND lineage.canonicality_state IN ('canonical', 'safe', 'finalized')
        ON CONFLICT DO NOTHING
        "#,
    )
    .bind(chain_id)
    .bind(target_block)
    .execute(&mut **transaction)
    .await
    .map_err(|error| {
        ProjectError::database("failed to scope registry root permission dependents", error)
    })?;
    Ok(())
}
