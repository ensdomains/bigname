use sqlx::{Postgres, Transaction};

use crate::{ProjectError, Result};

#[allow(clippy::too_many_arguments)]
pub(super) async fn include_changed_child_proofs(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    from_block: i64,
    to_block: i64,
    target_block: i64,
    retain_retracted: bool,
) -> Result<()> {
    sqlx::query(
        "INSERT INTO project_scope_names
         SELECT DISTINCT registration.logical_name_id
         FROM migration_discovery_associations association
         JOIN chain_lineage association_lineage
           ON association_lineage.chain_id = association.chain_id
          AND association_lineage.block_hash = association.block_hash
          AND association_lineage.block_number = association.block_number
         JOIN normalized_events registration
           ON registration.chain_id = association.chain_id
          AND registration.after_state ->> 'registry_contract_instance_id' =
              association.registry_contract_instance_id::text
         JOIN chain_lineage registration_lineage
           ON registration_lineage.chain_id = registration.chain_id
          AND registration_lineage.block_hash = registration.block_hash
          AND registration_lineage.block_number = registration.block_number
         WHERE association.chain_id = $1
           AND association.block_number BETWEEN $2 AND $3
           AND association.correlation_kind = 'migration_registry_creation'
           AND association.canonicality_state IN ('canonical', 'safe', 'finalized')
           AND association_lineage.canonicality_state IN (
               'canonical', 'safe', 'finalized'
           )
           AND registration.block_number <= $4
           AND registration.event_kind = 'RegistrationGranted'
           AND registration.consumer_visibility = 'activated'
           AND registration.canonicality_state IN ('canonical', 'safe', 'finalized')
           AND registration_lineage.canonicality_state IN (
               'canonical', 'safe', 'finalized'
           )
           AND registration.logical_name_id IS NOT NULL
         UNION
         SELECT current.logical_name_id
         FROM name_current current
         WHERE $5
           AND current.provenance #>> '{authority_selection,proof_kind}' =
               'positive_v2_child_registration'
           AND current.provenance ->> 'chain_id' = $1
         ON CONFLICT DO NOTHING",
    )
    .bind(chain_id)
    .bind(from_block)
    .bind(to_block)
    .bind(target_block)
    .bind(retain_retracted)
    .execute(&mut **transaction)
    .await
    .map_err(|error| ProjectError::database("failed to scope changed child proofs", error))?;
    Ok(())
}

pub(super) async fn include_topology_dependents(
    transaction: &mut Transaction<'_, Postgres>,
) -> Result<()> {
    sqlx::query(
        "INSERT INTO project_scope_names
         SELECT logical_name_id FROM project_scope_children
         ON CONFLICT DO NOTHING",
    )
    .execute(&mut **transaction)
    .await
    .map_err(|error| {
        ProjectError::database("failed to scope authority topology dependents", error)
    })?;
    Ok(())
}

pub(super) async fn include_latest_arm_resources(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    target_block: i64,
) -> Result<()> {
    sqlx::query(
        "INSERT INTO project_scope_resources
         SELECT DISTINCT ON (binding.logical_name_id, binding.authority_arm)
                binding.resource_id
         FROM surface_bindings binding
         JOIN project_scope_names scope USING (logical_name_id)
         JOIN chain_lineage lineage
           ON lineage.chain_id = binding.chain_id
          AND lineage.block_hash = binding.block_hash
          AND lineage.block_number = binding.block_number
         WHERE binding.chain_id = $1
           AND binding.block_number <= $2
           AND binding.canonicality_state IN ('canonical', 'safe', 'finalized')
           AND lineage.canonicality_state IN ('canonical', 'safe', 'finalized')
         ORDER BY binding.logical_name_id, binding.authority_arm,
                  binding.block_number DESC,
                  COALESCE(
                      (binding.provenance ->> 'transaction_index')::bigint, -1
                  ) DESC,
                  COALESCE((binding.provenance ->> 'log_index')::bigint, -1) DESC,
                  binding.surface_binding_id DESC
         ON CONFLICT DO NOTHING",
    )
    .bind(chain_id)
    .bind(target_block)
    .execute(&mut **transaction)
    .await
    .map_err(|error| ProjectError::database("failed to scope authority resources", error))?;
    Ok(())
}
