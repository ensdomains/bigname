use sqlx::{Postgres, Transaction};

use crate::{ProjectError, Result};

pub(super) async fn include_current_edges(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
) -> Result<u64> {
    sqlx::query(
        "INSERT INTO project_scope_children
         SELECT child.parent_logical_name_id
         FROM children_current child
         JOIN (
             SELECT logical_name_id FROM project_scope_names
             UNION
             SELECT logical_name_id FROM project_scope_children
         ) scope
           ON scope.logical_name_id IN (
               child.parent_logical_name_id, child.child_logical_name_id
           )
         WHERE child.provenance ->> 'chain_id' = $1
         UNION
         SELECT child.child_logical_name_id
         FROM children_current child
         JOIN (
             SELECT logical_name_id FROM project_scope_names
             UNION
             SELECT logical_name_id FROM project_scope_children
         ) scope
           ON scope.logical_name_id IN (
               child.parent_logical_name_id, child.child_logical_name_id
           )
         WHERE child.provenance ->> 'chain_id' = $1
         UNION
         SELECT child.parent_logical_name_id
         FROM children_current child
         JOIN project_changed_events changed
           ON changed.namespace = child.namespace
          AND lower(changed.after_state ->> 'labelhash') = lower(child.labelhash)
         WHERE child.provenance ->> 'chain_id' = $1
         UNION
         SELECT child.child_logical_name_id
         FROM children_current child
         JOIN project_changed_events changed
           ON changed.namespace = child.namespace
          AND lower(changed.after_state ->> 'labelhash') = lower(child.labelhash)
         WHERE child.provenance ->> 'chain_id' = $1
         ON CONFLICT DO NOTHING",
    )
    .bind(chain_id)
    .execute(&mut **transaction)
    .await
    .map(|result| result.rows_affected())
    .map_err(|error| ProjectError::database("failed to close current child topology scope", error))
}

pub(super) async fn include_event_edges(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    target_block: i64,
) -> Result<u64> {
    let v1 = sqlx::query(
        "WITH edges AS (
             SELECT event.namespace || ':' || lower(endpoint.parent_node) AS parent_id,
                    event.namespace || ':' || lower(endpoint.child_node) AS child_id
             FROM normalized_events event
             JOIN chain_lineage lineage
               ON lineage.chain_id = event.chain_id
              AND lineage.block_number = event.block_number
              AND lineage.block_hash = event.block_hash
             CROSS JOIN LATERAL (
                 VALUES
                     (event.after_state ->> 'node',
                      event.after_state ->> 'child_node'),
                     (event.before_state ->> 'node',
                      event.before_state ->> 'child_node')
             ) endpoint(parent_node, child_node)
             WHERE event.chain_id = $1
               AND event.block_number <= $2
               AND event.event_kind = 'SubregistryChanged'
               AND event.source_family IN (
                   'ens_v1_registry_l1', 'basenames_base_registry'
               )
               AND event.consumer_visibility = 'activated'
               AND event.canonicality_state IN ('canonical', 'safe', 'finalized')
               AND lineage.canonicality_state IN ('canonical', 'safe', 'finalized')
               AND endpoint.parent_node IS NOT NULL
               AND btrim(endpoint.parent_node) <> ''
               AND endpoint.child_node IS NOT NULL
               AND btrim(endpoint.child_node) <> ''
         ), matching AS (
             SELECT edge.parent_id, edge.child_id
             FROM edges edge
             WHERE EXISTS (
                 SELECT 1
                 FROM (
                     SELECT logical_name_id FROM project_scope_names
                     UNION
                     SELECT logical_name_id FROM project_scope_children
                 ) scope
                 WHERE scope.logical_name_id IN (edge.parent_id, edge.child_id)
             )
         )
         INSERT INTO project_scope_children
         SELECT parent_id FROM matching
         UNION
         SELECT child_id FROM matching
         ON CONFLICT DO NOTHING",
    )
    .bind(chain_id)
    .bind(target_block)
    .execute(&mut **transaction)
    .await
    .map_err(|error| ProjectError::database("failed to close v1 event topology scope", error))?
    .rows_affected();

    // The child builder joins a name's latest staged subregistry address to registration history
    // for that contract instance. Treat the parent and every referenced registration as the two
    // endpoints of an ENSv2 topology edge, including the pointer replaced by the changed event.
    let v2 = sqlx::query(
        "WITH edges AS (
             SELECT DISTINCT topology.logical_name_id AS parent_id,
                    registration.logical_name_id AS child_id
             FROM normalized_events topology
             JOIN chain_lineage topology_lineage
               ON topology_lineage.chain_id = topology.chain_id
              AND topology_lineage.block_number = topology.block_number
              AND topology_lineage.block_hash = topology.block_hash
             CROSS JOIN LATERAL (
                 VALUES (topology.after_state ->> 'subregistry'),
                        (topology.before_state ->> 'subregistry')
             ) pointer(address)
             JOIN contract_instance_addresses address
               ON address.chain_id = topology.chain_id
              AND lower(address.address) = lower(pointer.address)
              AND (address.active_from_block_number IS NULL
                   OR address.active_from_block_number <= $2)
              AND (address.active_to_block_number IS NULL
                   OR address.active_to_block_number > $2)
              AND address.deactivated_at IS NULL
             JOIN normalized_events registration
               ON registration.chain_id = $1
              AND registration.after_state ->> 'registry_contract_instance_id' =
                  address.contract_instance_id::text
             JOIN chain_lineage registration_lineage
               ON registration_lineage.chain_id = registration.chain_id
              AND registration_lineage.block_number = registration.block_number
              AND registration_lineage.block_hash = registration.block_hash
             WHERE topology.chain_id = $1
               AND topology.block_number <= $2
               AND topology.event_kind = 'SubregistryChanged'
               AND topology.source_family IN ('ens_v2_root_l1', 'ens_v2_registry_l1')
               AND topology.consumer_visibility = 'activated'
               AND topology.canonicality_state IN ('canonical', 'safe', 'finalized')
               AND topology_lineage.canonicality_state IN (
                   'canonical', 'safe', 'finalized'
               )
               AND topology.logical_name_id IS NOT NULL
               AND pointer.address IS NOT NULL
               AND btrim(pointer.address) <> ''
               AND registration.block_number <= $2
               AND registration.event_kind IN (
                   'RegistrationGranted', 'RegistrationReserved',
                   'RegistrationRenewed', 'RegistrationReleased'
               )
               AND registration.source_family IN ('ens_v2_root_l1', 'ens_v2_registry_l1')
               AND registration.consumer_visibility = 'activated'
               AND registration.canonicality_state IN ('canonical', 'safe', 'finalized')
               AND registration_lineage.canonicality_state IN (
                   'canonical', 'safe', 'finalized'
               )
               AND registration.logical_name_id IS NOT NULL
         ), matching AS (
             SELECT edge.parent_id, edge.child_id
             FROM edges edge
             WHERE EXISTS (
                 SELECT 1
                 FROM (
                     SELECT logical_name_id FROM project_scope_names
                     UNION
                     SELECT logical_name_id FROM project_scope_children
                 ) scope
                 WHERE scope.logical_name_id IN (edge.parent_id, edge.child_id)
             )
         )
         INSERT INTO project_scope_children
         SELECT parent_id FROM matching
         UNION
         SELECT child_id FROM matching
         ON CONFLICT DO NOTHING",
    )
    .bind(chain_id)
    .bind(target_block)
    .execute(&mut **transaction)
    .await
    .map_err(|error| ProjectError::database("failed to close v2 event topology scope", error))?
    .rows_affected();
    Ok(v1 + v2)
}
