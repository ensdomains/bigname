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
    include_changed_parent_migration_children(transaction, chain_id, target_block).await?;
    Ok(())
}

async fn include_changed_parent_migration_children(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    target_block: i64,
) -> Result<()> {
    sqlx::query(
        r#"
        WITH changed_parents AS (
            SELECT DISTINCT event.logical_name_id, event.namespace
            FROM project_changed_events event
            WHERE event.source_family = 'ens_v2_migration_l1'
              AND event.event_kind = 'MigrationApplied'
              AND event.logical_name_id IS NOT NULL
        ), candidates AS (
            SELECT parent.logical_name_id FROM changed_parents parent
            UNION
            SELECT child.child_logical_name_id
            FROM changed_parents parent
            JOIN children_current child
              ON child.parent_logical_name_id = parent.logical_name_id
             AND child.provenance ->> 'chain_id' = $1
            UNION
            SELECT event.namespace || ':' || lower(event.after_state ->> 'child_node')
            FROM changed_parents parent
            JOIN normalized_events event
              ON event.chain_id = $1
             AND event.namespace = parent.namespace
             AND event.namespace || ':' || lower(event.after_state ->> 'node') =
                 parent.logical_name_id
            JOIN chain_lineage lineage
              ON lineage.chain_id = event.chain_id
             AND lineage.block_hash = event.block_hash
             AND lineage.block_number = event.block_number
            WHERE event.source_family = 'ens_v1_registry_l1'
              AND event.event_kind = 'SubregistryChanged'
              AND event.consumer_visibility = 'activated'
              AND event.block_number <= $2
              AND event.after_state ->> 'child_node' IS NOT NULL
              AND btrim(event.after_state ->> 'child_node') <> ''
              AND event.canonicality_state IN ('canonical', 'safe', 'finalized')
              AND lineage.canonicality_state IN ('canonical', 'safe', 'finalized')
        )
        INSERT INTO project_scope_children
        SELECT logical_name_id FROM candidates
        ON CONFLICT DO NOTHING
        "#,
    )
    .bind(chain_id)
    .bind(target_block)
    .execute(&mut **transaction)
    .await
    .map_err(|error| {
        ProjectError::database("failed to scope changed parent migration children", error)
    })?;
    Ok(())
}

pub(super) async fn include_topology_dependents(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    target_block: i64,
) -> Result<()> {
    sqlx::query(
        "INSERT INTO project_scope_names
         SELECT child.logical_name_id
         FROM project_scope_children child
         WHERE EXISTS (
             SELECT 1
             FROM normalized_events registration
             JOIN migration_discovery_associations association
               ON association.chain_id = registration.chain_id
              AND association.registry_contract_instance_id::text =
                  registration.after_state ->> 'registry_contract_instance_id'
              AND association.correlation_kind = 'migration_registry_creation'
             WHERE registration.chain_id = $1
               AND registration.logical_name_id = child.logical_name_id
               AND registration.block_number <= $2
               AND registration.event_kind = 'RegistrationGranted'
               AND registration.consumer_visibility = 'activated'
               AND registration.canonicality_state IN (
                   'canonical', 'safe', 'finalized'
               )
         )
         OR EXISTS (
             SELECT 1
             FROM name_current current
             WHERE current.logical_name_id = child.logical_name_id
               AND current.provenance ->> 'chain_id' = $1
               AND current.provenance #>> '{authority_selection,proof_kind}' =
                   'positive_v2_child_registration'
         )
         ON CONFLICT DO NOTHING",
    )
    .bind(chain_id)
    .bind(target_block)
    .execute(&mut **transaction)
    .await
    .map_err(|error| {
        ProjectError::database("failed to scope authority topology dependents", error)
    })?;
    loop {
        let added = sqlx::query(
            r#"
            INSERT INTO project_scope_expiry_names
            SELECT DISTINCT registration.logical_name_id
            FROM project_scope_expiry_names parent
            JOIN LATERAL (
                SELECT topology.after_state ->> 'subregistry' AS address
                FROM normalized_events topology
                JOIN chain_lineage topology_lineage
                  ON topology_lineage.chain_id = topology.chain_id
                 AND topology_lineage.block_number = topology.block_number
                 AND topology_lineage.block_hash = topology.block_hash
                WHERE topology.chain_id = $1
                  AND topology.logical_name_id = parent.logical_name_id
                  AND topology.block_number <= $2
                  AND topology.event_kind = 'SubregistryChanged'
                  AND topology.source_family IN (
                      'ens_v2_root_l1', 'ens_v2_registry_l1'
                  )
                  AND topology.consumer_visibility = 'activated'
                  AND topology.canonicality_state IN (
                      'canonical', 'safe', 'finalized'
                  )
                  AND topology_lineage.canonicality_state IN (
                      'canonical', 'safe', 'finalized'
                  )
                ORDER BY topology.block_number DESC NULLS LAST,
                         topology.transaction_index DESC NULLS LAST,
                         topology.log_index DESC NULLS LAST,
                         topology.normalized_event_id DESC
                LIMIT 1
            ) pointer ON pointer.address IS NOT NULL
                     AND btrim(pointer.address) <> ''
            JOIN contract_instance_addresses address
              ON address.chain_id = $1
             AND lower(address.address) = lower(pointer.address)
             AND (address.active_from_block_number IS NULL
                  OR address.active_from_block_number <= $2)
             AND (address.active_to_block_number IS NULL
                  OR address.active_to_block_number > $2)
             AND address.deactivated_at IS NULL
            JOIN LATERAL (
                SELECT DISTINCT head.logical_name_id
                FROM (
                    SELECT DISTINCT ON (
                               candidate.logical_name_id,
                               candidate.lifecycle_key
                           )
                           candidate.logical_name_id, candidate.event_kind
                    FROM (
                        SELECT event.*,
                               COALESCE(
                                   event.resource_id::text,
                                   (
                                       SELECT linked.resource_id::text
                                       FROM normalized_events linked
                                       JOIN chain_lineage linked_lineage
                                         ON linked_lineage.chain_id = linked.chain_id
                                        AND linked_lineage.block_number = linked.block_number
                                        AND linked_lineage.block_hash = linked.block_hash
                                       WHERE linked.chain_id = event.chain_id
                                         AND linked.logical_name_id = event.logical_name_id
                                         AND linked.block_number <= $2
                                         AND linked.resource_id IS NOT NULL
                                         AND linked.event_kind IN (
                                             'RegistrationGranted',
                                             'RegistrationReserved'
                                         )
                                         AND linked.source_family IN (
                                             'ens_v2_root_l1',
                                             'ens_v2_registry_l1'
                                         )
                                         AND linked.consumer_visibility = 'activated'
                                         AND linked.canonicality_state IN (
                                             'canonical', 'safe', 'finalized'
                                         )
                                         AND linked_lineage.canonicality_state IN (
                                             'canonical', 'safe', 'finalized'
                                         )
                                         AND COALESCE(
                                             linked.after_state ->> 'registry_contract_instance_id',
                                             linked.raw_fact_ref ->> 'emitting_address',
                                             linked.after_state ->> 'registry'
                                         ) = COALESCE(
                                             event.after_state ->> 'registry_contract_instance_id',
                                             event.raw_fact_ref ->> 'emitting_address',
                                             event.after_state ->> 'registry'
                                         )
                                         AND linked.after_state ->> 'token_id' =
                                             event.after_state ->> 'token_id'
                                       ORDER BY linked.block_number DESC NULLS LAST,
                                                linked.normalized_event_id DESC
                                       LIMIT 1
                                   ),
                                   NULLIF(CONCAT(
                                       event.after_state ->> 'registry_contract_instance_id',
                                       ':', event.after_state ->> 'token_id'
                                   ), ':')
                               ) AS lifecycle_key
                        FROM normalized_events event
                        JOIN chain_lineage lineage
                          ON lineage.chain_id = event.chain_id
                         AND lineage.block_number = event.block_number
                         AND lineage.block_hash = event.block_hash
                        WHERE event.chain_id = $1
                          AND event.after_state ->> 'registry_contract_instance_id' =
                              address.contract_instance_id::text
                          AND event.block_number <= $2
                          AND event.event_kind IN (
                              'RegistrationGranted', 'RegistrationReserved',
                              'RegistrationRenewed', 'RegistrationReleased'
                          )
                          AND event.source_family IN (
                              'ens_v2_root_l1', 'ens_v2_registry_l1'
                          )
                          AND event.consumer_visibility = 'activated'
                          AND event.canonicality_state IN (
                              'canonical', 'safe', 'finalized'
                          )
                          AND lineage.canonicality_state IN (
                              'canonical', 'safe', 'finalized'
                          )
                          AND event.logical_name_id IS NOT NULL
                    ) candidate
                    WHERE candidate.lifecycle_key IS NOT NULL
                    ORDER BY candidate.logical_name_id,
                             candidate.lifecycle_key,
                             candidate.block_number DESC NULLS LAST,
                             candidate.transaction_index DESC NULLS LAST,
                             candidate.log_index DESC NULLS LAST,
                             candidate.normalized_event_id DESC
                ) head
                WHERE head.event_kind IN (
                    'RegistrationGranted', 'RegistrationReserved',
                    'RegistrationRenewed'
                )
            ) registration ON TRUE
            ON CONFLICT DO NOTHING
            "#,
        )
        .bind(chain_id)
        .bind(target_block)
        .execute(&mut **transaction)
        .await
        .map_err(|error| ProjectError::database("failed to close expiry topology scope", error))?
        .rows_affected();
        if added == 0 {
            break;
        }
    }
    for table in ["project_scope_names", "project_scope_children"] {
        let statement = format!(
            "INSERT INTO {table}
             SELECT logical_name_id FROM project_scope_expiry_names
             ON CONFLICT DO NOTHING"
        );
        sqlx::query(&statement)
            .execute(&mut **transaction)
            .await
            .map_err(|error| {
                ProjectError::database("failed to publish expiry topology scope", error)
            })?;
    }
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
