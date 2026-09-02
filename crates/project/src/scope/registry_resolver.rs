use sqlx::{Postgres, Transaction};

use crate::{ProjectError, Result};

#[cfg(test)]
#[path = "plan_tests.rs"]
mod plan_tests;

const REGISTRY_RESOLVER_PARENT_SCOPE_SQL: &str = "INSERT INTO project_scope_names
     SELECT DISTINCT edge.namespace || ':' || lower(edge.after_state ->> 'node')
     FROM project_changed_events pointer
     CROSS JOIN LATERAL (
         SELECT edge.*
         FROM normalized_events edge
         WHERE edge.chain_id = $1
           AND edge.chain_id = pointer.chain_id
           AND edge.namespace = pointer.namespace
           AND edge.event_kind = 'SubregistryChanged'
           AND edge.source_family IN ('ens_v1_registry_l1', 'basenames_base_registry')
           AND edge.consumer_visibility = 'activated'
           AND edge.canonicality_state IN ('canonical', 'safe', 'finalized')
           AND edge.after_state ->> 'node' IS NOT NULL
           AND btrim(edge.after_state ->> 'node') <> ''
           AND edge.after_state ->> 'child_node' IS NOT NULL
           AND btrim(edge.after_state ->> 'child_node') <> ''
           AND edge.namespace || ':' || lower(edge.after_state ->> 'child_node') =
               pointer.logical_name_id
           AND edge.block_number <= $2
         OFFSET 0
     ) edge
     JOIN LATERAL (
         SELECT owner.after_state ->> 'owner_getter' AS owner_getter
         FROM normalized_events owner
         JOIN chain_lineage owner_lineage
           ON owner_lineage.chain_id = owner.chain_id
          AND owner_lineage.block_number = owner.block_number
          AND owner_lineage.block_hash = owner.block_hash
         WHERE owner.chain_id = pointer.chain_id
           AND (
               owner.logical_name_id = pointer.logical_name_id
               OR (
                   owner.logical_name_id IS NULL
                   AND owner.resource_id = pointer.resource_id
               )
           )
           AND owner.event_kind = 'AuthorityTransferred'
           AND owner.source_family IN ('ens_v1_registry_l1', 'basenames_base_registry')
           AND owner.block_number <= $2
           AND owner.consumer_visibility = 'activated'
           AND owner.canonicality_state IN ('canonical', 'safe', 'finalized')
           AND owner_lineage.canonicality_state IN ('canonical', 'safe', 'finalized')
         ORDER BY owner.block_number DESC NULLS LAST,
                  owner.transaction_index DESC NULLS LAST,
                  owner.log_index DESC NULLS LAST,
                  owner.event_identity DESC
         LIMIT 1
     ) ownership ON ownership.owner_getter =
         '0x0000000000000000000000000000000000000000'
     JOIN chain_lineage lineage
       ON lineage.chain_id = edge.chain_id
      AND lineage.block_number = edge.block_number
      AND lineage.block_hash = edge.block_hash
     WHERE pointer.event_kind = 'ResolverChanged'
       AND pointer.source_family IN ('ens_v1_registry_l1', 'basenames_base_registry')
       AND pointer.logical_name_id IS NOT NULL
       AND lineage.canonicality_state IN ('canonical', 'safe', 'finalized')
     ON CONFLICT DO NOTHING";

pub(super) async fn include_parent_names(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    target_block: i64,
) -> Result<()> {
    sqlx::query(REGISTRY_RESOLVER_PARENT_SCOPE_SQL)
        .bind(chain_id)
        .bind(target_block)
        .execute(&mut **transaction)
        .await
        .map_err(|error| {
            ProjectError::database("failed to scope registry-resolver parent names", error)
        })?;
    Ok(())
}
