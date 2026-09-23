use sqlx::{Postgres, Transaction};

use crate::{ProjectError, Result};

// This history includes only rows that the node branches below can select. Keep their
// source, manifest and lineage predicates in place; staging is not serving admission.
pub(super) const STAGE_HISTORY_SQL: &str = include_str!("node_record_events/history.sql");
const INDEX_HISTORY_SQL: &str = include_str!("node_record_events/index.sql");

pub(crate) async fn prepare(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    target_block: i64,
) -> Result<()> {
    sqlx::query(STAGE_HISTORY_SQL)
        .bind(chain_id)
        .bind(target_block)
        .execute(&mut **transaction)
        .await
        .map_err(|error| ProjectError::database("failed to stage node record history", error))?;
    for statement in INDEX_HISTORY_SQL.split(';') {
        if statement.trim().is_empty() {
            continue;
        }
        sqlx::query(statement)
            .execute(&mut **transaction)
            .await
            .map_err(|error| {
                ProjectError::database("failed to index node record history", error)
            })?;
    }
    Ok(())
}

pub(crate) const SCOPED_NODE_RECORD_EVENT_IDS_SQL: &str = r#"
SELECT record.normalized_event_id
FROM name_surfaces surface
JOIN chain_lineage surface_lineage
  ON surface_lineage.chain_id = surface.chain_id
 AND (surface_lineage.block_number, surface_lineage.block_hash) =
     (surface.block_number, surface.block_hash)
JOIN (
    SELECT DISTINCT event.resource_id, event.logical_name_id,
           event.source_family AS pointer_source_family, event.namespace,
           lower(event.after_state ->> 'resolver') AS resolver_address
    FROM project_scope_resources resource_scope
    JOIN normalized_events event USING (resource_id)
    JOIN chain_lineage lineage USING (chain_id, block_number, block_hash)
    WHERE event.chain_id = $1 AND event.block_number <= $2
      AND event.resource_id IS NOT NULL AND event.logical_name_id IS NOT NULL
      AND event.event_kind = 'ResolverChanged'
      AND event.consumer_visibility = 'activated'
      AND (
          event.source_family IN (
              'ens_v1_registry_l1',
              'ens_v1_registrar_l1',
              'ens_v1_wrapper_l1',
              'basenames_base_registry'
          ) OR (
              event.source_family IN ('ens_v2_registry_l1', 'ens_v2_root_l1')
              AND EXISTS (
                  SELECT 1
                  FROM project_declared_resolver_addresses declaration
                  WHERE declaration.namespace = event.namespace
                    AND declaration.resolver_address =
                        lower(event.after_state ->> 'resolver')
              )
          )
      )
      AND event.canonicality_state IN ('canonical', 'safe', 'finalized')
      AND lineage.canonicality_state IN ('canonical', 'safe', 'finalized')
) pointer USING (logical_name_id)
JOIN LATERAL (
    SELECT event.normalized_event_id, event.chain_id,
           event.block_number, event.block_hash
    FROM project_node_record_history event
    WHERE pointer.pointer_source_family IN (
              'ens_v1_registry_l1', 'ens_v1_registrar_l1',
              'ens_v1_wrapper_l1', 'ens_v2_registry_l1', 'ens_v2_root_l1'
          )
      AND event.chain_id = $1
      AND event.logical_name_id IS NULL
      AND event.source_family = 'ens_v1_resolver_l1'
      AND lower(event.after_state ->> 'node') = lower(surface.namehash)
      AND lower(COALESCE(
              NULLIF(event.after_state ->> 'resolver', ''),
              NULLIF(event.raw_fact_ref ->> 'emitting_address', '')
          )) = pointer.resolver_address
      AND event.block_number <= $2
      AND event.consumer_visibility = 'activated'
      AND event.event_kind IN ('RecordChanged', 'RecordVersionChanged')
      AND event.canonicality_state IN ('canonical', 'safe', 'finalized')
    UNION ALL
    SELECT event.normalized_event_id, event.chain_id,
           event.block_number, event.block_hash
    FROM project_node_record_history event
    JOIN project_declared_resolver_addresses declaration
      ON declaration.namespace = pointer.namespace
     AND declaration.resolver_address = pointer.resolver_address
     AND declaration.source_family = 'ens_v2_resolver_l1'
     AND declaration.classification_role = 'public_resolver_v2'
     AND declaration.manifest_id = event.source_manifest_id
    WHERE pointer.pointer_source_family IN ('ens_v2_registry_l1', 'ens_v2_root_l1')
      AND event.chain_id = $1 AND event.namespace = pointer.namespace
      AND event.logical_name_id IS NULL
      AND event.source_family = 'ens_v2_resolver_l1'
      AND lower(event.after_state ->> 'node') = lower(surface.namehash)
      AND lower(COALESCE(NULLIF(event.after_state ->> 'resolver', ''),
                        NULLIF(event.raw_fact_ref ->> 'emitting_address', ''))) =
          pointer.resolver_address
      AND event.block_number <= $2
      AND event.consumer_visibility = 'activated'
      AND event.event_kind IN ('RecordChanged', 'RecordVersionChanged')
      AND event.canonicality_state IN ('canonical', 'safe', 'finalized')
    UNION ALL
    -- A pointer to a declared ENSv1 mirror resolver is served through whichever declared ENSv1
    -- resolver the mirror finds for the queried node (builders/record_inventory/mirror.rs), so
    -- stage that node's writes on every declared ENSv1 resolver.
    SELECT event.normalized_event_id, event.chain_id,
           event.block_number, event.block_hash
    FROM project_node_record_history event
    JOIN project_declared_resolver_addresses ensv1
      ON ensv1.source_family = 'ens_v1_resolver_l1'
     AND ensv1.resolver_address = lower(COALESCE(
             NULLIF(event.after_state ->> 'resolver', ''),
             NULLIF(event.raw_fact_ref ->> 'emitting_address', '')
         ))
    WHERE pointer.pointer_source_family IN ('ens_v2_registry_l1', 'ens_v2_root_l1')
      AND EXISTS (
          SELECT 1
          FROM project_declared_resolver_addresses mirror
          WHERE mirror.namespace = pointer.namespace
            AND mirror.resolver_address = pointer.resolver_address
            AND mirror.classification_role = 'ensv1_mirror_resolver'
      )
      AND event.chain_id = $1
      AND event.logical_name_id IS NULL
      AND event.source_family = 'ens_v1_resolver_l1'
      AND lower(event.after_state ->> 'node') = lower(surface.namehash)
      AND event.block_number <= $2
      AND event.consumer_visibility = 'activated'
      AND event.event_kind IN ('RecordChanged', 'RecordVersionChanged')
      AND event.canonicality_state IN ('canonical', 'safe', 'finalized')
    UNION ALL
    SELECT event.normalized_event_id, event.chain_id,
           event.block_number, event.block_hash
    FROM project_node_record_history event
    WHERE pointer.pointer_source_family = 'basenames_base_registry'
      AND event.chain_id = $1
      AND event.logical_name_id IS NULL
      AND event.source_family = 'basenames_base_resolver'
      AND lower(event.after_state ->> 'node') = lower(surface.namehash)
      AND lower(COALESCE(
              NULLIF(event.after_state ->> 'resolver', ''),
              NULLIF(event.raw_fact_ref ->> 'emitting_address', '')
          )) = pointer.resolver_address
      AND event.block_number <= $2
      AND event.consumer_visibility = 'activated'
      AND event.event_kind IN ('RecordChanged', 'RecordVersionChanged')
      AND event.canonicality_state IN ('canonical', 'safe', 'finalized')
) record ON TRUE
JOIN chain_lineage record_lineage
  ON record_lineage.chain_id = record.chain_id
 AND (record_lineage.block_number, record_lineage.block_hash) =
     (record.block_number, record.block_hash)
WHERE (EXISTS (SELECT 1 FROM project_scope_names scope
               WHERE scope.logical_name_id = surface.logical_name_id)
       OR EXISTS (SELECT 1 FROM project_scope_children scope
                  WHERE scope.logical_name_id = surface.logical_name_id))
  AND surface.chain_id = $1 AND surface.block_number <= $2
  AND surface.canonicality_state IN ('canonical', 'safe', 'finalized')
  AND surface_lineage.canonicality_state IN ('canonical', 'safe', 'finalized')
  AND pointer.resolver_address NOT IN (
      '0x0000000000000000000000000000000000000000', ''
  )
  AND record_lineage.canonicality_state IN ('canonical', 'safe', 'finalized')
UNION
-- The ENSv1 registry's resolver for a scoped name's node, whether or not the pointer event is
-- linked to the name (a pre-surface pointer keeps null logical_name_id and resource_id). The
-- ENSv1 mirror resolver walk reads these by node (builders/record_inventory/mirror.rs).
SELECT event.normalized_event_id
FROM name_surfaces surface
JOIN project_node_record_history event
  ON event.chain_id = surface.chain_id
 AND event.namespace = surface.namespace
 AND lower(event.after_state ->> 'node') = lower(surface.namehash)
WHERE (EXISTS (SELECT 1 FROM project_scope_names scope
               WHERE scope.logical_name_id = surface.logical_name_id)
       OR EXISTS (SELECT 1 FROM project_scope_children scope
                  WHERE scope.logical_name_id = surface.logical_name_id))
  AND surface.chain_id = $1 AND surface.block_number <= $2
  AND surface.canonicality_state IN ('canonical', 'safe', 'finalized')
  AND event.event_kind = 'ResolverChanged'
  AND event.source_family IN ('ens_v1_registry_l1', 'ens_v1_registrar_l1', 'ens_v1_wrapper_l1')
  AND event.logical_name_id IS NULL
  AND event.block_number <= $2
  AND event.consumer_visibility = 'activated'
  AND event.canonicality_state IN ('canonical', 'safe', 'finalized')
"#;
