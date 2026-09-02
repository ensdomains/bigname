pub(crate) const SCOPED_NODE_RECORD_EVENT_IDS_SQL: &str = r#"
SELECT record.normalized_event_id
FROM (
    SELECT logical_name_id FROM project_scope_names
    UNION
    SELECT logical_name_id FROM project_scope_children
) scope
JOIN name_surfaces surface USING (logical_name_id)
JOIN chain_lineage surface_lineage
  ON surface_lineage.chain_id = surface.chain_id
 AND (surface_lineage.block_number, surface_lineage.block_hash) =
     (surface.block_number, surface.block_hash)
JOIN (
    SELECT DISTINCT event.resource_id, event.logical_name_id,
           event.source_family AS pointer_source_family,
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
    FROM normalized_events event
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
    FROM normalized_events event
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
WHERE surface.chain_id = $1 AND surface.block_number <= $2
  AND surface.canonicality_state IN ('canonical', 'safe', 'finalized')
  AND surface_lineage.canonicality_state IN ('canonical', 'safe', 'finalized')
  AND pointer.resolver_address NOT IN (
      '0x0000000000000000000000000000000000000000', ''
  )
  AND record_lineage.canonicality_state IN ('canonical', 'safe', 'finalized')
"#;
