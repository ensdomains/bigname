/* project:stage.node_record_events.history */
CREATE TEMP TABLE project_node_record_history ON COMMIT DROP AS
WITH wanted_nodes AS MATERIALIZED (
    SELECT DISTINCT lower(surface.namehash) AS node
    FROM name_surfaces surface
    JOIN (SELECT logical_name_id FROM project_scope_names
          UNION SELECT logical_name_id FROM project_scope_children) scope
      USING (logical_name_id)
    WHERE surface.chain_id = $1 AND surface.block_number <= $2
      AND surface.canonicality_state IN ('canonical', 'safe', 'finalized')
)
SELECT event.normalized_event_id, chain_id, namespace, logical_name_id, source_family,
       source_manifest_id, event_kind, block_number, block_hash,
       consumer_visibility, canonicality_state, after_state, raw_fact_ref
FROM wanted_nodes wanted
JOIN normalized_events event ON lower(event.after_state ->> 'node') = wanted.node
WHERE chain_id = $1 AND block_number <= $2
  AND logical_name_id IS NULL
  AND consumer_visibility = 'activated'
  AND canonicality_state IN ('canonical', 'safe', 'finalized')
  AND after_state ->> 'node' IS NOT NULL
  AND ((event_kind IN ('RecordChanged', 'RecordVersionChanged')
        AND source_family IN ('ens_v1_resolver_l1', 'ens_v2_resolver_l1', 'basenames_base_resolver'))
       OR (event_kind = 'ResolverChanged'
           AND source_family IN ('ens_v1_registry_l1', 'ens_v1_registrar_l1', 'ens_v1_wrapper_l1')))
