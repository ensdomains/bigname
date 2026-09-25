/* project:scope.wrapper_names_from_registrars */
-- Each correlated probe is bounded by a newly consumed resource or name. OFFSET (0) keeps
-- these bounds even for a broad frontier; the generic OFFSET 0 removal must not undo them.
-- Deduplicate the resource/name relationship before checking its wrapper history.
WITH scoped_registrars AS MATERIALIZED (
    SELECT scope.resource_id, registrar.logical_name_id
    FROM project_scope_resources scope
    CROSS JOIN LATERAL (
        SELECT DISTINCT registrar.namespace || ':' ||
               lower(registrar.after_state ->> 'namehash') AS logical_name_id
        FROM normalized_events registrar
        WHERE registrar.resource_id = scope.resource_id
          AND registrar.chain_id = $1
          AND registrar.block_number <= $2
          AND registrar.source_family = 'ens_v1_registrar_l1'
          AND registrar.consumer_visibility = 'activated'
          AND registrar.canonicality_state IN ('canonical', 'safe', 'finalized')
          AND registrar.after_state ->> 'namehash' IS NOT NULL
          AND btrim(registrar.after_state ->> 'namehash') <> ''
          AND EXISTS (
              SELECT 1 FROM chain_lineage lineage
              WHERE lineage.chain_id = registrar.chain_id
                AND lineage.block_hash = registrar.block_hash
                AND lineage.block_number = registrar.block_number
                AND lineage.canonicality_state IN ('canonical', 'safe', 'finalized')
              OFFSET (0)
          )
        OFFSET (0)
    ) registrar
)
INSERT INTO project_scope_names
SELECT DISTINCT registrar.logical_name_id
FROM scoped_registrars registrar
WHERE EXISTS (
    SELECT 1 FROM normalized_events wrapper
    WHERE wrapper.logical_name_id = registrar.logical_name_id
      AND wrapper.after_state ->> 'wrapped_registrar_resource_id' = registrar.resource_id::text
      AND wrapper.chain_id = $1
      AND wrapper.block_number <= $2
      AND wrapper.source_family = 'ens_v1_wrapper_l1'
      AND wrapper.event_kind = 'SurfaceBound'
      AND wrapper.consumer_visibility = 'activated'
      AND wrapper.canonicality_state IN ('canonical', 'safe', 'finalized')
      AND EXISTS (
          SELECT 1 FROM chain_lineage lineage
          WHERE lineage.chain_id = wrapper.chain_id
            AND lineage.block_hash = wrapper.block_hash
            AND lineage.block_number = wrapper.block_number
            AND lineage.canonicality_state IN ('canonical', 'safe', 'finalized')
          OFFSET (0)
      )
    OFFSET (0)
)
ON CONFLICT DO NOTHING
