/* project:scope.names_from_unnamed_leases */
-- Only names historically bound to each new resource can be added. Check registrar history
-- once per distinct resource/namehash relationship, rather than multiplying every lifecycle
-- event by every historical binding. These keyed fences remain for broad frontiers too.
INSERT INTO project_scope_names
SELECT DISTINCT surface.logical_name_id
FROM project_scope_resources scope
CROSS JOIN LATERAL (
    SELECT DISTINCT binding.logical_name_id, surface.namehash
    FROM surface_bindings binding
    JOIN name_surfaces surface ON surface.logical_name_id = binding.logical_name_id
    JOIN LATERAL (
        SELECT canonicality_state FROM chain_lineage
        WHERE chain_id = binding.chain_id
          AND block_hash = binding.block_hash
          AND block_number = binding.block_number
        OFFSET (0)
    ) lineage ON TRUE
    WHERE binding.resource_id = scope.resource_id
      AND {CANONICAL_BINDING}
    OFFSET (0)
) surface
WHERE EXISTS (
    SELECT 1 FROM normalized_events registrar
    WHERE registrar.resource_id = scope.resource_id
      AND {UNNAMED_LEASE_ROW}
    OFFSET (0)
)
ON CONFLICT DO NOTHING
