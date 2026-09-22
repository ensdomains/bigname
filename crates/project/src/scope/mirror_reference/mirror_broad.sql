-- Conservative work budget, not an eligibility predicate. Duplicates or unrelated
-- history may choose bulk earlier; both strategies compute the same dependency set.
-- LIMIT bounds the evidence needed to choose bulk rather than counting the chain.
SELECT CASE WHEN EXISTS (
    SELECT 1 FROM (
        SELECT 1 FROM project_scope_resources
        UNION ALL SELECT 1 FROM project_scope_names
        UNION ALL SELECT 1 FROM project_mirror_changed_nodes
    ) seeds OFFSET 256 LIMIT 1
) THEN TRUE ELSE EXISTS (
    WITH resources AS MATERIALIZED (
        SELECT scope.resource_id FROM project_scope_resources scope
        WHERE NOT EXISTS (SELECT 1 FROM project_mirror_seen_resources seen
                          WHERE seen.resource_id = scope.resource_id)
    ), history AS MATERIALIZED (
        SELECT event.namespace, lower(event.after_state ->> 'node') AS namehash
        FROM resources scope JOIN LATERAL (
            SELECT namespace, after_state FROM normalized_events
            WHERE resource_id = scope.resource_id AND chain_id = $1 AND block_number <= $2
              AND canonicality_state IN ('canonical', 'safe', 'finalized') LIMIT 257
        ) event ON TRUE LIMIT 257
    ), nodes AS (
        SELECT namespace, namehash FROM history
        UNION ALL SELECT namespace, namehash FROM project_mirror_changed_nodes
    ), surfaces AS (
        SELECT surface.namespace, surface.raw_labels
        FROM project_scope_names scope JOIN name_surfaces surface USING (logical_name_id)
        WHERE NOT EXISTS (SELECT 1 FROM project_mirror_seen_names seen
                          WHERE seen.logical_name_id = scope.logical_name_id)
        UNION ALL
        SELECT surface.namespace, surface.raw_labels
        FROM nodes node JOIN name_surfaces surface
          ON surface.namespace = node.namespace AND lower(surface.namehash) = node.namehash
    ), work AS (
        SELECT 1 FROM history
        UNION ALL
        SELECT 1 FROM surfaces seed JOIN LATERAL (
            SELECT 1 FROM name_surfaces
            WHERE namespace = seed.namespace AND raw_labels @> seed.raw_labels
              AND cardinality(seed.raw_labels) > 0
              AND raw_labels[cardinality(raw_labels)-cardinality(seed.raw_labels)+1:
                             cardinality(raw_labels)] = seed.raw_labels
            LIMIT 257
        ) candidate ON TRUE
    )
    SELECT 1 FROM work OFFSET 256 LIMIT 1
) END
