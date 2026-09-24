-- Keyed LATERAL probes keep a small frontier from becoming a chain-wide scan.
-- Keep admission predicates inside the probes so the matching partial indexes apply.
WITH frontier_resources AS MATERIALIZED (
    INSERT INTO project_mirror_seen_resources SELECT resource_id FROM project_scope_resources
    ON CONFLICT DO NOTHING RETURNING resource_id
), frontier_names AS MATERIALIZED (
    INSERT INTO project_mirror_seen_names SELECT logical_name_id FROM project_scope_names
    ON CONFLICT DO NOTHING RETURNING logical_name_id
), changed_nodes AS MATERIALIZED (
    DELETE FROM project_mirror_changed_nodes RETURNING namespace, namehash, resource_id
), resource_nodes AS MATERIALIZED (
    SELECT DISTINCT event.namespace, lower(COALESCE(event.after_state ->> 'child_node', event.after_state ->> 'namehash', event.after_state ->> 'node')) AS namehash
    FROM frontier_resources scope JOIN LATERAL (
        SELECT * FROM normalized_events WHERE resource_id = scope.resource_id
          AND canonicality_state IN ('canonical', 'safe', 'finalized')
          AND chain_id = $1 AND block_number <= $2 OFFSET 0
    ) event ON TRUE
    JOIN chain_lineage lineage USING(chain_id, block_number, block_hash)
    WHERE event.chain_id = $1 AND event.block_number <= $2
      AND event.event_kind = 'ResolverChanged'
      AND event.source_family IN ('ens_v1_registry_l1', 'ens_v1_registrar_l1', 'ens_v1_wrapper_l1')
      AND COALESCE(event.after_state ->> 'child_node', event.after_state ->> 'namehash', event.after_state ->> 'node') IS NOT NULL
      AND event.consumer_visibility = 'activated'
      AND event.canonicality_state IN ('canonical', 'safe', 'finalized')
      AND lineage.canonicality_state IN ('canonical', 'safe', 'finalized')
    UNION SELECT namespace, namehash FROM changed_nodes
), consulted_seeds AS MATERIALIZED (
    SELECT surface.namespace, surface.raw_labels
    FROM frontier_names scope JOIN name_surfaces surface USING(logical_name_id)
    JOIN chain_lineage lineage USING(chain_id, block_number, block_hash)
    WHERE surface.chain_id = $1 AND surface.block_number <= $2
      AND surface.canonicality_state IN ('canonical', 'safe', 'finalized')
      AND lineage.canonicality_state IN ('canonical', 'safe', 'finalized')
    UNION
    SELECT surface.namespace, surface.raw_labels
    FROM resource_nodes node JOIN LATERAL (
        SELECT * FROM name_surfaces
        WHERE namespace = node.namespace AND lower(namehash) = node.namehash OFFSET 0
    ) surface ON TRUE
    JOIN chain_lineage lineage USING(chain_id, block_number, block_hash)
    WHERE surface.chain_id = $1 AND surface.block_number <= $2
      AND surface.canonicality_state IN ('canonical', 'safe', 'finalized')
      AND lineage.canonicality_state IN ('canonical', 'safe', 'finalized')
), queried_names AS MATERIALIZED (
    SELECT DISTINCT surface.logical_name_id
    FROM consulted_seeds seed JOIN LATERAL (
        SELECT * FROM name_surfaces
        WHERE namespace = seed.namespace AND raw_labels @> seed.raw_labels
          AND cardinality(seed.raw_labels) > 0
          AND raw_labels[cardinality(raw_labels)-cardinality(seed.raw_labels)+1:
                         cardinality(raw_labels)] = seed.raw_labels OFFSET 0
    ) surface ON TRUE
    WHERE surface.chain_id = $1 AND surface.block_number <= $2
      AND surface.canonicality_state IN ('canonical', 'safe', 'finalized')
), pointer_candidates AS MATERIALIZED (
    SELECT event.* FROM frontier_resources scope JOIN LATERAL (
        SELECT * FROM normalized_events WHERE resource_id = scope.resource_id
          AND canonicality_state IN ('canonical', 'safe', 'finalized')
          AND chain_id = $1 AND block_number <= $2 OFFSET 0
    ) event ON TRUE
    WHERE event.chain_id = $1 AND event.block_number <= $2 AND event.event_kind = 'ResolverChanged'
    UNION
    SELECT event.* FROM queried_names scope JOIN LATERAL (
        SELECT * FROM normalized_events WHERE logical_name_id = scope.logical_name_id
          AND canonicality_state IN ('canonical', 'safe', 'finalized')
          AND chain_id = $1 AND block_number <= $2 OFFSET 0
    ) event ON TRUE
    WHERE event.chain_id = $1 AND event.block_number <= $2 AND event.event_kind = 'ResolverChanged'
), mirrors AS MATERIALIZED (
    SELECT DISTINCT event.resource_id, event.logical_name_id
    FROM pointer_candidates event
    JOIN chain_lineage lineage USING(chain_id, block_number, block_hash)
    JOIN project_declared_resolver_addresses declaration
      ON declaration.resolver_address = lower(event.after_state ->> 'resolver')
     AND declaration.classification_role = 'ensv1_mirror_resolver'
    WHERE event.source_family IN ('ens_v2_registry_l1', 'ens_v2_root_l1')
      AND event.resource_id IS NOT NULL AND event.logical_name_id IS NOT NULL
      AND event.consumer_visibility = 'activated'
      AND event.canonicality_state IN ('canonical', 'safe', 'finalized')
      AND lineage.canonicality_state IN ('canonical', 'safe', 'finalized')
), walks AS MATERIALIZED (
    SELECT DISTINCT mirror.resource_id AS mirror_resource_id, surface.namespace,
           surface.raw_labels[position:cardinality(surface.raw_labels)] AS suffix
    FROM mirrors mirror JOIN name_surfaces surface USING(logical_name_id)
    JOIN chain_lineage lineage USING(chain_id, block_number, block_hash)
    CROSS JOIN generate_series(1, cardinality(surface.raw_labels)) position
    WHERE surface.chain_id = $1 AND surface.block_number <= $2
      AND surface.canonicality_state IN ('canonical', 'safe', 'finalized')
      AND lineage.canonicality_state IN ('canonical', 'safe', 'finalized')
), consulted AS MATERIALIZED (
    SELECT DISTINCT walk.mirror_resource_id, surface.logical_name_id, surface.namespace,
           lower(surface.namehash) AS namehash
    FROM walks walk JOIN LATERAL (
        SELECT * FROM name_surfaces
        WHERE namespace = walk.namespace AND raw_labels = walk.suffix OFFSET 0
    ) surface ON TRUE
    JOIN chain_lineage lineage USING(chain_id, block_number, block_hash)
    WHERE surface.chain_id = $1 AND surface.block_number <= $2
      AND surface.canonicality_state IN ('canonical', 'safe', 'finalized')
      AND lineage.canonicality_state IN ('canonical', 'safe', 'finalized')
), wanted AS MATERIALIZED (
    SELECT DISTINCT namespace, namehash FROM consulted
), nodes AS MATERIALIZED (
    SELECT DISTINCT event.namespace, lower(COALESCE(event.after_state ->> 'child_node', event.after_state ->> 'namehash', event.after_state ->> 'node')) AS namehash, event.resource_id
    FROM wanted JOIN LATERAL (
        SELECT * FROM normalized_events
        WHERE namespace = wanted.namespace AND lower(COALESCE(after_state ->> 'child_node', after_state ->> 'namehash', after_state ->> 'node')) = wanted.namehash
          AND chain_id = $1 AND block_number <= $2
          AND event_kind = 'ResolverChanged'
          AND source_family IN ('ens_v1_registry_l1', 'ens_v1_registrar_l1', 'ens_v1_wrapper_l1')
          AND COALESCE(after_state ->> 'child_node', after_state ->> 'namehash', after_state ->> 'node') IS NOT NULL
          AND consumer_visibility = 'activated'
          AND canonicality_state IN ('canonical', 'safe', 'finalized') OFFSET 0
    ) event ON TRUE
    JOIN chain_lineage lineage USING(chain_id, block_number, block_hash)
    WHERE event.chain_id = $1 AND event.block_number <= $2
      AND event.event_kind = 'ResolverChanged'
      AND event.source_family IN ('ens_v1_registry_l1', 'ens_v1_registrar_l1', 'ens_v1_wrapper_l1')
      AND COALESCE(event.after_state ->> 'child_node', event.after_state ->> 'namehash', event.after_state ->> 'node') IS NOT NULL
      AND event.consumer_visibility = 'activated'
      AND event.canonicality_state IN ('canonical', 'safe', 'finalized')
      AND lineage.canonicality_state IN ('canonical', 'safe', 'finalized')
), rebuilt AS MATERIALIZED (
    SELECT consulted.mirror_resource_id, consulted.logical_name_id,
           node.resource_id AS consulted_resource_id
    FROM consulted JOIN nodes node USING(namespace, namehash)
    WHERE EXISTS (SELECT 1 FROM frontier_resources scope WHERE scope.resource_id = consulted.mirror_resource_id)
       OR EXISTS (SELECT 1 FROM frontier_resources scope WHERE scope.resource_id = node.resource_id)
       OR EXISTS (SELECT 1 FROM frontier_names scope WHERE scope.logical_name_id = consulted.logical_name_id)
       OR EXISTS (SELECT 1 FROM changed_nodes changed WHERE changed.namespace = consulted.namespace
                   AND changed.namehash = consulted.namehash
                   AND changed.resource_id IS NOT DISTINCT FROM node.resource_id)
), inserted_names AS (
    INSERT INTO project_scope_names SELECT DISTINCT logical_name_id FROM rebuilt ON CONFLICT DO NOTHING
)
INSERT INTO project_scope_resources
SELECT mirror_resource_id FROM rebuilt
UNION SELECT consulted_resource_id FROM rebuilt WHERE consulted_resource_id IS NOT NULL
ON CONFLICT DO NOTHING
