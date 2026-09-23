CREATE TEMP TABLE project_mirror_frontier_resources ON COMMIT DROP AS
WITH added AS (
    INSERT INTO project_mirror_seen_resources SELECT scope.resource_id FROM project_scope_resources scope
    WHERE NOT EXISTS (SELECT 1 FROM project_mirror_seen_resources seen WHERE seen.resource_id = scope.resource_id)
    ON CONFLICT DO NOTHING RETURNING resource_id
) SELECT * FROM added;
ANALYZE project_mirror_frontier_resources;
CREATE TEMP TABLE project_mirror_frontier_names ON COMMIT DROP AS
WITH added AS (
    INSERT INTO project_mirror_seen_names SELECT scope.logical_name_id FROM project_scope_names scope
    WHERE NOT EXISTS (SELECT 1 FROM project_mirror_seen_names seen WHERE seen.logical_name_id = scope.logical_name_id)
    ON CONFLICT DO NOTHING RETURNING logical_name_id
) SELECT * FROM added;
ANALYZE project_mirror_frontier_names;
CREATE TEMP TABLE project_mirror_frontier_changed ON COMMIT DROP AS
WITH changed AS (DELETE FROM project_mirror_changed_nodes RETURNING *) SELECT * FROM changed;
ANALYZE project_mirror_frontier_changed;
CREATE TEMP TABLE project_mirror_resource_nodes ON COMMIT DROP AS
    SELECT DISTINCT event.namespace, lower(event.after_state ->> 'node') AS namehash
    FROM project_mirror_frontier_resources scope JOIN LATERAL (
        SELECT * FROM normalized_events WHERE resource_id = scope.resource_id
          AND canonicality_state IN ('canonical', 'safe', 'finalized')
          AND chain_id = $1 AND block_number <= $2 AND event_kind = 'ResolverChanged' OFFSET 0
    ) event ON TRUE
    JOIN chain_lineage lineage USING(chain_id, block_number, block_hash)
    WHERE event.chain_id = $1 AND event.block_number <= $2
      AND event.event_kind = 'ResolverChanged'
      AND event.source_family IN ('ens_v1_registry_l1', 'ens_v1_registrar_l1', 'ens_v1_wrapper_l1')
      AND event.after_state ->> 'node' IS NOT NULL
      AND event.consumer_visibility = 'activated'
      AND event.canonicality_state IN ('canonical', 'safe', 'finalized')
      AND lineage.canonicality_state IN ('canonical', 'safe', 'finalized')
    UNION SELECT namespace, namehash FROM project_mirror_frontier_changed;
ANALYZE project_mirror_resource_nodes;
CREATE TEMP TABLE project_mirror_seeds ON COMMIT DROP AS
WITH candidates AS (    SELECT surface.namespace, surface.raw_labels
    FROM project_mirror_frontier_names scope JOIN name_surfaces surface USING(logical_name_id)
    JOIN chain_lineage lineage USING(chain_id, block_number, block_hash)
    WHERE surface.chain_id = $1 AND surface.block_number <= $2
      AND surface.canonicality_state IN ('canonical', 'safe', 'finalized')
      AND lineage.canonicality_state IN ('canonical', 'safe', 'finalized')
    UNION
    SELECT surface.namespace, surface.raw_labels
    FROM project_mirror_resource_nodes node JOIN LATERAL (
        SELECT * FROM name_surfaces
        WHERE namespace = node.namespace AND lower(namehash) = node.namehash OFFSET 0
    ) surface ON TRUE
    JOIN chain_lineage lineage USING(chain_id, block_number, block_hash)
    WHERE surface.chain_id = $1 AND surface.block_number <= $2
      AND surface.canonicality_state IN ('canonical', 'safe', 'finalized')
      AND lineage.canonicality_state IN ('canonical', 'safe', 'finalized')), added AS (
    INSERT INTO project_mirror_seen_seeds
    SELECT candidate.namespace, candidate.raw_labels FROM candidates candidate
    -- An earlier completed traversal of a nonempty ancestor already discovered every
    -- descendant pointer. Keep processing these new endpoints against the cached pairs,
    -- but do not repeat the descendant/history probes. Exact array suffixes retain
    -- namespace boundaries and repeated labels. The empty root never subsumes a seed.
    -- The seen set has no unique key (see mirror.sql), so an exact repeat, including the
    -- empty root, is skipped here. The hash equalities match the seen-seed index.
    WHERE NOT EXISTS (
        SELECT 1 FROM project_mirror_seen_seeds seen
        WHERE seen.namespace = candidate.namespace
          AND hash_array_extended(seen.raw_labels, 0) = hash_array_extended(candidate.raw_labels, 0)
          AND seen.raw_labels = candidate.raw_labels
    ) AND NOT EXISTS (
        SELECT 1 FROM generate_series(1, cardinality(candidate.raw_labels)) position
        JOIN project_mirror_seen_seeds seen
          ON seen.namespace = candidate.namespace
         AND hash_array_extended(seen.raw_labels, 0) =
             hash_array_extended(candidate.raw_labels[position:cardinality(candidate.raw_labels)], 0)
         AND seen.raw_labels = candidate.raw_labels[position:cardinality(candidate.raw_labels)]
        WHERE cardinality(seen.raw_labels) > 0
    )
    RETURNING namespace, raw_labels
) SELECT * FROM added;
ANALYZE project_mirror_seeds;
CREATE TEMP TABLE project_mirror_queried_names ON COMMIT DROP AS
    SELECT DISTINCT surface.logical_name_id
    FROM project_mirror_seeds seed JOIN LATERAL (
        SELECT * FROM name_surfaces
        -- The hash containment matches name_surfaces_project_label_hashes_idx and the array
        -- containment decides the match, so a hash collision never changes the result.
        WHERE namespace = seed.namespace
          AND label_hashes(raw_labels) @> label_hashes(seed.raw_labels)
          AND raw_labels @> seed.raw_labels
          AND cardinality(seed.raw_labels) > 0
          AND raw_labels[cardinality(raw_labels)-cardinality(seed.raw_labels)+1:
                         cardinality(raw_labels)] = seed.raw_labels OFFSET 0
    ) surface ON TRUE
    WHERE surface.chain_id = $1 AND surface.block_number <= $2
      AND surface.canonicality_state IN ('canonical', 'safe', 'finalized');
ANALYZE project_mirror_queried_names;
CREATE TEMP TABLE project_mirror_pointer_candidates ON COMMIT DROP AS
SELECT DISTINCT event.resource_id, event.logical_name_id
FROM project_mirror_frontier_resources scope JOIN LATERAL (
    SELECT event.resource_id, event.logical_name_id
    FROM normalized_events event
    JOIN chain_lineage lineage USING(chain_id, block_number, block_hash)
    JOIN project_declared_resolver_addresses declaration
      ON declaration.resolver_address = lower(event.after_state ->> 'resolver')
     AND declaration.classification_role = 'ensv1_mirror_resolver'
    WHERE event.resource_id = scope.resource_id AND event.logical_name_id IS NOT NULL
      AND event.chain_id = $1 AND event.block_number <= $2
      AND event.event_kind = 'ResolverChanged'
      AND event.source_family IN ('ens_v2_registry_l1', 'ens_v2_root_l1')
      AND event.consumer_visibility = 'activated'
      AND event.canonicality_state IN ('canonical', 'safe', 'finalized')
      AND lineage.canonicality_state IN ('canonical', 'safe', 'finalized') OFFSET 0
) event ON TRUE
UNION
SELECT DISTINCT event.resource_id, event.logical_name_id
FROM project_mirror_queried_names scope JOIN LATERAL (
    SELECT event.resource_id, event.logical_name_id
    FROM normalized_events event
    JOIN chain_lineage lineage USING(chain_id, block_number, block_hash)
    JOIN project_declared_resolver_addresses declaration
      ON declaration.resolver_address = lower(event.after_state ->> 'resolver')
     AND declaration.classification_role = 'ensv1_mirror_resolver'
    WHERE event.logical_name_id = scope.logical_name_id AND event.resource_id IS NOT NULL
      AND event.chain_id = $1 AND event.block_number <= $2
      AND event.event_kind = 'ResolverChanged'
      AND event.source_family IN ('ens_v2_registry_l1', 'ens_v2_root_l1')
      AND event.consumer_visibility = 'activated'
      AND event.canonicality_state IN ('canonical', 'safe', 'finalized')
      AND lineage.canonicality_state IN ('canonical', 'safe', 'finalized') OFFSET 0
) event ON TRUE;
ANALYZE project_mirror_pointer_candidates;
CREATE TEMP TABLE project_mirror_new_pointers ON COMMIT DROP AS
WITH added AS (
    INSERT INTO project_mirror_seen_pointers SELECT resource_id, logical_name_id FROM project_mirror_pointer_candidates
    ON CONFLICT DO NOTHING RETURNING resource_id, logical_name_id
) SELECT * FROM added;
ANALYZE project_mirror_new_pointers;
CREATE TEMP TABLE project_mirror_walks ON COMMIT DROP AS
    SELECT DISTINCT mirror.resource_id AS mirror_resource_id, surface.namespace,
           surface.raw_labels[position:cardinality(surface.raw_labels)] AS suffix
    FROM project_mirror_new_pointers mirror JOIN name_surfaces surface USING(logical_name_id)
    JOIN chain_lineage lineage USING(chain_id, block_number, block_hash)
    CROSS JOIN generate_series(1, cardinality(surface.raw_labels)) position
    WHERE surface.chain_id = $1 AND surface.block_number <= $2
      AND surface.canonicality_state IN ('canonical', 'safe', 'finalized')
      AND lineage.canonicality_state IN ('canonical', 'safe', 'finalized');
ANALYZE project_mirror_walks;
CREATE TEMP TABLE project_mirror_suffixes ON COMMIT DROP AS
SELECT DISTINCT namespace, suffix FROM project_mirror_walks;
ANALYZE project_mirror_suffixes;
CREATE TEMP TABLE project_mirror_surfaces ON COMMIT DROP AS
    SELECT DISTINCT walk.suffix, surface.logical_name_id, surface.namespace,
           lower(surface.namehash) AS namehash
    FROM project_mirror_suffixes walk JOIN LATERAL (
        SELECT * FROM name_surfaces
        -- The hash equality matches name_surfaces_project_suffix_hash_idx and the array
        -- equality decides the match, so a hash collision never changes the result.
        -- The statements in this file are split on semicolons, so comments have none.
        WHERE namespace = walk.namespace
          AND hash_array_extended(raw_labels, 0) = hash_array_extended(walk.suffix, 0)
          AND raw_labels = walk.suffix OFFSET 0
    ) surface ON TRUE
    JOIN chain_lineage lineage USING(chain_id, block_number, block_hash)
    WHERE surface.chain_id = $1 AND surface.block_number <= $2
      AND surface.canonicality_state IN ('canonical', 'safe', 'finalized')
      AND lineage.canonicality_state IN ('canonical', 'safe', 'finalized');
ANALYZE project_mirror_surfaces;
CREATE TEMP TABLE project_mirror_consulted ON COMMIT DROP AS
SELECT DISTINCT walk.mirror_resource_id, surface.logical_name_id, surface.namespace, surface.namehash
FROM project_mirror_walks walk JOIN project_mirror_surfaces surface USING(namespace, suffix);
ANALYZE project_mirror_consulted;
CREATE TEMP TABLE project_mirror_wanted ON COMMIT DROP AS
WITH added AS (
    INSERT INTO project_mirror_seen_nodes SELECT DISTINCT namespace, namehash FROM project_mirror_consulted
    ON CONFLICT DO NOTHING RETURNING namespace, namehash
) SELECT * FROM added;
ANALYZE project_mirror_wanted;
INSERT INTO project_mirror_cached_nodes
    SELECT DISTINCT event.namespace, lower(event.after_state ->> 'node') AS namehash, event.resource_id
    FROM project_mirror_wanted JOIN LATERAL (
        SELECT * FROM normalized_events
        WHERE namespace = project_mirror_wanted.namespace AND lower(after_state ->> 'node') = project_mirror_wanted.namehash
          AND chain_id = $1 AND block_number <= $2
          AND event_kind = 'ResolverChanged'
          AND source_family IN ('ens_v1_registry_l1', 'ens_v1_registrar_l1', 'ens_v1_wrapper_l1')
          AND after_state ->> 'node' IS NOT NULL
          AND consumer_visibility = 'activated'
          AND canonicality_state IN ('canonical', 'safe', 'finalized') OFFSET 0
    ) event ON TRUE
    JOIN chain_lineage lineage USING(chain_id, block_number, block_hash)
    WHERE event.chain_id = $1 AND event.block_number <= $2
      AND event.event_kind = 'ResolverChanged'
      AND event.source_family IN ('ens_v1_registry_l1', 'ens_v1_registrar_l1', 'ens_v1_wrapper_l1')
      AND event.after_state ->> 'node' IS NOT NULL
      AND event.consumer_visibility = 'activated'
      AND event.canonicality_state IN ('canonical', 'safe', 'finalized')
      AND lineage.canonicality_state IN ('canonical', 'safe', 'finalized')
ON CONFLICT DO NOTHING;
ANALYZE project_mirror_cached_nodes;
-- Keep mirror-to-node links separate from node-to-history resources. Joining both
-- axes here would materialize millions of redundant mirror/resource combinations.
INSERT INTO project_mirror_links
SELECT mirror_resource_id, logical_name_id, namespace, namehash
FROM project_mirror_consulted ON CONFLICT DO NOTHING;
ANALYZE project_mirror_links;
