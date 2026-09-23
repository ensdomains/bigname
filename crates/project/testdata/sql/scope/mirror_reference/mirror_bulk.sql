CREATE TEMP TABLE project_mirror_walk ON COMMIT DROP AS
WITH mirror_pointers AS (
             SELECT DISTINCT event.resource_id, event.logical_name_id
             FROM normalized_events event
             JOIN chain_lineage lineage
               ON lineage.chain_id = event.chain_id
              AND lineage.block_hash = event.block_hash
              AND lineage.block_number = event.block_number
             JOIN project_declared_resolver_addresses declaration
               ON declaration.resolver_address = lower(event.after_state ->> 'resolver')
              AND declaration.classification_role = 'ensv1_mirror_resolver'
             WHERE event.chain_id = $1
               AND event.block_number <= $2
               AND event.event_kind = 'ResolverChanged'
               AND event.source_family IN ('ens_v2_registry_l1', 'ens_v2_root_l1')
               AND event.resource_id IS NOT NULL
               AND event.logical_name_id IS NOT NULL
               AND event.consumer_visibility = 'activated'
               AND event.canonicality_state IN ('canonical', 'safe', 'finalized')
               AND lineage.canonicality_state IN ('canonical', 'safe', 'finalized')

), surfaces AS (
             SELECT surface.logical_name_id, surface.namespace, surface.namehash,
                    surface.raw_labels
             FROM name_surfaces surface
             JOIN chain_lineage lineage
               ON lineage.chain_id = surface.chain_id
              AND lineage.block_hash = surface.block_hash
              AND lineage.block_number = surface.block_number
             WHERE surface.chain_id = $1
               AND surface.block_number <= $2
               AND surface.canonicality_state IN ('canonical', 'safe', 'finalized')
               AND lineage.canonicality_state IN ('canonical', 'safe', 'finalized')

)
SELECT DISTINCT mirror.resource_id AS mirror_resource_id, queried.namespace,
       queried.raw_labels[position:cardinality(queried.raw_labels)] AS suffix
FROM mirror_pointers mirror JOIN surfaces queried USING(logical_name_id)
CROSS JOIN generate_series(1, cardinality(queried.raw_labels)) position;
ANALYZE project_mirror_walk;
CREATE TEMP TABLE project_mirror_reference_consulted ON COMMIT DROP AS
SELECT DISTINCT walk.mirror_resource_id, surface.logical_name_id, surface.namespace,
       lower(surface.namehash) AS namehash
FROM project_mirror_walk walk
JOIN name_surfaces surface ON surface.namespace=walk.namespace AND surface.raw_labels=walk.suffix
JOIN chain_lineage lineage ON lineage.chain_id=surface.chain_id
 AND lineage.block_hash=surface.block_hash AND lineage.block_number=surface.block_number
WHERE surface.chain_id=$1 AND surface.block_number <= $2
AND surface.canonicality_state IN ('canonical','safe','finalized')
AND lineage.canonicality_state IN ('canonical','safe','finalized');
ANALYZE project_mirror_reference_consulted;
CREATE TEMP TABLE project_mirror_pairs ON COMMIT DROP AS
WITH wanted AS MATERIALIZED (SELECT DISTINCT namespace,namehash FROM project_mirror_reference_consulted),
v1_nodes AS (
             SELECT event.namespace, lower(event.after_state ->> 'node') AS namehash,
                    event.resource_id,
                    bool_or(changed.normalized_event_id IS NOT NULL) AS changed
             FROM wanted JOIN normalized_events event
 ON event.namespace=wanted.namespace AND lower(event.after_state ->> 'node')=wanted.namehash
             JOIN chain_lineage lineage
               ON lineage.chain_id = event.chain_id
              AND lineage.block_hash = event.block_hash
              AND lineage.block_number = event.block_number
             LEFT JOIN project_changed_events changed USING (normalized_event_id)
             WHERE event.chain_id = $1
               AND event.block_number <= $2
               AND event.event_kind = 'ResolverChanged'
               AND event.source_family IN (
                   'ens_v1_registry_l1', 'ens_v1_registrar_l1', 'ens_v1_wrapper_l1'
               )
               AND event.after_state ->> 'node' IS NOT NULL
               AND event.consumer_visibility = 'activated'
               AND event.canonicality_state IN ('canonical', 'safe', 'finalized')
               AND lineage.canonicality_state IN ('canonical', 'safe', 'finalized')
             GROUP BY event.namespace, lower(event.after_state ->> 'node'), event.resource_id

)
SELECT DISTINCT consulted.mirror_resource_id, consulted.logical_name_id AS consulted_logical_name_id,
       node.resource_id AS consulted_resource_id, node.changed
FROM project_mirror_reference_consulted consulted JOIN v1_nodes node USING(namespace,namehash);
CREATE INDEX ON project_mirror_pairs(mirror_resource_id);
CREATE INDEX ON project_mirror_pairs(consulted_resource_id);
CREATE INDEX ON project_mirror_pairs(consulted_logical_name_id);
ANALYZE project_mirror_pairs;
DROP TABLE project_mirror_walk, project_mirror_reference_consulted;
