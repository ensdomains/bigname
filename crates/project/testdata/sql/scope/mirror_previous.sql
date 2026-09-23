WITH mirror_pointers AS (
             SELECT event.resource_id, event.logical_name_id
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
         ),
         v1_nodes AS (
             SELECT event.namespace, lower(event.after_state ->> 'node') AS namehash,
                    event.resource_id,
                    bool_or(changed.normalized_event_id IS NOT NULL) AS changed
             FROM normalized_events event
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
         ),
         surfaces AS (
             SELECT DISTINCT surface.logical_name_id, surface.namespace, surface.namehash,
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
               AND (
                   EXISTS (
                       SELECT 1 FROM mirror_pointers mirror
                       WHERE mirror.logical_name_id = surface.logical_name_id
                   )
                   OR EXISTS (
                       SELECT 1 FROM v1_nodes node
                       WHERE node.namespace = surface.namespace
                         AND node.namehash = lower(surface.namehash)
                   )
               )
         ),
         -- Every registry node the mirror's resolver walk consults for a mirrored name: the name
         -- itself and each proper ancestor below the root.
         walk AS (
             SELECT mirror.resource_id AS mirror_resource_id,
                    queried.namespace,
                    queried.raw_labels[position : cardinality(queried.raw_labels)] AS suffix
             FROM mirror_pointers mirror
             JOIN surfaces queried ON queried.logical_name_id = mirror.logical_name_id
             CROSS JOIN generate_series(1, cardinality(queried.raw_labels)) AS position
         ),
         pairs AS (
             SELECT walk.mirror_resource_id,
                    consulted.logical_name_id AS consulted_logical_name_id,
                    node.resource_id AS consulted_resource_id,
                    node.changed
             FROM walk
             JOIN surfaces consulted
               ON consulted.namespace = walk.namespace
              AND consulted.raw_labels = walk.suffix
             JOIN v1_nodes node
               ON node.namespace = consulted.namespace
              AND node.namehash = lower(consulted.namehash)
         ),
         rebuilt AS (
             SELECT DISTINCT pair.*
             FROM pairs pair
             WHERE EXISTS (
                     SELECT 1 FROM project_scope_resources scope
                     WHERE scope.resource_id = pair.mirror_resource_id
                 )
                OR EXISTS (
                     SELECT 1 FROM project_scope_resources scope
                     WHERE scope.resource_id = pair.consulted_resource_id
                 )
                OR EXISTS (
                     SELECT 1 FROM project_scope_names scope
                     WHERE scope.logical_name_id = pair.consulted_logical_name_id
                 )
                OR pair.changed
         ),
         scoped_names AS (
             INSERT INTO project_scope_names
             SELECT DISTINCT consulted_logical_name_id FROM rebuilt
             ON CONFLICT DO NOTHING
         )
         INSERT INTO project_scope_resources
         SELECT mirror_resource_id FROM rebuilt
         UNION
         SELECT consulted_resource_id FROM rebuilt WHERE consulted_resource_id IS NOT NULL
         ON CONFLICT DO NOTHING;
