WITH frontier AS MATERIALIZED (
             INSERT INTO project_inventory_seen_resources SELECT resource_id FROM project_scope_resources
             ON CONFLICT DO NOTHING RETURNING resource_id
         )
         INSERT INTO project_scope_names
         SELECT DISTINCT event.logical_name_id
         FROM frontier scope
         JOIN normalized_events event USING (resource_id)
         JOIN chain_lineage lineage
           ON lineage.chain_id = event.chain_id
          AND lineage.block_hash = event.block_hash
          AND lineage.block_number = event.block_number
         WHERE event.chain_id = $1
           AND event.block_number <= $2
           AND event.event_kind = 'ResolverChanged'
           AND event.logical_name_id IS NOT NULL
           AND event.consumer_visibility = 'activated'
           AND event.canonicality_state IN ('canonical', 'safe', 'finalized')
           AND lineage.canonicality_state IN ('canonical', 'safe', 'finalized')
         ON CONFLICT DO NOTHING;
