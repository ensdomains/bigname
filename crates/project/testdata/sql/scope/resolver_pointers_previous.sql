-- Literal resource resolver history from 2abf62296b5228e1a14aa101ffed09f106125ac5.
ANALYZE project_scope_resources;
INSERT INTO project_scope_resolver_permission_history
         SELECT lower(candidate.resolver_address)
         FROM project_scope_resources scope
         JOIN normalized_events event USING (resource_id)
         JOIN chain_lineage lineage
           ON lineage.chain_id = event.chain_id
          AND lineage.block_hash = event.block_hash
          AND lineage.block_number = event.block_number
         CROSS JOIN LATERAL (VALUES
             
    (CASE WHEN event.event_kind = 'PermissionChanged'
          THEN event.after_state #>> '{scope,resolver_address}' END),
    (CASE WHEN event.event_kind = 'PermissionChanged'
          THEN event.before_state #>> '{scope,resolver_address}' END)

         ) candidate(resolver_address)
         WHERE event.chain_id = $1
           AND event.event_kind = 'PermissionChanged'
           AND event.consumer_visibility = 'activated'
           AND event.block_number <= $2
           AND event.canonicality_state IN ('canonical', 'safe', 'finalized')
           AND lineage.canonicality_state IN ('canonical', 'safe', 'finalized')
           AND candidate.resolver_address IS NOT NULL
           AND btrim(candidate.resolver_address) <> ''
           AND lower(candidate.resolver_address) <>
               '0x0000000000000000000000000000000000000000'
         ON CONFLICT DO NOTHING;
INSERT INTO project_scope_resolvers
         SELECT lower(pointer.resolver_address)
         FROM (
             SELECT inventory.provenance ->> 'resolver_address' AS resolver_address
             FROM record_inventory_current inventory
             JOIN project_scope_resources scope USING (resource_id)
             WHERE inventory.provenance ->> 'chain_id' = $1
             UNION ALL
             SELECT DISTINCT event.after_state ->> 'resolver' AS resolver_address
             FROM normalized_events event
             JOIN project_scope_resources scope USING (resource_id)
             JOIN chain_lineage lineage
               ON lineage.chain_id = event.chain_id
              AND lineage.block_hash = event.block_hash
              AND lineage.block_number = event.block_number
             WHERE event.chain_id = $1
               AND event.event_kind = 'ResolverChanged'
               AND event.consumer_visibility = 'activated'
               AND event.block_number <= $2
               AND event.canonicality_state IN ('canonical', 'safe', 'finalized')
               AND lineage.canonicality_state IN ('canonical', 'safe', 'finalized')
             UNION ALL
             SELECT permission.scope_detail ->> 'resolver_address'
             FROM permissions_current permission
             JOIN project_scope_resources scope USING (resource_id)
             WHERE permission.scope_kind = 'resolver'
               AND permission.scope_detail ->> 'chain_id' = $1
             UNION ALL
             SELECT resolver_address
             FROM project_scope_resolver_permission_history
         ) pointer
         WHERE pointer.resolver_address IS NOT NULL
           AND btrim(pointer.resolver_address) <> ''
           AND lower(pointer.resolver_address) <>
               '0x0000000000000000000000000000000000000000'
         ON CONFLICT DO NOTHING;
