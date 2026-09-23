         SELECT DISTINCT pointer.logical_name_id, inventory.resource_id
         FROM project_changed_events record
         JOIN name_surfaces surface
           ON surface.chain_id = record.chain_id
          AND surface.namehash = lower(record.after_state ->> 'node')
          AND lower(surface.namehash) = lower(record.after_state ->> 'node')
          AND surface.canonicality_state IN ('canonical', 'safe', 'finalized')
         JOIN normalized_events pointer
           ON pointer.chain_id = record.chain_id
          AND pointer.logical_name_id = surface.logical_name_id
          AND pointer.resource_id IS NOT NULL
          AND pointer.event_kind = 'ResolverChanged'
          AND pointer.source_family IN ('ens_v2_registry_l1', 'ens_v2_root_l1')
          AND pointer.canonicality_state IN ('canonical', 'safe', 'finalized')
         JOIN record_inventory_current inventory
           ON inventory.resource_id = pointer.resource_id
          AND (inventory.provenance ->> 'resolver_pointer_event_id')::bigint =
              pointer.normalized_event_id
          AND inventory.provenance ->> 'chain_id' = record.chain_id
          AND inventory.support_status = 'supported'
         JOIN resolver_current resolver
           ON resolver.chain_id = record.chain_id
          AND lower(resolver.resolver_address) =
              lower(pointer.after_state ->> 'resolver')
          AND resolver.support_status = 'supported'
          AND (resolver.declared_summary #>> '{classification,source_family}' =
              'ens_v1_resolver_l1'
           OR (resolver.declared_summary #>> '{classification,source_family}' =
                   'ens_v2_resolver_l1'
               AND resolver.declared_summary #>> '{classification,role}' =
                   'public_resolver_v2'))
          AND resolver.declared_summary #>> '{classification,basis}' =
              'manifest_declared_address'
         JOIN project_declared_resolver_addresses declaration
           ON declaration.manifest_id =
              (resolver.provenance ->> 'manifest_id')::bigint
          AND declaration.namespace = pointer.namespace
          AND declaration.resolver_address =
              lower(pointer.after_state ->> 'resolver')
         WHERE record.chain_id = $1
           AND record.event_kind IN ('RecordChanged', 'RecordVersionChanged')
           AND record.source_family =
               resolver.declared_summary #>> '{classification,source_family}'
           AND (record.source_family <> 'ens_v2_resolver_l1'
                OR (record.namespace = pointer.namespace
                    AND record.source_manifest_id = declaration.manifest_id))
           AND record.logical_name_id IS NULL
           AND lower(COALESCE(
                   NULLIF(record.after_state ->> 'resolver', ''),
                   NULLIF(record.raw_fact_ref ->> 'emitting_address', '')
               )) = lower(pointer.after_state ->> 'resolver')