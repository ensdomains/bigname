-- Literal binding operators from 2abf62296b5228e1a14aa101ffed09f106125ac5, in original call order.
INSERT INTO project_scope_resources
         SELECT DISTINCT ON (binding.logical_name_id, binding.authority_arm)
                binding.resource_id
         FROM surface_bindings binding
         JOIN project_scope_names scope USING (logical_name_id)
         JOIN chain_lineage lineage
           ON lineage.chain_id = binding.chain_id
          AND lineage.block_hash = binding.block_hash
          AND lineage.block_number = binding.block_number
         WHERE binding.chain_id = $1
           AND binding.block_number <= $2
           AND binding.canonicality_state IN ('canonical', 'safe', 'finalized')
           AND lineage.canonicality_state IN ('canonical', 'safe', 'finalized')
         ORDER BY binding.logical_name_id, binding.authority_arm,
                  binding.block_number DESC,
                  COALESCE(
                      (binding.provenance ->> 'transaction_index')::bigint, -1
                  ) DESC,
                  COALESCE((binding.provenance ->> 'log_index')::bigint, -1) DESC,
                  binding.surface_binding_id DESC
         ON CONFLICT DO NOTHING;
INSERT INTO project_scope_resources
         SELECT DISTINCT pointer.resource_id
         FROM project_scope_names scope
         JOIN normalized_events pointer USING (logical_name_id)
         JOIN chain_lineage lineage
           ON lineage.chain_id = pointer.chain_id
          AND lineage.block_number = pointer.block_number
          AND lineage.block_hash = pointer.block_hash
         WHERE pointer.chain_id = $1
           AND pointer.block_number <= $2
           AND pointer.event_kind = 'ResolverChanged'
           AND pointer.source_family IN (
               'ens_v1_registry_l1', 'basenames_base_registry', 'ens_v2_root_l1'
           )
           AND pointer.resource_id IS NOT NULL
           AND pointer.consumer_visibility = 'activated'
           AND pointer.canonicality_state IN ('canonical', 'safe', 'finalized')
           AND lineage.canonicality_state IN ('canonical', 'safe', 'finalized')
         ON CONFLICT DO NOTHING;
WITH scoped_registrars AS (
             SELECT DISTINCT registrar.resource_id,
                    registrar.namespace || ':' ||
                        lower(registrar.after_state ->> 'namehash') AS logical_name_id
             FROM project_scope_resources scope
             JOIN normalized_events registrar USING (resource_id)
             JOIN chain_lineage registrar_lineage
               ON registrar_lineage.chain_id = registrar.chain_id
              AND registrar_lineage.block_hash = registrar.block_hash
              AND registrar_lineage.block_number = registrar.block_number
             WHERE registrar.chain_id = $1
               AND registrar.block_number <= $2
               AND registrar.source_family = 'ens_v1_registrar_l1'
               AND registrar.consumer_visibility = 'activated'
               AND registrar.canonicality_state IN ('canonical', 'safe', 'finalized')
               AND registrar_lineage.canonicality_state IN (
                   'canonical', 'safe', 'finalized'
               )
               AND registrar.after_state ->> 'namehash' IS NOT NULL
               AND btrim(registrar.after_state ->> 'namehash') <> ''
         )
         INSERT INTO project_scope_names
         SELECT DISTINCT wrapper.logical_name_id
         FROM scoped_registrars registrar
         JOIN normalized_events wrapper
           ON wrapper.logical_name_id = registrar.logical_name_id
          AND wrapper.after_state ->> 'wrapped_registrar_resource_id' =
              registrar.resource_id::text
         JOIN chain_lineage wrapper_lineage
           ON wrapper_lineage.chain_id = wrapper.chain_id
          AND wrapper_lineage.block_hash = wrapper.block_hash
          AND wrapper_lineage.block_number = wrapper.block_number
         WHERE wrapper.chain_id = $1
           AND wrapper.block_number <= $2
           AND wrapper.source_family = 'ens_v1_wrapper_l1'
           AND wrapper.event_kind = 'SurfaceBound'
           AND wrapper.consumer_visibility = 'activated'
           AND wrapper.canonicality_state IN ('canonical', 'safe', 'finalized')
           AND wrapper_lineage.canonicality_state IN (
               'canonical', 'safe', 'finalized'
           )
         ON CONFLICT DO NOTHING;
INSERT INTO project_scope_names
         SELECT DISTINCT binding.logical_name_id
         FROM project_scope_resources scope
         JOIN normalized_events registrar USING (resource_id)
         JOIN surface_bindings binding USING (resource_id)
         JOIN name_surfaces surface
           ON surface.logical_name_id = binding.logical_name_id
         JOIN chain_lineage lineage
           ON lineage.chain_id = binding.chain_id
          AND lineage.block_hash = binding.block_hash
          AND lineage.block_number = binding.block_number
         WHERE binding.chain_id = $1
           AND binding.block_number <= $2
           AND binding.authority_arm = 'ens_v1'
           AND binding.canonicality_state IN ('canonical', 'safe', 'finalized')
           AND lineage.canonicality_state IN ('canonical', 'safe', 'finalized')
           AND registrar.logical_name_id IS NULL
               AND registrar.source_family = 'ens_v1_registrar_l1'
               AND registrar.event_kind IN (
                   'RegistrationGranted', 'RegistrationRenewed', 'RegistrationReleased',
                   'ExpiryChanged', 'TokenControlTransferred'
               )
               AND registrar.chain_id = $1
               AND registrar.block_number <= $2
               AND registrar.canonicality_state IN ('canonical', 'safe', 'finalized')
               AND lower(registrar.after_state ->> 'namehash') = lower(surface.namehash)
         ON CONFLICT DO NOTHING;
INSERT INTO project_scope_resources
         SELECT binding.resource_id
         FROM surface_bindings binding
         JOIN project_scope_names scope USING (logical_name_id)
         JOIN chain_lineage lineage
           ON lineage.chain_id = binding.chain_id
          AND lineage.block_hash = binding.block_hash
          AND lineage.block_number = binding.block_number
         WHERE binding.chain_id = $1
           AND binding.block_number <= $2
           AND binding.canonicality_state IN ('canonical', 'safe', 'finalized')
           AND lineage.canonicality_state IN ('canonical', 'safe', 'finalized')
           AND binding.active_from < (
               SELECT block_timestamp + interval '1 second' FROM chain_lineage
               WHERE chain_id = $1 AND block_hash = $3 AND block_number = $2
           )
           AND (
               binding.active_to IS NULL OR binding.active_to >= (
                   SELECT block_timestamp + interval '1 second' FROM chain_lineage
                   WHERE chain_id = $1 AND block_hash = $3 AND block_number = $2
               )
           )
         ON CONFLICT DO NOTHING;
WITH scoped_wrappers AS (
             SELECT wrapper.normalized_event_id
             FROM project_scope_resources scope
             JOIN normalized_events wrapper
               ON wrapper.resource_id = scope.resource_id
             WHERE wrapper.source_family = 'ens_v1_wrapper_l1'
               AND wrapper.event_kind = 'SurfaceBound'
             UNION
             SELECT wrapper.normalized_event_id
             FROM project_scope_names scope
             JOIN normalized_events wrapper USING (logical_name_id)
             WHERE wrapper.source_family = 'ens_v1_wrapper_l1'
               AND wrapper.event_kind = 'SurfaceBound'
         )
         INSERT INTO project_scope_resources
         SELECT DISTINCT registrar.resource_id
         FROM scoped_wrappers scope
         JOIN normalized_events wrapper USING (normalized_event_id)
         JOIN chain_lineage wrapper_lineage
           ON wrapper_lineage.chain_id = wrapper.chain_id
          AND wrapper_lineage.block_hash = wrapper.block_hash
          AND wrapper_lineage.block_number = wrapper.block_number
         JOIN resources registrar
           ON registrar.chain_id = wrapper.chain_id
          AND registrar.resource_id =
              (wrapper.after_state ->> 'wrapped_registrar_resource_id')::uuid
         WHERE wrapper.chain_id = $1
           AND wrapper.block_number <= $2
           AND wrapper.source_family = 'ens_v1_wrapper_l1'
           AND wrapper.event_kind = 'SurfaceBound'
           AND wrapper.consumer_visibility = 'activated'
           AND wrapper.canonicality_state IN ('canonical', 'safe', 'finalized')
           AND wrapper_lineage.canonicality_state IN (
               'canonical', 'safe', 'finalized'
           )
         ON CONFLICT DO NOTHING;
INSERT INTO project_scope_resources
         SELECT DISTINCT binding.resource_id
         FROM project_scope_names scope
         JOIN surface_bindings binding USING (logical_name_id)
         JOIN name_surfaces surface USING (logical_name_id)
         JOIN chain_lineage lineage
           ON lineage.chain_id = binding.chain_id
          AND lineage.block_hash = binding.block_hash
          AND lineage.block_number = binding.block_number
         WHERE binding.chain_id = $1
           AND binding.block_number <= $2
           AND binding.authority_arm = 'ens_v1'
           AND binding.canonicality_state IN ('canonical', 'safe', 'finalized')
           AND lineage.canonicality_state IN ('canonical', 'safe', 'finalized')
           AND EXISTS (
               SELECT 1 FROM normalized_events registrar
               WHERE registrar.resource_id = binding.resource_id
                 AND registrar.logical_name_id IS NULL
               AND registrar.source_family = 'ens_v1_registrar_l1'
               AND registrar.event_kind IN (
                   'RegistrationGranted', 'RegistrationRenewed', 'RegistrationReleased',
                   'ExpiryChanged', 'TokenControlTransferred'
               )
               AND registrar.chain_id = $1
               AND registrar.block_number <= $2
               AND registrar.canonicality_state IN ('canonical', 'safe', 'finalized')
               AND lower(registrar.after_state ->> 'namehash') = lower(surface.namehash)
           )
         ON CONFLICT DO NOTHING;
INSERT INTO project_scope_names
         SELECT binding.logical_name_id
         FROM surface_bindings binding
         JOIN project_scope_resources scope USING (resource_id)
         JOIN chain_lineage lineage
           ON lineage.chain_id = binding.chain_id
          AND lineage.block_hash = binding.block_hash
          AND lineage.block_number = binding.block_number
         WHERE binding.chain_id = $1
           AND binding.block_number <= $2
           AND binding.canonicality_state IN ('canonical', 'safe', 'finalized')
           AND lineage.canonicality_state IN ('canonical', 'safe', 'finalized')
           AND binding.active_from < (
               SELECT block_timestamp + interval '1 second' FROM chain_lineage
               WHERE chain_id = $1 AND block_hash = $3 AND block_number = $2
           )
           AND (
               binding.active_to IS NULL OR binding.active_to >= (
                   SELECT block_timestamp + interval '1 second' FROM chain_lineage
                   WHERE chain_id = $1 AND block_hash = $3 AND block_number = $2
               )
           )
         ON CONFLICT DO NOTHING;
