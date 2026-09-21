CREATE TEMP TABLE project_name_serving ON COMMIT DROP AS
         SELECT authority.logical_name_id,
                pointer.resource_id AS serving_resource_id,
                pointer.chain_id AS resolver_chain_id,
                lower(pointer.after_state ->> 'resolver') AS resolver_address,
                pointer.normalized_event_id AS pointer_event_id,
                pointer.event_identity AS pointer_event_identity,
                pointer.block_number AS pointer_block_number,
                pointer.transaction_index AS pointer_transaction_index,
                pointer.log_index AS pointer_log_index,
                'retained_registry_resolver_pointer'::text AS read_reachability_basis,
                authority.owner_getter_reason
         FROM project_name_authority authority
         JOIN LATERAL (
             SELECT event.*
             FROM project_events event
             WHERE event.logical_name_id = authority.logical_name_id
               AND event.event_kind = 'ResolverChanged'
               AND event.source_family IN (
                   'ens_v1_registry_l1', 'basenames_base_registry'
               )
               AND event.resource_id = authority.ownerless_registry_resource_id
             ORDER BY event.block_number DESC NULLS LAST,
                      event.transaction_index DESC NULLS LAST,
                      event.log_index DESC NULLS LAST,
                      event.event_identity DESC
             LIMIT 1
         ) pointer ON TRUE
         JOIN project_resources resource
           ON resource.resource_id = pointer.resource_id
          AND resource.token_lineage_id IS NULL
         WHERE authority.known_ownerless_registry
           AND NULLIF(lower(pointer.after_state ->> 'resolver'), '') IS NOT NULL
           AND lower(pointer.after_state ->> 'resolver') <>
               '0x0000000000000000000000000000000000000000'
         UNION ALL
         -- An ENSv2 root-registry TLD whose registration was never observed has a token resource
         -- and a resolver pointer but no surface binding, so no authority is selected. The root
         -- registry keys the pointer by token and returns it while the label is unexpired
         -- (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L150-L155 @ ens_v2@a971bd64)
         -- (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L255-L258 @ ens_v2@a971bd64);
         -- the pointer serves as the TLD's serving resource without projecting authority. A
         -- reservation (owner zero, `LabelReserved`) does not withdraw it: the root registry sets
         -- and returns the reservation's resolver the same way
         -- (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L463-L478 @ ens_v2@a971bd64).
         -- The state-derived expiry clear and release name the resource but no logical name, so the
         -- resource is followed from the name-linked pointer; a release at or after the pointer, or
         -- a later zero/null pointer, withdraws it.
         SELECT authority.logical_name_id,
                pointer.resource_id AS serving_resource_id,
                pointer.chain_id AS resolver_chain_id,
                lower(pointer.after_state ->> 'resolver') AS resolver_address,
                pointer.normalized_event_id AS pointer_event_id,
                pointer.event_identity AS pointer_event_identity,
                pointer.block_number AS pointer_block_number,
                pointer.transaction_index AS pointer_transaction_index,
                pointer.log_index AS pointer_log_index,
                'root_registry_resolver_pointer'::text AS read_reachability_basis,
                NULL::text AS owner_getter_reason
         FROM project_name_authority authority
         JOIN LATERAL (
             SELECT event.*
             FROM project_events event
             WHERE event.event_kind = 'ResolverChanged'
               AND event.source_family = 'ens_v2_root_l1'
               AND event.resource_id IS NOT NULL
               AND (
                   event.logical_name_id = authority.logical_name_id
                   OR (
                       event.logical_name_id IS NULL
                       AND EXISTS (
                           SELECT 1 FROM project_events linked
                           WHERE linked.logical_name_id = authority.logical_name_id
                             AND linked.resource_id = event.resource_id
                             AND linked.source_family = 'ens_v2_root_l1'
                             AND linked.event_kind = 'ResolverChanged'
                       )
                   )
               )
             ORDER BY event.block_number DESC NULLS LAST,
                      event.transaction_index DESC NULLS LAST,
                      event.log_index DESC NULLS LAST,
                      event.event_identity DESC
             LIMIT 1
         ) pointer ON TRUE
         WHERE authority.unsupported_reason = 'current_authority_not_projected'
           AND authority.selected_binding_id IS NULL
           AND NULLIF(lower(pointer.after_state ->> 'resolver'), '') IS NOT NULL
           AND lower(pointer.after_state ->> 'resolver') <>
               '0x0000000000000000000000000000000000000000'
           AND NOT EXISTS (
               SELECT 1 FROM project_events release
               WHERE release.resource_id = pointer.resource_id
                 AND release.source_family = 'ens_v2_root_l1'
                 AND release.event_kind = 'RegistrationReleased'
                 AND (
                     release.block_number,
                     COALESCE(release.transaction_index, -1),
                     COALESCE(release.log_index, -1)
                 ) >= (
                     pointer.block_number,
                     COALESCE(pointer.transaction_index, -1),
                     COALESCE(pointer.log_index, -1)
                 )
           );
