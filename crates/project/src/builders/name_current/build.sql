
        WITH v2_lifecycle_events AS (
            SELECT event.*, COALESCE(event.resource_id::text, (
                SELECT linked.resource_id::text FROM project_events linked
                WHERE linked.logical_name_id = event.logical_name_id AND linked.resource_id IS NOT NULL
                  AND linked.event_kind IN ('RegistrationGranted', 'RegistrationReserved') AND linked.source_family IN ('ens_v2_root_l1', 'ens_v2_registry_l1', 'ens_v2_registrar_l1')
                  AND COALESCE(linked.after_state ->> 'registry_contract_instance_id', linked.raw_fact_ref ->> 'emitting_address', linked.after_state ->> 'registry') = COALESCE(event.after_state ->> 'registry_contract_instance_id', event.raw_fact_ref ->> 'emitting_address', event.after_state ->> 'registry') AND linked.after_state ->> 'token_id' = event.after_state ->> 'token_id'
                ORDER BY linked.block_number DESC NULLS LAST, linked.normalized_event_id DESC LIMIT 1
            ), NULLIF(CONCAT(COALESCE(event.after_state ->> 'registry_contract_instance_id', event.raw_fact_ref ->> 'emitting_address', event.after_state ->> 'registry'), ':', event.after_state ->> 'token_id'), ':')) AS lifecycle_key
            FROM project_events event
            WHERE event.source_family IN ('ens_v2_root_l1', 'ens_v2_registry_l1', 'ens_v2_registrar_l1')
        )
        INSERT INTO project_stage_name_current (
            logical_name_id, namespace, raw_name, namehash,
            surface_binding_id, resource_id, serving_resource_id,
            token_lineage_id, binding_kind,
            declared_summary, support_status, unsupported_reason, provenance,
            chain_positions, canonicality_summary, manifest_version
        )
        SELECT surface.logical_name_id, surface.namespace, surface.raw_name,
               surface.namehash, row_identity.surface_binding_id, row_identity.resource_id,
               serving.serving_resource_id,
               CASE WHEN row_identity.resource_id IS NULL THEN NULL
                   ELSE resource.token_lineage_id END,
               row_identity.binding_kind,
               jsonb_build_object(
                   'registration', jsonb_build_object(
                       'status', CASE
                           WHEN selected_authority.known_ownerless_registry
                               THEN 'unregistered'
                           ELSE CASE selected_registration.event_kind
                               WHEN 'RegistrationReleased' THEN 'released'
                               WHEN 'RegistrationReserved' THEN 'reserved'
                               WHEN 'RegistrationGranted' THEN 'active'
                               WHEN 'RegistrationRenewed' THEN 'active'
                               ELSE CASE WHEN binding.resource_id IS NOT NULL
                                   THEN 'active' ELSE NULL END
                           END
                       END,
                       'authority_kind', authority_context.authority_kind,
                       'authority_key', authority_context.authority_key,
                       -- The registration's identity is its BaseRegistrar lease, also while the
                       -- name is wrapped and whether it was wrapped at or after registration.
                       -- ENSv2 registrations keep the bound resource as their identity.
                       'resource_id', CASE
                           WHEN NOT COALESCE(selected_registration.is_v2_lifecycle, false)
                               THEN lifecycle.registrar_resource_id END,
                       'registrant', CASE WHEN NOT effective_wrapper.owner_lapsed
                           THEN registrant.registrant END,
                       'expiry', CASE
                           WHEN selected_registration.is_v2_lifecycle
                            AND selected_registration.event_kind IS NOT NULL
                            AND selected_registration.resource_id IS DISTINCT FROM binding.resource_id
                               THEN selected_registration.after_state -> 'expiry'
                           ELSE COALESCE(to_jsonb(expiry.expiry_seconds), CASE
                               WHEN selected_registration.is_v2_lifecycle
                                   THEN selected_registration.after_state -> 'expiry' END,
                               -- A wrapped ENSv1 name with no registrar lease (a wrapped
                               -- subname) expires when its NameWrapper entry does; that is
                               -- the only expiry the chain holds for it.
                               CASE WHEN wrapper.wrapper_state IS NOT NULL
                                     AND NOT COALESCE(selected_registration.is_v2_lifecycle, false)
                                   THEN to_jsonb(wrapper_expiry.servable_expiry_seconds) END)
                       END,
                       'registered_at', registration_grant.block_timestamp,
                       'created_at', created.block_timestamp,
                       'released_at', selected_registration.after_state -> 'released_at',
                       'latest_event_kind', CASE
                           WHEN selected_registration.event_kind = 'RegistrationReserved' THEN selected_registration.event_kind
                           WHEN selected_registration.is_v2_lifecycle THEN COALESCE(v2_registration_latest.event_kind, selected_registration.event_kind)
                           ELSE COALESCE(registration_latest.event_kind,
                               selected_registration.event_kind)
                       END
                   ) || CASE
                       WHEN selected_authority.known_ownerless_registry
                           THEN jsonb_build_object(
                               'authority_kind', NULL, 'authority_key', NULL,
                               'registrant', NULL, 'expiry', NULL
                           )
                       -- A released ENSv1 lease whose custody was not revived is a tombstone:
                       -- the registrar lease is gone, and whether nothing current owns the node
                       -- or the registry still holds the owner a transfer without `reclaim`
                       -- left behind, no current registrant or authority is served. `expiry`
                       -- stays the lapsed lease's own expiry, and the holder and authority the
                       -- lease had when it lapsed move into `lapsed_registration`, a block only
                       -- a tombstone carries and nothing reads as current state.
                       WHEN COALESCE(selected_authority.released_v1_tombstone, false)
                           THEN jsonb_build_object('authority_kind', NULL, 'authority_key', NULL,
                               'registrant', NULL,
                               'lapsed_registration', jsonb_build_object(
                                   'registrant', registrant.registrant,
                                   'authority_kind', lapsed_authority.authority_kind,
                                   'authority_key', lapsed_authority.authority_key,
                                   'released_at', selected_registration.after_state -> 'released_at'))
                       -- An ENSv2 registration lapsed by path expiry keeps its lapsed expiry as
                       -- a readable detail (the registry entry still holds it); an explicit
                       -- release clears the entry, so nothing current remains.
                       WHEN selected_registration.event_kind = 'RegistrationReleased'
                        AND selected_authority.selected_authority_arm = 'ens_v2'
                        AND selected_registration.after_state ->> 'source_event' = 'RegistryPathExpired'
                       THEN jsonb_build_object('authority_kind', NULL, 'authority_key', NULL,
                           'registrant', NULL)
                       WHEN selected_registration.event_kind = 'RegistrationReleased'
                        AND selected_authority.selected_authority_arm = 'ens_v2'
                       THEN jsonb_build_object('authority_kind', NULL, 'authority_key', NULL,
                           'registrant', NULL, 'expiry', NULL) ELSE '{}'::jsonb END,
                   'control', CASE
                       WHEN selected_authority.known_ownerless_registry
                           THEN jsonb_build_object('status', 'unregistered')
                       WHEN COALESCE(selected_authority.released_v1_tombstone, false)
                           THEN jsonb_build_object('status', 'unregistered')
                       WHEN selected_registration.event_kind = 'RegistrationReleased'
                        AND selected_authority.selected_authority_arm = 'ens_v2'
                           THEN jsonb_build_object('status', 'unregistered')
                       WHEN COALESCE(resource.provenance ->> 'authority_kind',
                           registration_grant.after_state ->> 'authority_kind') IN ('wrapper', 'name_wrapper')
                           THEN jsonb_build_object('status', 'unsupported', 'unsupported_reason',
                               'ENSv1 wrapper effective control is not yet projected')
                       ELSE jsonb_build_object(
                           'status', CASE
                               WHEN selected_registration.event_kind = 'RegistrationReserved'
                                   THEN selected_registration.after_state ->> 'status'
                               ELSE COALESCE(status.after_state ->> 'status',
                                   selected_registration.after_state ->> 'status')
                           END,
                           'expiry', CASE
                               WHEN COALESCE(expiry.expiry_seconds, CASE
                                        WHEN wrapper.wrapper_state IS NOT NULL
                                         AND NOT COALESCE(selected_registration.is_v2_lifecycle, false)
                                            THEN wrapper_expiry.servable_expiry_seconds END)
                                    IS NULL THEN NULL
                               ELSE to_jsonb(to_char(to_timestamp(COALESCE(expiry.expiry_seconds,
                                        wrapper_expiry.servable_expiry_seconds))
                                   AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"'))
                           END,
                           'registrant', CASE WHEN NOT effective_wrapper.owner_lapsed
                               THEN registrant.registrant END,
                           'registry_owner', control_owner.registry_owner,
                           'latest_event_kind', control.latest_event_kind
                       )
                   END,
                   'resolver', jsonb_build_object(
                       'chain_id', CASE
                           WHEN resolver.resolver_address IS NOT NULL AND resolver.resolver_address <> '0x0000000000000000000000000000000000000000'
                            AND (
                                -- The serving pointer is served as selected, including through a
                                -- root-registry TLD reservation.
                                resolver.normalized_event_id = serving.pointer_event_id
                                OR (
                                    NOT (COALESCE(selected_registration.event_kind, '') IN ('RegistrationReleased', 'RegistrationReserved') AND selected_authority.selected_authority_arm = 'ens_v2')
                                    AND NOT COALESCE(selected_authority.released_v1_tombstone, false)
                                )
                            )
                               THEN resolver.chain_id
                           ELSE NULL
                       END,
                       'address', CASE
                           WHEN resolver.resolver_address IS NOT NULL AND resolver.resolver_address <> '0x0000000000000000000000000000000000000000'
                            AND (
                                -- The serving pointer is served as selected, including through a
                                -- root-registry TLD reservation.
                                resolver.normalized_event_id = serving.pointer_event_id
                                OR (
                                    NOT (COALESCE(selected_registration.event_kind, '') IN ('RegistrationReleased', 'RegistrationReserved') AND selected_authority.selected_authority_arm = 'ens_v2')
                                    AND NOT COALESCE(selected_authority.released_v1_tombstone, false)
                                )
                            )
                               THEN resolver.resolver_address
                           ELSE NULL
                       END,
                       'latest_event_kind', resolver.event_kind
                   ),
                   'record_inventory', jsonb_build_object(
                       'status', 'unsupported',
                       'unsupported_reason',
                           'record_inventory remains unsupported in the ENSv1 name_current rebuild'
                   ),
                   'history', jsonb_build_object(
                       'surface_head', surface_history.pointer,
                       'resource_head', resource_history.pointer
                   ),
                   'coverage', jsonb_build_object(
                       'status', 'projected',
                       'exhaustiveness', 'not_asserted',
                       'source_classes_considered', CASE
                           WHEN corpus.has_ens_v2 THEN jsonb_build_array(
                               'ens_v2_root_l1', 'ens_v2_registry_l1', 'ens_v2_registrar_l1'
                           )
                           WHEN surface.namespace IN ('ens', 'basenames')
                               THEN jsonb_build_array('ensv1_registry_path')
                           ELSE '[]'::jsonb
                       END,
                       'unsupported_reason', to_jsonb(support.unsupported_reason),
                       'enumeration_basis', CASE
                           WHEN serving.serving_resource_id IS NOT NULL
                               THEN 'event_linked_registry_resolver'
                           WHEN corpus.has_ens_v2 THEN 'exact_name_profile'
                           ELSE 'exact_name'
                       END
                   )
               ) || CASE
                   WHEN effective_wrapper.wrapper_state IS NOT NULL
                       THEN jsonb_build_object(
                           'wrapper_state', effective_wrapper.wrapper_state,
                           'wrapper_fuses', jsonb_build_object(
                               'fuses', effective_wrapper.fuses,
                               'cannot_unwrap', (effective_wrapper.fuses & 1) <> 0,
                               'cannot_burn_fuses', (effective_wrapper.fuses & 2) <> 0,
                               'cannot_transfer', (effective_wrapper.fuses & 4) <> 0,
                               'cannot_set_resolver', (effective_wrapper.fuses & 8) <> 0,
                               'cannot_set_ttl', (effective_wrapper.fuses & 16) <> 0,
                               'cannot_create_subdomain', (effective_wrapper.fuses & 32) <> 0,
                               'cannot_approve', (effective_wrapper.fuses & 64) <> 0,
                               'parent_cannot_control', (effective_wrapper.fuses & 65536) <> 0,
                               'is_dot_eth', (effective_wrapper.fuses & 131072) <> 0,
                               'can_extend_expiry', (effective_wrapper.fuses & 262144) <> 0
                           )
                       )
                   ELSE '{}'::jsonb
               END,
               support.support_status,
               support.unsupported_reason,
               jsonb_build_object(
                   'chain_id', $1,
                   'surface_block_number', surface.block_number,
                   'registrant_event_id', registrant.normalized_event_id,
                   'selected_event_ids', COALESCE(evidence.event_ids, '[]'::jsonb),
                   'raw_fact_refs', COALESCE(evidence.raw_fact_refs, '[]'::jsonb),
                   'manifest_versions', COALESCE(
                       evidence.manifest_versions, '[]'::jsonb
                   ),
                   'derivation_kind', 'name_current_rebuild',
                   'authority_selection', jsonb_strip_nulls(jsonb_build_object(
                       'authority_arm', selected_authority.selected_authority_arm,
                       'surface_binding_id', selected_authority.selected_binding_id,
                       'resource_id', selected_authority.selected_resource_id,
                       'epoch_start_position', selected_authority.authority_epoch_start_position,
                       'proof_kind', selected_authority.authority_proof_kind,
                       'proof_event_id', selected_authority.authority_proof_event_id,
                       'proof_event_identity', selected_authority.authority_proof_event_identity,
                       'transition_id', selected_authority.authority_transition_id,
                       'lifecycle_state', selected_authority.lifecycle_state,
                       'deployment_profile', selected_authority.deployment_profile,
                       'resource_authority_context', selected_authority.resource_authority_context,
                       'unsupported_reason', selected_authority.unsupported_reason
                   )),
                   'read_reachability', jsonb_strip_nulls(jsonb_build_object(
                       'serving_resource_id', serving.serving_resource_id,
                       'basis', serving.read_reachability_basis,
                       'owner_getter_reason', serving.owner_getter_reason,
                       'pointer_event_id', serving.pointer_event_id,
                       'pointer_event_identity', serving.pointer_event_identity
                   ))
               ) || jsonb_strip_nulls(jsonb_build_object(
                   'resolver_pointer_source_family', resolver.source_family
               )),
               jsonb_build_object(
                   CASE $1
                       WHEN 'ethereum-mainnet' THEN 'ethereum'
                       WHEN 'base-mainnet' THEN 'base'
                       ELSE $1
                   END,
                   jsonb_build_object(
                       'chain_id', $1,
                       'block_number', $2,
                       'block_hash', $3,
                       'timestamp', (
                           SELECT lineage.block_timestamp
                           FROM chain_lineage lineage
                           WHERE lineage.chain_id = $1
                             AND lineage.block_number = $2
                             AND lineage.block_hash = $3
                       )
                   )
               ),
               jsonb_build_object(
                   'state', 'canonical_lineage',
                   'target_block_number', $2,
                   'target_block_hash', $3
               ),
               GREATEST(
                   COALESCE(registration.manifest_version, 1),
                   COALESCE(authority.manifest_version, 1),
                   COALESCE(resolver.manifest_version, 1),
                   COALESCE(evidence.manifest_version, 1),
                   COALESCE(
                       NULLIF(surface.provenance ->> 'manifest_version', '')::bigint,
                       NULLIF(binding.provenance ->> 'manifest_version', '')::bigint,
                       NULLIF(resource.provenance ->> 'manifest_version', '')::bigint,
                       1
                   )
               )
        FROM project_surfaces surface
        LEFT JOIN project_name_authority selected_authority USING (logical_name_id)
        LEFT JOIN project_name_serving serving USING (logical_name_id)
        LEFT JOIN project_bindings binding USING (logical_name_id)
        LEFT JOIN LATERAL (
            SELECT event.* FROM project_authority_events event
            WHERE event.logical_name_id = surface.logical_name_id AND COALESCE(selected_authority.selected_authority_arm, 'ens_v2') <> 'ens_v2'
              AND event.event_kind IN ('RegistrationGranted', 'RegistrationRenewed', 'RegistrationReleased', 'RegistrationReserved')
            ORDER BY event.block_number DESC NULLS LAST, event.transaction_index DESC NULLS LAST,
                     event.log_index DESC NULLS LAST, event.normalized_event_id DESC
            LIMIT 1
        ) registration ON TRUE
        LEFT JOIN LATERAL (
            SELECT event.event_kind FROM project_authority_events event
            WHERE event.logical_name_id = surface.logical_name_id AND COALESCE(selected_authority.selected_authority_arm, 'ens_v2') <> 'ens_v2'
              AND event.event_kind IN ('RegistrationGranted', 'RegistrationRenewed', 'RegistrationReleased', 'RegistrationReserved', 'ExpiryChanged')
            ORDER BY event.block_number DESC NULLS LAST, event.transaction_index DESC NULLS LAST,
                     event.log_index DESC NULLS LAST, event.normalized_event_id DESC
            LIMIT 1
        ) registration_latest ON TRUE
        LEFT JOIN LATERAL (
            SELECT event.event_kind, event.after_state, event.resource_id, event.lifecycle_key
            FROM (SELECT DISTINCT ON (event.lifecycle_key) event.* FROM v2_lifecycle_events event
            WHERE event.logical_name_id = surface.logical_name_id AND (
                  event.event_kind IN ('RegistrationGranted', 'RegistrationReserved') OR
                  (event.event_kind = 'RegistrationReleased' AND ((event.after_state ->> 'source_event' = 'RegistryPathExpired' AND event.after_state ->> 'derived_from' = 'interpreter_state' AND event.after_state ->> 'terminal_reason' = 'registry_name_binding_expired')
                        OR EXISTS (SELECT 1 FROM v2_lifecycle_events active WHERE active.logical_name_id = event.logical_name_id AND active.lifecycle_key = event.lifecycle_key
                            AND active.event_kind IN ('RegistrationGranted', 'RegistrationReserved') AND ROW(COALESCE(active.block_number, -1), active.normalized_event_id) < ROW(COALESCE(event.block_number, -1), event.normalized_event_id)
                            AND NOT EXISTS (SELECT 1 FROM v2_lifecycle_events expiry WHERE expiry.logical_name_id = event.logical_name_id AND expiry.lifecycle_key = event.lifecycle_key
                                AND expiry.event_kind = 'RegistrationReleased' AND expiry.after_state ->> 'source_event' = 'RegistryPathExpired' AND expiry.after_state ->> 'derived_from' = 'interpreter_state' AND expiry.after_state ->> 'terminal_reason' = 'registry_name_binding_expired' AND ROW(COALESCE(expiry.block_number, -1), expiry.normalized_event_id) BETWEEN ROW(COALESCE(active.block_number, -1), active.normalized_event_id) AND ROW(COALESCE(event.block_number, -1), event.normalized_event_id)
                            )))
              ))
              AND NOT EXISTS (SELECT 1 FROM v2_lifecycle_events later WHERE later.logical_name_id = event.logical_name_id AND later.lifecycle_key = event.lifecycle_key
                    AND ((event.event_kind = 'RegistrationReleased' AND later.event_kind IN ('RegistrationGranted', 'RegistrationReserved')) OR (event.event_kind <> 'RegistrationReleased' AND later.event_kind = 'RegistrationReleased'))
                    AND ROW(COALESCE(later.block_number, -1), later.normalized_event_id) > ROW(COALESCE(event.block_number, -1), event.normalized_event_id)
              )
            ORDER BY event.lifecycle_key, event.block_number DESC NULLS LAST, event.normalized_event_id DESC) event
            ORDER BY (binding.resource_id IS NOT NULL AND event.lifecycle_key IS NOT DISTINCT FROM binding.resource_id::text AND event.event_kind <> 'RegistrationReleased') DESC,
                     (event.event_kind = 'RegistrationReleased'),
                     (binding.resource_id IS NOT NULL AND event.lifecycle_key IS NOT DISTINCT FROM binding.resource_id::text) DESC,
                     event.block_number DESC NULLS LAST, event.normalized_event_id DESC
            LIMIT 1
        ) registration_current ON TRUE
        CROSS JOIN LATERAL (
            SELECT CASE WHEN arm.is_v2 THEN CASE WHEN arm.use_event THEN registration_current.event_kind END ELSE registration.event_kind END AS event_kind,
                   CASE WHEN arm.is_v2 THEN CASE WHEN arm.use_event THEN registration_current.after_state END ELSE registration.after_state END AS after_state,
                   CASE WHEN arm.is_v2 THEN CASE WHEN arm.use_event THEN registration_current.resource_id END ELSE registration.resource_id END AS resource_id,
                   CASE WHEN arm.is_v2 THEN CASE WHEN arm.use_event THEN registration_current.lifecycle_key END END AS lifecycle_key, arm.is_v2 AS is_v2_lifecycle
            FROM (SELECT COALESCE(selected_authority.selected_authority_arm, 'ens_v2') = 'ens_v2' AS is_v2, NOT (registration_current.event_kind = 'RegistrationReleased' AND binding.resource_id IS NOT NULL AND registration_current.resource_id IS DISTINCT FROM binding.resource_id) AS use_event) arm
        ) selected_registration CROSS JOIN LATERAL (
            SELECT COALESCE((
                SELECT (current_wrapper.after_state ->> 'wrapped_registrar_resource_id')::uuid
                FROM project_events current_wrapper
                WHERE current_wrapper.resource_id = selected_registration.resource_id
                  AND current_wrapper.event_kind = 'SurfaceBound'
                  AND current_wrapper.source_family = 'ens_v1_wrapper_l1'
                  AND current_wrapper.after_state ->> 'wrapped_registrar_resource_id' IS NOT NULL
                ORDER BY current_wrapper.block_number DESC NULLS LAST,
                         current_wrapper.normalized_event_id DESC
                LIMIT 1
            ), selected_registration.resource_id) AS registrar_resource_id
        ) lifecycle CROSS JOIN LATERAL (
            SELECT CASE WHEN identity.mismatch THEN NULL ELSE binding.surface_binding_id END AS surface_binding_id,
                   CASE WHEN identity.mismatch THEN NULL ELSE binding.resource_id END AS resource_id, CASE WHEN identity.mismatch THEN NULL ELSE binding.binding_kind END AS binding_kind,
                   CASE WHEN identity.has_lifecycle THEN selected_registration.resource_id ELSE binding.resource_id END AS event_resource_id FROM (SELECT selected_registration.is_v2_lifecycle AND selected_registration.event_kind IS NOT NULL AS has_lifecycle,
                   selected_registration.is_v2_lifecycle AND selected_registration.event_kind IS NOT NULL AND selected_registration.resource_id IS DISTINCT FROM binding.resource_id AS mismatch) identity) row_identity
        LEFT JOIN LATERAL (
            SELECT event.event_kind FROM v2_lifecycle_events event
            WHERE selected_registration.is_v2_lifecycle AND event.logical_name_id = surface.logical_name_id
              AND event.lifecycle_key IS NOT DISTINCT FROM COALESCE(selected_registration.lifecycle_key, row_identity.event_resource_id::text)
              AND event.event_kind IN ('RegistrationGranted', 'RegistrationRenewed', 'RegistrationReleased', 'RegistrationReserved', 'ExpiryChanged')
            ORDER BY event.block_number DESC NULLS LAST, event.transaction_index DESC NULLS LAST,
                     event.log_index DESC NULLS LAST, event.normalized_event_id DESC LIMIT 1
        ) v2_registration_latest ON TRUE
        LEFT JOIN project_resources resource ON resource.resource_id = row_identity.event_resource_id LEFT JOIN LATERAL (
            SELECT event.*, CASE
                WHEN event.source_family = 'ens_v1_registrar_l1'
                 AND event.after_state ->> 'state_derived' = 'true'
                 AND event.after_state ->> 'surface_materialization' = 'true'
                 AND event.after_state ->> 'registrar_surface_snapshot' = 'true'
                THEN to_timestamp((event.after_state ->> 'original_registered_at')::bigint)
                ELSE lineage.block_timestamp END AS block_timestamp
            FROM project_authority_events event
            LEFT JOIN chain_lineage lineage
              ON lineage.chain_id = event.chain_id
             AND lineage.block_number = event.block_number
             AND lineage.block_hash = event.block_hash
            WHERE event.logical_name_id = surface.logical_name_id AND (NOT selected_registration.is_v2_lifecycle OR EXISTS (SELECT 1 FROM v2_lifecycle_events selected_event WHERE selected_event.normalized_event_id = event.normalized_event_id AND selected_event.lifecycle_key IS NOT DISTINCT FROM COALESCE(selected_registration.lifecycle_key, row_identity.event_resource_id::text)))
              AND event.event_kind = 'RegistrationGranted'
            ORDER BY event.block_number DESC NULLS LAST,
                     event.transaction_index DESC NULLS LAST, event.log_index DESC NULLS LAST,
                     event.normalized_event_id DESC
            LIMIT 1
        ) registration_grant ON TRUE
        LEFT JOIN LATERAL (
            SELECT event.after_state ->> 'authority_kind' AS authority_kind,
                   event.after_state ->> 'authority_key' AS authority_key
            FROM project_authority_events event
            WHERE event.logical_name_id = surface.logical_name_id AND (NOT selected_registration.is_v2_lifecycle OR EXISTS (SELECT 1 FROM v2_lifecycle_events selected_event WHERE selected_event.normalized_event_id = event.normalized_event_id AND selected_event.lifecycle_key IS NOT DISTINCT FROM COALESCE(selected_registration.lifecycle_key, row_identity.event_resource_id::text)))
              AND (
                    event.event_kind IN ('RegistrationGranted', 'AuthorityEpochChanged')
                 OR (event.event_kind = 'SurfaceBound' AND event.after_state @>
                     '{"state_derived":true,"authority_kind":"registry_only"}')
              )
              -- A successor lease granted by `registerOnly` under a registry-only binding names
              -- the registration, not the authority: the registrar did not touch the registry,
              -- so the binding's registry-only epoch stays the authority the name is under.
              -- (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L118-L152 @ ens_v1@91c966f)
              AND NOT (
                  event.event_kind = 'RegistrationGranted'
                  AND EXISTS (
                      SELECT 1 FROM project_registry_only_handoffs handoff
                      WHERE handoff.surface_binding_id = binding.surface_binding_id
                        AND handoff.lease_resource_id = event.resource_id
                        AND handoff.lease_resource_id <> handoff.predecessor_resource_id
                  )
              )
            ORDER BY event.block_number DESC NULLS LAST,
                     event.transaction_index DESC NULLS LAST, event.log_index DESC NULLS LAST,
                     event.normalized_event_id DESC
            LIMIT 1
        ) authority_context ON TRUE
        LEFT JOIN LATERAL (
            -- The authority the released lease binding had before its closing epoch cleared
            -- it: the NameWrapper for a lease that lapsed while wrapped. Only a released
            -- tombstone serves it, inside `lapsed_registration`.
            SELECT event.after_state ->> 'authority_kind' AS authority_kind,
                   event.after_state ->> 'authority_key' AS authority_key
            FROM project_authority_events event
            WHERE COALESCE(selected_authority.released_v1_tombstone, false)
              AND event.resource_id = resource.resource_id
              AND event.event_kind IN ('RegistrationGranted', 'AuthorityEpochChanged')
              AND event.after_state ->> 'authority_kind' IS NOT NULL
            ORDER BY event.block_number DESC NULLS LAST,
                     event.transaction_index DESC NULLS LAST, event.log_index DESC NULLS LAST,
                     event.normalized_event_id DESC
            LIMIT 1
        ) lapsed_authority ON TRUE
        LEFT JOIN LATERAL (
            SELECT lower(CASE event.event_kind
                       WHEN 'TokenControlTransferred' THEN event.after_state ->> 'to'
                       WHEN 'RegistrationReleased' THEN event.before_state ->> 'registrant'
                       ELSE event.after_state ->> 'registrant'
                   END) AS registrant,
                   event.normalized_event_id
            FROM project_registration_events event
            WHERE event.logical_name_id = surface.logical_name_id AND (NOT selected_registration.is_v2_lifecycle OR EXISTS (SELECT 1 FROM v2_lifecycle_events selected_event WHERE selected_event.normalized_event_id = event.normalized_event_id AND selected_event.lifecycle_key IS NOT DISTINCT FROM COALESCE(selected_registration.lifecycle_key, row_identity.event_resource_id::text)))
              AND event.event_kind IN (
                  'RegistrationGranted', 'RegistrationReleased', 'TokenControlTransferred'
              )
              AND NOT (
                  event.event_kind = 'RegistrationReleased'
                  AND event.source_family = 'ens_v1_registrar_l1'
                  AND EXISTS (
                      SELECT 1
                      FROM project_events wrapper_binding
                      WHERE wrapper_binding.logical_name_id = event.logical_name_id
                        AND wrapper_binding.source_family = 'ens_v1_wrapper_l1'
                        AND wrapper_binding.event_kind = 'SurfaceBound'
                        -- The release names the BaseRegistrar token owner, which for a wrapped
                        -- lease is the NameWrapper contract: wrapping moves the registrar token
                        -- to the NameWrapper, and registering through it mints the token to the
                        -- NameWrapper. The holder is the NameWrapper token owner, so the fold
                        -- skips the release of a lease the name's wrap stands for: the wrap
                        -- recorded the lease, or a controller event granted the lease in the
                        -- wrap's transaction after NameWrapped recorded nothing.
                        -- (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L265 @ ens_v1@91c966f)
                        -- (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L297 @ ens_v1@91c966f)
                        AND (
                            wrapper_binding.after_state ->>
                                'wrapped_registrar_resource_id' = event.resource_id::text
                            OR EXISTS (
                                SELECT 1 FROM project_events registration
                                WHERE registration.resource_id = event.resource_id
                                  AND registration.source_family = 'ens_v1_registrar_l1'
                                  AND registration.event_kind = 'RegistrationGranted'
                                  AND registration.logical_name_id =
                                      wrapper_binding.logical_name_id
                                  AND registration.transaction_hash =
                                      wrapper_binding.transaction_hash
                            )
                        )
                  )
              )
              AND CASE event.event_kind
                      WHEN 'TokenControlTransferred' THEN event.after_state ->> 'to'
                      WHEN 'RegistrationReleased' THEN event.before_state ->> 'registrant'
                      ELSE event.after_state ->> 'registrant'
                  END IS NOT NULL
            ORDER BY event.block_number DESC NULLS LAST,
                     event.transaction_index DESC NULLS LAST, event.log_index DESC NULLS LAST,
                     event.normalized_event_id DESC
            LIMIT 1
        ) registrant ON TRUE
        LEFT JOIN LATERAL (
            SELECT CASE
                       WHEN jsonb_typeof(event.after_state -> 'expiry') = 'number'
                        AND (event.after_state ->> 'expiry')::numeric =
                            trunc((event.after_state ->> 'expiry')::numeric)
                        AND (event.after_state ->> 'expiry')::numeric BETWEEN
                            -377705116800 AND 253402300799
                           THEN (event.after_state ->> 'expiry')::bigint
                       ELSE NULL
                   END AS expiry_seconds
            FROM project_authority_events event
            WHERE event.logical_name_id = surface.logical_name_id AND (NOT selected_registration.is_v2_lifecycle OR EXISTS (SELECT 1 FROM v2_lifecycle_events selected_event WHERE selected_event.normalized_event_id = event.normalized_event_id AND selected_event.lifecycle_key IS NOT DISTINCT FROM COALESCE(selected_registration.lifecycle_key, row_identity.event_resource_id::text)))
              AND event.event_kind IN (
                  'RegistrationGranted', 'RegistrationRenewed', 'RegistrationReleased',
                  'ExpiryChanged'
              )
              AND NOT (
                  event.event_kind = 'ExpiryChanged'
                  AND (
                      event.source_family = 'ens_v1_wrapper_l1'
                      OR (
                          event.source_family = 'ens_v1_registrar_l1'
                          AND COALESCE(event.after_state ->> 'source_event', '') =
                              'NameRenewed'
                          AND COALESCE(event.after_state ->> 'authority_kind', '') =
                              'wrapper'
                      )
                  )
              )
              AND (
                  event.event_kind = 'RegistrationGranted'
                  OR jsonb_typeof(event.after_state -> 'expiry') = 'number'
              )
            ORDER BY event.block_number DESC NULLS LAST,
                     event.transaction_index DESC NULLS LAST, event.log_index DESC NULLS LAST,
                     event.normalized_event_id DESC
            LIMIT 1
        ) expiry ON TRUE
        LEFT JOIN LATERAL (
            SELECT CASE event.after_state ->> 'wrapper_state'
                       WHEN 'wrapped' THEN 'wrapped'
                       WHEN 'emancipated' THEN 'emancipated'
                       WHEN 'locked' THEN 'locked'
                   END AS wrapper_state,
                   CASE
                       WHEN jsonb_typeof(event.after_state -> 'fuses') = 'number'
                        AND (event.after_state ->> 'fuses')::numeric >= 0
                        AND (event.after_state ->> 'fuses')::numeric <= 4294967295
                           THEN (event.after_state ->> 'fuses')::bigint
                   END AS fuses
            FROM project_authority_events event
            WHERE event.resource_id = resource.resource_id
              AND event.event_kind = 'PermissionScopeChanged'
              AND event.source_family = 'ens_v1_wrapper_l1'
            ORDER BY event.block_number DESC NULLS LAST,
                     event.transaction_index DESC NULLS LAST,
                     event.log_index DESC NULLS LAST,
                     event.normalized_event_id DESC
            LIMIT 1
        ) wrapper ON TRUE
        LEFT JOIN LATERAL (
            SELECT CASE
                       WHEN jsonb_typeof(event.after_state -> 'expiry') = 'number'
                        AND (event.after_state ->> 'expiry')::numeric >= 0
                        AND (event.after_state ->> 'expiry')::numeric <=
                            18446744073709551615
                           THEN (event.after_state ->> 'expiry')::numeric
                   END AS expiry_seconds,
                   -- The same word as a servable timestamp: a wrapped name without a registrar
                   -- lease (a wrapped subname) expires when its NameWrapper entry does. Zero
                   -- means the parent set no expiry; words past the timestamp range are dropped
                   -- like malformed registrar expiries.
                   CASE
                       WHEN jsonb_typeof(event.after_state -> 'expiry') = 'number'
                        AND (event.after_state ->> 'expiry')::numeric =
                            trunc((event.after_state ->> 'expiry')::numeric)
                        AND (event.after_state ->> 'expiry')::numeric BETWEEN 1 AND 253402300799
                           THEN (event.after_state ->> 'expiry')::bigint
                   END AS servable_expiry_seconds
            FROM project_authority_events event
            WHERE event.resource_id = resource.resource_id
              AND event.event_kind = 'ExpiryChanged'
              AND (
                    event.source_family = 'ens_v1_wrapper_l1'
                 OR (
                        event.source_family = 'ens_v1_registrar_l1'
                    AND event.after_state ->> 'source_event' = 'NameRenewed'
                    AND event.after_state ->> 'authority_kind' = 'wrapper'
                 )
              )
            ORDER BY event.block_number DESC NULLS LAST,
                     event.transaction_index DESC NULLS LAST,
                     event.log_index DESC NULLS LAST,
                     event.normalized_event_id DESC
            LIMIT 1
        ) wrapper_expiry ON TRUE
        LEFT JOIN LATERAL (
            SELECT extract(epoch FROM lineage.block_timestamp) AS epoch_seconds
            FROM chain_lineage lineage
            WHERE lineage.chain_id = $1
              AND lineage.block_number = $2
              AND lineage.block_hash = $3
        ) target_time ON TRUE
        LEFT JOIN LATERAL (
            SELECT CASE
                       WHEN wrapper.wrapper_state IS NULL
                         OR wrapper.fuses IS NULL
                         OR wrapper_expiry.expiry_seconds IS NULL
                         OR target_time.epoch_seconds IS NULL THEN NULL
                       WHEN wrapper_expiry.expiry_seconds < target_time.epoch_seconds
                        AND wrapper.wrapper_state IN ('emancipated', 'locked') THEN NULL
                       ELSE wrapper.wrapper_state
                   END AS wrapper_state,
                   CASE
                       WHEN wrapper.fuses IS NULL
                         OR wrapper_expiry.expiry_seconds IS NULL
                         OR target_time.epoch_seconds IS NULL THEN NULL
                       WHEN wrapper_expiry.expiry_seconds < target_time.epoch_seconds THEN 0
                       ELSE wrapper.fuses
                   END AS fuses,
                   -- Past its own expiry the NameWrapper reports no owner for a name whose
                   -- PARENT_CANNOT_CONTROL fuse was burned, also while the registrar lease is
                   -- still live (a renewal made on the BaseRegistrar alone does not move it).
                   -- (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L843-L856 @ ens_v1@91c966f)
                   COALESCE(wrapper.wrapper_state IN ('emancipated', 'locked')
                       AND wrapper.fuses IS NOT NULL
                       AND wrapper_expiry.expiry_seconds < target_time.epoch_seconds,
                       false) AS owner_lapsed
        ) effective_wrapper ON TRUE
        LEFT JOIN LATERAL (
            SELECT lineage.block_timestamp
            FROM project_events event
            JOIN chain_lineage lineage
              ON lineage.chain_id = event.chain_id
             AND lineage.block_number = event.block_number
             AND lineage.block_hash = event.block_hash
            WHERE event.logical_name_id = surface.logical_name_id
            ORDER BY event.block_number,
                     event.transaction_index NULLS FIRST,
                     event.log_index NULLS FIRST,
                     event.normalized_event_id
            LIMIT 1
        ) created ON TRUE
        LEFT JOIN LATERAL (
            SELECT event.*
            FROM project_authority_events event
            WHERE event.logical_name_id = surface.logical_name_id AND (NOT selected_registration.is_v2_lifecycle OR EXISTS (SELECT 1 FROM v2_lifecycle_events selected_event WHERE selected_event.normalized_event_id = event.normalized_event_id AND selected_event.lifecycle_key IS NOT DISTINCT FROM COALESCE(selected_registration.lifecycle_key, row_identity.event_resource_id::text))) AND event.after_state ? 'status'
            ORDER BY event.block_number DESC NULLS LAST,
                     event.transaction_index DESC NULLS LAST,
                     event.log_index DESC NULLS LAST,
                     event.normalized_event_id DESC
            LIMIT 1
        ) status ON TRUE
        LEFT JOIN LATERAL (
            SELECT lower(CASE
                       WHEN event.after_state ->> 'owner_word_unmasked' = 'true'
                           THEN NULL
                       WHEN selected_registration.is_v2_lifecycle AND event.event_kind = 'TokenControlTransferred'
                           THEN event.after_state ->> 'to'
                       -- A numeric lease disclosed by a later readable observation has no
                       -- name-attached registry transfer: its registry owner was proven equal to
                       -- the registrar owner at disclosure and travels in the snapshot's getter.
                       WHEN event.event_kind = 'RegistrationGranted'
                           THEN event.after_state ->> 'owner_getter'
                       ELSE COALESCE(
                           event.after_state ->> 'registry_owner',
                           event.after_state ->> 'owner'
                       )
                   END) AS registry_owner
            FROM project_authority_events event
            WHERE event.logical_name_id = surface.logical_name_id AND (NOT selected_registration.is_v2_lifecycle OR EXISTS (SELECT 1 FROM v2_lifecycle_events selected_event WHERE selected_event.normalized_event_id = event.normalized_event_id AND selected_event.lifecycle_key IS NOT DISTINCT FROM COALESCE(selected_registration.lifecycle_key, row_identity.event_resource_id::text)))
              AND (
                    event.event_kind IN ('AuthorityTransferred', 'AuthorityEpochChanged')
                 OR (selected_registration.is_v2_lifecycle AND event.event_kind = 'TokenControlTransferred')
                 OR (event.event_kind = 'SurfaceBound' AND event.after_state @>
                     '{"state_derived":true,"authority_kind":"registry_only"}')
                 OR (event.event_kind = 'RegistrationGranted' AND event.after_state @>
                     '{"state_derived":true,"registrar_surface_snapshot":true}')
              )
            ORDER BY event.block_number DESC NULLS LAST,
                     event.transaction_index DESC NULLS LAST,
                     event.log_index DESC NULLS LAST,
                     event.normalized_event_id DESC
            LIMIT 1
        ) control_owner ON TRUE
        LEFT JOIN LATERAL (
            SELECT event.event_kind AS latest_event_kind
            FROM project_authority_events event
            WHERE event.logical_name_id = surface.logical_name_id AND (NOT selected_registration.is_v2_lifecycle OR EXISTS (SELECT 1 FROM v2_lifecycle_events selected_event WHERE selected_event.normalized_event_id = event.normalized_event_id AND selected_event.lifecycle_key IS NOT DISTINCT FROM COALESCE(selected_registration.lifecycle_key, row_identity.event_resource_id::text)))
              AND event.event_kind IN (
                  'TokenControlTransferred', 'AuthorityTransferred',
                  'AuthorityEpochChanged'
              )
            ORDER BY event.block_number DESC NULLS LAST,
                     event.transaction_index DESC NULLS LAST,
                     event.log_index DESC NULLS LAST,
                     event.normalized_event_id DESC
            LIMIT 1
        ) control ON TRUE
        LEFT JOIN LATERAL (
            SELECT event.* FROM project_authority_events event
            WHERE event.logical_name_id = surface.logical_name_id AND (NOT selected_registration.is_v2_lifecycle OR EXISTS (SELECT 1 FROM v2_lifecycle_events selected_event WHERE selected_event.normalized_event_id = event.normalized_event_id AND selected_event.lifecycle_key IS NOT DISTINCT FROM COALESCE(selected_registration.lifecycle_key, row_identity.event_resource_id::text)))
              AND event.event_kind IN (
                  'AuthorityTransferred', 'TokenControlTransferred',
                  'AuthorityEpochChanged'
              )
            ORDER BY event.block_number DESC NULLS LAST,
                     event.transaction_index DESC NULLS LAST,
                     event.log_index DESC NULLS LAST,
                     event.normalized_event_id DESC
            LIMIT 1
        ) authority ON TRUE
        LEFT JOIN LATERAL (
            SELECT event.*,
                   lower(event.after_state ->> 'resolver') AS resolver_address
            FROM (
                SELECT selected.* FROM project_authority_events selected
                UNION ALL
                SELECT pointer.* FROM project_events pointer
                WHERE pointer.normalized_event_id = serving.pointer_event_id
                  AND NOT EXISTS (
                      SELECT 1 FROM project_authority_events selected
                      WHERE selected.normalized_event_id = pointer.normalized_event_id
                  )
            ) event
            WHERE event.logical_name_id = surface.logical_name_id
              AND event.event_kind = 'ResolverChanged'
              AND (
                  event.normalized_event_id = serving.pointer_event_id
                  OR NOT selected_registration.is_v2_lifecycle
                  OR (binding.resource_id IS NULL AND event.resource_id IS NULL)
                  OR (binding.resource_id IS NOT NULL AND EXISTS (
                      SELECT 1 FROM v2_lifecycle_events selected_event
                      WHERE selected_event.normalized_event_id = event.normalized_event_id
                        AND selected_event.lifecycle_key IS NOT DISTINCT FROM COALESCE(
                            selected_registration.lifecycle_key,
                            row_identity.event_resource_id::text
                        )
                  ))
              )
            ORDER BY event.block_number DESC NULLS LAST,
                     event.transaction_index DESC NULLS LAST,
                     event.log_index DESC NULLS LAST,
                     event.normalized_event_id DESC
            LIMIT 1
        ) resolver ON TRUE
        LEFT JOIN LATERAL (
            SELECT event.block_number,
                   jsonb_build_object(
                       'normalized_event_id', event.normalized_event_id,
                       'event_kind', event.event_kind,
                       'chain_position', jsonb_strip_nulls(jsonb_build_object(
                           'chain_id', event.chain_id,
                           'block_number', event.block_number,
                           'block_hash', event.block_hash,
                           'timestamp', lineage.block_timestamp
                       ))
                   ) AS pointer
            FROM project_events event
            LEFT JOIN chain_lineage lineage
              ON lineage.chain_id = event.chain_id
             AND lineage.block_number = event.block_number
             AND lineage.block_hash = event.block_hash
            WHERE event.logical_name_id = surface.logical_name_id
            ORDER BY event.block_number DESC NULLS LAST,
                     event.transaction_index DESC NULLS LAST,
                     event.log_index DESC NULLS LAST,
                     event.normalized_event_id DESC
            LIMIT 1
        ) surface_history ON TRUE
        LEFT JOIN LATERAL (
            SELECT event.block_number,
                   jsonb_build_object(
                       'normalized_event_id', event.normalized_event_id,
                       'event_kind', event.event_kind,
                       'chain_position', jsonb_strip_nulls(jsonb_build_object(
                           'chain_id', event.chain_id,
                           'block_number', event.block_number,
                           'block_hash', event.block_hash,
                           'timestamp', lineage.block_timestamp
                       ))
                   ) AS pointer
            FROM project_authority_events event
            LEFT JOIN chain_lineage lineage
              ON lineage.chain_id = event.chain_id
             AND lineage.block_number = event.block_number
             AND lineage.block_hash = event.block_hash
            WHERE event.logical_name_id = surface.logical_name_id
              AND event.resource_id = resource.resource_id
            ORDER BY event.block_number DESC NULLS LAST,
                     event.transaction_index DESC NULLS LAST,
                     event.log_index DESC NULLS LAST,
                     event.normalized_event_id DESC
            LIMIT 1
        ) resource_history ON TRUE
        LEFT JOIN LATERAL (
            SELECT jsonb_agg(to_jsonb(event.normalized_event_id)
                             ORDER BY event.normalized_event_id) AS event_ids,
                   jsonb_agg(event.raw_fact_ref
                             ORDER BY event.normalized_event_id) AS raw_fact_refs,
                   jsonb_agg(jsonb_build_object(
                       'source_manifest_id', event.source_manifest_id,
                       'source_family', event.source_family,
                       'manifest_version', event.manifest_version
                   ) ORDER BY event.normalized_event_id) AS manifest_versions,
                   max(event.manifest_version) AS manifest_version
            FROM project_events event
            WHERE event.logical_name_id = surface.logical_name_id
        ) evidence ON TRUE
        LEFT JOIN LATERAL (
            SELECT COALESCE(bool_or(
                       event.source_family IN (
                           'ens_v2_root_l1', 'ens_v2_registry_l1', 'ens_v2_registrar_l1'
                       )
                   ), false) AS has_ens_v2,
                   COALESCE(bool_or(
                       event.source_family LIKE 'ens_v1_%'
                   ), false) AS has_ens_v1
            FROM project_events event
            WHERE event.logical_name_id = surface.logical_name_id
        ) corpus ON TRUE
        LEFT JOIN LATERAL (
            SELECT EXISTS (
                       SELECT 1
                       FROM project_events event
                       JOIN project_manifests manifest
                         ON manifest.manifest_id = event.source_manifest_id
                        AND manifest.manifest_version = event.manifest_version
                        AND manifest.source_family = event.source_family
                       WHERE event.logical_name_id = surface.logical_name_id
                         AND event.source_family = 'ens_v2_registry_l1'
                         AND manifest.namespace = 'ens'
                         AND manifest.chain_id = 'ethereum-sepolia'
                         AND manifest.deployment_label IN (
                             'ens_v2_sepolia_post_audit', 'ens_v2_sepolia_hackathon'
                         )
                   )
                   AND EXISTS (
                       SELECT 1
                       FROM project_events event
                       JOIN project_manifests manifest
                         ON manifest.manifest_id = event.source_manifest_id
                        AND manifest.manifest_version = event.manifest_version
                        AND manifest.source_family = event.source_family
                       WHERE event.logical_name_id = surface.logical_name_id
                         AND event.source_family = 'ens_v2_registrar_l1'
                         AND manifest.namespace = 'ens'
                         AND manifest.chain_id = 'ethereum-sepolia'
                         AND manifest.deployment_label IN (
                             'ens_v2_sepolia_post_audit', 'ens_v2_sepolia_hackathon'
                         )
                         AND manifest.manifest_payload
                             -> 'capability_flags'
                             -> 'exact_name_profile'
                             ->> 'status' = 'supported'
                   ) OR EXISTS (
                       -- A validated migration replaces the registrar qualification only
                       -- for its exact current successor in the admitted registry profile.
                       SELECT 1
                       FROM project_events boundary
                       JOIN project_manifests migration_manifest
                         ON migration_manifest.manifest_id = boundary.source_manifest_id
                        AND migration_manifest.manifest_version = boundary.manifest_version
                        AND migration_manifest.source_family = boundary.source_family
                       JOIN project_events successor
                         ON successor.chain_id = boundary.chain_id
                        AND successor.namespace = boundary.namespace
                        AND successor.logical_name_id = boundary.logical_name_id
                        AND successor.resource_id = selected_authority.selected_resource_id
                        AND successor.event_kind = 'SurfaceBound'
                        AND successor.source_family = 'ens_v2_registry_l1'
                        AND successor.after_state ->> 'surface_binding_id' =
                            selected_authority.selected_binding_id::text
                        AND successor.block_number = boundary.block_number
                        AND successor.transaction_index = boundary.transaction_index
                       JOIN project_manifests registry_manifest
                         ON registry_manifest.manifest_id = successor.source_manifest_id
                        AND registry_manifest.manifest_version = successor.manifest_version
                        AND registry_manifest.source_family = successor.source_family
                       JOIN contract_instance_addresses registry_address
                         ON registry_address.chain_id = successor.chain_id
                        AND registry_address.contract_instance_id::text =
                            boundary.after_state ->> 'successor_registry_contract_instance_id'
                        AND lower(registry_address.address) =
                            lower(successor.raw_fact_ref ->> 'emitting_address')
                        AND COALESCE(registry_address.active_from_block_number, 0)
                            <= successor.block_number
                        AND (registry_address.active_to_block_number IS NULL
                             OR registry_address.active_to_block_number >= successor.block_number)
                       WHERE surface.namespace = 'ens'
                         AND boundary.namespace = surface.namespace
                         AND boundary.logical_name_id = surface.logical_name_id
                         AND boundary.chain_id = 'ethereum-sepolia'
                         AND selected_authority.selected_authority_arm = 'ens_v2'
                         AND selected_authority.unsupported_reason IS NULL
                         AND selected_authority.authority_proof_kind =
                             'migration_authority_transition'
                         AND boundary.normalized_event_id =
                             selected_authority.authority_proof_event_id
                         AND boundary.event_identity =
                             selected_authority.authority_proof_event_identity
                         AND boundary.event_kind = 'MigrationApplied'
                         AND boundary.source_family = 'ens_v2_migration_l1'
                         AND boundary.consumer_visibility = 'activated'
                         AND boundary.canonicality_state IN ('canonical', 'safe', 'finalized')
                         AND boundary.after_state #>> '{successor_binding,binding_id}' =
                             selected_authority.selected_binding_id::text
                         AND boundary.after_state #>> '{successor_binding,resource_id}' =
                             selected_authority.selected_resource_id::text
                         AND migration_manifest.namespace = boundary.namespace
                         AND migration_manifest.chain_id = boundary.chain_id
                         AND migration_manifest.deployment_label IN ('ens_v2_sepolia_post_audit', 'ens_v2_sepolia_hackathon')
                         AND registry_manifest.namespace = successor.namespace
                         AND registry_manifest.chain_id = successor.chain_id
                         AND registry_manifest.deployment_label IN ('ens_v2_sepolia_post_audit', 'ens_v2_sepolia_hackathon')
                         AND (
                             -- The successor registry is either declared in the admitted
                             -- registry profile or was created on chain by an admitted
                             -- migration (LockedMigrationController / WrapperRegistry deploy a
                             -- WrapperRegistry per migrated name and announce it), which is the
                             -- same registry-creation proof the authority builder accepts for
                             -- positive child registrations.
                             EXISTS (
                                 SELECT 1 FROM jsonb_array_elements(COALESCE(
                                     registry_manifest.manifest_payload -> 'contracts', '[]'::jsonb
                                 )) declaration
                                 WHERE declaration ->> 'role' = 'registry'
                                   AND lower(declaration ->> 'address') = lower(registry_address.address)
                                   AND (declaration ->> 'start_block' IS NULL
                                        OR (declaration ->> 'start_block')::bigint <= successor.block_number)
                             )
                             OR EXISTS (
                                 SELECT 1
                                 FROM migration_discovery_associations created
                                 JOIN discovery_edges created_edge
                                   ON created_edge.chain_id = created.chain_id
                                  AND created_edge.edge_kind = 'registry_announcement'
                                  AND created_edge.to_contract_instance_id =
                                      created.registry_contract_instance_id
                                  AND created_edge.source_manifest_id = created.source_manifest_id
                                  AND created_edge.active_from_block_number = created.block_number
                                  AND created_edge.active_from_block_hash = created.block_hash
                                  AND (created_edge.provenance ->> 'transaction_index')::bigint =
                                      created.transaction_index
                                  AND (created_edge.provenance ->> 'log_index')::bigint =
                                      created.log_index
                                 JOIN chain_lineage created_lineage
                                   ON created_lineage.chain_id = created.chain_id
                                  AND created_lineage.block_hash = created.block_hash
                                  AND created_lineage.block_number = created.block_number
                                 WHERE created.chain_id = successor.chain_id
                                   AND created.correlation_kind = 'migration_registry_creation'
                                   AND created.registry_contract_instance_id =
                                       registry_address.contract_instance_id
                                   AND lower(created.registry_address) =
                                       lower(registry_address.address)
                                   AND created.block_number <= successor.block_number
                                   AND created.canonicality_state IN ('canonical', 'safe', 'finalized')
                                   AND created_lineage.canonicality_state IN ('canonical', 'safe', 'finalized')
                                   AND created_edge.canonicality_state IN ('canonical', 'safe', 'finalized')
                                   AND (created_edge.active_to_block_number IS NULL
                                        OR created_edge.active_to_block_number >= successor.block_number)
                             )
                         )
                   ) OR (
                       -- A positive ENSv2 child registration is already proven by the authority
                       -- builder against a migration-created, announced registry under a
                       -- migrated parent; the exact profile follows that chain of custody.
                       -- Every operand is coalesced: a NULL here would make `supported`
                       -- NULL and the support CASE below would fall through to 'supported'.
                       COALESCE(selected_authority.selected_authority_arm, '') = 'ens_v2'
                       AND selected_authority.unsupported_reason IS NULL
                       AND COALESCE(selected_authority.authority_proof_kind, '') =
                           'positive_v2_child_registration'
                   ) AS supported
        ) ens_v2_profile ON TRUE
        CROSS JOIN LATERAL (
            SELECT CASE
                       WHEN selected_authority.unsupported_reason IS NOT NULL
                           THEN 'unsupported'
                       WHEN selected_authority.selected_authority_arm = 'ens_v2'
                        AND NOT ens_v2_profile.supported
                           THEN 'unsupported'
                       ELSE 'supported'
                   END AS support_status,
                   CASE
                       WHEN selected_authority.unsupported_reason IS NOT NULL
                           THEN selected_authority.unsupported_reason
                       WHEN selected_authority.selected_authority_arm = 'ens_v2'
                        AND NOT ens_v2_profile.supported
                           THEN 'ensv2_exact_name_profile_shadow'
                       ELSE NULL
                   END AS unsupported_reason
        ) support
        WHERE surface.visibility_state = 'active'
          AND surface.raw_name <> ''
        ORDER BY surface.logical_name_id
