-- Events that belong to each name's selected authority. The join is a plain equality on the
-- name: `.eth` BaseRegistrar lifecycle rows written before the label was known are given their
-- name while staging (`bind_resource_events`), so this statement never has to search the rows
-- that carry no name.
CREATE TEMP TABLE project_authority_events ON COMMIT DROP AS
SELECT DISTINCT ON (event.normalized_event_id) event.*
FROM project_events event
JOIN project_name_authority authority
  ON authority.logical_name_id = event.logical_name_id
WHERE (
      (
          authority.unsupported_reason IS NULL
          AND (
              event.resource_id = authority.selected_resource_id
              OR (
                  event.resource_id IS NULL
                  AND CASE
                      WHEN event.source_family LIKE 'ens_v1_%' THEN 'ens_v1'
                      WHEN event.source_family LIKE 'ens_v2_%' THEN 'ens_v2'
                      WHEN event.source_family LIKE 'basenames_%' THEN 'basenames'
                  END = authority.selected_authority_arm
              )
              OR (
                  authority.selected_authority_arm = 'ens_v1'
                  AND event.event_kind IN (
                      'RegistrationGranted', 'RegistrationRenewed',
                      'RegistrationReleased', 'ExpiryChanged',
                      'TokenControlTransferred'
                  )
                  AND event.source_family = 'ens_v1_registrar_l1'
                  AND COALESCE(
                      NULLIF(event.after_state ->> 'authority_kind', ''),
                      'registrar'
                  ) = 'registrar'
                  AND (
                      EXISTS (
                          SELECT 1
                          FROM project_bindings selected_binding
                          JOIN LATERAL (
                              SELECT predecessor.*
                              FROM project_binding_candidates predecessor
                              WHERE predecessor.logical_name_id =
                                    selected_binding.logical_name_id
                                AND predecessor.authority_arm = 'ens_v1'
                                AND (
                                    predecessor.block_number,
                                    COALESCE(
                                        (predecessor.provenance ->> 'transaction_index')::bigint,
                                        -1
                                    ),
                                    COALESCE(
                                        (predecessor.provenance ->> 'log_index')::bigint, -1
                                    )
                                ) < (
                                    selected_binding.block_number,
                                    COALESCE(
                                        (selected_binding.provenance ->> 'transaction_index')::bigint,
                                        -1
                                    ),
                                    COALESCE(
                                        (selected_binding.provenance ->> 'log_index')::bigint,
                                        -1
                                    )
                                )
                              ORDER BY predecessor.block_number DESC,
                                       COALESCE(
                                           (predecessor.provenance ->> 'transaction_index')::bigint,
                                           -1
                                       ) DESC,
                                       COALESCE(
                                           (predecessor.provenance ->> 'log_index')::bigint,
                                           -1
                                       ) DESC,
                                       predecessor.surface_binding_id DESC
                              LIMIT 1
                          ) predecessor ON TRUE
                          WHERE selected_binding.logical_name_id =
                                authority.logical_name_id
                            AND predecessor.resource_id = event.resource_id
                      )
                      OR EXISTS (
                          SELECT 1
                          FROM project_events selected_wrapper
                          JOIN project_events registration
                            ON registration.resource_id = event.resource_id
                           AND registration.source_family =
                               'ens_v1_registrar_l1'
                           AND registration.event_kind = 'RegistrationGranted'
                           AND (
                               -- Rule 1: a named grant in the wrap's own transaction. When a
                               -- controller event grants the lease it follows NameWrapped in
                               -- the transaction, so the wrap could not record the lease.
                               -- (upstream: .refs/ens_v1/deployments/mainnet/WrappedETHRegistrarController.json:L656 @ ens_v1@91c966f)
                               (registration.logical_name_id =
                                    selected_wrapper.logical_name_id
                                AND registration.transaction_hash =
                                    selected_wrapper.transaction_hash
                                AND event.event_kind <> 'TokenControlTransferred')
                               -- Rule 2: the registrar lease the wrap recorded.
                               OR (selected_wrapper.after_state ->>
                                       'wrapped_registrar_resource_id' =
                                       registration.resource_id::text
                                   AND (
                                       event.event_kind <> 'TokenControlTransferred'
                                       OR event.transaction_hash IS DISTINCT FROM
                                          selected_wrapper.transaction_hash
                                       OR lower(event.after_state ->> 'to') IS DISTINCT FROM
                                          lower(selected_wrapper.raw_fact_ref ->>
                                                'emitting_address')
                                   )
                                   AND (registration.logical_name_id =
                                        selected_wrapper.logical_name_id
                                        OR (registration.logical_name_id IS NULL
                                            AND lower(registration.after_state ->> 'namehash') =
                                                lower(selected_wrapper.after_state ->> 'node'))))
                           )
                          WHERE selected_wrapper.logical_name_id =
                                authority.logical_name_id
                            AND selected_wrapper.resource_id =
                                authority.selected_resource_id
                            AND selected_wrapper.source_family =
                                'ens_v1_wrapper_l1'
                            AND selected_wrapper.event_kind = 'SurfaceBound'
                      )
                  )
                  AND EXISTS (
                      SELECT 1 FROM project_events wrapper
                      WHERE wrapper.logical_name_id = authority.logical_name_id
                        AND wrapper.resource_id = authority.selected_resource_id
                        AND wrapper.source_family = 'ens_v1_wrapper_l1'
                        AND wrapper.event_kind = 'PermissionScopeChanged'
                  )
              )
              OR (
                  authority.selected_authority_arm IN ('ens_v1', 'basenames')
                  AND event.event_kind IN (
                      'RegistrationGranted', 'RegistrationRenewed',
                      'RegistrationReleased', 'RegistrationReserved',
                      'ExpiryChanged', 'TokenControlTransferred'
                  )
                  AND EXISTS (
                      SELECT 1 FROM project_events fallback
                      WHERE fallback.logical_name_id = authority.logical_name_id
                        AND fallback.resource_id = authority.selected_resource_id
                        AND fallback.event_kind = 'AuthorityEpochChanged'
                        AND fallback.after_state ->> 'authority_kind' = 'registry_only'
                  )
                  AND EXISTS (
                      SELECT 1
                      FROM project_bindings selected_binding
                      -- The binding this one replaced and the BaseRegistrar lease the name
                      -- has under it, staged once for this window and the released-tombstone
                      -- rule alike.
                      JOIN project_registry_only_handoffs handoff
                        ON handoff.surface_binding_id = selected_binding.surface_binding_id
                      WHERE selected_binding.logical_name_id =
                            authority.logical_name_id
                        AND (
                            event.resource_id IN (
                                handoff.predecessor_resource_id, handoff.lease_resource_id
                            )
                            OR (
                                event.event_kind IN (
                                    'RegistrationGranted', 'RegistrationReleased'
                                )
                                AND event.source_family =
                                    'ens_v1_registrar_l1'
                                AND EXISTS (
                                    SELECT 1
                                    FROM project_events wrapper
                                    WHERE wrapper.logical_name_id =
                                          authority.logical_name_id
                                      AND wrapper.resource_id =
                                          handoff.predecessor_resource_id
                                      AND wrapper.source_family =
                                          'ens_v1_wrapper_l1'
                                      AND wrapper.event_kind = 'SurfaceBound'
                                      AND wrapper.after_state ->>
                                          'wrapped_registrar_resource_id' =
                                          event.resource_id::text
                                      AND lower(wrapper.after_state ->> 'node') =
                                          lower(event.after_state ->> 'namehash')
                                )
                            )
                        )
                        AND (
                            (
                                event.source_family = 'ens_v1_registrar_l1'
                                AND (
                                    event.resource_id IN (
                                        handoff.predecessor_resource_id,
                                        handoff.lease_resource_id
                                    )
                                    OR event.event_kind = 'RegistrationGranted'
                                )
                            )
                            OR (
                                event.block_number,
                                COALESCE(event.transaction_index, -1),
                                COALESCE(event.log_index, -1)
                            ) >= (
                                handoff.predecessor_block_number,
                                handoff.predecessor_transaction_index,
                                handoff.predecessor_log_index
                            )
                        )
                        AND (
                            (
                                event.block_number,
                                COALESCE(event.transaction_index, -1),
                                COALESCE(event.log_index, -1)
                            ) <= (
                                handoff.block_number,
                                handoff.transaction_index,
                                handoff.log_index
                            )
                            -- The window above bounds what can decide control. The lease the
                            -- name has is not bounded by it: a registrar token transferred
                            -- without `reclaim` leaves the registry owner as it was, and a
                            -- later `renew` writes only the lease's expiry, so the retained
                            -- lease goes on being renewed, and lapses, under the registry-only
                            -- binding; and once it lapsed, `registerOnly` can grant the name
                            -- again without touching the registry, so the successor lease is
                            -- granted, renewed and released under the same binding. Only the
                            -- lifecycle rows of the one lease the name has pass; no control
                            -- fold reads their kinds, and the registration's authority fold
                            -- leaves the successor grant out. A later transfer of that lease's
                            -- token names a registrant but must not decide control, so it is
                            -- not admitted here; `registration_events.sql` reads it.
                            -- (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L118-L152 @ ens_v1@91c966f)
                            -- (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L157-L169 @ ens_v1@91c966f)
                            -- (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L172-L174 @ ens_v1@91c966f)
                            OR (
                                authority.selected_authority_arm = 'ens_v1'
                                AND event.source_family = 'ens_v1_registrar_l1'
                                AND event.event_kind IN (
                                    'RegistrationGranted', 'RegistrationRenewed',
                                    'ExpiryChanged', 'RegistrationReleased'
                                )
                                AND COALESCE(
                                    NULLIF(event.after_state ->> 'authority_kind', ''),
                                    'registrar'
                                ) = 'registrar'
                                AND handoff.lease_resource_id = event.resource_id
                            )
                        )
                  )
              )
          )
      )
      OR (
          authority.unsupported_reason = 'current_authority_not_projected'
          AND ((event.event_kind = 'ResolverChanged' AND event.resource_id IS NULL)
               OR (event.source_family = 'ens_v1_registrar_l1'
                   AND event.event_kind IN ('RegistrationGranted',
                       'RegistrationRenewed', 'RegistrationReleased',
                       'ExpiryChanged', 'TokenControlTransferred')))
      )
  )
  AND (
      authority.authority_proof_event_id IS NULL
      OR (
          event.block_number,
          COALESCE(event.transaction_index, -1),
          COALESCE(event.log_index, -1)
      ) >= (
          (authority.authority_epoch_start_position ->> 'block_number')::bigint,
          COALESCE(
              (authority.authority_epoch_start_position ->> 'transaction_index')::bigint,
              -1
          ),
          COALESCE(
              (authority.authority_epoch_start_position ->> 'log_index')::bigint, -1
          )
      )
      -- A lease registered before the wrap predates the wrapper's authority epoch. Rows that
      -- staging named through the selected wrapper's recorded lease are still that name's lease.
      OR (
          event.source_family = 'ens_v1_registrar_l1'
          AND event.event_kind IN (
              'RegistrationGranted', 'RegistrationRenewed', 'ExpiryChanged',
              'TokenControlTransferred'
          )
          AND EXISTS (
              SELECT 1 FROM project_wrapper_linked_events linked
              WHERE linked.normalized_event_id = event.normalized_event_id
          )
          AND EXISTS (
              SELECT 1 FROM project_events selected_wrapper
              WHERE selected_wrapper.logical_name_id =
                    authority.logical_name_id
                AND selected_wrapper.resource_id =
                    authority.selected_resource_id
                AND selected_wrapper.source_family = 'ens_v1_wrapper_l1'
                AND selected_wrapper.event_kind = 'SurfaceBound'
                AND (
                    event.event_kind <> 'TokenControlTransferred'
                    OR event.transaction_hash IS DISTINCT FROM
                       selected_wrapper.transaction_hash
                    OR lower(event.after_state ->> 'to') IS DISTINCT FROM
                       lower(selected_wrapper.raw_fact_ref ->>
                             'emitting_address')
                )
                AND selected_wrapper.after_state ->>
                    'wrapped_registrar_resource_id' = event.resource_id::text
                AND lower(selected_wrapper.after_state ->> 'node') =
                    lower(event.after_state ->> 'namehash')
          )
      )
  )
ORDER BY event.normalized_event_id
