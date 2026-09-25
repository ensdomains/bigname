/* project:builders.name_authority.registration_events */
-- The rows that can name a registrant. A later wrap moves the registrar token into the
-- NameWrapper; that custody transfer names the NameWrapper contract, not a registrant, so it is
-- left out. The NameWrapped owner and later wrapper transfers are served as they occur on chain.
-- `wrapETH2LD` transfers the token from the registrant to the wrapper itself.
-- (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L264-L265 @ ens_v1@91c966f)
-- `NameWrapped` carries the caller-chosen `wrappedOwner`, which cannot be the wrapper contract.
-- (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L894-L903 @ ens_v1@91c966f)
-- (upstream: .refs/ens_v1/contracts/wrapper/ERC1155Fuse.sol:L252-L255 @ ens_v1@91c966f)
--
-- The lease a registry-only binding stands for is live under it, and its token can be
-- transferred again without `reclaim`: the registry owner stays as it was, so the binding stays
-- selected, while the token's holder, the registrant, changes. `project_authority_events` admits
-- only that lease's grant, renewal, expiry and release rows past the binding's position, because
-- the control folds read every row admitted there and a registrar token transfer must not decide
-- control while the registry-only binding is the authority. So the transfers of exactly that
-- lease, positioned after the binding opened, are read here from `project_events` and reach the
-- registrant alone. `project_registry_only_handoffs` names the lease: the one the binding
-- replaced, or the successor `registerOnly` granted under it.
-- (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L172-L175 @ ens_v1@91c966f)
CREATE TEMP TABLE project_registration_events ON COMMIT DROP AS
SELECT event.*
FROM (
    SELECT admitted.*
    FROM project_authority_events admitted
    WHERE admitted.event_kind IN (
        'RegistrationGranted', 'RegistrationReleased', 'TokenControlTransferred'
    )
    UNION ALL
    SELECT transfer.*
    FROM project_events transfer
    WHERE transfer.event_kind = 'TokenControlTransferred'
      AND transfer.source_family = 'ens_v1_registrar_l1'
      AND COALESCE(NULLIF(transfer.after_state ->> 'authority_kind', ''), 'registrar')
          = 'registrar'
      AND EXISTS (
          SELECT 1
          FROM project_name_authority authority
          JOIN project_bindings selected_binding
            ON selected_binding.logical_name_id = authority.logical_name_id
          JOIN project_registry_only_handoffs handoff
            ON handoff.surface_binding_id = selected_binding.surface_binding_id
          WHERE authority.logical_name_id = transfer.logical_name_id
            AND authority.unsupported_reason IS NULL
            AND authority.selected_authority_arm = 'ens_v1'
            AND handoff.lease_resource_id = transfer.resource_id
            AND (
                transfer.block_number,
                COALESCE(transfer.transaction_index, -1),
                COALESCE(transfer.log_index, -1)
            ) > (
                handoff.block_number,
                handoff.transaction_index,
                handoff.log_index
            )
      )
      AND NOT EXISTS (
          SELECT 1
          FROM project_authority_events admitted
          WHERE admitted.normalized_event_id = transfer.normalized_event_id
      )
) event
WHERE NOT (
      event.event_kind = 'TokenControlTransferred'
      AND event.source_family = 'ens_v1_registrar_l1'
      AND EXISTS (
          SELECT 1
          FROM project_authority_events wrapper_binding
          JOIN project_authority_events registrar_grant
            ON registrar_grant.resource_id = event.resource_id
           AND registrar_grant.source_family = 'ens_v1_registrar_l1'
           AND registrar_grant.event_kind = 'RegistrationGranted'
           AND registrar_grant.transaction_hash IS DISTINCT FROM
               wrapper_binding.transaction_hash
          WHERE wrapper_binding.logical_name_id = event.logical_name_id
            AND wrapper_binding.source_family = 'ens_v1_wrapper_l1'
            AND wrapper_binding.event_kind = 'SurfaceBound'
            AND wrapper_binding.transaction_hash = event.transaction_hash
            AND wrapper_binding.after_state ->>
                'wrapped_registrar_resource_id' = event.resource_id::text
            AND lower(event.after_state ->> 'to') =
                lower(wrapper_binding.raw_fact_ref ->> 'emitting_address')
      )
  )
