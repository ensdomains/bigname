-- The rows that can name a registrant. A later wrap moves the registrar token into the
-- NameWrapper; that custody transfer names the NameWrapper contract, not a registrant, so it is
-- left out. The NameWrapped owner and later wrapper transfers are served as they occur on chain.
-- `wrapETH2LD` transfers the token from the registrant to the wrapper itself.
-- (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L264-L265 @ ens_v1@91c966f)
-- `NameWrapped` carries the caller-chosen `wrappedOwner`, which cannot be the wrapper contract.
-- (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L894-L903 @ ens_v1@91c966f)
-- (upstream: .refs/ens_v1/contracts/wrapper/ERC1155Fuse.sol:L252-L255 @ ens_v1@91c966f)
CREATE TEMP TABLE project_registration_events ON COMMIT DROP AS
SELECT event.*
FROM project_authority_events event
WHERE event.event_kind IN (
    'RegistrationGranted', 'RegistrationReleased', 'TokenControlTransferred'
)
  AND NOT (
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
