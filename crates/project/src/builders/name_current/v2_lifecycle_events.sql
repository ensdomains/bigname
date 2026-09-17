-- Every ENSv2 registry, root-registry and registrar event with the key of the registration it
-- belongs to: the token resource when the event names one, else the resource of the latest grant
-- or reservation of the same registry and token id, else the registry and token id themselves.
-- `name_current` reads these rows several times per name, so they are staged once, narrowed to
-- the columns it reads, and indexed by name and by event.
CREATE TEMP TABLE project_v2_lifecycle_events ON COMMIT DROP AS
SELECT event.normalized_event_id, event.logical_name_id, event.resource_id, event.event_kind,
       event.after_state, event.block_number, event.transaction_index, event.log_index,
       COALESCE(event.resource_id::text, (
           SELECT linked.resource_id::text FROM project_events linked
           WHERE linked.logical_name_id = event.logical_name_id AND linked.resource_id IS NOT NULL
             AND linked.event_kind IN ('RegistrationGranted', 'RegistrationReserved')
             AND linked.source_family IN ('ens_v2_root_l1', 'ens_v2_registry_l1', 'ens_v2_registrar_l1')
             AND COALESCE(linked.after_state ->> 'registry_contract_instance_id',
                     linked.raw_fact_ref ->> 'emitting_address', linked.after_state ->> 'registry')
               = COALESCE(event.after_state ->> 'registry_contract_instance_id',
                     event.raw_fact_ref ->> 'emitting_address', event.after_state ->> 'registry')
             AND linked.after_state ->> 'token_id' = event.after_state ->> 'token_id'
           ORDER BY linked.block_number DESC NULLS LAST, linked.normalized_event_id DESC LIMIT 1
       ), NULLIF(CONCAT(COALESCE(event.after_state ->> 'registry_contract_instance_id',
                     event.raw_fact_ref ->> 'emitting_address', event.after_state ->> 'registry'),
                 ':', event.after_state ->> 'token_id'), ':')) AS lifecycle_key
FROM project_events event
WHERE event.source_family IN ('ens_v2_root_l1', 'ens_v2_registry_l1', 'ens_v2_registrar_l1')
