        WITH v2_lifecycle_events AS (
            SELECT event.*, COALESCE(event.resource_id::text, (
                SELECT linked.resource_id::text FROM project_events linked
                WHERE linked.logical_name_id = event.logical_name_id AND linked.resource_id IS NOT NULL
                  AND linked.event_kind IN ('RegistrationGranted', 'RegistrationReserved') AND linked.source_family IN ('ens_v2_root_l1', 'ens_v2_registry_l1', 'ens_v2_registrar_l1')
                  AND COALESCE(linked.after_state ->> 'registry_contract_instance_id', linked.raw_fact_ref ->> 'emitting_address', linked.after_state ->> 'registry') = COALESCE(event.after_state ->> 'registry_contract_instance_id', event.raw_fact_ref ->> 'emitting_address', event.after_state ->> 'registry') AND linked.after_state ->> 'token_id' = event.after_state ->> 'token_id'
                ORDER BY linked.block_number DESC NULLS LAST, linked.normalized_event_id DESC LIMIT 1
            ), NULLIF(CONCAT(COALESCE(event.after_state ->> 'registry_contract_instance_id', event.raw_fact_ref ->> 'emitting_address', event.after_state ->> 'registry'), ':', event.after_state ->> 'token_id'), ':')) AS lifecycle_key,
            (event.event_kind = 'RegistrationReserved' AND EXISTS (
                SELECT 1 FROM chain_lineage lineage
                WHERE lineage.chain_id = event.chain_id AND lineage.block_hash = event.block_hash
                  AND lineage.block_number = event.block_number
                  AND CASE WHEN jsonb_typeof(event.after_state -> 'expiry') = 'number'
                          THEN (event.after_state ->> 'expiry')::numeric <=
                               extract(epoch FROM lineage.block_timestamp)
                          ELSE FALSE END
            )) AS expired_when_written
            FROM project_events event
            WHERE event.source_family IN ('ens_v2_root_l1', 'ens_v2_registry_l1', 'ens_v2_registrar_l1')
        )
