                   AND EXISTS (
                       SELECT 1
                       FROM attributed_events sibling
                       WHERE sibling.attributed_resource_id = event.attributed_resource_id
                         AND sibling.chain_id = event.chain_id
                         AND sibling.block_number = event.block_number
                         AND sibling.transaction_hash IS NOT DISTINCT FROM event.transaction_hash
                         AND sibling.transaction_index IS NOT DISTINCT FROM event.transaction_index
                         AND sibling.log_index = event.log_index + 1
                         AND sibling.event_kind = 'RecordChanged'
                         AND sibling.after_state ->> 'record_key' = 'addr:60'
                         AND sibling.after_state ->> 'source_event' = 'AddrChanged'
                   ) AS coin60_compatibility_source,
