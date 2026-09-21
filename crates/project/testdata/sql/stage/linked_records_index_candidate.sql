-- Local plan fixture and proposed additive production index; not applied by Project.
CREATE INDEX normalized_events_linked_resolver_history_idx
ON normalized_events (chain_id, lower(after_state ->> 'resolver'), block_number)
WHERE consumer_visibility = 'activated'
  AND canonicality_state IN ('canonical', 'safe', 'finalized')
  AND (event_kind IN ('ResolverRecordLinked', 'ResolverPermissionArgument')
       OR (event_kind = 'RecordChanged'
           AND after_state ->> 'storage_model' = 'resolver_record_id'));
