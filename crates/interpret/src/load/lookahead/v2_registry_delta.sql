-- Every readable ENSv2 event in [$2, $3) filed under one of the whole-registry keys $4
-- (`<registry>:*`), with that key. Unlike events.sql this returns every row, not the latest
-- per state key: the rows are ordered so that, within each state key, events.sql's winner
-- comes last, and a retained registry keeps the last row it folds per key. The array must stay
-- identical to normalized_events_v2_key_probe_idx and to `v2_event_keys` in the adapter crate.
SELECT lower(split_part(event.raw_fact_ref ->> '{state_scope}', ':', 1)) || ':*' AS registry,
       jsonb_build_object(
           'chain_id', event.chain_id, 'namespace', event.namespace,
           'logical_name_id', event.logical_name_id, 'resource_id', event.resource_id,
           'event_kind', event.event_kind, 'source_family', event.source_family,
           'manifest_version', event.manifest_version, 'source_manifest_id', event.source_manifest_id,
           'emitting_address', event.raw_fact_ref ->> 'emitting_address',
           '{state_key}', event.raw_fact_ref ->> '{state_key}',
           'event_identity', event.event_identity, '{state_scope}', event.raw_fact_ref ->> '{state_scope}',
           'block_number', event.block_number, 'normalized_event_id', event.normalized_event_id,
           '{transaction_index}', event.transaction_index,
           '{log_index}', event.log_index,
           'after_state', event.after_state
       ) AS body,
       lineage.block_timestamp
FROM normalized_events event
JOIN LATERAL (
    SELECT lineage.block_timestamp
    FROM chain_lineage lineage
    WHERE lineage.chain_id = event.chain_id
      AND lineage.block_number = event.block_number AND lineage.block_hash = event.block_hash
      AND lineage.canonicality_state IN ('canonical','safe','finalized')
    LIMIT 1
) lineage ON TRUE
WHERE {v2_keys} && $4::text[]
  AND event.chain_id = $1 AND event.block_number >= $2 AND event.block_number < $3
  AND event.source_family LIKE 'ens\_v2\_%'
  AND event.canonicality_state IN ('canonical','safe','finalized')
ORDER BY event.block_number, event.transaction_index NULLS FIRST,
  event.log_index NULLS FIRST, event.normalized_event_id
