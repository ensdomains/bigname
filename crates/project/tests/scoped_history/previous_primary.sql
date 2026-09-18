SELECT event.normalized_event_id
FROM normalized_events event
CROSS JOIN LATERAL (
    VALUES
        (lower(event.after_state ->> 'address'),
         event.after_state ->> 'coin_type',
         event.after_state ->> 'namespace'),
        (lower(event.before_state ->> 'address'),
         event.before_state ->> 'coin_type',
         event.before_state ->> 'namespace'),
        (lower(event.after_state -> 'primary_claim_source' ->> 'address'),
         event.after_state -> 'primary_claim_source' ->> 'coin_type',
         event.after_state -> 'primary_claim_source' ->> 'namespace'),
        (lower(event.before_state -> 'primary_claim_source' ->> 'address'),
         event.before_state -> 'primary_claim_source' ->> 'coin_type',
         event.before_state -> 'primary_claim_source' ->> 'namespace')
) candidate(address, coin_type, namespace)
JOIN project_scope_primary scope
  ON scope.address = candidate.address
 AND scope.coin_type = candidate.coin_type
 AND scope.namespace = candidate.namespace
WHERE event.chain_id = $1 AND event.block_number <= $2
  AND event.event_kind IN ('ReverseChanged', 'RecordChanged')
  AND event.canonicality_state IN ('canonical', 'safe', 'finalized')
