SELECT matched.normalized_event_id
FROM project_scope_primary scope
CROSS JOIN LATERAL (
    SELECT normalized_event_id
    FROM normalized_events
    WHERE chain_id = $1 AND block_number <= $2
      AND event_kind IN ('ReverseChanged', 'RecordChanged')
      AND canonicality_state IN ('canonical', 'safe', 'finalized')
      AND (lower(after_state ->> 'address')) IS NOT NULL
      AND (after_state ->> 'coin_type') IS NOT NULL
      AND (after_state ->> 'namespace') IS NOT NULL
      AND lower(after_state ->> 'address') = scope.address
      AND after_state ->> 'coin_type' = scope.coin_type
      AND after_state ->> 'namespace' = scope.namespace
    UNION ALL
    SELECT normalized_event_id
    FROM normalized_events
    WHERE chain_id = $1 AND block_number <= $2
      AND event_kind IN ('ReverseChanged', 'RecordChanged')
      AND canonicality_state IN ('canonical', 'safe', 'finalized')
      AND (lower(before_state ->> 'address')) IS NOT NULL
      AND (before_state ->> 'coin_type') IS NOT NULL
      AND (before_state ->> 'namespace') IS NOT NULL
      AND lower(before_state ->> 'address') = scope.address
      AND before_state ->> 'coin_type' = scope.coin_type
      AND before_state ->> 'namespace' = scope.namespace
    UNION ALL
    SELECT normalized_event_id
    FROM normalized_events
    WHERE chain_id = $1 AND block_number <= $2
      AND event_kind IN ('ReverseChanged', 'RecordChanged')
      AND canonicality_state IN ('canonical', 'safe', 'finalized')
      AND (lower(after_state -> 'primary_claim_source' ->> 'address')) IS NOT NULL
      AND (after_state -> 'primary_claim_source' ->> 'coin_type') IS NOT NULL
      AND (after_state -> 'primary_claim_source' ->> 'namespace') IS NOT NULL
      AND lower(after_state -> 'primary_claim_source' ->> 'address') = scope.address
      AND after_state -> 'primary_claim_source' ->> 'coin_type' = scope.coin_type
      AND after_state -> 'primary_claim_source' ->> 'namespace' = scope.namespace
    UNION ALL
    SELECT normalized_event_id
    FROM normalized_events
    WHERE chain_id = $1 AND block_number <= $2
      AND event_kind IN ('ReverseChanged', 'RecordChanged')
      AND canonicality_state IN ('canonical', 'safe', 'finalized')
      AND (lower(before_state -> 'primary_claim_source' ->> 'address')) IS NOT NULL
      AND (before_state -> 'primary_claim_source' ->> 'coin_type') IS NOT NULL
      AND (before_state -> 'primary_claim_source' ->> 'namespace') IS NOT NULL
      AND lower(before_state -> 'primary_claim_source' ->> 'address') = scope.address
      AND before_state -> 'primary_claim_source' ->> 'coin_type' = scope.coin_type
      AND before_state -> 'primary_claim_source' ->> 'namespace' = scope.namespace
    OFFSET 0
) matched
