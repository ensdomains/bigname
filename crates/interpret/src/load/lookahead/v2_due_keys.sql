-- The ENSv2 state key of every retained registry event whose expiry lies in ($3, $4]: a
-- superset of the tokens a refresh in the batch can release. No LIMIT: every due token must
-- be loaded. The expiry expression must stay identical to normalized_events_v2_due_probe_idx;
-- the key to the first element of `v2_keys.sql`.
SELECT DISTINCT lower(split_part(event.raw_fact_ref ->> '{state_scope}', ':', 1)) || ':' || lower(left(split_part(event.raw_fact_ref ->> '{state_scope}', ':', 3), greatest(length(split_part(event.raw_fact_ref ->> '{state_scope}', ':', 3)) - 8, 0))) || '00000000' AS key
FROM normalized_events event
JOIN LATERAL (
    SELECT 1 FROM chain_lineage lineage
    WHERE lineage.chain_id = event.chain_id
      AND lineage.block_number = event.block_number
      AND lineage.block_hash = event.block_hash
      AND lineage.canonicality_state IN ('canonical','safe','finalized')
    LIMIT 1
) readable ON TRUE
WHERE event.chain_id = $1 AND event.block_number < $2
  AND event.canonicality_state IN ('canonical','safe','finalized')
  AND event.source_family IN ('ens_v2_registry_l1','ens_v2_root_l1')
  AND event.raw_fact_ref ? '{state_scope}'
  AND (CASE WHEN jsonb_typeof(event.after_state -> 'expiry') IN ('number','string')
        AND event.after_state ->> 'expiry' ~ '^[+-]?[0-9]+$'
        AND length(ltrim(event.after_state ->> 'expiry', '+-0')) <= 19
      THEN ((CASE WHEN left(event.after_state ->> 'expiry', 1) = '-' THEN '-' ELSE '' END)
        || COALESCE(NULLIF(ltrim(event.after_state ->> 'expiry', '+-0'), ''), '0'))::numeric
    END) > $3::bigint::numeric
  AND (CASE WHEN jsonb_typeof(event.after_state -> 'expiry') IN ('number','string')
        AND event.after_state ->> 'expiry' ~ '^[+-]?[0-9]+$'
        AND length(ltrim(event.after_state ->> 'expiry', '+-0')) <= 19
      THEN ((CASE WHEN left(event.after_state ->> 'expiry', 1) = '-' THEN '-' ELSE '' END)
        || COALESCE(NULLIF(ltrim(event.after_state ->> 'expiry', '+-0'), ''), '0'))::numeric
    END) <= $4::bigint::numeric
