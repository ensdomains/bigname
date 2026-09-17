-- Candidate superset, not a claim these registrations are still live. No LIMIT: every due
-- name must be loaded, however many fall due at one timestamp.
-- The expiry expression must stay identical to normalized_events_v1_due_probe_idx in
-- schema-v2/baseline/05_normalized_events.sql; numeric arithmetic avoids overflow.
SELECT DISTINCT event.namespace || ':' || lower(COALESCE(
    event.after_state ->> 'child_node', event.after_state ->> 'namehash', event.after_state ->> 'node'
)) AS name
FROM normalized_events event
CROSS JOIN LATERAL (
    SELECT CASE WHEN jsonb_typeof(event.after_state -> 'expiry') IN ('number','string')
        AND event.after_state ->> 'expiry' ~ '^[+-]?[0-9]+$'
        AND length(ltrim(event.after_state ->> 'expiry', '+-0')) <= 19
      THEN ((CASE WHEN left(event.after_state ->> 'expiry', 1) = '-' THEN '-' ELSE '' END)
        || COALESCE(NULLIF(ltrim(event.after_state ->> 'expiry', '+-0'), ''), '0'))::numeric
    END AS expiry
) parsed
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
  AND event.source_family = 'ens_v1_registrar_l1'
  AND event.event_kind IN ('RegistrationGranted','RegistrationRenewed','TokenControlTransferred')
  -- The adapter releases only when timestamp > expiry + grace. At predecessor equality
  -- it was still live; at last-block equality it is still live. Shift grace to the bounds.
  -- A parameter-NULL OR leaves the lower bound as a filter in generic plans.
  -- The existing i64 bound below makes this minimum equivalent when no predecessor exists.
  AND parsed.expiry >= COALESCE(
      $3::bigint::numeric - $5::bigint::numeric,
      '-9223372036854775808'::numeric
  )
  AND parsed.expiry < $4::bigint::numeric - $5::bigint::numeric
  -- parse_i64 rejects out-of-range expiries; checked_add overflow means never due.
  AND parsed.expiry >= '-9223372036854775808'::numeric
  AND parsed.expiry <= '9223372036854775807'::numeric
  AND parsed.expiry + $5::bigint::numeric BETWEEN '-9223372036854775808'::numeric
                                        AND '9223372036854775807'::numeric
  AND COALESCE(event.after_state ->> 'child_node', event.after_state ->> 'namehash', event.after_state ->> 'node') IS NOT NULL
