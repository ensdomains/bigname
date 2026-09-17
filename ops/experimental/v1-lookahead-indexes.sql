-- ISOLATED BLUE-BRAIN EXPERIMENT ONLY. Install explicitly before enabling lookahead.
-- Run outside a transaction. The physical mainnet relation is bigname_phase.normalized_events.
-- Numeric parsing is total even for malformed/oversized string values: strip leading zeros,
-- limit significant digits before casting, and let the query enforce signed-i64 bounds.
-- Keep this expression identical to crates/interpret/src/load/lookahead/due_names.sql (apart from the event qualifier).
CREATE INDEX CONCURRENTLY IF NOT EXISTS normalized_events_v1_due_probe_idx
ON bigname_phase.normalized_events (
    chain_id,
    (CASE WHEN jsonb_typeof(after_state -> 'expiry') IN ('number','string')
        AND after_state ->> 'expiry' ~ '^[+-]?[0-9]+$'
        AND length(ltrim(after_state ->> 'expiry', '+-0')) <= 19
      THEN ((CASE WHEN left(after_state ->> 'expiry', 1) = '-' THEN '-' ELSE '' END)
        || COALESCE(NULLIF(ltrim(after_state ->> 'expiry', '+-0'), ''), '0'))::numeric
    END),
    block_number
)
WHERE canonicality_state IN ('canonical','safe','finalized')
  AND source_family = 'ens_v1_registrar_l1'
  AND event_kind IN ('RegistrationGranted','RegistrationRenewed','TokenControlTransferred');

-- Route NewOwner to its child, never all siblings through the parent's logical name.
-- The expression also covers unnamed registry/wrapper facts. Match events.sql exactly.
CREATE INDEX CONCURRENTLY IF NOT EXISTS normalized_events_v1_direct_node_probe_idx
ON bigname_phase.normalized_events (
    chain_id,
    (COALESCE(namespace || ':' || lower(COALESCE(after_state ->> 'child_node', after_state ->> 'namehash', after_state ->> 'node', after_state #>> '{grant_source,node}', after_state #>> '{revocation_source,node}')), logical_name_id)),
    block_number
)
WHERE canonicality_state IN ('canonical','safe','finalized')
  AND source_family LIKE 'ens\_v1\_%';

-- Verify both rows exist and both flags are true before enabling lookahead.
SELECT indexrelid::regclass AS index_name, indisvalid, indisready
FROM pg_index
WHERE indexrelid IN (
    to_regclass('bigname_phase.normalized_events_v1_due_probe_idx'),
    to_regclass('bigname_phase.normalized_events_v1_direct_node_probe_idx')
);
-- IF NOT EXISTS does not repair an interrupted, invalid concurrent build. Stop the
-- experimental runner, DROP INDEX CONCURRENTLY only that invalid index, rerun its
-- CREATE above, and repeat the two-row validity check. Baseline indexes stay intact.
