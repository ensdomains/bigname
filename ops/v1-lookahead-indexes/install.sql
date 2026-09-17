-- Run with psql -X -v ON_ERROR_STOP=1, outside any transaction.
-- These indexes can be preinstalled while the existing runner is processing batches.
-- A long Interpret batch can hold the writer transaction a concurrent build waits for.
-- Bound each build, rather than aborting that expected wait after a few seconds.
SET lock_timeout = '0';
SET statement_timeout = '6h';

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

CREATE INDEX CONCURRENTLY IF NOT EXISTS normalized_events_v1_direct_node_probe_idx
    ON bigname_phase.normalized_events (
        chain_id,
        (COALESCE(namespace || ':' || lower(COALESCE(after_state ->> 'child_node', after_state ->> 'namehash', after_state ->> 'node', after_state #>> '{grant_source,node}', after_state #>> '{revocation_source,node}')), logical_name_id)),
        block_number
    )
    WHERE canonicality_state IN ('canonical','safe','finalized')
      AND source_family LIKE 'ens\_v1\_%';

SELECT indexrelid::regclass AS index_name, indisvalid, indisready,
       pg_size_pretty(pg_relation_size(indexrelid)) AS index_size,
       pg_get_indexdef(indexrelid) AS definition
FROM pg_index
WHERE indexrelid IN (
    to_regclass('bigname_phase.normalized_events_v1_due_probe_idx'),
    to_regclass('bigname_phase.normalized_events_v1_direct_node_probe_idx')
);
