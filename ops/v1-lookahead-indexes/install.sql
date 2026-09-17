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

-- Printed first so the receipt shows the flags even when the check below fails.
SELECT indexrelid::regclass AS index_name, indisvalid, indisready,
       pg_size_pretty(pg_relation_size(indexrelid)) AS index_size,
       pg_get_indexdef(indexrelid) AS definition
FROM pg_index
WHERE indexrelid IN (
    to_regclass('bigname_phase.normalized_events_v1_due_probe_idx'),
    to_regclass('bigname_phase.normalized_events_v1_direct_node_probe_idx')
);

-- IF NOT EXISTS matches on the name alone, so an interrupted concurrent build
-- leaves an invalid index that the statements above then skip, and an earlier
-- manual build can leave a valid index with other keys or another predicate,
-- or a table, view, or other relation that is not an index under one of these
-- names. Fail here instead of reporting success; README.md describes the recovery.
-- The definition check matches the one in the schema-migration
-- 20260917150000_normalized_events_v1_lookahead_indexes.sql. PostgreSQL always
-- prints the table's schema name, and a type's schema name only when the session
-- search_path does not include it, so the schema name is removed before comparing;
-- it prints the expiry CASE expression over several indented lines, so runs of
-- whitespace are collapsed to one space.
DO $check$
DECLARE
    checked_index text;
    expected_definition text;
    found_definition text;
    found_kind text;
BEGIN
    FOR checked_index, expected_definition IN
        SELECT * FROM (VALUES
            ('normalized_events_v1_due_probe_idx',
             $def$CREATE INDEX normalized_events_v1_due_probe_idx ON normalized_events USING btree (chain_id, ( CASE WHEN ((jsonb_typeof((after_state -> 'expiry'::text)) = ANY (ARRAY['number'::text, 'string'::text])) AND ((after_state ->> 'expiry'::text) ~ '^[+-]?[0-9]+$'::text) AND (length(ltrim((after_state ->> 'expiry'::text), '+-0'::text)) <= 19)) THEN (( CASE WHEN ("left"((after_state ->> 'expiry'::text), 1) = '-'::text) THEN '-'::text ELSE ''::text END || COALESCE(NULLIF(ltrim((after_state ->> 'expiry'::text), '+-0'::text), ''::text), '0'::text)))::numeric ELSE NULL::numeric END), block_number) WHERE ((canonicality_state = ANY (ARRAY['canonical'::canonicality_state, 'safe'::canonicality_state, 'finalized'::canonicality_state])) AND (source_family = 'ens_v1_registrar_l1'::text) AND (event_kind = ANY (ARRAY['RegistrationGranted'::text, 'RegistrationRenewed'::text, 'TokenControlTransferred'::text])))$def$),
            ('normalized_events_v1_direct_node_probe_idx',
             $def$CREATE INDEX normalized_events_v1_direct_node_probe_idx ON normalized_events USING btree (chain_id, COALESCE(((namespace || ':'::text) || lower(COALESCE((after_state ->> 'child_node'::text), (after_state ->> 'namehash'::text), (after_state ->> 'node'::text), (after_state #>> '{grant_source,node}'::text[]), (after_state #>> '{revocation_source,node}'::text[])))), logical_name_id), block_number) WHERE ((canonicality_state = ANY (ARRAY['canonical'::canonicality_state, 'safe'::canonicality_state, 'finalized'::canonicality_state])) AND (source_family ~~ 'ens\_v1\_%'::text))$def$)
        ) AS reviewed(index_name, definition)
    LOOP
        SELECT CASE relkind
                   WHEN 'i' THEN 'index'
                   WHEN 'I' THEN 'partitioned index'
                   WHEN 'r' THEN 'table'
                   WHEN 'p' THEN 'partitioned table'
                   WHEN 'v' THEN 'view'
                   WHEN 'm' THEN 'materialized view'
                   WHEN 'S' THEN 'sequence'
                   WHEN 'f' THEN 'foreign table'
                   WHEN 'c' THEN 'composite type'
                   ELSE 'relation of kind ' || relkind::text
               END
        INTO found_kind
        FROM pg_class
        WHERE oid = to_regclass('bigname_phase.' || checked_index);
        IF found_kind <> 'index' THEN
            RAISE EXCEPTION
                'bigname_phase.% is a %, not an index, so the index was never built; remove or rename that relation, then follow ops/v1-lookahead-indexes/README.md before retrying',
                checked_index, found_kind;
        END IF;

        IF NOT EXISTS (
            SELECT 1
            FROM pg_index
            WHERE indexrelid = to_regclass('bigname_phase.' || checked_index)
              AND indrelid = to_regclass('bigname_phase.normalized_events')
              AND indisvalid
              AND indisready
        ) THEN
            RAISE EXCEPTION
                '% is missing from bigname_phase.normalized_events or is not valid and ready; follow the recovery steps in ops/v1-lookahead-indexes/README.md before retrying',
                checked_index;
        END IF;

        SELECT regexp_replace(
                   replace(pg_get_indexdef(indexrelid), 'bigname_phase.', ''),
                   '\s+', ' ', 'g')
        INTO found_definition
        FROM pg_index
        WHERE indexrelid = to_regclass('bigname_phase.' || checked_index);
        IF found_definition <> expected_definition THEN
            RAISE EXCEPTION
                '% exists but does not have the reviewed definition; found "%", expected "%"; follow the recovery steps in ops/v1-lookahead-indexes/README.md before retrying',
                checked_index, found_definition, expected_definition;
        END IF;
    END LOOP;
END
$check$;
