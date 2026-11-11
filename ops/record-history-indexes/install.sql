-- Run with psql -X -v ON_ERROR_STOP=1, outside any transaction.
-- These indexes can be preinstalled while the existing runner is processing batches.
-- A long Interpret batch can hold the writer transaction a concurrent build waits for.
-- Bound each build, rather than aborting that expected wait after a few seconds.
SET lock_timeout = '0';
SET statement_timeout = '6h';

-- IF NOT EXISTS matches on the name alone, so an interrupted concurrent build leaves an
-- invalid index that the statements below then skip, and an earlier manual build can leave a
-- valid index with other keys or another predicate, or a table, view, or other relation that
-- is not an index, under one of these names. This check runs twice. Before the builds it
-- refuses any name that is already taken by something other than the reviewed, valid and
-- ready index; a name that resolves to nothing passes. After the builds it also requires
-- every index to exist, so the script fails instead of reporting success. README.md describes
-- the recovery.
-- The definitions are the ones the schema-migration
-- 20261111120000_normalized_events_record_history_indexes.sql checks. The function's own SET
-- search_path clause makes search_path pg_catalog while it runs, so PostgreSQL always prints
-- the table's schema name, and the expected text keeps it. Its SET quote_all_identifiers
-- clause turns that setting off for the same read: when the session has it on, PostgreSQL
-- prints every identifier in double quotes and a healthy index would be refused. PostgreSQL
-- puts the session's settings back when the function returns or raises, so the CREATE INDEX
-- statements below are not affected. The function lives in pg_temp and disappears with the
-- session.
CREATE OR REPLACE FUNCTION pg_temp.check_record_history_indexes(require_built boolean)
RETURNS void
LANGUAGE plpgsql
SET search_path = pg_catalog
SET quote_all_identifiers = off
AS $check$
DECLARE
    checked_index text;
    expected_definition text;
    found_definition text;
    found_kind text;
    found_table oid;
BEGIN
    FOR checked_index, expected_definition IN
        SELECT * FROM (VALUES
            ('normalized_events_record_key_history_idx',
             $def$CREATE INDEX normalized_events_record_key_history_idx ON bigname_phase.normalized_events USING btree (chain_id, ((after_state ->> 'record_key'::text)), block_number DESC NULLS LAST) WHERE ((event_kind = 'RecordChanged'::text) AND (consumer_visibility = 'activated'::text) AND (canonicality_state = ANY (ARRAY['canonical'::bigname_phase.canonicality_state, 'safe'::bigname_phase.canonicality_state, 'finalized'::bigname_phase.canonicality_state])))$def$),
            ('normalized_events_record_emitter_key_history_idx',
             $def$CREATE INDEX normalized_events_record_emitter_key_history_idx ON bigname_phase.normalized_events USING btree (chain_id, lower((raw_fact_ref ->> 'emitting_address'::text)), ((after_state ->> 'record_key'::text)), block_number DESC NULLS LAST) WHERE ((event_kind = 'RecordChanged'::text) AND (consumer_visibility = 'activated'::text) AND (canonicality_state = ANY (ARRAY['canonical'::bigname_phase.canonicality_state, 'safe'::bigname_phase.canonicality_state, 'finalized'::bigname_phase.canonicality_state])))$def$),
            ('normalized_events_record_reset_history_idx',
             $def$CREATE INDEX normalized_events_record_reset_history_idx ON bigname_phase.normalized_events USING btree (chain_id, block_number DESC NULLS LAST) WHERE ((event_kind = 'RecordVersionChanged'::text) AND (consumer_visibility = 'activated'::text) AND (canonicality_state = ANY (ARRAY['canonical'::bigname_phase.canonicality_state, 'safe'::bigname_phase.canonicality_state, 'finalized'::bigname_phase.canonicality_state])))$def$)
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
        IF found_kind IS NULL AND NOT require_built THEN
            CONTINUE;
        END IF;
        IF found_kind <> 'index' THEN
            RAISE EXCEPTION
                'bigname_phase.% is a %, not an index, so the index was never built; remove or rename that relation, then follow ops/record-history-indexes/README.md before retrying',
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
            SELECT indrelid
            INTO found_table
            FROM pg_index
            WHERE indexrelid = to_regclass('bigname_phase.' || checked_index);
            RAISE EXCEPTION
                '% is missing from bigname_phase.normalized_events or is not valid and ready; follow the recovery steps in ops/record-history-indexes/README.md before retrying',
                checked_index
                USING HINT = CASE
                    WHEN found_table IS NULL THEN
                        'No relation has this name. Rerun this script to build the index.'
                    WHEN found_table <> to_regclass('bigname_phase.normalized_events') THEN
                        format('An index on %s holds this name. Rename or remove it, then rerun this script.', found_table::regclass)
                    ELSE
                        format('An interrupted concurrent build leaves an invalid index. Confirm in pg_stat_progress_create_index that no build is still running, run DROP INDEX CONCURRENTLY bigname_phase.%I, then rerun this script.', checked_index)
                END;
        END IF;

        SELECT pg_get_indexdef(indexrelid)
        INTO found_definition
        FROM pg_index
        WHERE indexrelid = to_regclass('bigname_phase.' || checked_index);
        IF found_definition <> expected_definition THEN
            RAISE EXCEPTION
                '% exists but does not have the reviewed definition; found "%", expected "%"; follow the recovery steps in ops/record-history-indexes/README.md before retrying',
                checked_index, found_definition, expected_definition;
        END IF;
    END LOOP;
END
$check$;

-- Refuse before building anything; see the comment above.
DO $$ BEGIN PERFORM pg_temp.check_record_history_indexes(false); END $$;

CREATE INDEX CONCURRENTLY IF NOT EXISTS normalized_events_record_key_history_idx
    ON bigname_phase.normalized_events (
        chain_id,
        (after_state ->> 'record_key'),
        block_number DESC NULLS LAST
    )
    WHERE event_kind = 'RecordChanged'
      AND consumer_visibility = 'activated'
      AND canonicality_state IN ('canonical', 'safe', 'finalized');

CREATE INDEX CONCURRENTLY IF NOT EXISTS normalized_events_record_emitter_key_history_idx
    ON bigname_phase.normalized_events (
        chain_id,
        lower(raw_fact_ref ->> 'emitting_address'),
        (after_state ->> 'record_key'),
        block_number DESC NULLS LAST
    )
    WHERE event_kind = 'RecordChanged'
      AND consumer_visibility = 'activated'
      AND canonicality_state IN ('canonical', 'safe', 'finalized');

CREATE INDEX CONCURRENTLY IF NOT EXISTS normalized_events_record_reset_history_idx
    ON bigname_phase.normalized_events (
        chain_id,
        block_number DESC NULLS LAST
    )
    WHERE event_kind = 'RecordVersionChanged'
      AND consumer_visibility = 'activated'
      AND canonicality_state IN ('canonical', 'safe', 'finalized');

-- Printed first so the receipt shows the flags even when the check below fails.
SELECT indexrelid::regclass AS index_name, indisvalid, indisready,
       pg_size_pretty(pg_relation_size(indexrelid)) AS index_size,
       pg_get_indexdef(indexrelid) AS definition
FROM pg_index
WHERE indexrelid IN (
    to_regclass('bigname_phase.normalized_events_record_key_history_idx'),
    to_regclass('bigname_phase.normalized_events_record_emitter_key_history_idx'),
    to_regclass('bigname_phase.normalized_events_record_reset_history_idx')
) ORDER BY index_name;

-- Every index must now exist, belong to bigname_phase.normalized_events, be valid and ready,
-- and have the reviewed definition.
DO $$ BEGIN PERFORM pg_temp.check_record_history_indexes(true); END $$;
