-- API read-only access paths for history pages filtered by one record key: `/v1/events`, name
-- history and address history with `record_key`. An unanchored page reads one RecordChanged
-- arm and one RecordVersionChanged arm per chain (crates/storage/src/history/record_key_source.rs).
-- Three partial indexes serve them:
-- - normalized_events_record_key_history_idx: a key's writes on one chain, in page order.
-- - normalized_events_record_emitter_key_history_idx: a key's writes by one emitter on one
--   chain, in page order, for a read scoped by resolver or contract_address.
-- - normalized_events_record_reset_history_idx: record-version resets on one chain, in page
--   order.
-- Without them each arm walks the chain's events in page order and tests the key on every
-- row. No rows, phase markers, catalogue version or content-hash inputs change, and no redo
-- is required.
--
-- Populated tables require valid prebuilds with the reviewed definitions, built concurrently
-- with ops/record-history-indexes/install.sql. The prebuilds are adopted without a build and
-- without a lock on normalized_events. Only an empty table builds the indexes here, and it
-- takes its lock immediately or not at all, so a concurrent insert cannot turn the
-- population check into a blocking build. This is the shape of
-- 20261102140000_normalized_events_registry_roles_index.sql.
--
-- CREATE INDEX IF NOT EXISTS matches on the name alone. An interrupted concurrent build
-- leaves an invalid index under the name, and a wrong manual build leaves another definition
-- or a relation that is not an index. The checks at the end refuse each of them. They compare
-- each definition as pg_get_indexdef prints it with search_path set to pg_catalog, so
-- PostgreSQL prints schema names, and quote_all_identifiers off, so it prints no extra quotes.
-- Both settings are transaction-local and put back before the block returns.
--
-- An empty schema-migration database has no phase baseline yet, so this schema-migration is a
-- no-op there, and phase-runner init-schema installs the same indexes.
DO $migration$
DECLARE
    checked_index text;
    expected_definition text;
    missing text;
    found_definition text;
    found_kind text;
    previous_search_path text;
    previous_quote_all_identifiers text;
BEGIN
    IF to_regclass('bigname_phase.normalized_events') IS NULL THEN
        RETURN;
    END IF;

    missing := concat_ws(', ',
        CASE WHEN to_regclass('bigname_phase.normalized_events_record_key_history_idx') IS NULL
            THEN 'normalized_events_record_key_history_idx' END,
        CASE WHEN to_regclass('bigname_phase.normalized_events_record_emitter_key_history_idx') IS NULL
            THEN 'normalized_events_record_emitter_key_history_idx' END,
        CASE WHEN to_regclass('bigname_phase.normalized_events_record_reset_history_idx') IS NULL
            THEN 'normalized_events_record_reset_history_idx' END);
    IF missing <> '' THEN
        BEGIN
            LOCK TABLE bigname_phase.normalized_events IN SHARE MODE NOWAIT;
        EXCEPTION WHEN lock_not_available THEN
            RAISE EXCEPTION 'normalized_events is busy; prebuild % with ops/record-history-indexes/install.sql before this schema-migration', missing;
        END;
        IF EXISTS (SELECT 1 FROM bigname_phase.normalized_events) THEN
            RAISE EXCEPTION 'missing prebuilt index % on populated normalized_events; prebuild it with ops/record-history-indexes/install.sql, as ops/record-history-indexes/README.md describes, before this schema-migration', missing;
        END IF;
        CREATE INDEX IF NOT EXISTS normalized_events_record_key_history_idx
            ON bigname_phase.normalized_events (
                chain_id,
                (after_state ->> 'record_key'),
                block_number DESC NULLS LAST
            )
            WHERE event_kind = 'RecordChanged'
              AND consumer_visibility = 'activated'
              AND canonicality_state IN ('canonical', 'safe', 'finalized');
        CREATE INDEX IF NOT EXISTS normalized_events_record_emitter_key_history_idx
            ON bigname_phase.normalized_events (
                chain_id,
                lower(raw_fact_ref ->> 'emitting_address'),
                (after_state ->> 'record_key'),
                block_number DESC NULLS LAST
            )
            WHERE event_kind = 'RecordChanged'
              AND consumer_visibility = 'activated'
              AND canonicality_state IN ('canonical', 'safe', 'finalized');
        CREATE INDEX IF NOT EXISTS normalized_events_record_reset_history_idx
            ON bigname_phase.normalized_events (
                chain_id,
                block_number DESC NULLS LAST
            )
            WHERE event_kind = 'RecordVersionChanged'
              AND consumer_visibility = 'activated'
              AND canonicality_state IN ('canonical', 'safe', 'finalized');
    END IF;

    -- Every name below is schema-qualified or lives in pg_catalog.
    previous_search_path := current_setting('search_path');
    PERFORM set_config('search_path', 'pg_catalog', true);
    -- The expected text below has no quoted identifiers.
    previous_quote_all_identifiers := current_setting('quote_all_identifiers');
    PERFORM set_config('quote_all_identifiers', 'off', true);

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
        IF found_kind IS NULL THEN
            RAISE EXCEPTION
                '% does not exist although bigname_phase.normalized_events does; prebuild it with ops/record-history-indexes/install.sql, then run the schema-migrations again',
                checked_index;
        END IF;
        IF found_kind <> 'index' THEN
            RAISE EXCEPTION
                'bigname_phase.% is a %, not an index, so the index was never built; remove or rename that relation, then run the schema-migrations again',
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
                '% exists but is not a valid and ready index on bigname_phase.normalized_events; follow the recovery steps in ops/record-history-indexes/README.md, then run the schema-migrations again',
                checked_index;
        END IF;

        SELECT pg_get_indexdef(indexrelid)
        INTO found_definition
        FROM pg_index
        WHERE indexrelid = to_regclass('bigname_phase.' || checked_index);
        IF found_definition <> expected_definition THEN
            RAISE EXCEPTION
                '% exists but does not have the reviewed definition; found "%", expected "%"; follow the recovery steps in ops/record-history-indexes/README.md, then run the schema-migrations again',
                checked_index, found_definition, expected_definition;
        END IF;
    END LOOP;

    PERFORM set_config('search_path', previous_search_path, true);
    PERFORM set_config('quote_all_identifiers', previous_quote_all_identifiers, true);
END
$migration$;
