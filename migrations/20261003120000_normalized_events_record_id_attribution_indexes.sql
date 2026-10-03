-- Existing schema-v2 databases gain the two indexes the record-ID arm of history's record
-- attribution (crates/storage/src/history/attribution) reads: a selected record's
-- RecordChanged writes by resolver and record id, and the ResolverRecordLinked rows on a
-- pointer's resolver at its node or the zero node. Without them each history read with a
-- record-ID resolver walks every RecordChanged row of the chain. Prebuild both concurrently on
-- a large initialized database as docs/deployment.md describes, so this schema-migration finds
-- them and skips the build. An empty schema-migration database has no phase baseline yet, so
-- this schema-migration is a no-op there and phase-runner init-schema installs the same indexes.
--
-- Index only; no row changes. CREATE INDEX IF NOT EXISTS matches on the name alone, so the
-- block ends with the same validity and definition check as
-- 20261001130000_normalized_events_v2_lookahead_indexes.sql, under the same transaction-local
-- search_path and quote_all_identifiers settings, put back before it returns.
DO $migration$
DECLARE
    checked_index text;
    expected_definition text;
    found_definition text;
    found_kind text;
    previous_search_path text;
    previous_quote_all_identifiers text;
BEGIN
    IF to_regclass('bigname_phase.normalized_events') IS NULL THEN
        RETURN;
    END IF;

    CREATE INDEX IF NOT EXISTS normalized_events_record_id_write_idx
        ON bigname_phase.normalized_events (
            chain_id,
            lower(after_state ->> 'resolver'),
            (after_state ->> 'resolver_record_id')
        )
        WHERE event_kind = 'RecordChanged'
          AND after_state ->> 'storage_model' = 'resolver_record_id'
          AND consumer_visibility = 'activated'
          AND canonicality_state IN ('canonical', 'safe', 'finalized');

    CREATE INDEX IF NOT EXISTS normalized_events_record_id_link_idx
        ON bigname_phase.normalized_events (
            chain_id,
            lower(after_state ->> 'resolver'),
            lower(after_state ->> 'node')
        )
        WHERE event_kind = 'ResolverRecordLinked'
          AND after_state ->> 'storage_model' = 'resolver_record_id'
          AND consumer_visibility = 'activated'
          AND canonicality_state IN ('canonical', 'safe', 'finalized');

    -- Every name below is schema-qualified or lives in pg_catalog.
    previous_search_path := current_setting('search_path');
    PERFORM set_config('search_path', 'pg_catalog', true);
    -- The expected text below has no quoted identifiers.
    previous_quote_all_identifiers := current_setting('quote_all_identifiers');
    PERFORM set_config('quote_all_identifiers', 'off', true);

    FOR checked_index, expected_definition IN
        SELECT * FROM (VALUES
            ('normalized_events_record_id_write_idx',
             $def$CREATE INDEX normalized_events_record_id_write_idx ON bigname_phase.normalized_events USING btree (chain_id, lower((after_state ->> 'resolver'::text)), ((after_state ->> 'resolver_record_id'::text))) WHERE ((event_kind = 'RecordChanged'::text) AND ((after_state ->> 'storage_model'::text) = 'resolver_record_id'::text) AND (consumer_visibility = 'activated'::text) AND (canonicality_state = ANY (ARRAY['canonical'::bigname_phase.canonicality_state, 'safe'::bigname_phase.canonicality_state, 'finalized'::bigname_phase.canonicality_state])))$def$),
            ('normalized_events_record_id_link_idx',
             $def$CREATE INDEX normalized_events_record_id_link_idx ON bigname_phase.normalized_events USING btree (chain_id, lower((after_state ->> 'resolver'::text)), lower((after_state ->> 'node'::text))) WHERE ((event_kind = 'ResolverRecordLinked'::text) AND ((after_state ->> 'storage_model'::text) = 'resolver_record_id'::text) AND (consumer_visibility = 'activated'::text) AND (canonicality_state = ANY (ARRAY['canonical'::bigname_phase.canonicality_state, 'safe'::bigname_phase.canonicality_state, 'finalized'::bigname_phase.canonicality_state])))$def$)
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
                '% does not exist although bigname_phase.normalized_events does; build it as docs/deployment.md (History record attribution indexes) describes, then run the schema-migrations again',
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
                '% exists but is not a valid and ready index on bigname_phase.normalized_events; follow the recovery steps in docs/deployment.md (History record attribution indexes), then run the schema-migrations again',
                checked_index;
        END IF;

        SELECT pg_get_indexdef(indexrelid)
        INTO found_definition
        FROM pg_index
        WHERE indexrelid = to_regclass('bigname_phase.' || checked_index);
        IF found_definition <> expected_definition THEN
            RAISE EXCEPTION
                '% exists but does not have the reviewed definition; found "%", expected "%"; follow the recovery steps in docs/deployment.md (History record attribution indexes), then run the schema-migrations again',
                checked_index, found_definition, expected_definition;
        END IF;
    END LOOP;

    PERFORM set_config('search_path', previous_search_path, true);
    PERFORM set_config('quote_all_identifiers', previous_quote_all_identifiers, true);
END
$migration$;
