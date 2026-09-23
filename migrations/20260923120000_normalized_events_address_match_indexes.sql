-- Prebuild these indexes concurrently on large initialized databases using
-- ops/address-history-indexes/install.sql before applying schema-migrations.
--
-- They serve the anchor lookup of the address history read
-- (crates/storage/src/history/address_matches.rs): one partial expression index
-- per kind of event that can hand a name or resource to an address. They change
-- access paths only; no stored row and no interpreter content hash input changes.
--
-- CREATE INDEX IF NOT EXISTS matches on the name alone. An interrupted concurrent
-- prebuild leaves an invalid index under the right name, and a wrong manual
-- prebuild leaves a valid index with other keys or another predicate, or a table,
-- view, or other relation that is not an index under the right name. The
-- statements below then succeed without building anything. The check at the end
-- stops the run instead of recording success over an index the address history
-- read cannot use. It is the check
-- 20260917150000_normalized_events_v1_lookahead_indexes.sql makes for the ENSv1
-- lookahead indexes, read the same way: pg_get_indexdef under search_path
-- pg_catalog and quote_all_identifiers off, both put back before the block
-- returns, with the expected text exactly as the fresh baseline index prints
-- under those settings. schema-v2/apply-check.sh proves it for the baseline,
-- this schema-migration, and install.sql.
--
-- To recover, follow ops/address-history-indexes/README.md: confirm no build is
-- running, drop only the named index with DROP INDEX CONCURRENTLY, rerun
-- install.sql, then run the schema-migrations again.
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

    CREATE INDEX IF NOT EXISTS normalized_events_address_registrant_match_idx
        ON bigname_phase.normalized_events (lower(COALESCE(after_state ->> 'registrant', '')))
        WHERE event_kind = 'RegistrationGranted'
          AND consumer_visibility = 'activated'
          AND canonicality_state IN ('canonical', 'safe', 'finalized');

    CREATE INDEX IF NOT EXISTS normalized_events_address_token_holder_match_idx
        ON bigname_phase.normalized_events (lower(COALESCE(after_state ->> 'to', '')))
        WHERE event_kind = 'TokenControlTransferred'
          AND consumer_visibility = 'activated'
          AND canonicality_state IN ('canonical', 'safe', 'finalized');

    CREATE INDEX IF NOT EXISTS normalized_events_address_registry_owner_match_idx
        ON bigname_phase.normalized_events (lower(COALESCE(after_state ->> 'owner', '')))
        WHERE event_kind = 'AuthorityTransferred'
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
            ('normalized_events_address_registrant_match_idx',
             $def$CREATE INDEX normalized_events_address_registrant_match_idx ON bigname_phase.normalized_events USING btree (lower(COALESCE((after_state ->> 'registrant'::text), ''::text))) WHERE ((event_kind = 'RegistrationGranted'::text) AND (consumer_visibility = 'activated'::text) AND (canonicality_state = ANY (ARRAY['canonical'::bigname_phase.canonicality_state, 'safe'::bigname_phase.canonicality_state, 'finalized'::bigname_phase.canonicality_state])))$def$),
            ('normalized_events_address_token_holder_match_idx',
             $def$CREATE INDEX normalized_events_address_token_holder_match_idx ON bigname_phase.normalized_events USING btree (lower(COALESCE((after_state ->> 'to'::text), ''::text))) WHERE ((event_kind = 'TokenControlTransferred'::text) AND (consumer_visibility = 'activated'::text) AND (canonicality_state = ANY (ARRAY['canonical'::bigname_phase.canonicality_state, 'safe'::bigname_phase.canonicality_state, 'finalized'::bigname_phase.canonicality_state])))$def$),
            ('normalized_events_address_registry_owner_match_idx',
             $def$CREATE INDEX normalized_events_address_registry_owner_match_idx ON bigname_phase.normalized_events USING btree (lower(COALESCE((after_state ->> 'owner'::text), ''::text))) WHERE ((event_kind = 'AuthorityTransferred'::text) AND (consumer_visibility = 'activated'::text) AND (canonicality_state = ANY (ARRAY['canonical'::bigname_phase.canonicality_state, 'safe'::bigname_phase.canonicality_state, 'finalized'::bigname_phase.canonicality_state])))$def$)
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
                '% does not exist although bigname_phase.normalized_events does; build it with ops/address-history-indexes/install.sql as ops/address-history-indexes/README.md describes, then run the schema-migrations again',
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
                '% exists but is not a valid and ready index on bigname_phase.normalized_events; follow the recovery steps in ops/address-history-indexes/README.md, then run the schema-migrations again',
                checked_index;
        END IF;

        SELECT pg_get_indexdef(indexrelid)
        INTO found_definition
        FROM pg_index
        WHERE indexrelid = to_regclass('bigname_phase.' || checked_index);
        IF found_definition <> expected_definition THEN
            RAISE EXCEPTION
                '% exists but does not have the reviewed definition; found "%", expected "%"; follow the recovery steps in ops/address-history-indexes/README.md, then run the schema-migrations again',
                checked_index, found_definition, expected_definition;
        END IF;
    END LOOP;

    PERFORM set_config('search_path', previous_search_path, true);
    PERFORM set_config('quote_all_identifiers', previous_quote_all_identifiers, true);
END
$migration$;
