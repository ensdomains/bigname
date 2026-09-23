-- Prebuild this index concurrently on large initialized databases using
-- ops/events-order-index/install.sql before applying schema-migrations. An
-- ordinary CREATE INDEX blocks writes to normalized_events for the whole build.
--
-- The index serves history and event pages in their newest-first order read
-- forward and in their oldest-first order read backward.
--
-- CREATE INDEX IF NOT EXISTS matches on the name alone. An interrupted concurrent
-- prebuild leaves an invalid index under the right name, and a wrong manual
-- prebuild leaves a valid index with other keys or another key order, or a
-- table, view, or other relation that is not an index under the right name. The
-- statement below then succeeds without building anything. The check at the end
-- stops the run instead of recording success over an index the page reads cannot
-- use. It is the check
-- 20260917150000_normalized_events_v1_lookahead_indexes.sql makes for the ENSv1
-- lookahead indexes.
--
-- The definition is compared as PostgreSQL prints it with pg_get_indexdef, so
-- key order, sort direction, null placement, and uniqueness are all covered.
-- search_path is set to pg_catalog while the definition is read, so PostgreSQL
-- always prints the table's schema name, and the expected text keeps it.
-- quote_all_identifiers is turned off for the same read: when a caller has it
-- on, PostgreSQL prints every identifier in double quotes and a healthy index
-- would be refused. Both changes are transaction-local, and the block puts the
-- previous values back before it returns. When the block raises, the
-- transaction, or the savepoint around it, rolls the changes back. The CREATE
-- INDEX statement runs before the settings change.
--
-- To recover, follow ops/events-order-index/README.md: confirm no build is
-- running, drop only the named index with DROP INDEX CONCURRENTLY, rerun
-- install.sql, then run the schema-migrations again.
DO $migration$
DECLARE
    checked_index text := 'normalized_events_chain_block_number_desc_idx';
    expected_definition text := 'CREATE INDEX normalized_events_chain_block_number_desc_idx ON bigname_phase.normalized_events USING btree (chain_id, block_number DESC NULLS LAST)';
    found_definition text;
    found_kind text;
    previous_search_path text;
    previous_quote_all_identifiers text;
BEGIN
    IF to_regclass('bigname_phase.normalized_events') IS NULL THEN
        RETURN;
    END IF;

    CREATE INDEX IF NOT EXISTS normalized_events_chain_block_number_desc_idx
        ON bigname_phase.normalized_events (chain_id, block_number DESC NULLS LAST);

    -- Every name below is schema-qualified or lives in pg_catalog.
    previous_search_path := current_setting('search_path');
    PERFORM set_config('search_path', 'pg_catalog', true);
    -- The expected text above has no quoted identifiers.
    previous_quote_all_identifiers := current_setting('quote_all_identifiers');
    PERFORM set_config('quote_all_identifiers', 'off', true);

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
            '% does not exist although bigname_phase.normalized_events does; build it with ops/events-order-index/install.sql as ops/events-order-index/README.md describes, then run the schema-migrations again',
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
            '% exists but is not a valid and ready index on bigname_phase.normalized_events; follow the recovery steps in ops/events-order-index/README.md, then run the schema-migrations again',
            checked_index;
    END IF;

    SELECT pg_get_indexdef(indexrelid)
    INTO found_definition
    FROM pg_index
    WHERE indexrelid = to_regclass('bigname_phase.' || checked_index);
    IF found_definition <> expected_definition THEN
        RAISE EXCEPTION
            '% exists but does not have the reviewed definition; found "%", expected "%"; follow the recovery steps in ops/events-order-index/README.md, then run the schema-migrations again',
            checked_index, found_definition, expected_definition;
    END IF;

    PERFORM set_config('search_path', previous_search_path, true);
    PERFORM set_config('quote_all_identifiers', previous_quote_all_identifiers, true);
END
$migration$;
