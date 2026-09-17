-- Checks two indexes; builds, drops, and changes nothing.
--
-- 20260917120000_discovery_edges_observation_history_idx.sql and
-- 20260917130000_discovery_edges_reopen_idx.sql use CREATE INDEX IF NOT EXISTS,
-- which matches on the name alone. An interrupted concurrent prebuild
-- (ops/discovery-history-index/install.sql, ops/discovery-reopen-index/install.sql)
-- leaves an invalid index under the right name, and a wrong manual prebuild
-- leaves a valid index with other keys or another predicate, or a table, view,
-- or other relation that is not an index under the right name. Those two files
-- then succeed without building anything. This file stops the run instead of
-- recording success over an index the intended queries cannot use.
--
-- The definition is compared as PostgreSQL prints it with pg_get_indexdef, so
-- key order, expressions, ordering, operator classes, uniqueness, and the
-- predicate are all covered. PostgreSQL adds the schema name to the table
-- always and to the enum type only when the session search_path does not
-- include it. The printed text is never rewritten to even that out, because a
-- text replacement cannot tell a schema name from the same characters inside
-- a string literal: an index on provenance ->> 'bigname_phase.observation_key'
-- would then compare equal to the reviewed one. Instead search_path is set to
-- pg_catalog while the definitions are read, so PostgreSQL always prints both
-- schema names, and the expected text keeps them. The expected text is how the
-- fresh baseline index prints under that search_path; schema-v2/apply-check.sh
-- proves it for the baseline, these schema-migrations, and both install.sql
-- files.
--
-- The search_path change is transaction-local, and the block puts the previous
-- value back before it returns, so later statements in the same transaction
-- see the search_path they would have seen without this file. When the block
-- raises, the transaction, or the savepoint around it, rolls the change back.
--
-- To recover, follow the README beside the matching install.sql: confirm no build
-- is running, drop only the named index with DROP INDEX CONCURRENTLY, rerun
-- install.sql, then run the schema-migrations again.
--
-- Fresh migration databases may not yet contain the phase baseline; that case
-- passes, and the baseline installed afterwards carries both indexes.
--
-- When bigname_phase.discovery_edges exists, a name that resolves to nothing
-- fails too. The two earlier files build their index whenever the table exists,
-- and every source revision that carries them also carries both indexes in
-- schema-v2/baseline, so no supported order reaches this file with the table
-- present and an index absent. It only happens when the index was dropped, or
-- the table was installed without it after the earlier files were already
-- recorded. Those files never run again, so passing here would leave the
-- intended queries unindexed for good.
DO $migration$
DECLARE
    checked_index text;
    expected_definition text;
    found_definition text;
    found_kind text;
    previous_search_path text;
BEGIN
    IF to_regclass('bigname_phase.discovery_edges') IS NULL THEN
        RETURN;
    END IF;

    -- Every name below is schema-qualified or lives in pg_catalog.
    previous_search_path := current_setting('search_path');
    PERFORM set_config('search_path', 'pg_catalog', true);

    FOR checked_index, expected_definition IN
        SELECT * FROM (VALUES
            ('discovery_edges_observation_history_idx',
             'CREATE INDEX discovery_edges_observation_history_idx ON bigname_phase.discovery_edges USING btree (chain_id, from_contract_instance_id, edge_kind, ((provenance ->> ''observation_key''::text)), active_from_block_number) WHERE (canonicality_state <> ''orphaned''::bigname_phase.canonicality_state)'),
            ('discovery_edges_reopen_idx',
             'CREATE INDEX discovery_edges_reopen_idx ON bigname_phase.discovery_edges USING btree (chain_id, from_contract_instance_id, edge_kind, active_from_block_number, ((provenance ->> ''observation_key''::text)))')
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
                '% does not exist although bigname_phase.discovery_edges does; build it with the matching install.sql as ops/discovery-history-index/README.md or ops/discovery-reopen-index/README.md describes, then run the schema-migrations again',
                checked_index;
        END IF;
        IF found_kind <> 'index' THEN
            RAISE EXCEPTION
                'bigname_phase.% is a %, not an index, so the index was never built; remove or rename that relation, build the index with the matching install.sql as ops/discovery-history-index/README.md or ops/discovery-reopen-index/README.md describes, then run the schema-migrations again',
                checked_index, found_kind;
        END IF;

        IF EXISTS (
            SELECT 1
            FROM pg_index
            WHERE indexrelid = to_regclass('bigname_phase.' || checked_index)
              AND NOT (
                  indrelid = to_regclass('bigname_phase.discovery_edges')
                  AND indisvalid
                  AND indisready
              )
        ) THEN
            RAISE EXCEPTION
                '% exists but is not a valid and ready index on bigname_phase.discovery_edges; follow the recovery steps in ops/discovery-history-index/README.md or ops/discovery-reopen-index/README.md, then run the schema-migrations again',
                checked_index;
        END IF;

        SELECT pg_get_indexdef(indexrelid)
        INTO found_definition
        FROM pg_index
        WHERE indexrelid = to_regclass('bigname_phase.' || checked_index);
        IF found_definition IS NOT NULL
           AND found_definition <> expected_definition
        THEN
            RAISE EXCEPTION
                '% exists but does not have the reviewed definition; found "%", expected "%"; follow the recovery steps in ops/discovery-history-index/README.md or ops/discovery-reopen-index/README.md, then run the schema-migrations again',
                checked_index, found_definition, expected_definition;
        END IF;
    END LOOP;

    PERFORM set_config('search_path', previous_search_path, true);
END
$migration$;
