-- Checks two indexes; builds, drops, and changes nothing.
--
-- 20260917120000_discovery_edges_observation_history_idx.sql and
-- 20260917130000_discovery_edges_reopen_idx.sql use CREATE INDEX IF NOT EXISTS,
-- which matches on the name alone. An interrupted concurrent prebuild
-- (ops/discovery-history-index/install.sql, ops/discovery-reopen-index/install.sql)
-- leaves an invalid index under the right name, and a wrong manual prebuild
-- leaves a valid index with other keys or another predicate. Those two files
-- then succeed without building anything. This file stops the run instead of
-- recording success over an index the intended queries cannot use.
--
-- The definition is compared as PostgreSQL prints it with pg_get_indexdef, so
-- key order, expressions, ordering, operator classes, uniqueness, and the
-- predicate are all covered. PostgreSQL adds the schema name to the table
-- always and to the enum type only when the session search_path does not
-- include it, so the schema name is removed before comparing. The expected
-- text is how the fresh baseline index prints; schema-v2/apply-check.sh proves
-- it for the baseline, these schema-migrations, and both install.sql files.
--
-- To recover, follow the README beside the matching install.sql: confirm no build
-- is running, drop only the named index with DROP INDEX CONCURRENTLY, rerun
-- install.sql, then run the schema-migrations again.
--
-- Fresh migration databases may not yet contain the phase baseline, and an index
-- that does not exist is not this file's concern: both cases pass.
DO $migration$
DECLARE
    checked_index text;
    expected_definition text;
    found_definition text;
BEGIN
    IF to_regclass('bigname_phase.discovery_edges') IS NULL THEN
        RETURN;
    END IF;

    FOR checked_index, expected_definition IN
        SELECT * FROM (VALUES
            ('discovery_edges_observation_history_idx',
             'CREATE INDEX discovery_edges_observation_history_idx ON discovery_edges USING btree (chain_id, from_contract_instance_id, edge_kind, ((provenance ->> ''observation_key''::text)), active_from_block_number) WHERE (canonicality_state <> ''orphaned''::canonicality_state)'),
            ('discovery_edges_reopen_idx',
             'CREATE INDEX discovery_edges_reopen_idx ON discovery_edges USING btree (chain_id, from_contract_instance_id, edge_kind, active_from_block_number, ((provenance ->> ''observation_key''::text)))')
        ) AS reviewed(index_name, definition)
    LOOP
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

        SELECT replace(pg_get_indexdef(indexrelid), 'bigname_phase.', '')
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
END
$migration$;
