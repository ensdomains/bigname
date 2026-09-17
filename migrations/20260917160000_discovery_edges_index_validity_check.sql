-- Checks two indexes; builds, drops, and changes nothing.
--
-- 20260917120000_discovery_edges_observation_history_idx.sql and
-- 20260917130000_discovery_edges_reopen_idx.sql use CREATE INDEX IF NOT EXISTS,
-- which matches on the name alone. An interrupted concurrent prebuild
-- (ops/discovery-history-index/install.sql, ops/discovery-reopen-index/install.sql)
-- leaves an invalid index under the right name, and those two files then succeed
-- without building anything. This file stops the run instead of recording success
-- over an index no query can use.
--
-- To recover, follow the README beside the matching install.sql: confirm no build
-- is running, drop only the invalid index with DROP INDEX CONCURRENTLY, rerun
-- install.sql, then run the schema-migrations again.
--
-- Fresh migration databases may not yet contain the phase baseline, and an index
-- that does not exist is not this file's concern: both cases pass.
DO $migration$
DECLARE
    checked_index text;
BEGIN
    IF to_regclass('bigname_phase.discovery_edges') IS NULL THEN
        RETURN;
    END IF;

    FOREACH checked_index IN ARRAY ARRAY[
        'discovery_edges_observation_history_idx',
        'discovery_edges_reopen_idx'
    ]
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
    END LOOP;
END
$migration$;
