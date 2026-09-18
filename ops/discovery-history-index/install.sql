-- Run with psql -X -v ON_ERROR_STOP=1, outside any transaction.
-- This index can be preinstalled while the existing runner is processing batches.
-- A long Interpret batch can hold the writer transaction a concurrent build waits for.
-- Bound the whole build, rather than aborting that expected wait after a few seconds.
SET lock_timeout = '0';
SET statement_timeout = '30min';
CREATE INDEX CONCURRENTLY IF NOT EXISTS discovery_edges_observation_history_idx
    ON bigname_phase.discovery_edges (
        chain_id,
        from_contract_instance_id,
        edge_kind,
        (provenance ->> 'observation_key'),
        active_from_block_number
    )
    WHERE canonicality_state <> 'orphaned';

-- Printed first so the receipt shows the flags even when the check below fails.
SELECT indexrelid::regclass AS index_name, indisvalid, indisready,
       pg_size_pretty(pg_relation_size(indexrelid)) AS index_size,
       pg_get_indexdef(indexrelid) AS definition
FROM pg_index
WHERE indexrelid = to_regclass('bigname_phase.discovery_edges_observation_history_idx');

-- IF NOT EXISTS matches on the name alone, so an interrupted concurrent build
-- leaves an invalid index that the statement above then skips, and an earlier
-- manual build can leave a valid index with other keys or another predicate,
-- or a table, view, or other relation that is not an index under this name.
-- Fail here instead of reporting success; README.md describes the recovery.
-- The definition check matches the one in the schema-migration
-- 20260917160000_discovery_edges_index_validity_check.sql. PostgreSQL always
-- prints the table's schema name, and a type's schema name only when the session
-- search_path does not include it. The printed text is not rewritten to even
-- that out, because a text replacement would also change a string literal such
-- as a JSON key. Instead search_path is pg_catalog while the definition is
-- read, so both schema names are always printed, and the expected text keeps
-- them. quote_all_identifiers is turned off for the same read: when the session
-- has it on, PostgreSQL prints every identifier in double quotes and the healthy
-- index would be refused. The quotes are not stripped from the printed text
-- either. Both changes are local to this DO statement's own transaction (this
-- file runs outside a transaction block), and the previous values are put back
-- anyway.
DO $check$
DECLARE
    expected_definition constant text :=
        'CREATE INDEX discovery_edges_observation_history_idx ON bigname_phase.discovery_edges USING btree (chain_id, from_contract_instance_id, edge_kind, ((provenance ->> ''observation_key''::text)), active_from_block_number) WHERE (canonicality_state <> ''orphaned''::bigname_phase.canonicality_state)';
    found_definition text;
    found_kind text;
    previous_search_path constant text := current_setting('search_path');
    previous_quote_all_identifiers constant text :=
        current_setting('quote_all_identifiers');
BEGIN
    -- Every name below is schema-qualified or lives in pg_catalog.
    PERFORM set_config('search_path', 'pg_catalog', true);
    -- The expected text above has no quoted identifiers.
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
    WHERE oid = to_regclass('bigname_phase.discovery_edges_observation_history_idx');
    IF found_kind <> 'index' THEN
        RAISE EXCEPTION
            'bigname_phase.discovery_edges_observation_history_idx is a %, not an index, so the index was never built; remove or rename that relation, then follow ops/discovery-history-index/README.md before retrying',
            found_kind;
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM pg_index
        WHERE indexrelid = to_regclass(
                  'bigname_phase.discovery_edges_observation_history_idx'
              )
          AND indrelid = to_regclass('bigname_phase.discovery_edges')
          AND indisvalid
          AND indisready
    ) THEN
        RAISE EXCEPTION
            'discovery_edges_observation_history_idx is missing from bigname_phase.discovery_edges or is not valid and ready; follow the recovery steps in ops/discovery-history-index/README.md before retrying';
    END IF;

    SELECT pg_get_indexdef(indexrelid)
    INTO found_definition
    FROM pg_index
    WHERE indexrelid = to_regclass('bigname_phase.discovery_edges_observation_history_idx');
    IF found_definition <> expected_definition THEN
        RAISE EXCEPTION
            'discovery_edges_observation_history_idx exists but does not have the reviewed definition; found "%", expected "%"; follow the recovery steps in ops/discovery-history-index/README.md before retrying',
            found_definition, expected_definition;
    END IF;

    PERFORM set_config('search_path', previous_search_path, true);
    PERFORM set_config('quote_all_identifiers', previous_quote_all_identifiers, true);
END
$check$;
