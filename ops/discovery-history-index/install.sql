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
-- manual build can leave a valid index with other keys or another predicate.
-- Fail here instead of reporting success; README.md describes the recovery.
-- The definition check matches the one in the schema-migration
-- 20260917160000_discovery_edges_index_validity_check.sql. PostgreSQL always
-- prints the table's schema name, and a type's schema name only when the session
-- search_path does not include it, so the schema name is removed before comparing.
DO $check$
DECLARE
    expected_definition constant text :=
        'CREATE INDEX discovery_edges_observation_history_idx ON discovery_edges USING btree (chain_id, from_contract_instance_id, edge_kind, ((provenance ->> ''observation_key''::text)), active_from_block_number) WHERE (canonicality_state <> ''orphaned''::canonicality_state)';
    found_definition text;
BEGIN
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

    SELECT replace(pg_get_indexdef(indexrelid), 'bigname_phase.', '')
    INTO found_definition
    FROM pg_index
    WHERE indexrelid = to_regclass('bigname_phase.discovery_edges_observation_history_idx');
    IF found_definition <> expected_definition THEN
        RAISE EXCEPTION
            'discovery_edges_observation_history_idx exists but does not have the reviewed definition; found "%", expected "%"; follow the recovery steps in ops/discovery-history-index/README.md before retrying',
            found_definition, expected_definition;
    END IF;
END
$check$;
