-- Run with psql -X -v ON_ERROR_STOP=1, outside any transaction.
-- An existing Interpret batch may hold the transaction this build waits for.
SET lock_timeout = '0';
SET statement_timeout = '30min';
CREATE INDEX CONCURRENTLY IF NOT EXISTS discovery_edges_reopen_idx
    ON bigname_phase.discovery_edges (
        chain_id,
        from_contract_instance_id,
        edge_kind,
        active_from_block_number,
        (provenance ->> 'observation_key')
    );

-- Printed first so the receipt shows the flags even when the check below fails.
SELECT indexrelid::regclass AS index_name, indisvalid, indisready,
       pg_size_pretty(pg_relation_size(indexrelid)) AS index_size,
       pg_get_indexdef(indexrelid) AS definition
FROM pg_index
WHERE indexrelid = to_regclass('bigname_phase.discovery_edges_reopen_idx');

-- IF NOT EXISTS matches on the name alone, so an interrupted concurrent build
-- leaves an invalid index that the statement above then skips. Fail here instead
-- of reporting success; README.md describes the recovery.
DO $check$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_index
        WHERE indexrelid = to_regclass('bigname_phase.discovery_edges_reopen_idx')
          AND indrelid = to_regclass('bigname_phase.discovery_edges')
          AND indisvalid
          AND indisready
    ) THEN
        RAISE EXCEPTION
            'discovery_edges_reopen_idx is missing from bigname_phase.discovery_edges or is not valid and ready; follow the recovery steps in ops/discovery-reopen-index/README.md before retrying';
    END IF;
END
$check$;
