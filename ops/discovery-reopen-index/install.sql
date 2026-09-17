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

SELECT indexrelid::regclass AS index_name, indisvalid, indisready,
       pg_size_pretty(pg_relation_size(indexrelid)) AS index_size,
       pg_get_indexdef(indexrelid) AS definition
FROM pg_index
WHERE indexrelid = to_regclass('bigname_phase.discovery_edges_reopen_idx');
