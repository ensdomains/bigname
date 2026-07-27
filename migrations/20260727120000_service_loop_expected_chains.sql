ALTER TABLE service_loop_heartbeats
    ADD COLUMN expected_chain_ids TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[];

UPDATE service_loop_heartbeats AS process
SET expected_chain_ids = ARRAY(
    SELECT chain.scope_id
    FROM service_loop_heartbeats AS chain
    WHERE chain.service_name = process.service_name
      AND chain.instance_id = process.instance_id
      AND chain.scope_kind = 'chain'
    ORDER BY chain.scope_id
)
WHERE process.service_name = 'indexer'
  AND process.scope_kind = 'process'
  AND process.scope_id = 'process';

ALTER TABLE service_loop_heartbeats
    ADD CONSTRAINT service_loop_heartbeats_expected_chains_check CHECK (
        scope_kind = 'process'
        OR cardinality(expected_chain_ids) = 0
    );
