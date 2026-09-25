-- Existing schema-v2 databases gain the access paths the owned key family loop
-- (TYR-36 step 2) reads once per block: the resolver discovery edges and the
-- contract addresses that start or stop at the block, which reclassify
-- resolvers in project_resolver_classification. Interpret writes both tables;
-- the indexes only add read paths and change no row. An empty
-- schema-migration database has no phase baseline yet, so this migration is a
-- no-op there and phase-runner init-schema installs the same indexes.
DO $migration$
BEGIN
IF to_regclass('bigname_phase.name_current') IS NULL THEN
    RETURN;
END IF;

EXECUTE $ddl$
CREATE INDEX IF NOT EXISTS discovery_edges_resolver_from_block_idx
    ON bigname_phase.discovery_edges (chain_id, active_from_block_number)
    WHERE edge_kind = 'resolver'
$ddl$;
EXECUTE $ddl$
CREATE INDEX IF NOT EXISTS discovery_edges_resolver_to_block_idx
    ON bigname_phase.discovery_edges (chain_id, active_to_block_number)
    WHERE edge_kind = 'resolver' AND active_to_block_number IS NOT NULL
$ddl$;
EXECUTE $ddl$
CREATE INDEX IF NOT EXISTS contract_instance_addresses_from_block_idx
    ON bigname_phase.contract_instance_addresses (chain_id, active_from_block_number)
$ddl$;
EXECUTE $ddl$
CREATE INDEX IF NOT EXISTS contract_instance_addresses_to_block_idx
    ON bigname_phase.contract_instance_addresses (chain_id, active_to_block_number)
    WHERE active_to_block_number IS NOT NULL
$ddl$;
END
$migration$;
