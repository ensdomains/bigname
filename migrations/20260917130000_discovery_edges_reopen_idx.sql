-- Prebuild concurrently on large initialized databases with
-- ops/discovery-reopen-index/install.sql before applying schema-migrations.
DO $migration$
BEGIN
    IF to_regclass('bigname_phase.discovery_edges') IS NULL THEN
        RETURN;
    END IF;

    CREATE INDEX IF NOT EXISTS discovery_edges_reopen_idx
    ON bigname_phase.discovery_edges (
        chain_id,
        from_contract_instance_id,
        edge_kind,
        active_from_block_number,
        (provenance ->> 'observation_key')
    );
END
$migration$;
