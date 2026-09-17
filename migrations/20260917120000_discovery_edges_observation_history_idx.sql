-- Prebuild this index concurrently on large live databases using
-- ops/discovery-history-index/install.sql before running schema-migrations.
-- Fresh migration databases may not yet contain the phase baseline.
DO $migration$
BEGIN
    IF to_regclass('bigname_phase.discovery_edges') IS NULL THEN
        RETURN;
    END IF;

    CREATE INDEX IF NOT EXISTS discovery_edges_observation_history_idx
    ON bigname_phase.discovery_edges (
        chain_id,
        from_contract_instance_id,
        edge_kind,
        (provenance ->> 'observation_key'),
        active_from_block_number
    )
    WHERE canonicality_state <> 'orphaned';
END
$migration$;
