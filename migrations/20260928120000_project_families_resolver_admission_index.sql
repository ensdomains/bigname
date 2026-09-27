-- Existing schema-v2 databases gain the access path the owned key family loop
-- (TYR-54) uses to classify a block's touched resolvers: from each resolver's
-- contract instance, one active resolver edge per active manifest, instead of a
-- scan of every resolver edge active at the block. Interpret writes
-- discovery_edges; the index only adds a read path and changes no row. An
-- empty schema-migration database has no phase baseline yet, so this migration
-- is a no-op there and phase-runner init-schema installs the same index.
DO $migration$
BEGIN
IF to_regclass('bigname_phase.name_current') IS NULL THEN
    RETURN;
END IF;

EXECUTE $ddl$
CREATE INDEX IF NOT EXISTS project_families_discovery_edges_resolver_admission_idx
    ON bigname_phase.discovery_edges (
        chain_id,
        to_contract_instance_id,
        source_manifest_id,
        active_from_block_number
    )
    WHERE edge_kind = 'resolver' AND deactivated_at IS NULL
$ddl$;
END
$migration$;
