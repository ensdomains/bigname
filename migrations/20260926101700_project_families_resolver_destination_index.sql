-- Existing schema-v2 databases gain the access path the owned key family loop
-- (TYR-36 step 2) uses to ask whether a contract address that starts or stops
-- at a block is the destination of any resolver edge, deactivated edges
-- included, since a stop boundary must still reclassify its resolver. The
-- existing destination index covers active edges only. Interpret writes
-- discovery_edges; the index only adds a read path and changes no row. An
-- empty schema-migration database has no phase baseline yet, so this migration
-- is a no-op there and phase-runner init-schema installs the same index.
DO $migration$
BEGIN
IF to_regclass('bigname_phase.name_current') IS NULL THEN
    RETURN;
END IF;

EXECUTE $ddl$
CREATE INDEX IF NOT EXISTS project_families_discovery_edges_resolver_destination_idx
    ON bigname_phase.discovery_edges (chain_id, to_contract_instance_id)
    WHERE edge_kind = 'resolver'
$ddl$;
END
$migration$;
