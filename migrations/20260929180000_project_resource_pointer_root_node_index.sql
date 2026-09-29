-- Existing schema-v2 databases gain the index the composed name reader's
-- resource pointer lookup (storage:families.name.resource_pointers) uses for
-- its root-registry arm: the ENSv2 root registry resource pointers of a chain
-- (project_resource_pointer rows) by namespace and namehash. With the primary
-- key serving the by-resource arm, the lookup is a BitmapOr of two index probes
-- instead of a read of every pointer row of the chain. The index is partial on
-- the same predicate the lookup uses, so it holds only the root registry
-- pointers. Index only; no
-- column or row changes. An empty schema-migration database has no phase
-- baseline yet, so this schema-migration is a no-op there and phase-runner
-- init-schema installs the same index. The guard names the indexed table, not
-- name_current, which 20260929160000_remove_served_projections.sql drops.
DO $migration$
BEGIN
IF to_regclass('bigname_phase.project_resource_pointer') IS NULL THEN
    RETURN;
END IF;

EXECUTE $ddl$
CREATE INDEX IF NOT EXISTS project_resource_pointer_root_node_idx
    ON bigname_phase.project_resource_pointer (chain_id, namespace, namehash)
    WHERE source_family = 'ens_v2_root_l1'
$ddl$;
END
$migration$;
