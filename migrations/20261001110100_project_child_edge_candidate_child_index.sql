-- Existing schema-v2 databases gain the index the child relation
-- (storage:families.topology.children) uses to check that an ENSv1 or Basenames
-- edge is its child's latest across parents: it looks the child's other edge
-- candidates up by chain, namespace and child node. The primary key leads with
-- the parent node, so each check read every edge candidate of the chain.
-- Index only; no column or row changes. An empty schema-migration database has no
-- phase baseline yet, so this schema-migration is a no-op there and phase-runner
-- init-schema installs the same index. The guard names the indexed table, not
-- name_current, which 20260929160000_remove_served_projections.sql drops.
DO $migration$
BEGIN
IF to_regclass('bigname_phase.project_child_edge_candidate') IS NULL THEN
    RETURN;
END IF;

EXECUTE $ddl$
CREATE INDEX IF NOT EXISTS project_child_edge_candidate_child_idx
    ON bigname_phase.project_child_edge_candidate (chain_id, namespace, child_node)
$ddl$;
END
$migration$;
