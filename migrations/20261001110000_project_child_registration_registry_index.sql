-- Existing schema-v2 databases gain the index the registry labels read
-- (storage:families.topology.children, the ENSv2 candidates of a registry's
-- labels and of a parent's subnames) uses to find the child registrations of one
-- registry contract instance. The primary key leads with the child name, which
-- that join does not bind, so it read every registration row of the chain.
-- Index only; no column or row changes. An empty schema-migration database has no
-- phase baseline yet, so this schema-migration is a no-op there and phase-runner
-- init-schema installs the same index. The guard names the indexed table, not
-- name_current, which 20260929160000_remove_served_projections.sql drops.
DO $migration$
BEGIN
IF to_regclass('bigname_phase.project_child_registration_state') IS NULL THEN
    RETURN;
END IF;

EXECUTE $ddl$
CREATE INDEX IF NOT EXISTS project_child_registration_state_registry_idx
    ON bigname_phase.project_child_registration_state (chain_id, registry_contract_instance_id)
$ddl$;
END
$migration$;
