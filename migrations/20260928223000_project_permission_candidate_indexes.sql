-- Resource cursor seeks for account permission pages; additive indexes only.
-- Prebuild concurrently on large databases as documented in docs/deployment.md.
DO $migration$
BEGIN
IF to_regclass('bigname_phase.name_current') IS NULL THEN
    RETURN;
END IF;
EXECUTE $ddl$
CREATE INDEX IF NOT EXISTS project_grant_subject_resource_idx
    ON bigname_phase.project_grant (subject COLLATE "C", resource_id, scope COLLATE "C")
$ddl$;
EXECUTE $ddl$
CREATE INDEX IF NOT EXISTS project_registry_binding_observation_owner_target_idx
    ON bigname_phase.project_registry_binding_observation (chain_id, registry_contract, registry_owner, target_resource_id)
$ddl$;
EXECUTE $ddl$
CREATE INDEX IF NOT EXISTS project_registry_binding_observation_owner_resource_idx
    ON bigname_phase.project_registry_binding_observation (chain_id, registry_contract, registry_owner, resource_id)
$ddl$;
EXECUTE $ddl$
CREATE INDEX IF NOT EXISTS project_grant_resource_subject_idx
    ON bigname_phase.project_grant (resource_id, subject COLLATE "C", scope COLLATE "C")
$ddl$;
END
$migration$;
