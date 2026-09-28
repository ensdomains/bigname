-- Existing schema-v2 databases gain the indexes the permission and resolver
-- collection reads use under the publication switch (TYR-36 step 7b slice 4,
-- GET /v1/permissions and GET /v1/resolvers/{chain_id}/{address}/roles): the
-- raw grants (F8) by subject and by chain and scope, the account approvals (F9)
-- by subject, and the registry-binding observations (F2c) by resource and by
-- registry contract and owner. Index only; no column or row changes, and the
-- families are not reset. An empty schema-migration database has no phase
-- baseline yet, so this migration is a no-op there and phase-runner
-- init-schema installs the same indexes.
DO $migration$
BEGIN
IF to_regclass('bigname_phase.name_current') IS NULL THEN
    RETURN;
END IF;

EXECUTE $ddl$
CREATE INDEX IF NOT EXISTS project_grant_subject_idx
    ON bigname_phase.project_grant (subject)
$ddl$;
EXECUTE $ddl$
CREATE INDEX IF NOT EXISTS project_grant_scope_idx
    ON bigname_phase.project_grant (chain_id, scope)
$ddl$;
EXECUTE $ddl$
CREATE INDEX IF NOT EXISTS project_account_approval_subject_idx
    ON bigname_phase.project_account_approval (subject, authority_kind)
$ddl$;
EXECUTE $ddl$
CREATE INDEX IF NOT EXISTS project_registry_binding_observation_resource_idx
    ON bigname_phase.project_registry_binding_observation (chain_id, resource_id)
$ddl$;
EXECUTE $ddl$
CREATE INDEX IF NOT EXISTS project_registry_binding_observation_owner_idx
    ON bigname_phase.project_registry_binding_observation
        (chain_id, registry_contract, registry_owner)
$ddl$;
END
$migration$;
