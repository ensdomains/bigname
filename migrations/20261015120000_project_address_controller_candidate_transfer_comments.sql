-- Existing schema-v2 databases gain the project_address_controller_candidate comments that name
-- the ENSv2 registry or root registry TokenControlTransferred, which Project now keeps as a
-- controller candidate for its recipient (docs/api-v1.md § Manager). The comments replace the
-- table, action and subject statements 06_projections.sql wrote.
-- Comments only. No column, index or row changes. An empty schema-migration database has no
-- phase baseline yet, so this migration is a no-op there and phase-runner init-schema installs
-- the same comments.
DO $migration$
BEGIN
IF to_regclass('bigname_phase.project_address_controller_candidate') IS NULL THEN
    RETURN;
END IF;

EXECUTE $ddl$
COMMENT ON TABLE bigname_phase.project_address_controller_candidate IS
    'Project-owned controller candidates of family F13: every named controller event (AuthorityTransferred, state-derived registry-only SurfaceBound, resource-scoped PermissionChanged, and an ENSv2 registry or root registry TokenControlTransferred, whose recipient takes the sender''s roles) with its resource and position, never pruned, so a read folds the candidates the served admission keeps (address_names.rs:115-283).'
$ddl$;

EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_address_controller_candidate.action IS
    'This value is set for an AuthorityTransferred, a SurfaceBound, an ENSv2 registry or root registry TokenControlTransferred and a PermissionChanged whose effective powers hold resource_control, and revoke for any other resource-scoped PermissionChanged, before any read-time mask.'
$ddl$;

EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_address_controller_candidate.subject IS
    'This value is the lower-cased controller the event names: the registry owner (the zero address for a masked owner word), the SurfaceBound owner, the permission subject or the ENSv2 transfer recipient.'
$ddl$;
END
$migration$;
