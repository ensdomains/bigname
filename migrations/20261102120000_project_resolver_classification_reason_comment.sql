-- Existing schema-v2 databases gain the project_resolver_classification.unsupported_reason
-- comment that names composite_resolver_not_projected, the reason of a declared ENSV2Resolver
-- (docs/manifests.md § ENSV2Resolver declaration). The comment replaces the one
-- 06_projections.sql wrote.
-- Comments only. No column, index or row changes. An empty schema-migration database has no
-- phase baseline yet, so this migration is a no-op there and phase-runner init-schema installs
-- the same comment. Applied again, it changes nothing.
DO $migration$
BEGIN
IF to_regclass('bigname_phase.project_resolver_classification') IS NULL THEN
    RETURN;
END IF;

EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_resolver_classification.unsupported_reason IS
    'This value is the reason when unsupported: resolver_not_declared, resolver_implementation_unknown, resolver_implementation_not_declared, composite_resolver_not_projected for a declared ENSV2Resolver, or resolver_manifest_not_active for a resolver with candidates but no active manifest of its family, which the served build leaves out (one such row per resolver).'
$ddl$;
END
$migration$;
