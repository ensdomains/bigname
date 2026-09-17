-- Existing schema-v2 databases gain the Project-owned `resource_restrictions` column on the
-- per-resource permission summary: the registration-level restriction block (NameWrapper
-- state/fuses/expiry, ENSv2 locked roles) that `GET /v1/permissions` serves. The column is
-- additive and rebuilt by Project; an empty schema-migration database has no phase baseline
-- yet, so this migration is a no-op there and phase-runner init-schema installs the same column.
DO $migration$
BEGIN
IF to_regclass('bigname_phase.permissions_current_resource_summary') IS NULL THEN
    RETURN;
END IF;

EXECUTE $ddl$
ALTER TABLE bigname_phase.permissions_current_resource_summary
    ADD COLUMN IF NOT EXISTS resource_restrictions jsonb
$ddl$;
EXECUTE $ddl$
ALTER TABLE bigname_phase.permissions_current_resource_summary
    DROP CONSTRAINT IF EXISTS permissions_current_resource_summary_restrictions_check
$ddl$;
EXECUTE $ddl$
ALTER TABLE bigname_phase.permissions_current_resource_summary
    ADD CONSTRAINT permissions_current_resource_summary_restrictions_check
    CHECK (resource_restrictions IS NULL OR jsonb_typeof(resource_restrictions) = 'object')
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.permissions_current_resource_summary.resource_restrictions IS
    'The registration-level restriction block: NameWrapper state, expiry-effective fuses, and expiry, or ENSv2 locked roles.'
$ddl$;
END
$migration$;
