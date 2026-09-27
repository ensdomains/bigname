-- Existing schema-v2 databases gain the column on project_repair_record that
-- records the family marker generation a rebuild's reset wrote (TYR-36 step
-- 2). Blocks rebuilt since the reset are the generations above it, which
-- times the rebuild's statistics refreshes across runs. The column is
-- additive and unread by every served path. An empty schema-migration
-- database has no phase baseline yet, so this migration is a no-op there and
-- phase-runner init-schema installs the same column.
DO $migration$
BEGIN
IF to_regclass('bigname_phase.name_current') IS NULL THEN
    RETURN;
END IF;

EXECUTE $ddl$
ALTER TABLE bigname_phase.project_repair_record
    ADD COLUMN IF NOT EXISTS reset_sequence bigint
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_repair_record.reset_sequence IS
    'This value is the family marker generation the rebuild''s reset wrote; null for an undo-then-replay.'
$ddl$;
END
$migration$;
