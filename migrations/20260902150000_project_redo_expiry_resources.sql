-- Extend the existing path-expiry replay handoff without rewriting its applied
-- schema-migration. A schema-migration database may still precede baseline
-- installation, so leave that empty path to init-schema.
DO $migration$
BEGIN
    IF to_regclass('bigname_phase.project_redo_expiry_roots') IS NULL THEN
        RETURN;
    END IF;

    ALTER TABLE bigname_phase.project_redo_expiry_roots
        ADD COLUMN IF NOT EXISTS resource_id uuid;
    ALTER TABLE bigname_phase.project_redo_expiry_roots
        ALTER COLUMN logical_name_id DROP NOT NULL;

    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conrelid = 'bigname_phase.project_redo_expiry_roots'::regclass
          AND conname = 'project_redo_expiry_roots_scope_check'
    ) THEN
        ALTER TABLE bigname_phase.project_redo_expiry_roots
            ADD CONSTRAINT project_redo_expiry_roots_scope_check
            CHECK (logical_name_id IS NOT NULL OR resource_id IS NOT NULL);
    END IF;

    COMMENT ON TABLE bigname_phase.project_redo_expiry_roots IS
        'Interpret preserves logical names or permission resources from deleted state-derived ENSv2 path-expiry releases here until Project publishes a covering redo.';
    COMMENT ON COLUMN bigname_phase.project_redo_expiry_roots.logical_name_id IS
        'When present, this value seeds bounded traversal from the name whose deleted path-expiry release removed descendant projections.';
    COMMENT ON COLUMN bigname_phase.project_redo_expiry_roots.resource_id IS
        'When present, this value identifies the permission resource whose deleted path-expiry release must seed Project redo.';
    COMMENT ON COLUMN bigname_phase.project_redo_expiry_roots.recorded_at IS
        'This time records the Interpret redo that first captured the path-expiry release for pending Project repair.';
END
$migration$;
