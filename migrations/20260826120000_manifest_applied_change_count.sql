-- Existing schema-v2 databases gain the per-manifest applied-change counter used by
-- manifest synchronization event identity. An empty schema-migration database has no phase
-- baseline yet, so this step is a no-op there; phase-runner init-schema installs the
-- same column afterward.
DO $migration$
BEGIN
    IF to_regclass('bigname_phase.manifest_versions') IS NULL THEN
        RETURN;
    END IF;

    ALTER TABLE bigname_phase.manifest_versions
        ADD COLUMN IF NOT EXISTS applied_change_count bigint NOT NULL DEFAULT 0;

    UPDATE bigname_phase.manifest_versions manifest
    SET applied_change_count = history.applied_change_count
    FROM (
        SELECT source_manifest_id, count(*)::bigint AS applied_change_count
        FROM bigname_phase.normalized_events
        WHERE event_kind = 'SourceManifestUpdated'
          AND source_manifest_id IS NOT NULL
        GROUP BY source_manifest_id
    ) history
    WHERE manifest.manifest_id = history.source_manifest_id
      AND manifest.applied_change_count = 0;

    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conrelid = 'bigname_phase.manifest_versions'::regclass
          AND conname = 'manifest_versions_applied_change_count_check'
    ) THEN
        ALTER TABLE bigname_phase.manifest_versions
            ADD CONSTRAINT manifest_versions_applied_change_count_check
            CHECK (applied_change_count >= 0) NOT VALID;
    END IF;

    ALTER TABLE bigname_phase.manifest_versions
        VALIDATE CONSTRAINT manifest_versions_applied_change_count_check;

    COMMENT ON COLUMN bigname_phase.manifest_versions.applied_change_count IS
        'This value counts the manifest changes that synchronization has applied.';
END
$migration$;
