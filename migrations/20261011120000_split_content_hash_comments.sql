-- Existing schema-v2 databases gain the comments that name which content hash each hash column
-- records once the interpreter content hash and the projection content hash are separate
-- (docs/storage.md § Interpretation replay). Ingest, Verify and Interpret rows record the
-- interpreter content hash; Project and Live rows, and the three Project publication stamps,
-- record the projection content hash. The comments replace the statements 10_phase_state.sql,
-- 20260926100000 and 20261005170000 wrote.
-- Comments only; no column, index or row changes. An empty schema-migration database has no
-- phase baseline yet, so this migration is a no-op there and phase-runner init-schema installs
-- the same comments.
DO $migration$
BEGIN
IF to_regclass('bigname_phase.chain_phase_state') IS NULL THEN
    RETURN;
END IF;

EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.chain_phase_state.input_content_hash IS
    'This value identifies the inputs the phase ran under: the interpreter content hash on Ingest, Verify and Interpret, the projection content hash on Project and Live, or a manifest-authority invalidation marker.'
$ddl$;

IF to_regclass('bigname_phase.project_family_marker') IS NOT NULL THEN
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_family_marker.input_content_hash IS
    'This value is the projection content hash of the binary that applied the block.'
$ddl$;
END IF;

IF to_regclass('bigname_phase.project_repair_record') IS NOT NULL THEN
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_repair_record.completed_input_hash IS
    'This value is the projection content hash the completing loop ran under; null until complete.'
$ddl$;
END IF;

IF to_regclass('bigname_phase.project_history_catalogue_marker') IS NOT NULL THEN
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_history_catalogue_marker.input_content_hash IS
    'The projection content hash of the family publication.'
$ddl$;
END IF;
END
$migration$;
