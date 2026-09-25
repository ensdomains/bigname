-- Existing schema-v2 databases gain the columns the owned key family loop
-- (TYR-36 step 2) records with every family block: the whole input token and
-- the admission epoch the block read inside its own transaction, on
-- project_family_marker, and whether a repair record holds a captured input
-- revision, on project_repair_record. The columns are additive and unread by
-- every served path. An empty schema-migration database has no phase baseline
-- yet, so this migration is a no-op there and phase-runner init-schema
-- installs the same columns.
DO $migration$
BEGIN
IF to_regclass('bigname_phase.name_current') IS NULL THEN
    RETURN;
END IF;

EXECUTE $ddl$
ALTER TABLE bigname_phase.project_family_marker
    ADD COLUMN IF NOT EXISTS interpret_redo_in_progress boolean,
    ADD COLUMN IF NOT EXISTS project_redo_attempt bigint,
    ADD COLUMN IF NOT EXISTS project_redo_mode text,
    ADD COLUMN IF NOT EXISTS project_redo_from bigint,
    ADD COLUMN IF NOT EXISTS project_redo_to bigint,
    ADD COLUMN IF NOT EXISTS admission_epoch text
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_family_marker.interpret_input_content_hash IS
    'This value is the Interpret row''s input_content_hash the last block read inside its own transaction, the first half of the input revision.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_family_marker.interpret_redo_attempt IS
    'This value is the Interpret row''s redo_attempt_generation the last block read inside its own transaction, the second half of the input revision.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_family_marker.interpret_redo_in_progress IS
    'This value is the Interpret row''s redo_in_progress the last block read; always false after a block, since no block applies while Interpret is in redo, and null on a reset marker.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_family_marker.project_redo_attempt IS
    'This value is the Project row''s redo_attempt_generation the last block read inside its own transaction.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_family_marker.project_redo_mode IS
    'This value is the Project row''s redo_mode the last block read, null when no redo was open.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_family_marker.project_redo_from IS
    'This value is the Project row''s redo_from_block_number the last block read.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_family_marker.project_redo_to IS
    'This value is the Project row''s redo_to_block_number the last block read.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_family_marker.admission_epoch IS
    'This value names the latest SourceManifestUpdated event of every manifest the chain reads, as the last block saw it; a block that sees another epoch classifies every stored resolver again.'
$ddl$;
EXECUTE $ddl$
ALTER TABLE bigname_phase.project_repair_record
    ADD COLUMN IF NOT EXISTS prefix_recorded boolean NOT NULL DEFAULT false
$ddl$;
IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'project_repair_record_prefix_recorded_check'
      AND conrelid = 'bigname_phase.project_repair_record'::regclass
) THEN
    EXECUTE $ddl$
    ALTER TABLE bigname_phase.project_repair_record
        ADD CONSTRAINT project_repair_record_prefix_recorded_check
        CHECK (state <> 'undoing' OR NOT prefix_recorded)
    $ddl$;
END IF;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_repair_record.prefix_recorded IS
    'This value is true once the replay or rebuild captured its input revision in prefix_interpret_input_content_hash and prefix_interpret_redo_attempt, which may both be null when the chain has no Interpret row; false while undoing.'
$ddl$;
END
$migration$;
