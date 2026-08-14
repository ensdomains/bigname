-- Existing active Ingest redo rows remain NULL and therefore cannot resume
-- their evidence until the current binary restarts the redo from its range start.
DO $migration$
BEGIN
    IF to_regclass('bigname_phase.chain_phase_state') IS NULL THEN
        RETURN;
    END IF;

    ALTER TABLE bigname_phase.chain_phase_state
        ADD COLUMN IF NOT EXISTS redo_manifest_authority_fingerprint text;

    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conrelid = 'bigname_phase.chain_phase_state'::regclass
          AND conname = 'chain_phase_state_ingest_redo_manifest_authority_check'
    ) THEN
        ALTER TABLE bigname_phase.chain_phase_state
            ADD CONSTRAINT chain_phase_state_ingest_redo_manifest_authority_check
            CHECK (
                redo_manifest_authority_fingerprint IS NULL
                OR (
                    phase_name = 'ingest'
                    AND redo_in_progress
                    AND redo_manifest_authority_fingerprint ~ '^[0-9a-f]{64}$'
                )
            ) NOT VALID;
    END IF;

    ALTER TABLE bigname_phase.chain_phase_state
        VALIDATE CONSTRAINT chain_phase_state_ingest_redo_manifest_authority_check;

    COMMENT ON COLUMN bigname_phase.chain_phase_state.redo_manifest_authority_fingerprint IS
        'For an active Ingest redo, this value binds resumable numeric and per-source boundary evidence to the chain''s active manifest rows, excluding normalizer_version.';
END
$migration$;
