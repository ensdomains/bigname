-- Existing rows remain NULL. Only an active Ingest redo records load-derived
-- per-source boundary markers, and every terminal redo path clears them.
DO $migration$
BEGIN
    IF to_regclass('bigname_phase.chain_phase_state') IS NULL THEN
        RETURN;
    END IF;

    ALTER TABLE bigname_phase.chain_phase_state
        ADD COLUMN IF NOT EXISTS redo_source_boundary_markers jsonb;

    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conrelid = 'bigname_phase.chain_phase_state'::regclass
          AND conname = 'chain_phase_state_ingest_redo_source_boundaries_check'
    ) THEN
        ALTER TABLE bigname_phase.chain_phase_state
            ADD CONSTRAINT chain_phase_state_ingest_redo_source_boundaries_check CHECK (
                redo_source_boundary_markers IS NULL
                OR (
                    phase_name = 'ingest'
                    AND redo_in_progress
                    AND jsonb_typeof(redo_source_boundary_markers) = 'object'
                    AND redo_source_boundary_markers <> '{}'::jsonb
                )
            ) NOT VALID;
    END IF;

    ALTER TABLE bigname_phase.chain_phase_state
        VALIDATE CONSTRAINT chain_phase_state_ingest_redo_source_boundaries_check;

    COMMENT ON COLUMN bigname_phase.chain_phase_state.redo_source_boundary_markers IS
        'This object maps each Ingest source key to a block number and hash returned by a boundary load during the active redo.';
END
$migration$;
