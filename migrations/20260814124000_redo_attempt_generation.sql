-- Existing rows start at generation zero. Each subsequent redo begin increments
-- the row-local counter before any batch context is created.
DO $migration$
BEGIN
    IF to_regclass('bigname_phase.chain_phase_state') IS NULL THEN
        RETURN;
    END IF;

    ALTER TABLE bigname_phase.chain_phase_state
        ADD COLUMN IF NOT EXISTS redo_attempt_generation bigint NOT NULL DEFAULT 0;

    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conrelid = 'bigname_phase.chain_phase_state'::regclass
          AND conname = 'chain_phase_state_redo_attempt_generation_check'
    ) THEN
        ALTER TABLE bigname_phase.chain_phase_state
            ADD CONSTRAINT chain_phase_state_redo_attempt_generation_check
            CHECK (redo_attempt_generation >= 0) NOT VALID;
    END IF;

    ALTER TABLE bigname_phase.chain_phase_state
        VALIDATE CONSTRAINT chain_phase_state_redo_attempt_generation_check;

    COMMENT ON COLUMN bigname_phase.chain_phase_state.redo_attempt_generation IS
        'This nonnegative counter increments whenever an explicit redo begins and fences its progress writes to that attempt.';
END
$migration$;
