-- TYR-114: retain Project's requested invalidation separately from actual undo/rebuild/replay.
-- No row rewrite or publication reset. Legacy active requests adopt their existing bounds at
-- the next fenced redo begin. An empty phase schema remains empty for production bootstrap.
DO $migration$
BEGIN
IF to_regclass('bigname_phase.chain_phase_state') IS NULL THEN RETURN; END IF;
ALTER TABLE bigname_phase.chain_phase_state
    ADD COLUMN IF NOT EXISTS redo_requested_from_block_number bigint,
    ADD COLUMN IF NOT EXISTS redo_requested_to_block_number bigint;
IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conrelid = 'bigname_phase.chain_phase_state'::regclass
      AND conname = 'chain_phase_state_project_redo_request_check'
) THEN
    ALTER TABLE bigname_phase.chain_phase_state ADD
CONSTRAINT chain_phase_state_project_redo_request_check CHECK (
        (redo_requested_from_block_number IS NULL AND redo_requested_to_block_number IS NULL)
        OR (
            phase_name = 'project' AND redo_in_progress
            AND redo_requested_from_block_number IS NOT NULL
            AND redo_requested_to_block_number IS NOT NULL
            AND redo_requested_from_block_number >= redo_from_block_number
            AND redo_requested_to_block_number <= redo_to_block_number
            AND redo_requested_to_block_number >= redo_requested_from_block_number
        )
    );
END IF;
COMMENT ON COLUMN bigname_phase.chain_phase_state.redo_requested_from_block_number IS
    'For Project redo, the first requested invalidation block; execution may undo or rebuild below it. NULL on legacy active rows until the next begin.';
COMMENT ON COLUMN bigname_phase.chain_phase_state.redo_requested_to_block_number IS
    'For Project redo, the last requested invalidation block; execution may replay above it to the standing publication. NULL on legacy active rows until the next begin.';
COMMENT ON COLUMN bigname_phase.chain_phase_state.redo_from_block_number IS
    'This value is the first block in the active redo execution extent; Project retains requested invalidation separately.';
COMMENT ON COLUMN bigname_phase.chain_phase_state.redo_to_block_number IS
    'This value is the last block in the active redo execution extent; Project retains requested invalidation separately.';
END
$migration$;
