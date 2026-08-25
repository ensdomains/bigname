COMMENT ON COLUMN chain_phase_state.redo_attempt_generation IS
    'This nonnegative, row-local counter increments whenever an explicit redo begins or a required redo stamp is installed or extended, including whenever resumable markers are invalidated.';
