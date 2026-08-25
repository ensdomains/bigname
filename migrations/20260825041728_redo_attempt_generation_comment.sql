DO $migration$
BEGIN
    IF to_regclass('bigname_phase.chain_phase_state') IS NULL THEN
        RETURN;
    END IF;

    EXECUTE $ddl$
        COMMENT ON COLUMN bigname_phase.chain_phase_state.redo_attempt_generation IS
            'This nonnegative, row-local counter increments when an explicit redo begins and when the phase runner installs or extends a required redo stamp for a downstream phase (Interpret/Project). Manifest-synchronization Ingest stamps do not advance it; their superseded progress writes are fenced by the cleared manifest-authority fingerprint and stamped last_error instead.'
    $ddl$;
END
$migration$;
