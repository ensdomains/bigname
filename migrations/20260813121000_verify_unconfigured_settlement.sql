-- An empty schema-migration database has no phase baseline yet. On an initialized
-- namespace this nullable marker deliberately leaves every existing row NULL, so
-- ordinary completed rows retain their completed-evidence validation path.
DO $migration$
BEGIN
    IF to_regclass('bigname_phase.chain_phase_state') IS NULL THEN
        RETURN;
    END IF;

    ALTER TABLE bigname_phase.chain_phase_state
        ADD COLUMN IF NOT EXISTS settled_while_unconfigured boolean;

    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conrelid = 'bigname_phase.chain_phase_state'::regclass
          AND conname = 'chain_phase_state_unconfigured_settlement_check'
    ) THEN
        ALTER TABLE bigname_phase.chain_phase_state
            ADD CONSTRAINT chain_phase_state_unconfigured_settlement_check CHECK (
                settled_while_unconfigured IS NULL
                OR settled_while_unconfigured
            ) NOT VALID;
    END IF;

    COMMENT ON COLUMN bigname_phase.chain_phase_state.settled_while_unconfigured IS
        'True only when startup settled an active phase row for a chain absent from runtime configuration; NULL identifies ordinary phase state.';
END
$migration$;
