-- Validation is separate so ordinary phase-state writers can continue while
-- PostgreSQL checks initialized namespaces. Fresh baselines already satisfy it.
DO $migration$
BEGIN
    IF to_regclass('bigname_phase.chain_phase_state') IS NULL THEN
        RETURN;
    END IF;

    IF EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conrelid = 'bigname_phase.chain_phase_state'::regclass
          AND conname = 'chain_phase_state_unconfigured_settlement_check'
          AND NOT convalidated
    ) THEN
        ALTER TABLE bigname_phase.chain_phase_state
            VALIDATE CONSTRAINT chain_phase_state_unconfigured_settlement_check;
    END IF;
END
$migration$;
