-- Validate the additive attempt-state invariant separately from the metadata-only DDL.
DO $migration$
BEGIN
    IF to_regclass('bigname_phase.primary_names_current') IS NULL THEN
        RETURN;
    END IF;

    IF EXISTS (
        SELECT 1
        FROM pg_constraint constraint_row
        WHERE constraint_row.conrelid =
                'bigname_phase.primary_names_current'::regclass
          AND constraint_row.conname =
                'primary_names_current_reverse_hydration_attempt_check'
          AND NOT constraint_row.convalidated
    ) THEN
        ALTER TABLE bigname_phase.primary_names_current
            VALIDATE CONSTRAINT
                primary_names_current_reverse_hydration_attempt_check;
    END IF;
END
$migration$;
