-- Validation ran in a separate transaction, so this lock covers only the metadata swap.
DO $migration$
BEGIN
    IF to_regclass('bigname_phase.normalized_events') IS NULL THEN
        RETURN;
    END IF;

    IF EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conrelid = 'bigname_phase.normalized_events'::regclass
          AND conname = 'normalized_events_derivation_kind_check_raw_block'
          AND convalidated
    ) THEN
        ALTER TABLE bigname_phase.normalized_events
            DROP CONSTRAINT IF EXISTS normalized_events_derivation_kind_check;
        ALTER TABLE bigname_phase.normalized_events
            RENAME CONSTRAINT normalized_events_derivation_kind_check_raw_block
            TO normalized_events_derivation_kind_check;
    END IF;
END
$migration$;
