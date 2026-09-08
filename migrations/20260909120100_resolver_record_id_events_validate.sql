-- Validate separately from the later metadata-swap transaction.
DO $migration$
BEGIN
    IF to_regclass('bigname_phase.normalized_events') IS NULL THEN
        RETURN;
    END IF;
    IF EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'bigname_phase.normalized_events'::regclass
          AND conname = 'normalized_events_event_kind_check_record_id'
          AND NOT convalidated
    ) THEN
        ALTER TABLE bigname_phase.normalized_events
            VALIDATE CONSTRAINT normalized_events_event_kind_check_record_id;
    END IF;
END
$migration$;
