-- Validation permits ordinary row-level writers while PostgreSQL scans existing rows. The
-- previous closed event-kind and derivation-kind constraints remain authoritative throughout.
DO $migration$
DECLARE
    constraint_name text;
BEGIN
    IF to_regclass('bigname_phase.normalized_events') IS NULL THEN
        RETURN;
    END IF;
    FOREACH constraint_name IN ARRAY ARRAY[
        'normalized_events_event_kind_check_v2',
        'normalized_events_derivation_kind_check_v2',
        'normalized_events_migration_correlation_ids_check_v2',
        'normalized_events_consumer_visibility_check_v2',
        'normalized_events_candidate_correlation_check_v2'
    ]
    LOOP
        IF EXISTS (
            SELECT 1 FROM pg_constraint constraint_row
            WHERE constraint_row.conrelid = 'bigname_phase.normalized_events'::regclass
              AND constraint_row.conname = constraint_name
              AND NOT constraint_row.convalidated
        ) THEN
            EXECUTE format(
                'ALTER TABLE bigname_phase.normalized_events VALIDATE CONSTRAINT %I',
                constraint_name
            );
        END IF;
    END LOOP;
END
$migration$;
