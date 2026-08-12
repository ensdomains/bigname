-- The replacement constraints were validated in a separate transaction, so this short
-- ACCESS EXCLUSIVE metadata swap does not span a historical-row scan.
DO $migration$
DECLARE
    constraint_pair text[];
BEGIN
    IF to_regclass('bigname_phase.normalized_events') IS NULL THEN
        RETURN;
    END IF;
    FOREACH constraint_pair SLICE 1 IN ARRAY ARRAY[
        ['normalized_events_event_kind_check', 'normalized_events_event_kind_check_v2'],
        ['normalized_events_derivation_kind_check', 'normalized_events_derivation_kind_check_v2'],
        ['normalized_events_migration_correlation_ids_check', 'normalized_events_migration_correlation_ids_check_v2'],
        ['normalized_events_consumer_visibility_check', 'normalized_events_consumer_visibility_check_v2'],
        ['normalized_events_candidate_correlation_check', 'normalized_events_candidate_correlation_check_v2']
    ]
    LOOP
        IF EXISTS (
            SELECT 1 FROM pg_constraint constraint_row
            WHERE constraint_row.conrelid = 'bigname_phase.normalized_events'::regclass
              AND constraint_row.conname = constraint_pair[2]
              AND constraint_row.convalidated
        ) THEN
            IF EXISTS (
                SELECT 1 FROM pg_constraint constraint_row
                WHERE constraint_row.conrelid = 'bigname_phase.normalized_events'::regclass
                  AND constraint_row.conname = constraint_pair[1]
            ) THEN
                EXECUTE format(
                    'ALTER TABLE bigname_phase.normalized_events DROP CONSTRAINT %I',
                    constraint_pair[1]
                );
            END IF;
            EXECUTE format(
                'ALTER TABLE bigname_phase.normalized_events RENAME CONSTRAINT %I TO %I',
                constraint_pair[2], constraint_pair[1]
            );
        END IF;
    END LOOP;
END
$migration$;
