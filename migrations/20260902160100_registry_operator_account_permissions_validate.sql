-- Validate registry-operator replacements without holding the later metadata-swap lock.
DO $migration$
DECLARE
    constraint_name text;
BEGIN
    IF to_regclass('bigname_phase.normalized_events') IS NULL THEN
        RETURN;
    END IF;

    FOREACH constraint_name IN ARRAY ARRAY[
        'normalized_events_event_kind_check_registry_operator',
        'normalized_events_derivation_kind_check_registry_operator'
    ] LOOP
        IF EXISTS (
            SELECT 1 FROM pg_constraint
            WHERE conrelid = 'bigname_phase.normalized_events'::regclass
              AND conname = constraint_name AND NOT convalidated
        ) THEN
            EXECUTE format(
                'ALTER TABLE bigname_phase.normalized_events VALIDATE CONSTRAINT %I',
                constraint_name
            );
        END IF;
    END LOOP;

    IF EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'bigname_phase.permissions_current_resource_summary'::regclass
          AND conname = 'permissions_current_resource_summary_registry_binding_check_v2'
          AND NOT convalidated
    ) THEN
        ALTER TABLE bigname_phase.permissions_current_resource_summary
            VALIDATE CONSTRAINT permissions_current_resource_summary_registry_binding_check_v2;
    END IF;
END
$migration$;
