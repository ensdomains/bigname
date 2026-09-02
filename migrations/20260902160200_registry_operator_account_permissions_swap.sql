-- Validation ran in separate transactions, so these locks cover metadata swaps only.
DO $migration$
BEGIN
    IF to_regclass('bigname_phase.normalized_events') IS NULL THEN
        RETURN;
    END IF;

    IF EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'bigname_phase.normalized_events'::regclass
          AND conname = 'normalized_events_event_kind_check_registry_operator'
          AND convalidated
    ) THEN
        ALTER TABLE bigname_phase.normalized_events
            DROP CONSTRAINT IF EXISTS normalized_events_event_kind_check,
            DROP CONSTRAINT IF EXISTS normalized_events_event_kind_check_v2;
        ALTER TABLE bigname_phase.normalized_events
            RENAME CONSTRAINT normalized_events_event_kind_check_registry_operator
            TO normalized_events_event_kind_check;
    END IF;

    IF EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'bigname_phase.normalized_events'::regclass
          AND conname = 'normalized_events_derivation_kind_check_registry_operator'
          AND convalidated
    ) THEN
        ALTER TABLE bigname_phase.normalized_events
            DROP CONSTRAINT IF EXISTS normalized_events_derivation_kind_check,
            DROP CONSTRAINT IF EXISTS normalized_events_derivation_kind_check_v2,
            DROP CONSTRAINT IF EXISTS normalized_events_derivation_kind_check_raw_block;
        ALTER TABLE bigname_phase.normalized_events
            RENAME CONSTRAINT normalized_events_derivation_kind_check_registry_operator
            TO normalized_events_derivation_kind_check;
    END IF;

    IF EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'bigname_phase.permissions_current_resource_summary'::regclass
          AND conname = 'permissions_current_resource_summary_registry_binding_check_v2'
          AND convalidated
    ) THEN
        ALTER TABLE bigname_phase.permissions_current_resource_summary
            DROP CONSTRAINT IF EXISTS permissions_current_resource_summary_registry_binding_check;
        ALTER TABLE bigname_phase.permissions_current_resource_summary
            RENAME CONSTRAINT permissions_current_resource_summary_registry_binding_check_v2
            TO permissions_current_resource_summary_registry_binding_check;
    END IF;
END
$migration$;
