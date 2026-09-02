DO $migration$
BEGIN
    IF to_regclass('bigname_phase.normalized_events') IS NULL THEN
        RETURN;
    END IF;
    CREATE INDEX IF NOT EXISTS normalized_events_v2_subregistry_pointer_scope_idx
    ON bigname_phase.normalized_events USING gin ((ARRAY[
        lower(after_state ->> 'subregistry'),
        lower(before_state ->> 'subregistry')
    ]))
    WHERE event_kind = 'SubregistryChanged'
      AND source_family IN ('ens_v2_root_l1', 'ens_v2_registry_l1')
      AND consumer_visibility = 'activated'
      AND canonicality_state IN ('canonical', 'safe', 'finalized')
      AND logical_name_id IS NOT NULL;
END
$migration$;
