-- Prebuild concurrently with ops/project-progressive/install.sql on large databases.
DO $migration$
BEGIN
    IF to_regclass('bigname_phase.normalized_events') IS NULL THEN RETURN; END IF;
    CREATE INDEX IF NOT EXISTS normalized_events_project_node_history_idx
    ON bigname_phase.normalized_events (chain_id, lower(after_state ->> 'node'), block_number)
    WHERE logical_name_id IS NULL
      AND consumer_visibility = 'activated'
      AND canonicality_state IN ('canonical', 'safe', 'finalized')
      AND after_state ->> 'node' IS NOT NULL
      AND ((event_kind IN ('RecordChanged', 'RecordVersionChanged')
            AND source_family IN ('ens_v1_resolver_l1', 'ens_v2_resolver_l1', 'basenames_base_resolver'))
           OR (event_kind = 'ResolverChanged'
               AND source_family IN ('ens_v1_registry_l1', 'ens_v1_registrar_l1', 'ens_v1_wrapper_l1')));
END;
$migration$;
