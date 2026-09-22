CREATE INDEX CONCURRENTLY IF NOT EXISTS normalized_events_project_node_history_idx
    ON bigname_phase.normalized_events (chain_id, lower(after_state ->> 'node'), block_number)
    WHERE logical_name_id IS NULL
      AND consumer_visibility = 'activated'
      AND canonicality_state IN ('canonical', 'safe', 'finalized')
      AND after_state ->> 'node' IS NOT NULL
      AND ((event_kind IN ('RecordChanged', 'RecordVersionChanged')
            AND source_family IN ('ens_v1_resolver_l1', 'ens_v2_resolver_l1', 'basenames_base_resolver'))
           OR (event_kind = 'ResolverChanged'
               AND source_family IN ('ens_v1_registry_l1', 'ens_v1_registrar_l1', 'ens_v1_wrapper_l1')));

CREATE INDEX CONCURRENTLY IF NOT EXISTS name_surfaces_project_labels_idx ON bigname_phase.name_surfaces USING gin(raw_labels);
CREATE INDEX CONCURRENTLY IF NOT EXISTS name_surfaces_project_suffix_idx ON bigname_phase.name_surfaces(namespace, raw_labels);
CREATE INDEX CONCURRENTLY IF NOT EXISTS name_surfaces_project_node_idx ON bigname_phase.name_surfaces(namespace, lower(namehash));
CREATE INDEX CONCURRENTLY IF NOT EXISTS normalized_events_project_v1_pointer_node_idx
    ON bigname_phase.normalized_events(chain_id, namespace, lower(after_state ->> 'node'), block_number)
    WHERE event_kind = 'ResolverChanged'
      AND source_family IN ('ens_v1_registry_l1', 'ens_v1_registrar_l1', 'ens_v1_wrapper_l1')
      AND after_state ->> 'node' IS NOT NULL
      AND consumer_visibility = 'activated'
      AND canonicality_state IN ('canonical', 'safe', 'finalized');
