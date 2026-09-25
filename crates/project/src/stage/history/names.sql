/* project:stage.history.names */
SELECT matched.normalized_event_id
FROM (
    SELECT logical_name_id FROM project_scope_names
    UNION
    SELECT logical_name_id FROM project_scope_children
) scope
CROSS JOIN LATERAL (
    SELECT normalized_event_id
    FROM normalized_events
    WHERE chain_id = $1 AND block_number <= $2
      AND (event_kind IN ('SubregistryChanged', 'AliasChanged')
           OR (event_kind = 'AuthorityTransferred'
               AND source_family IN ('ens_v1_registry_l1', 'basenames_base_registry')))
      AND canonicality_state IN ('canonical', 'safe', 'finalized')
      AND (namespace || ':' || lower(after_state ->> 'node')) IS NOT NULL
      AND (namespace || ':' || lower(after_state ->> 'node')) = scope.logical_name_id
    UNION ALL
    SELECT normalized_event_id
    FROM normalized_events
    WHERE chain_id = $1 AND block_number <= $2
      AND (event_kind IN ('SubregistryChanged', 'AliasChanged')
           OR (event_kind = 'AuthorityTransferred'
               AND source_family IN ('ens_v1_registry_l1', 'basenames_base_registry')))
      AND canonicality_state IN ('canonical', 'safe', 'finalized')
      AND (namespace || ':' || lower(after_state ->> 'child_node')) IS NOT NULL
      AND (namespace || ':' || lower(after_state ->> 'child_node')) = scope.logical_name_id
    UNION ALL
    SELECT normalized_event_id
    FROM normalized_events
    WHERE chain_id = $1 AND block_number <= $2
      AND (event_kind IN ('SubregistryChanged', 'AliasChanged')
           OR (event_kind = 'AuthorityTransferred'
               AND source_family IN ('ens_v1_registry_l1', 'basenames_base_registry')))
      AND canonicality_state IN ('canonical', 'safe', 'finalized')
      AND (after_state ->> 'to_logical_name_id') IS NOT NULL
      AND (after_state ->> 'to_logical_name_id') = scope.logical_name_id
    UNION ALL
    SELECT normalized_event_id
    FROM normalized_events
    WHERE chain_id = $1 AND block_number <= $2
      AND (event_kind IN ('SubregistryChanged', 'AliasChanged')
           OR (event_kind = 'AuthorityTransferred'
               AND source_family IN ('ens_v1_registry_l1', 'basenames_base_registry')))
      AND canonicality_state IN ('canonical', 'safe', 'finalized')
      AND (before_state ->> 'to_logical_name_id') IS NOT NULL
      AND (before_state ->> 'to_logical_name_id') = scope.logical_name_id
    OFFSET 0
) matched
