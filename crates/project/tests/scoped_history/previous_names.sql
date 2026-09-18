SELECT event.normalized_event_id
FROM normalized_events event
CROSS JOIN LATERAL (
    VALUES
        (event.namespace || ':' || lower(event.after_state ->> 'node')),
        (event.namespace || ':' || lower(event.after_state ->> 'child_node')),
        (event.after_state ->> 'to_logical_name_id'),
        (event.before_state ->> 'to_logical_name_id')
) candidate(logical_name_id)
JOIN (
    SELECT logical_name_id FROM project_scope_names
    UNION
    SELECT logical_name_id FROM project_scope_children
) scope
  ON scope.logical_name_id = candidate.logical_name_id
WHERE event.chain_id = $1 AND event.block_number <= $2
  AND (
      event.event_kind IN ('SubregistryChanged', 'AliasChanged')
      OR (
          event.event_kind = 'AuthorityTransferred'
          AND event.source_family IN (
              'ens_v1_registry_l1', 'basenames_base_registry'
          )
      )
  )
  AND event.canonicality_state IN ('canonical', 'safe', 'finalized')
