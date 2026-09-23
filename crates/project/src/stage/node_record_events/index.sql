CREATE INDEX project_node_record_history_lookup ON project_node_record_history
(lower(after_state ->> 'node'), source_family,
 lower(COALESCE(NULLIF(after_state ->> 'resolver', ''), NULLIF(raw_fact_ref ->> 'emitting_address', ''))));
ANALYZE project_node_record_history;
