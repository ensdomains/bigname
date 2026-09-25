/* project:stage.node_record_events.index.index_node_record_history_after_state */
CREATE INDEX project_node_record_history_lookup ON project_node_record_history
(lower(after_state ->> 'node'), source_family,
 lower(COALESCE(NULLIF(after_state ->> 'resolver', ''), NULLIF(raw_fact_ref ->> 'emitting_address', ''))));
/* project:stage.node_record_events.index.analyze_node_record_history */
ANALYZE project_node_record_history;
