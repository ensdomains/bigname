DO $migration$
BEGIN
    IF to_regclass('bigname_phase.normalized_events') IS NULL THEN
        RETURN;
    END IF;

    CREATE INDEX IF NOT EXISTS normalized_events_v1_subregistry_before_child_scope_idx
    ON bigname_phase.normalized_events (
        chain_id,
        (namespace || ':' || lower(before_state ->> 'child_node')),
        block_number
    )
-- issue-435-before-child-predicate-begin
WHERE event_kind = 'SubregistryChanged'
  AND source_family IN ('ens_v1_registry_l1', 'basenames_base_registry')
  AND consumer_visibility = 'activated'
  AND canonicality_state IN ('canonical', 'safe', 'finalized')
  AND before_state ->> 'node' IS NOT NULL
  AND btrim(before_state ->> 'node') <> ''
  AND before_state ->> 'child_node' IS NOT NULL
  AND btrim(before_state ->> 'child_node') <> ''
-- issue-435-before-child-predicate-end
    ;
END
$migration$;
