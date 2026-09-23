-- Prebuild concurrently with ops/project-progressive/install.sql on large databases.
DO $migration$
BEGIN
    IF to_regclass('bigname_phase.name_surfaces') IS NULL THEN RETURN; END IF;
CREATE INDEX IF NOT EXISTS name_surfaces_project_labels_idx ON bigname_phase.name_surfaces USING gin(raw_labels);
-- Labels are chain data of any length; a btree entry holding the array could exceed the
-- btree size limit and fail the insert, so the suffix lookup indexes a fixed-size hash.
DROP INDEX IF EXISTS bigname_phase.name_surfaces_project_suffix_idx;
CREATE INDEX IF NOT EXISTS name_surfaces_project_suffix_hash_idx ON bigname_phase.name_surfaces(namespace, hash_array_extended(raw_labels, 0));
CREATE INDEX IF NOT EXISTS name_surfaces_project_node_idx ON bigname_phase.name_surfaces(namespace, lower(namehash));
CREATE INDEX IF NOT EXISTS normalized_events_project_v1_pointer_node_idx
    ON bigname_phase.normalized_events(chain_id, namespace, lower(after_state ->> 'node'), block_number)
    WHERE event_kind = 'ResolverChanged'
      AND source_family IN ('ens_v1_registry_l1', 'ens_v1_registrar_l1', 'ens_v1_wrapper_l1')
      AND after_state ->> 'node' IS NOT NULL
      AND consumer_visibility = 'activated'
      AND canonicality_state IN ('canonical', 'safe', 'finalized');
END;
$migration$;
