-- Prebuild concurrently with ops/project-progressive/install.sql on large databases.
-- This file no longer builds the two whole-array label indexes an earlier version of it
-- built (name_surfaces_project_labels_idx, a GIN over raw_labels, and
-- name_surfaces_project_suffix_idx on (namespace, raw_labels)). Labels are chain data of
-- any length, and an entry of either index over about 2.7 KB fails the name_surfaces
-- insert, so an upgrade must never build them. 20260923140000_project_name_surfaces_label_indexes.sql
-- builds the fixed-size hash indexes that replace them and drops them where the earlier
-- version already built them. A deployment that recorded the earlier version of this
-- file must update its recorded checksum; see ops/project-progressive/README.md.
DO $migration$
BEGIN
    IF to_regclass('bigname_phase.name_surfaces') IS NULL THEN RETURN; END IF;
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
