-- Existing schema-v2 databases gain the composition pass stream and cursor comments of the
-- repair record that name the lookup inventories stream, which runs after the record id stream
-- and before the history catalogue build, and the names stream's workers, which claim the work
-- list's names not yet composed and leave the cursor null. The comments replace the ones
-- 20261027120000_project_compose_history_builder.sql wrote. The lookup name's serving resource
-- comment gains the pass window in which the names stream leaves it null and the lookup
-- inventories stream sets it.
-- Comments only. No column, index or row changes. An empty schema-migration database has no
-- phase baseline yet, so this migration is a no-op there and phase-runner init-schema installs
-- the same comments. Applied again, it changes nothing.
DO $migration$
BEGIN
IF to_regclass('bigname_phase.project_repair_record') IS NOT NULL THEN
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_repair_record.compose_stream IS
    'This value is the composition pass stream under way: 0 names, 1 record nodes, 2 record ids, 4 lookup inventories, 3 history catalogue build, run in that order; null outside a pass.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_repair_record.compose_cursor IS
    'This value is the last key the composition pass committed in its stream: the last resolver and node pair, as a JSON array, in the record node stream, the last resolver and record id pair, as a JSON array, in the record id stream, a lookup name in the lookup inventories stream, and the last step and partition with the partition counts and server version in the history catalogue build; null throughout the names stream, whose workers claim the work list''s names not yet composed, at a stream''s start and outside a pass.'
$ddl$;
END IF;
IF to_regclass('bigname_phase.project_lookup_name') IS NOT NULL THEN
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_lookup_name.record_serving_resource_id IS
    'Resource whose factored record inventory serves this name, or null when absent. In a rebuild''s composition pass the names stream writes it null and the lookup inventories stream sets it with the inventory it composes. Nothing serves the table in that window, since the family marker is not live until the rebuild''s completing block.'
$ddl$;
END IF;
END
$migration$;
