-- Existing schema-v2 databases gain the composition pass stream and cursor comments of the
-- repair record that name the streams this release adds: 5 the child listing's parents, 6 the
-- event counts, built once at the pass block, and 7 the address listing's registration leads,
-- decided once the names stream ended, with the order the pass runs every stream in. The
-- comments replace the ones 20261027130000_project_compose_stream_comments.sql wrote. A value
-- keeps the meaning a build that wrote it gave it: 3 and 4 are the history catalogue build and
-- the lookup inventories as before.
-- Comments only. No column, index or row changes. An empty schema-migration database has no
-- phase baseline yet, so this migration is a no-op there and phase-runner init-schema installs
-- the same comments. Applied again, it changes nothing.
DO $migration$
BEGIN
IF to_regclass('bigname_phase.project_repair_record') IS NOT NULL THEN
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_repair_record.compose_stream IS
    'This value is the composition pass stream under way: 0 names, 7 address listing leads, 5 child listing parents, 1 record nodes, 2 record ids, 4 lookup inventories, 6 event counts, 3 history catalogue build, run in that order; null outside a pass.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_repair_record.compose_cursor IS
    'This value is the last key the composition pass committed in its stream: the last parent, as a JSON array of namespace and node, in the child listing parents stream, the last resolver and node pair, as a JSON array, in the record node stream, the last resolver and record id pair, as a JSON array, in the record id stream, a lookup name in the lookup inventories stream, and the last step and partition with the partition counts and server version in the address listing leads, event count and history catalogue streams; null throughout the names stream, whose workers claim the work list''s names not yet composed, at a stream''s start and outside a pass.'
$ddl$;
END IF;
END
$migration$;
