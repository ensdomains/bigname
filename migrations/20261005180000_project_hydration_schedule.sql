-- Existing schema-v2 databases gain hydration's scheduling columns: on project_reverse_tuple
-- and project_node_record_value the largest Multicall3 aggregate a selector may next be sent in
-- and the count of reads in a row that observed nothing for it, and on the two derived work
-- indexes a copy of that count. An empty schema-migration database has no phase baseline yet,
-- so this schema-migration is a no-op there and phase-runner init-schema installs the same
-- columns.
--
-- Stop the family writer and apply this before starting the release that writes the columns.
-- The writer stores rows by column name, so the new writer run against a schema without the
-- columns silently drops the values it has no column for: every head would start splitting a
-- failed aggregate from its full size again. An older writer run against the new schema leaves
-- the columns null, which reads as no limit and no failure.
--
-- The columns are nullable and added without a default, so no row is rewritten and no family
-- is reset: a null limit is the full aggregate size. The family marker table is taken in
-- EXCLUSIVE mode first, the lock a family writer takes first, and held to commit, so no family
-- run writes a row while the columns appear.
DO $migration$
BEGIN
IF to_regclass('bigname_phase.project_reverse_tuple') IS NULL THEN
    RETURN;
END IF;
LOCK TABLE bigname_phase.project_family_marker IN EXCLUSIVE MODE;

ALTER TABLE bigname_phase.project_reverse_tuple
    ADD COLUMN IF NOT EXISTS attempt_limit integer,
    ADD COLUMN IF NOT EXISTS attempt_failures integer;
ALTER TABLE bigname_phase.project_node_record_value
    ADD COLUMN IF NOT EXISTS hydration_limit integer,
    ADD COLUMN IF NOT EXISTS hydration_failures integer;
ALTER TABLE bigname_phase.project_reverse_hydration_work
    ADD COLUMN IF NOT EXISTS attempt_failures integer;
ALTER TABLE bigname_phase.project_text_hydration_work
    ADD COLUMN IF NOT EXISTS hydration_failures integer;

COMMENT ON COLUMN bigname_phase.project_reverse_tuple.attempt_limit IS
    'This value is the largest Multicall3 aggregate hydration may next send the tuple in, left by a read whose aggregate failed as a whole; null when the last read answered the tuple or none failed. Scheduling state only.';
COMMENT ON COLUMN bigname_phase.project_reverse_tuple.attempt_failures IS
    'This value counts the hydration reads in a row that observed no name for the tuple, a failed aggregate or a failed call; null after a read that observed one. Scheduling state only; nothing reads it yet.';
COMMENT ON COLUMN bigname_phase.project_node_record_value.hydration_limit IS
    'This value is the largest Multicall3 aggregate hydration may next send the text selector in, left by a read whose aggregate failed as a whole; null when the last read answered the selector or none failed. Scheduling state only.';
COMMENT ON COLUMN bigname_phase.project_node_record_value.hydration_failures IS
    'This value counts the hydration reads in a row that observed no value for the text selector, a failed aggregate or a failed call; null after a read that observed one. Scheduling state only; nothing reads it yet.';
COMMENT ON COLUMN bigname_phase.project_reverse_hydration_work.attempt_failures IS
    'This value copies project_reverse_tuple.attempt_failures. Nothing orders or filters by it yet.';
COMMENT ON COLUMN bigname_phase.project_text_hydration_work.hydration_failures IS
    'This value copies project_node_record_value.hydration_failures. Nothing orders or filters by it yet.';
END
$migration$;
