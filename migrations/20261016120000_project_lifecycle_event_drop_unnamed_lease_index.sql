-- Existing schema-v2 databases lose project_lifecycle_event_unnamed_lease_idx, the partial
-- index on bigname_phase.project_lifecycle_event over the ENSv1 .eth registrar lease events
-- that carry no name yet (20260926100800_project_families_fidelity.sql). Project's read of
-- those events for a block's leases now probes the table's primary key on chain, state kind
-- and lease. With the partial index present, PostgreSQL judged it far smaller than it was,
-- read every row of it for the chain on each call, then filtered by lease. Only the index
-- changes: no table, column, row or comment does. The
-- baseline no longer defines it, so this schema-migration is a no-op on a database
-- initialized from it, and on an empty schema-migration database.
--
-- An absent index is skipped, so a rerun and a database where an operator dropped it by hand
-- first both pass. A relation of that name that is not an index on the table is not dropped:
-- the schema-migration fails without recording itself and changes nothing.
--
-- DROP INDEX takes an ACCESS EXCLUSIVE lock on project_lifecycle_event, held until this
-- schema-migration commits, and waits behind transactions that hold the table. While it
-- waits, new statements on the table wait behind it. The lock timeout below bounds the wait
-- to five seconds. On a timeout the schema-migration fails unrecorded, and a rerun is safe.
-- docs/deployment.md describes dropping the index with DROP INDEX CONCURRENTLY first, for a
-- database that cannot take the lock.
DO $migration$
DECLARE
    previous_lock_timeout text := current_setting('lock_timeout');
BEGIN
IF to_regclass('bigname_phase.project_lifecycle_event') IS NULL
    OR to_regclass('bigname_phase.project_lifecycle_event_unnamed_lease_idx') IS NULL THEN
    RETURN;
END IF;
IF NOT EXISTS (
    SELECT 1 FROM pg_catalog.pg_index
    WHERE indexrelid = to_regclass('bigname_phase.project_lifecycle_event_unnamed_lease_idx')
      AND indrelid = to_regclass('bigname_phase.project_lifecycle_event')
) THEN
    RAISE EXCEPTION
        'bigname_phase.project_lifecycle_event_unnamed_lease_idx is not an index on bigname_phase.project_lifecycle_event; it is not dropped';
END IF;

PERFORM set_config('lock_timeout', '5s', true);
DROP INDEX bigname_phase.project_lifecycle_event_unnamed_lease_idx;
PERFORM set_config('lock_timeout', previous_lock_timeout, true);
END
$migration$;
