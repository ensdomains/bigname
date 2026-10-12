-- Existing schema-v2 databases lose sixteen indexes that no statement reads, and the
-- function only one of them used. One is the GIN index on bigname_phase.name_surfaces over
-- bigname_phase.label_hashes(raw_labels), from
-- 20260923140000_project_name_surfaces_label_indexes.sql. The reads it was built for are
-- gone, and the function has no other caller. Fifteen are on
-- bigname_phase.normalized_events, built by the phase baseline and by 20260827130000,
-- 20260827130200, 20260827130300, 20260831150000, 20260917131000 and 20260922010100. Every
-- insert into either table maintained them. Only indexes and one function change: no
-- table, column, row or comment does. The baseline no longer defines any of them, so this
-- schema-migration is a no-op on a database initialized from it, and on an empty
-- schema-migration database.
--
-- A name that is absent is skipped, so a rerun, a database with the walk index set dropped
-- (ops/walk-index-set), and a database where an operator dropped some of them by hand first
-- all pass. An invalid index left by an interrupted concurrent drop is dropped like a valid
-- one. A relation of one of these names that is not an index on its table is not dropped,
-- and the function is dropped without CASCADE, so an object that still depends on it stops
-- the drop. Either way the schema-migration fails without recording itself and changes
-- nothing.
--
-- Each DROP INDEX takes an ACCESS EXCLUSIVE lock on its table, held until this
-- schema-migration commits, and waits behind transactions that hold the table. While it
-- waits, new statements on that table wait behind it. The lock timeout below bounds each
-- wait to five seconds. On a timeout the schema-migration fails unrecorded, and a rerun is
-- safe. docs/deployment.md describes dropping the indexes one at a time with
-- DROP INDEX CONCURRENTLY first, for a database that cannot take the lock.
DO $migration$
DECLARE
    dropped record;
    previous_lock_timeout text := current_setting('lock_timeout');
BEGIN
PERFORM set_config('lock_timeout', '5s', true);

FOR dropped IN
    SELECT * FROM (VALUES
        ('name_surfaces_project_label_hashes_idx', 'name_surfaces'),
        ('normalized_events_v1_subregistry_after_node_scope_idx', 'normalized_events'),
        ('normalized_events_v1_subregistry_before_node_scope_idx', 'normalized_events'),
        ('normalized_events_v1_subregistry_before_child_scope_idx', 'normalized_events'),
        ('normalized_events_v2_expiry_scope_idx', 'normalized_events'),
        ('normalized_events_resolver_alias_history_idx', 'normalized_events'),
        ('normalized_events_permission_after_resolver_history_idx', 'normalized_events'),
        ('normalized_events_permission_before_resolver_history_idx', 'normalized_events'),
        ('normalized_events_subregistry_registration_history_idx', 'normalized_events'),
        ('normalized_events_project_name_after_target_idx', 'normalized_events'),
        ('normalized_events_project_name_before_target_idx', 'normalized_events'),
        ('normalized_events_project_primary_after_idx', 'normalized_events'),
        ('normalized_events_project_primary_before_idx', 'normalized_events'),
        ('normalized_events_project_primary_after_source_idx', 'normalized_events'),
        ('normalized_events_project_primary_before_source_idx', 'normalized_events'),
        ('normalized_events_project_v1_pointer_node_idx', 'normalized_events')
    ) AS unread_index(index_name, table_name)
LOOP
    IF to_regclass('bigname_phase.' || dropped.index_name) IS NULL THEN
        CONTINUE;
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_catalog.pg_index
        WHERE indexrelid = to_regclass('bigname_phase.' || dropped.index_name)
          AND indrelid = to_regclass('bigname_phase.' || dropped.table_name)
    ) THEN
        RAISE EXCEPTION
            'bigname_phase.% is not an index on bigname_phase.%; it is not dropped',
            dropped.index_name, dropped.table_name;
    END IF;
    EXECUTE format('DROP INDEX bigname_phase.%I', dropped.index_name);
END LOOP;

-- After its index. No CASCADE: a leftover dependent stops the schema-migration.
DROP FUNCTION IF EXISTS bigname_phase.label_hashes(text[]);

PERFORM set_config('lock_timeout', previous_lock_timeout, true);
END
$migration$;
