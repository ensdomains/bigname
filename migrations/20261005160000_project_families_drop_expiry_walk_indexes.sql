-- Existing schema-v2 databases lose the three indexes only the event walk of the expiry
-- listing read (20260928140000_project_families_expiry_indexes.sql): the retained lifecycle
-- events and the NameWrapper states by integral expiry, and the retained events whose expiry
-- is a JSON number that is not an integral second. GET /v1/names now selects its names from
-- project_name_summary (20261005150000_project_name_summary_expiry_selector.sql) and no
-- statement reads these indexes. Indexes only; no column or row changes: expiry_seconds stays
-- on both tables. An empty schema-migration database has no phase baseline yet, so this
-- schema-migration is a no-op there and phase-runner init-schema installs no such index.
--
-- Apply it with the selector schema-migration, before the release that reads the selector
-- starts. An API from before that release still answers GET /v1/names without the indexes,
-- by scanning the two tables; nothing else read them. Each DROP INDEX takes a brief ACCESS
-- EXCLUSIVE lock on its table and waits behind transactions that hold the table.
--
-- A relation of one of these names that is not an index on its table is not dropped: the
-- schema-migration fails without recording itself.
DO $migration$
DECLARE
    dropped record;
BEGIN
IF to_regclass('bigname_phase.project_lifecycle_event') IS NULL THEN
    RETURN;
END IF;

FOR dropped IN
    SELECT * FROM (VALUES
        ('project_lifecycle_event_expiry_idx', 'project_lifecycle_event'),
        ('project_lifecycle_event_inexact_expiry_idx', 'project_lifecycle_event'),
        ('project_wrapper_state_expiry_idx', 'project_wrapper_state')
    ) AS walk_index(index_name, table_name)
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

COMMENT ON COLUMN bigname_phase.project_name_summary.public_authority IS
    'This value is the public authority the composed name row serves (ens_v0, ens_v1 or ens_v2); null when the row serves none (Basenames, an unresolved selection, an ownerless registry row) or the name composes no row. The authority filter of the expiry listing of GET /v1/names selects by it.';
END
$migration$;
