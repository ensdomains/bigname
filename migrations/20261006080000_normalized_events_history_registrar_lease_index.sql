-- Project read-only access path for storage:history.publication_memberships.
-- Populated tables require the exact valid concurrent prebuild. No rows, phase
-- markers, catalogue version or content-hash inputs change; no redo is required.
-- See ops/history-registrar-lease-index/README.md for installation and recovery.
-- Normalize pg_get_indexdef settings locally, then restore the caller settings.
DO $migration$
DECLARE
    checked_index text := 'normalized_events_history_registrar_lease_idx';
    expected_definition text := $def$CREATE INDEX normalized_events_history_registrar_lease_idx ON bigname_phase.normalized_events USING btree (resource_id) WHERE ((source_family = 'ens_v1_registrar_l1'::text) AND (canonicality_state <> 'orphaned'::bigname_phase.canonicality_state))$def$;
    found_definition text;
    found_kind text;
    previous_search_path text;
    previous_quote_all_identifiers text;
BEGIN
    IF to_regclass('bigname_phase.normalized_events') IS NULL THEN
        RETURN;
    END IF;

    -- An exact prebuild on populated tables is adopted without taking a writer lock.
    -- Only the empty-table build takes a lock, immediately or not at all, so a
    -- concurrent insert cannot turn the population check into a blocking build.
    IF to_regclass('bigname_phase.normalized_events_history_registrar_lease_idx') IS NULL THEN
        BEGIN
            LOCK TABLE bigname_phase.normalized_events IN SHARE MODE NOWAIT;
        EXCEPTION WHEN lock_not_available THEN
            RAISE EXCEPTION 'normalized_events is busy; run ops/history-registrar-lease-index/install.sql before this schema-migration';
        END;
        IF EXISTS (SELECT 1 FROM bigname_phase.normalized_events) THEN
            RAISE EXCEPTION 'missing prebuilt index normalized_events_history_registrar_lease_idx on populated normalized_events; run ops/history-registrar-lease-index/install.sql before this schema-migration';
        END IF;
        CREATE INDEX IF NOT EXISTS normalized_events_history_registrar_lease_idx
            ON bigname_phase.normalized_events (resource_id)
            WHERE source_family = 'ens_v1_registrar_l1'
              AND canonicality_state <> 'orphaned'::bigname_phase.canonicality_state;
    END IF;

    -- Every name below is schema-qualified or lives in pg_catalog.
    previous_search_path := current_setting('search_path');
    PERFORM set_config('search_path', 'pg_catalog', true);
    -- The expected text above has no quoted identifiers.
    previous_quote_all_identifiers := current_setting('quote_all_identifiers');
    PERFORM set_config('quote_all_identifiers', 'off', true);

    SELECT CASE relkind
               WHEN 'i' THEN 'index'
               WHEN 'I' THEN 'partitioned index'
               WHEN 'r' THEN 'table'
               WHEN 'p' THEN 'partitioned table'
               WHEN 'v' THEN 'view'
               WHEN 'm' THEN 'materialized view'
               WHEN 'S' THEN 'sequence'
               WHEN 'f' THEN 'foreign table'
               WHEN 'c' THEN 'composite type'
               ELSE 'relation of kind ' || relkind::text
           END
    INTO found_kind
    FROM pg_class
    WHERE oid = to_regclass('bigname_phase.' || checked_index);
    IF found_kind IS NULL THEN
        RAISE EXCEPTION
            '% does not exist although bigname_phase.normalized_events does; build it with ops/history-registrar-lease-index/install.sql as ops/history-registrar-lease-index/README.md describes, then run the schema-migrations again',
            checked_index;
    END IF;
    IF found_kind <> 'index' THEN
        RAISE EXCEPTION
            'bigname_phase.% is a %, not an index, so the index was never built; remove or rename that relation, then run the schema-migrations again',
            checked_index, found_kind;
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM pg_index
        WHERE indexrelid = to_regclass('bigname_phase.' || checked_index)
          AND indrelid = to_regclass('bigname_phase.normalized_events')
          AND indisvalid
          AND indisready
    ) THEN
        RAISE EXCEPTION
            '% exists but is not a valid and ready index on bigname_phase.normalized_events; follow the recovery steps in ops/history-registrar-lease-index/README.md, then run the schema-migrations again',
            checked_index;
    END IF;

    SELECT pg_get_indexdef(indexrelid)
    INTO found_definition
    FROM pg_index
    WHERE indexrelid = to_regclass('bigname_phase.' || checked_index);
    IF found_definition <> expected_definition THEN
        RAISE EXCEPTION
            '% exists but does not have the reviewed definition; found "%", expected "%"; follow the recovery steps in ops/history-registrar-lease-index/README.md, then run the schema-migrations again',
            checked_index, found_definition, expected_definition;
    END IF;

    PERFORM set_config('search_path', previous_search_path, true);
    PERFORM set_config('quote_all_identifiers', previous_quote_all_identifiers, true);
END
$migration$;
