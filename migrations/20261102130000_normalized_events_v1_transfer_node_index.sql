-- API read-only access path for the two Transfer arms of history's registry handoff read
-- (load_history_registry_handoffs in crates/storage/src/history/registry_handoffs.rs). A
-- partial index of the registry Transfer events, keyed by chain and name key, as
-- normalized_events_project_name_node_idx is. That index also keys every child's NewOwner
-- events under the parent node, so for a top-level node such as eth the Transfer arms read
-- all of them and keep none. No rows, phase markers, catalogue version or content-hash
-- inputs change, and no redo is required.
--
-- Populated tables require a valid prebuild with the reviewed definition, built
-- concurrently as docs/deployment.md describes. The prebuild is adopted without a build
-- and without a lock on normalized_events. Only an empty table builds the index here, and
-- it takes its lock immediately or not at all, so a concurrent insert cannot turn the
-- population check into a blocking build. This is the shape of
-- 20261006080000_normalized_events_history_registrar_lease_index.sql.
--
-- CREATE INDEX IF NOT EXISTS matches on the name alone. An interrupted concurrent build
-- leaves an invalid index under the name, and a wrong manual build leaves another
-- definition or a relation that is not an index. The check at the end refuses each of
-- them. It compares the definition as pg_get_indexdef prints it with search_path set to
-- pg_catalog, so PostgreSQL prints schema names, and quote_all_identifiers off, so it prints
-- no extra quotes. Both settings are transaction-local and put back before the block
-- returns.
--
-- An empty schema-migration database has no phase baseline yet, so this schema-migration
-- is a no-op there, and phase-runner init-schema installs the same index.
DO $migration$
DECLARE
    checked_index text := 'normalized_events_v1_transfer_node_idx';
    expected_definition text := $def$CREATE INDEX normalized_events_v1_transfer_node_idx ON bigname_phase.normalized_events USING btree (chain_id, (((namespace || ':'::text) || lower((after_state ->> 'node'::text)))), block_number) INCLUDE (normalized_event_id) WHERE ((event_kind = 'AuthorityTransferred'::text) AND (source_family = ANY (ARRAY['ens_v1_registry_l1'::text, 'basenames_base_registry'::text])) AND ((after_state ->> 'source_event'::text) = 'Transfer'::text) AND (canonicality_state = ANY (ARRAY['canonical'::bigname_phase.canonicality_state, 'safe'::bigname_phase.canonicality_state, 'finalized'::bigname_phase.canonicality_state])))$def$;
    found_definition text;
    found_kind text;
    previous_search_path text;
    previous_quote_all_identifiers text;
BEGIN
    IF to_regclass('bigname_phase.normalized_events') IS NULL THEN
        RETURN;
    END IF;

    IF to_regclass('bigname_phase.normalized_events_v1_transfer_node_idx') IS NULL THEN
        BEGIN
            LOCK TABLE bigname_phase.normalized_events IN SHARE MODE NOWAIT;
        EXCEPTION WHEN lock_not_available THEN
            RAISE EXCEPTION 'normalized_events is busy; prebuild normalized_events_v1_transfer_node_idx as docs/deployment.md describes before this schema-migration';
        END;
        IF EXISTS (SELECT 1 FROM bigname_phase.normalized_events) THEN
            RAISE EXCEPTION 'missing prebuilt index normalized_events_v1_transfer_node_idx on populated normalized_events; prebuild it as docs/deployment.md describes before this schema-migration';
        END IF;
        CREATE INDEX IF NOT EXISTS normalized_events_v1_transfer_node_idx
            ON bigname_phase.normalized_events (chain_id, (namespace || ':' || lower(after_state ->> 'node')), block_number)
            INCLUDE (normalized_event_id)
            WHERE event_kind = 'AuthorityTransferred'
              AND source_family IN ('ens_v1_registry_l1', 'basenames_base_registry')
              AND after_state ->> 'source_event' = 'Transfer'
              AND canonicality_state IN ('canonical', 'safe', 'finalized');
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
            '% does not exist although bigname_phase.normalized_events does; prebuild it as docs/deployment.md describes, then run the schema-migrations again',
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
            '% exists but is not a valid and ready index on bigname_phase.normalized_events; follow the recovery steps in docs/deployment.md, then run the schema-migrations again',
            checked_index;
    END IF;

    SELECT pg_get_indexdef(indexrelid)
    INTO found_definition
    FROM pg_index
    WHERE indexrelid = to_regclass('bigname_phase.' || checked_index);
    IF found_definition <> expected_definition THEN
        RAISE EXCEPTION
            '% exists but does not have the reviewed definition; found "%", expected "%"; follow the recovery steps in docs/deployment.md, then run the schema-migrations again',
            checked_index, found_definition, expected_definition;
    END IF;

    PERFORM set_config('search_path', previous_search_path, true);
    PERFORM set_config('quote_all_identifiers', previous_quote_all_identifiers, true);
END
$migration$;
