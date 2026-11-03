-- API read-only access path for GET /v1/names with `parent` (select_expiring_names and the
-- union of windows in crates/storage/src/families/name/list/expiring.rs). The statement
-- selects the names one label below the parent by labelhash path. This partial index keys
-- each surface the statement reads, an active surface that is not the empty root name, by
-- namespace and the hash of its labelhashes after the first, each hashed by
-- bigname_phase.entry_key_hashes. A parent with few names below it is then read from here, and
-- not from every name in the expiry window. The statement also compares the labelhashes
-- themselves, so a hash collision never lists a name. Interpret writes name_surfaces and this
-- index changes none of its rows, no phase marker, catalogue version or content-hash input, and
-- no redo is required. 20261012120000_name_surfaces_project_path_indexes.sql installs
-- entry_key_hashes and refuses one with another definition.
--
-- Populated tables require a valid prebuild with the reviewed definition, built
-- concurrently as docs/deployment.md describes. The prebuild is adopted without a build
-- and without a lock on name_surfaces. Only an empty table builds the index here, and it takes
-- its lock immediately or not at all, so a concurrent Interpret write cannot turn the
-- population check into a blocking build. This is the shape of
-- 20261103130000_project_resolver_link_order_index.sql.
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
    checked_index text := 'name_surfaces_project_parent_key_idx';
    expected_definition text := $def$CREATE INDEX name_surfaces_project_parent_key_idx ON bigname_phase.name_surfaces USING btree (namespace, hash_array_extended(bigname_phase.entry_key_hashes(labelhashes[2:]), (0)::bigint)) WHERE ((visibility_state = 'active'::text) AND (raw_name IS DISTINCT FROM ''::text))$def$;
    found_definition text;
    found_kind text;
    previous_search_path text;
    previous_quote_all_identifiers text;
BEGIN
    IF to_regclass('bigname_phase.name_surfaces') IS NULL THEN
        RETURN;
    END IF;

    IF to_regclass('bigname_phase.name_surfaces_project_parent_key_idx') IS NULL THEN
        BEGIN
            LOCK TABLE bigname_phase.name_surfaces IN SHARE MODE NOWAIT;
        EXCEPTION WHEN lock_not_available THEN
            RAISE EXCEPTION 'name_surfaces is busy; prebuild name_surfaces_project_parent_key_idx as docs/deployment.md describes before this schema-migration';
        END;
        IF EXISTS (SELECT 1 FROM bigname_phase.name_surfaces) THEN
            RAISE EXCEPTION 'missing prebuilt index name_surfaces_project_parent_key_idx on populated name_surfaces; prebuild it as docs/deployment.md describes before this schema-migration';
        END IF;
        CREATE INDEX IF NOT EXISTS name_surfaces_project_parent_key_idx
            ON bigname_phase.name_surfaces
                (namespace, hash_array_extended(bigname_phase.entry_key_hashes(labelhashes[2:]), 0))
            WHERE visibility_state = 'active' AND raw_name IS DISTINCT FROM '';
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
            '% does not exist although bigname_phase.name_surfaces does; prebuild it as docs/deployment.md describes, then run the schema-migrations again',
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
          AND indrelid = to_regclass('bigname_phase.name_surfaces')
          AND indisvalid
          AND indisready
    ) THEN
        RAISE EXCEPTION
            '% exists but is not a valid and ready index on bigname_phase.name_surfaces; follow the recovery steps in docs/deployment.md, then run the schema-migrations again',
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
