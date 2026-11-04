-- API read-only access path for the resolver links page (load_resolver_links_shadow in
-- crates/storage/src/families/topology/collections.rs), which GET
-- /v1/resolvers/{chain_id}/{address}/links serves. A partial index of the resolver links
-- with a non-zero record, keyed by chain and resolver and then in the page order, so the page
-- reads the resolver's links from its cursor and stops at the page size. Without it the
-- statement renders and sorts every link of the resolver for each page. No rows, phase
-- markers, catalogue version or content-hash inputs change, and no redo is required.
--
-- Populated tables require a valid prebuild with the reviewed definition, built
-- concurrently as docs/deployment.md describes. The prebuild is adopted without a build
-- and without a lock on project_resolver_link. Only an empty table builds the index here,
-- and it takes its lock immediately or not at all, so a concurrent family write cannot turn
-- the population check into a blocking build. This is the shape of
-- 20261102140000_normalized_events_registry_roles_index.sql.
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
    checked_index text := 'project_resolver_link_order_idx';
    expected_definition text := $def$CREATE INDEX project_resolver_link_order_idx ON bigname_phase.project_resolver_link USING btree (chain_id, resolver_address, lpad(record_id, 78, '0'::text), node) WHERE (record_id <> '0'::text)$def$;
    found_definition text;
    found_kind text;
    previous_search_path text;
    previous_quote_all_identifiers text;
BEGIN
    IF to_regclass('bigname_phase.project_resolver_link') IS NULL THEN
        RETURN;
    END IF;

    IF to_regclass('bigname_phase.project_resolver_link_order_idx') IS NULL THEN
        BEGIN
            LOCK TABLE bigname_phase.project_resolver_link IN SHARE MODE NOWAIT;
        EXCEPTION WHEN lock_not_available THEN
            RAISE EXCEPTION 'project_resolver_link is busy; prebuild project_resolver_link_order_idx as docs/deployment.md describes before this schema-migration';
        END;
        IF EXISTS (SELECT 1 FROM bigname_phase.project_resolver_link) THEN
            RAISE EXCEPTION 'missing prebuilt index project_resolver_link_order_idx on populated project_resolver_link; prebuild it as docs/deployment.md describes before this schema-migration';
        END IF;
        CREATE INDEX IF NOT EXISTS project_resolver_link_order_idx
            ON bigname_phase.project_resolver_link
                (chain_id, resolver_address, lpad(record_id, 78, '0'), node)
            WHERE record_id <> '0';
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
            '% does not exist although bigname_phase.project_resolver_link does; prebuild it as docs/deployment.md describes, then run the schema-migrations again',
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
          AND indrelid = to_regclass('bigname_phase.project_resolver_link')
          AND indisvalid
          AND indisready
    ) THEN
        RAISE EXCEPTION
            '% exists but is not a valid and ready index on bigname_phase.project_resolver_link; follow the recovery steps in docs/deployment.md, then run the schema-migrations again',
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
