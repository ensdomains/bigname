-- Existing schema-v2 databases gain four read-only btree indexes that API reads probe. Each
-- replaces a scan of its whole table:
--
-- - chain_lineage_timestamp_idx serves the history timestamp window
--   (resolve_chain_block_ranges in crates/storage/src/history/block_window.rs). It maps a
--   from_timestamp or to_timestamp bound to the first or last readable block, one ordered
--   probe for each bound.
-- - project_resource_pointer_namehash_resolver_idx serves the resource pointer joins of the
--   resolves_to candidates (CANDIDATES in crates/storage/src/families/records/candidates.rs),
--   which join on chain, namehash and resolver.
-- - name_surfaces_name_prefix_idx serves a prefix search over raw names
--   (storage:families.name.search_candidates). The text_pattern_ops operator class makes a
--   LIKE prefix a range on raw_name whatever the database collation. Its predicate is the one
--   of name_surfaces_name_order_idx, and the reader carries the same bounds.
-- - project_registry_pointer_resource_idx serves the registry pointer arm of
--   storage:families.records.names_reaching_resources, which looks pointers up by resource.
--
-- Indexes only. No column, row or comment changes.
--
-- An index that already exists under its name is checked and adopted, with no build and no
-- lock on its table. A missing index is built here with an ordinary CREATE INDEX, which
-- holds a SHARE lock on its table until this schema-migration commits and so blocks the
-- table's writers. On a large database, prebuild the missing indexes concurrently first, as
-- docs/deployment.md describes.
--
-- The check refuses a name that is not an index, is on another table, is not valid and
-- ready, or was built with another definition. CREATE INDEX IF NOT EXISTS matches on the
-- name alone, so an interrupted concurrent build leaves an invalid index that it would skip,
-- and the planner reads neither that nor an index over other columns. The check runs twice.
-- The first pass looks at every name before any build, so a bad prebuild is refused before a
-- long build of another index starts. The second pass runs after the builds and also refuses
-- a name that does not exist. The builds run just before it in the same transaction, so
-- nothing reaches that refusal and scripts/check-schema has no assertion for it. A refusal
-- has the same message from either pass.
--
-- The check compares definitions as pg_get_indexdef prints them, with search_path set to
-- pg_catalog so PostgreSQL always prints schema names, and quote_all_identifiers off so it
-- prints no extra quotes, the same way 20261012120000_name_surfaces_project_path_indexes.sql
-- does. A missing index is built from that same text, which names every object with its
-- schema. Both settings are transaction-local and put back before the block returns.
--
-- To recover, confirm in pg_stat_progress_create_index that no build is running, drop only
-- the named index with DROP INDEX CONCURRENTLY, build it again as docs/deployment.md
-- describes, then run the schema-migrations again.
--
-- An empty schema-migration database has no phase baseline yet, so this schema-migration
-- is a no-op there, and phase-runner init-schema installs the same indexes.
DO $migration$
DECLARE
    builds text[] := ARRAY[]::text[];
    check_pass integer;
    checked record;
    command text;
    found_definition text;
    found_kind text;
    previous_search_path text;
    previous_quote_all_identifiers text;
BEGIN
    IF to_regclass('bigname_phase.chain_lineage') IS NULL THEN
        RETURN;
    END IF;

    previous_search_path := current_setting('search_path');
    PERFORM set_config('search_path', 'pg_catalog', true);
    previous_quote_all_identifiers := current_setting('quote_all_identifiers');
    PERFORM set_config('quote_all_identifiers', 'off', true);

    FOR check_pass IN 1..2 LOOP
        FOR checked IN
            SELECT * FROM (VALUES
                ('chain_lineage_timestamp_idx', 'chain_lineage',
                 $def$CREATE INDEX chain_lineage_timestamp_idx ON bigname_phase.chain_lineage USING btree (chain_id, block_timestamp, block_number)$def$),
                ('project_resource_pointer_namehash_resolver_idx', 'project_resource_pointer',
                 $def$CREATE INDEX project_resource_pointer_namehash_resolver_idx ON bigname_phase.project_resource_pointer USING btree (chain_id, namehash, resolver_address)$def$),
                ('name_surfaces_name_prefix_idx', 'name_surfaces',
                 $def$CREATE INDEX name_surfaces_name_prefix_idx ON bigname_phase.name_surfaces USING btree (raw_name text_pattern_ops, namespace, namehash, logical_name_id) WHERE ((visibility_state = 'active'::text) AND (canonicality_state = ANY (ARRAY['canonical'::bigname_phase.canonicality_state, 'safe'::bigname_phase.canonicality_state, 'finalized'::bigname_phase.canonicality_state])) AND (octet_length(raw_name) <= 2000))$def$),
                ('project_registry_pointer_resource_idx', 'project_registry_pointer',
                 $def$CREATE INDEX project_registry_pointer_resource_idx ON bigname_phase.project_registry_pointer USING btree (chain_id, resource_id)$def$)
            ) AS reviewed(index_name, table_name, definition)
        LOOP
            found_kind := NULL;
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
            WHERE oid = to_regclass('bigname_phase.' || checked.index_name);
            IF found_kind IS NULL THEN
                IF check_pass = 1 THEN
                    -- Built from its reviewed text once the first pass has checked every name.
                    builds := array_append(builds, checked.definition);
                    CONTINUE;
                END IF;
                RAISE EXCEPTION
                    'bigname_phase.% does not exist after its build; run the schema-migrations again',
                    checked.index_name;
            END IF;
            IF found_kind <> 'index' THEN
                RAISE EXCEPTION
                    'bigname_phase.% is a %, not an index, so the index was never built; remove or rename that relation, then run the schema-migrations again',
                    checked.index_name, found_kind;
            END IF;
            IF NOT EXISTS (
                SELECT 1
                FROM pg_index
                WHERE indexrelid = to_regclass('bigname_phase.' || checked.index_name)
                  AND indrelid = to_regclass('bigname_phase.' || checked.table_name)
                  AND indisvalid
                  AND indisready
            ) THEN
                RAISE EXCEPTION
                    'bigname_phase.% exists but is not a valid and ready index on bigname_phase.%; drop it with DROP INDEX CONCURRENTLY, build it again as docs/deployment.md describes, then run the schema-migrations again',
                    checked.index_name, checked.table_name;
            END IF;
            found_definition := pg_get_indexdef(to_regclass('bigname_phase.' || checked.index_name));
            IF found_definition IS DISTINCT FROM checked.definition THEN
                RAISE EXCEPTION
                    'bigname_phase.% exists but does not have the reviewed definition; found "%", expected "%"; drop it with DROP INDEX CONCURRENTLY, build it again as docs/deployment.md describes, then run the schema-migrations again',
                    checked.index_name, found_definition, checked.definition;
            END IF;
        END LOOP;

        IF check_pass = 1 THEN
            FOREACH command IN ARRAY builds LOOP
                EXECUTE command;
            END LOOP;
        END IF;
    END LOOP;

    PERFORM set_config('search_path', previous_search_path, true);
    PERFORM set_config('quote_all_identifiers', previous_quote_all_identifiers, true);
END
$migration$;
