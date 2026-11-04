-- Existing schema-v2 databases gain the read-only btree indexes that a dense prefix search
-- reads (storage:families.name.search_candidates.prefix1 to prefix3 in
-- crates/storage/src/families/search_dictionary/candidates.rs). Each holds the raw-backed and
-- the structural search documents of up to 2000 bytes, keyed by spelling class, the first k
-- characters of the name, then the page order. An equality on the class and the bucket leaves
-- the entries in page order, so a search of a one, two or three character prefix reads its
-- bucket from the cursor and stops at the page limit. A longer prefix reads the
-- three-character bucket and filters it.
--
-- Indexes only. No column, row or comment changes.
--
-- An index that already exists under its name is checked and adopted, with no build and no
-- lock on its table. A missing index is built here with an ordinary CREATE INDEX, which
-- holds a SHARE lock on name_search_documents until this schema-migration commits and so
-- blocks Interpret's writes to it. On a large database, prebuild the missing indexes
-- concurrently first, as docs/deployment.md describes.
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
    IF to_regclass('bigname_phase.name_search_documents') IS NULL THEN
        RETURN;
    END IF;

    previous_search_path := current_setting('search_path');
    PERFORM set_config('search_path', 'pg_catalog', true);
    previous_quote_all_identifiers := current_setting('quote_all_identifiers');
    PERFORM set_config('quote_all_identifiers', 'off', true);

    FOR check_pass IN 1..2 LOOP
        FOR checked IN
            SELECT * FROM (VALUES
                ('name_search_documents_prefix1_idx', 'name_search_documents',
                 $def$CREATE INDEX name_search_documents_prefix1_idx ON bigname_phase.name_search_documents USING btree (spelling_class, "left"(name, 1), name, namespace, namehash) WHERE (spelling_class = ANY (ARRAY[0, 1]))$def$),
                ('name_search_documents_prefix2_idx', 'name_search_documents',
                 $def$CREATE INDEX name_search_documents_prefix2_idx ON bigname_phase.name_search_documents USING btree (spelling_class, "left"(name, 2), name, namespace, namehash) WHERE (spelling_class = ANY (ARRAY[0, 1]))$def$),
                ('name_search_documents_prefix3_idx', 'name_search_documents',
                 $def$CREATE INDEX name_search_documents_prefix3_idx ON bigname_phase.name_search_documents USING btree (spelling_class, "left"(name, 3), name, namespace, namehash) WHERE (spelling_class = ANY (ARRAY[0, 1]))$def$)
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
