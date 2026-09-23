-- Replace the two label-array indexes that 20260922010100_project_mirror_scope_indexes.sql
-- built on bigname_phase.name_surfaces. Labels are chain data with no length limit, and
-- both indexes stored label text in their entries: the btree on (namespace, raw_labels)
-- holds the whole array, and the GIN on raw_labels holds each label. An entry larger
-- than about 2.7 KB makes the name_surfaces insert fail. The replacements index
-- fixed-size 64-bit hashes, and every query that uses them also compares the labels
-- themselves, so a hash collision never changes a result.
--
-- Prebuild both indexes concurrently with ops/project-progressive/install.sql on a large
-- database; it also drops the two old indexes concurrently. The statements below then
-- recognise the prebuilt indexes by name, and the check at the end refuses a name that
-- is missing, invalid, not an index, on another table, or built with another
-- definition. It compares definitions as pg_get_indexdef prints them, with search_path
-- set to pg_catalog so PostgreSQL always prints schema names and quote_all_identifiers
-- off so it prints no extra quotes, the same way
-- 20260917150000_normalized_events_v1_lookahead_indexes.sql does. Both settings are
-- transaction-local and put back before the block returns.
--
-- To recover, follow ops/project-progressive/README.md: confirm no build is running,
-- drop only the named index with DROP INDEX CONCURRENTLY, rerun install.sql, then run
-- the schema-migrations again.
DO $migration$
DECLARE
    checked_index text;
    expected_definition text;
    found_definition text;
    found_kind text;
    found_function record;
    previous_search_path text;
    previous_quote_all_identifiers text;
BEGIN
    IF to_regclass('bigname_phase.name_surfaces') IS NULL THEN
        RETURN;
    END IF;

    -- One 64-bit hash per label, in label order. The body names pg_catalog functions
    -- explicitly so it does not depend on the caller's search_path.
    IF to_regprocedure('bigname_phase.label_hashes(text[])') IS NULL THEN
        CREATE FUNCTION bigname_phase.label_hashes(labels text[])
        RETURNS bigint[]
        LANGUAGE sql
        IMMUTABLE
        STRICT
        PARALLEL SAFE
        AS $label_hashes$
            SELECT ARRAY(SELECT pg_catalog.hashtextextended(label, 0) FROM pg_catalog.unnest(labels) AS label)
        $label_hashes$;
    END IF;
    SELECT btrim(regexp_replace(prosrc, '\s+', ' ', 'g')) AS body, provolatile, proisstrict,
           proparallel, prorettype
    INTO found_function
    FROM pg_proc
    WHERE oid = to_regprocedure('bigname_phase.label_hashes(text[])');
    IF found_function.body IS DISTINCT FROM
            'SELECT ARRAY(SELECT pg_catalog.hashtextextended(label, 0) FROM pg_catalog.unnest(labels) AS label)'
        OR found_function.provolatile <> 'i'
        OR NOT found_function.proisstrict
        OR found_function.proparallel <> 's'
        OR found_function.prorettype <> 'bigint[]'::regtype THEN
        RAISE EXCEPTION
            'bigname_phase.label_hashes(text[]) exists with another definition; follow ops/project-progressive/README.md, then run the schema-migrations again';
    END IF;

    DROP INDEX IF EXISTS bigname_phase.name_surfaces_project_suffix_idx;
    DROP INDEX IF EXISTS bigname_phase.name_surfaces_project_labels_idx;
    CREATE INDEX IF NOT EXISTS name_surfaces_project_suffix_hash_idx
        ON bigname_phase.name_surfaces (namespace, hash_array_extended(raw_labels, 0));
    CREATE INDEX IF NOT EXISTS name_surfaces_project_label_hashes_idx
        ON bigname_phase.name_surfaces USING gin (bigname_phase.label_hashes(raw_labels));

    previous_search_path := current_setting('search_path');
    PERFORM set_config('search_path', 'pg_catalog', true);
    previous_quote_all_identifiers := current_setting('quote_all_identifiers');
    PERFORM set_config('quote_all_identifiers', 'off', true);

    FOR checked_index, expected_definition IN
        SELECT * FROM (VALUES
            ('name_surfaces_project_suffix_hash_idx',
             'CREATE INDEX name_surfaces_project_suffix_hash_idx ON bigname_phase.name_surfaces USING btree (namespace, hash_array_extended(raw_labels, (0)::bigint))'),
            ('name_surfaces_project_label_hashes_idx',
             'CREATE INDEX name_surfaces_project_label_hashes_idx ON bigname_phase.name_surfaces USING gin (bigname_phase.label_hashes(raw_labels))')
        ) AS reviewed(index_name, definition)
    LOOP
        SELECT CASE relkind WHEN 'i' THEN 'index' ELSE 'relation of kind ' || relkind::text END
        INTO found_kind
        FROM pg_class
        WHERE oid = to_regclass('bigname_phase.' || checked_index);
        IF found_kind IS DISTINCT FROM 'index' THEN
            RAISE EXCEPTION
                'bigname_phase.% is missing or is a %, not an index; follow ops/project-progressive/README.md, then run the schema-migrations again',
                checked_index, COALESCE(found_kind, 'missing relation');
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
                '% exists but is not a valid and ready index on bigname_phase.name_surfaces; follow the recovery steps in ops/project-progressive/README.md, then run the schema-migrations again',
                checked_index;
        END IF;
        found_definition := pg_get_indexdef(to_regclass('bigname_phase.' || checked_index));
        IF found_definition IS DISTINCT FROM expected_definition THEN
            RAISE EXCEPTION
                '% exists but does not have the reviewed definition; found "%", expected "%"; follow the recovery steps in ops/project-progressive/README.md, then run the schema-migrations again',
                checked_index, found_definition, expected_definition;
        END IF;
    END LOOP;

    PERFORM set_config('search_path', previous_search_path, true);
    PERFORM set_config('quote_all_identifiers', previous_quote_all_identifiers, true);
END;
$migration$;
