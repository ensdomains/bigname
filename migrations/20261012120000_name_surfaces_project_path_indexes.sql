-- Existing schema-v2 databases gain the function and the two indexes the Project selection of
-- names under a changed ENSv2 path reads (project:families.resolution_paths.prepare). The
-- statement read every surface of the namespace up to three times in each publication. It now
-- finds a touched name's ancestors by the hash of each suffix of its label hash path, and
-- the names that hold a changed entry's label by the hash of the label's entry key. An
-- ENSv2 registry keys an entry by the label hash with its low 32 bits cleared, which
-- entry_key_hashes computes for each label hash before hashing.
-- (upstream: .refs/ens_v2_sepolia_20261001/contracts/src/utils/LibLabel.sol:L7-L16 @ ens_v2_sepolia_20261001@07e55a05)
-- (upstream: .refs/ens_v2_sepolia_20261001/contracts/src/registry/PermissionedRegistry.sol:L677-L693 @ ens_v2_sepolia_20261001@07e55a05)
-- The statement also compares the label hashes themselves, so a hash collision never
-- changes its result.
--
-- Neither index is partial, because the statement reads surfaces of every visibility and
-- chain state as path members. Both hold fixed-size 64-bit hashes, so no index entry
-- can exceed the entry size limit. Indexes and one function only. No column or row changes.
--
-- Each build holds a SHARE lock on name_surfaces until this schema-migration commits. On
-- a large database, create the function and prebuild both indexes concurrently first, as
-- docs/deployment.md describes. The statements below then find them by name and skip the
-- builds. An existing entry_key_hashes with another definition is refused, because an
-- index built on it would hold other hashes than the statement looks up.
--
-- The check refuses a name that is not an index, is on another table, is not valid and
-- ready, or was built with another definition. An interrupted concurrent build leaves an
-- invalid index, and the planner reads neither that nor an index over another expression,
-- so the statement would read every surface for each lookup. The check runs twice. The
-- first pass looks at every relation that already holds either name, so a bad prebuild is
-- refused before a long build of the other index starts. The second pass runs after the
-- builds. It catches a name that appeared between the first pass and the builds, and any
-- relation that made a build skip. A refusal has the same message from either pass, so
-- scripts/check-schema cannot tell which pass refused and does not assert the order. The
-- second pass also refuses a name that does not exist. The builds run just before it in
-- the same transaction, so nothing reaches that refusal and check-schema has no
-- assertion for it. The check
-- compares definitions as pg_get_indexdef prints them, with search_path set to pg_catalog
-- so PostgreSQL always prints schema names and quote_all_identifiers off so it prints no
-- extra quotes, the same way 20260923140000_project_name_surfaces_label_indexes.sql does.
-- Both settings are transaction-local and put back before the block returns.
--
-- To recover, confirm in pg_stat_progress_create_index that no build is running, drop
-- only the named index with DROP INDEX CONCURRENTLY, build it again as
-- docs/deployment.md describes, then run the schema-migrations again.
--
-- An empty schema-migration database has no phase baseline yet, so this schema-migration
-- is a no-op there and phase-runner init-schema installs the same function and indexes.
DO $migration$
DECLARE
    check_pass integer;
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

    -- One 64-bit hash of the entry key per label hash, in label order. The body names
    -- its functions in pg_catalog. Its || operator is unqualified and resolves in
    -- pg_catalog, which PostgreSQL searches first unless search_path names it later.
    IF to_regprocedure('bigname_phase.entry_key_hashes(text[])') IS NULL THEN
        CREATE FUNCTION bigname_phase.entry_key_hashes(labelhashes text[])
        RETURNS bigint[]
        LANGUAGE sql
        IMMUTABLE
        STRICT
        PARALLEL SAFE
        AS $entry_key_hashes$
            SELECT ARRAY(SELECT pg_catalog.hashtextextended(pg_catalog."left"(pg_catalog.lower(label), 58) || '00000000', 0) FROM pg_catalog.unnest(labelhashes) AS label)
        $entry_key_hashes$;
    END IF;
    SELECT btrim(regexp_replace(prosrc, '\s+', ' ', 'g')) AS body, provolatile, proisstrict,
           proparallel, prorettype
    INTO found_function
    FROM pg_proc
    WHERE oid = to_regprocedure('bigname_phase.entry_key_hashes(text[])');
    IF found_function.body IS DISTINCT FROM
            'SELECT ARRAY(SELECT pg_catalog.hashtextextended(pg_catalog."left"(pg_catalog.lower(label), 58) || ''00000000'', 0) FROM pg_catalog.unnest(labelhashes) AS label)'
        OR found_function.provolatile <> 'i'
        OR NOT found_function.proisstrict
        OR found_function.proparallel <> 's'
        OR found_function.prorettype <> 'bigint[]'::regtype THEN
        RAISE EXCEPTION
            'bigname_phase.entry_key_hashes(text[]) exists but does not have the reviewed definition; drop name_surfaces_project_entry_keys_idx and the function, then run the schema-migrations again';
    END IF;

    previous_search_path := current_setting('search_path');
    PERFORM set_config('search_path', 'pg_catalog', true);
    previous_quote_all_identifiers := current_setting('quote_all_identifiers');
    PERFORM set_config('quote_all_identifiers', 'off', true);

    FOR check_pass IN 1..2 LOOP
        FOR checked_index, expected_definition IN
            SELECT * FROM (VALUES
                ('name_surfaces_project_entry_keys_idx',
                 'CREATE INDEX name_surfaces_project_entry_keys_idx ON bigname_phase.name_surfaces USING gin (bigname_phase.entry_key_hashes(labelhashes))'),
                ('name_surfaces_project_path_hash_idx',
                 'CREATE INDEX name_surfaces_project_path_hash_idx ON bigname_phase.name_surfaces USING btree (namespace, hash_array_extended(labelhashes, (0)::bigint))')
            ) AS reviewed(index_name, definition)
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
            WHERE oid = to_regclass('bigname_phase.' || checked_index);
            IF found_kind IS NULL THEN
                -- The first pass leaves an absent name to the builds below.
                CONTINUE WHEN check_pass = 1;
                RAISE EXCEPTION
                    'bigname_phase.% does not exist after its build; run the schema-migrations again',
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
                    'bigname_phase.% exists but is not a valid and ready index on bigname_phase.name_surfaces; drop it with DROP INDEX CONCURRENTLY, build it again as docs/deployment.md describes, then run the schema-migrations again',
                    checked_index;
            END IF;
            found_definition := pg_get_indexdef(to_regclass('bigname_phase.' || checked_index));
            IF found_definition IS DISTINCT FROM expected_definition THEN
                RAISE EXCEPTION
                    'bigname_phase.% exists but does not have the reviewed definition; found "%", expected "%"; drop it with DROP INDEX CONCURRENTLY, build it again as docs/deployment.md describes, then run the schema-migrations again',
                    checked_index, found_definition, expected_definition;
            END IF;
        END LOOP;

        IF check_pass = 1 THEN
            -- Every name these statements use is schema-qualified or in pg_catalog, so
            -- they build the same indexes under the search_path set above.
            CREATE INDEX IF NOT EXISTS name_surfaces_project_entry_keys_idx
                ON bigname_phase.name_surfaces
                USING gin (bigname_phase.entry_key_hashes(labelhashes));
            CREATE INDEX IF NOT EXISTS name_surfaces_project_path_hash_idx
                ON bigname_phase.name_surfaces (namespace, hash_array_extended(labelhashes, 0));
        END IF;
    END LOOP;

    PERFORM set_config('search_path', previous_search_path, true);
    PERFORM set_config('quote_all_identifiers', previous_quote_all_identifiers, true);
END
$migration$;
