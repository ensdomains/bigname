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
-- index built on it would hold other hashes than the statement looks up. An index of
-- either name that is not valid is refused too. An interrupted concurrent build leaves
-- one, the planner never reads it, and the statement would then read every surface for
-- each lookup.
--
-- An empty schema-migration database has no phase baseline yet, so this schema-migration
-- is a no-op there and phase-runner init-schema installs the same function and indexes.
DO $migration$
DECLARE
    found_function record;
    checked_index text;
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

    FOREACH checked_index IN ARRAY ARRAY[
        'name_surfaces_project_entry_keys_idx', 'name_surfaces_project_path_hash_idx']
    LOOP
        IF EXISTS (
            SELECT 1 FROM pg_index
            WHERE indexrelid = to_regclass('bigname_phase.' || checked_index)
              AND NOT indisvalid
        ) THEN
            RAISE EXCEPTION
                'bigname_phase.% exists but is not valid; drop it with DROP INDEX CONCURRENTLY, build it again as docs/deployment.md describes, then run the schema-migrations again',
                checked_index;
        END IF;
    END LOOP;

    CREATE INDEX IF NOT EXISTS name_surfaces_project_entry_keys_idx
        ON bigname_phase.name_surfaces USING gin (bigname_phase.entry_key_hashes(labelhashes));
    CREATE INDEX IF NOT EXISTS name_surfaces_project_path_hash_idx
        ON bigname_phase.name_surfaces (namespace, hash_array_extended(labelhashes, 0));
END
$migration$;
