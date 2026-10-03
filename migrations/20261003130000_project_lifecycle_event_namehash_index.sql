-- Existing schema-v2 databases gain the index the child reads
-- (storage:families.topology.*, the subnames page and counts and the registry children an
-- address owns) use to find the registrar lease events of a child node with no name surface,
-- so a released lease stops serving its surviving registry owner. The primary key leads with
-- the lease's resource, which the child does not know, so without it each such child walks
-- every lifecycle row of the chain. Prebuild it concurrently on a large initialized database
-- as docs/deployment.md describes, so this schema-migration finds it and skips the build. An
-- empty schema-migration database has no phase baseline yet, so this schema-migration is a
-- no-op there and phase-runner init-schema installs the same index.
--
-- Index only; no row changes. CREATE INDEX IF NOT EXISTS matches on the name alone, so the
-- block ends with the same validity and definition check as
-- 20261001130000_normalized_events_v2_lookahead_indexes.sql, under the same transaction-local
-- search_path and quote_all_identifiers settings, put back before it returns.
DO $migration$
DECLARE
    checked_index text := 'project_lifecycle_event_namehash_idx';
    expected_definition text := $def$CREATE INDEX project_lifecycle_event_namehash_idx ON bigname_phase.project_lifecycle_event USING btree (chain_id, namehash)$def$;
    found_definition text;
    found_kind text;
    previous_search_path text;
    previous_quote_all_identifiers text;
BEGIN
    IF to_regclass('bigname_phase.project_lifecycle_event') IS NULL THEN
        RETURN;
    END IF;

    CREATE INDEX IF NOT EXISTS project_lifecycle_event_namehash_idx
        ON bigname_phase.project_lifecycle_event (chain_id, namehash);

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
            '% does not exist although bigname_phase.project_lifecycle_event does; build it as docs/deployment.md (Released registrar children) describes, then run the schema-migrations again',
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
          AND indrelid = to_regclass('bigname_phase.project_lifecycle_event')
          AND indisvalid
          AND indisready
    ) THEN
        RAISE EXCEPTION
            '% exists but is not a valid and ready index on bigname_phase.project_lifecycle_event; follow the recovery steps in docs/deployment.md (Released registrar children), then run the schema-migrations again',
            checked_index;
    END IF;

    SELECT pg_get_indexdef(indexrelid)
    INTO found_definition
    FROM pg_index
    WHERE indexrelid = to_regclass('bigname_phase.' || checked_index);
    IF found_definition <> expected_definition THEN
        RAISE EXCEPTION
            '% exists but does not have the reviewed definition; found "%", expected "%"; follow the recovery steps in docs/deployment.md (Released registrar children), then run the schema-migrations again',
            checked_index, found_definition, expected_definition;
    END IF;

    PERFORM set_config('search_path', previous_search_path, true);
    PERFORM set_config('quote_all_identifiers', previous_quote_all_identifiers, true);
END
$migration$;
