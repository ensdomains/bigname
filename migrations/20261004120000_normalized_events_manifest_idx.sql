-- Existing schema-v2 databases gain the index the manifest sync reads at every runner start:
-- the latest SourceManifestUpdated event of each manifest (lock_phase_writers in
-- crates/manifests/src/schema_v2_sync_state.rs and load_manifest_states in
-- schema_v2_event_history.rs). Without it PostgreSQL walks the primary key backward from the
-- newest event for each manifest, and those events were written early in the walk, so each
-- probe reads most of normalized_events. The name and definition are the ones the retired
-- public-schema baseline (20260430060000_baseline.sql) gave it. Prebuild it concurrently on a
-- large initialized database as docs/deployment.md describes, so this schema-migration finds
-- it and skips the build. An empty schema-migration database has no phase baseline yet, so
-- this schema-migration is a no-op there and phase-runner init-schema installs the same index.
--
-- Index only; no row changes. CREATE INDEX IF NOT EXISTS matches on the name alone, so the
-- block ends with the same validity and definition check as
-- 20261003130000_project_lifecycle_event_namehash_index.sql, under the same transaction-local
-- search_path and quote_all_identifiers settings, put back before it returns.
DO $migration$
DECLARE
    checked_index text := 'normalized_events_manifest_idx';
    expected_definition text := $def$CREATE INDEX normalized_events_manifest_idx ON bigname_phase.normalized_events USING btree (source_manifest_id, event_kind, normalized_event_id DESC) WHERE (source_manifest_id IS NOT NULL)$def$;
    found_definition text;
    found_kind text;
    previous_search_path text;
    previous_quote_all_identifiers text;
BEGIN
    IF to_regclass('bigname_phase.normalized_events') IS NULL THEN
        RETURN;
    END IF;

    CREATE INDEX IF NOT EXISTS normalized_events_manifest_idx
        ON bigname_phase.normalized_events (source_manifest_id, event_kind, normalized_event_id DESC)
        WHERE source_manifest_id IS NOT NULL;

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
            '% does not exist although bigname_phase.normalized_events does; build it as docs/deployment.md (Manifest sync index) describes, then run the schema-migrations again',
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
            '% exists but is not a valid and ready index on bigname_phase.normalized_events; follow the recovery steps in docs/deployment.md (Manifest sync index), then run the schema-migrations again',
            checked_index;
    END IF;

    SELECT pg_get_indexdef(indexrelid)
    INTO found_definition
    FROM pg_index
    WHERE indexrelid = to_regclass('bigname_phase.' || checked_index);
    IF found_definition <> expected_definition THEN
        RAISE EXCEPTION
            '% exists but does not have the reviewed definition; found "%", expected "%"; follow the recovery steps in docs/deployment.md (Manifest sync index), then run the schema-migrations again',
            checked_index, found_definition, expected_definition;
    END IF;

    PERFORM set_config('search_path', previous_search_path, true);
    PERFORM set_config('quote_all_identifiers', previous_quote_all_identifiers, true);
END
$migration$;
