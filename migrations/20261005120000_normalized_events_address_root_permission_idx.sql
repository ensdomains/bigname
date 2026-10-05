-- Prebuild this index concurrently on a large initialized database using
-- ops/address-history-indexes/install.sql before applying schema-migrations.
--
-- The address history read (crates/storage/src/history/filters.rs) also lists the registry
-- root role changes made to the address: the RootPermissionChanged rows whose subject is the
-- address. They name no name, and their resource is the registry's root resource, which every
-- holder of that registry shares, so no name or resource anchor reaches them. This partial
-- expression index keys those rows by the lowercased subject. It changes access paths only; no
-- stored row and no interpreter content hash input changes. An empty schema-migration database
-- has no phase baseline yet, so this schema-migration is a no-op there and phase-runner
-- init-schema installs the same index.
--
-- CREATE INDEX IF NOT EXISTS matches on the name alone, so the block ends with the validity and
-- definition check of 20260923120000_normalized_events_address_match_indexes.sql, under the
-- same transaction-local search_path and quote_all_identifiers settings, put back before it
-- returns. To recover, follow ops/address-history-indexes/README.md: confirm no build is
-- running, drop only the named index with DROP INDEX CONCURRENTLY, rerun install.sql, then run
-- the schema-migrations again.
DO $migration$
DECLARE
    checked_index text := 'normalized_events_address_root_permission_idx';
    expected_definition text := $def$CREATE INDEX normalized_events_address_root_permission_idx ON bigname_phase.normalized_events USING btree (lower((after_state ->> 'subject'::text)), block_number DESC NULLS LAST, log_index DESC NULLS LAST, normalized_event_id DESC) WHERE ((event_kind = 'RootPermissionChanged'::text) AND (consumer_visibility = 'activated'::text) AND (canonicality_state = ANY (ARRAY['canonical'::bigname_phase.canonicality_state, 'safe'::bigname_phase.canonicality_state, 'finalized'::bigname_phase.canonicality_state])))$def$;
    found_definition text;
    found_kind text;
    previous_search_path text;
    previous_quote_all_identifiers text;
BEGIN
    IF to_regclass('bigname_phase.normalized_events') IS NULL THEN
        RETURN;
    END IF;

    CREATE INDEX IF NOT EXISTS normalized_events_address_root_permission_idx
        ON bigname_phase.normalized_events (
            lower(after_state ->> 'subject'),
            block_number DESC NULLS LAST,
            log_index DESC NULLS LAST,
            normalized_event_id DESC
        )
        WHERE event_kind = 'RootPermissionChanged'
          AND consumer_visibility = 'activated'
          AND canonicality_state IN ('canonical', 'safe', 'finalized');

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
            '% does not exist although bigname_phase.normalized_events does; follow the recovery steps in ops/address-history-indexes/README.md, then run the schema-migrations again',
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
            '% exists but is not a valid and ready index on bigname_phase.normalized_events; follow the recovery steps in ops/address-history-indexes/README.md, then run the schema-migrations again',
            checked_index;
    END IF;

    SELECT pg_get_indexdef(indexrelid)
    INTO found_definition
    FROM pg_index
    WHERE indexrelid = to_regclass('bigname_phase.' || checked_index);
    IF found_definition <> expected_definition THEN
        RAISE EXCEPTION
            '% exists but does not have the reviewed definition; found "%", expected "%"; follow the recovery steps in ops/address-history-indexes/README.md, then run the schema-migrations again',
            checked_index, found_definition, expected_definition;
    END IF;

    PERFORM set_config('search_path', previous_search_path, true);
    PERFORM set_config('quote_all_identifiers', previous_quote_all_identifiers, true);
END
$migration$;
