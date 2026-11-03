-- API read-only access path for a registry's role counts (CURRENT_ROLES in
-- apps/api/src/v2/registries/role_counts.rs), which the registry detail and labels routes
-- read with include=counts. A partial index of the registry role-change rows that pass
-- that statement's constant terms on normalized_events, keyed by chain and emitting
-- registry and then in the statement's DISTINCT ON order. Without it the statement reads
-- the registry's whole event history through normalized_events_emitter_history_idx and
-- drops every row that is not a role change. No rows, phase markers, catalogue version or
-- content-hash inputs change, and no redo is required.
--
-- Populated tables require a valid prebuild with the reviewed definition, built
-- concurrently as docs/deployment.md describes. The prebuild is adopted without a build
-- and without a lock on normalized_events. Only an empty table builds the index here, and
-- it takes its lock immediately or not at all, so a concurrent insert cannot turn the
-- population check into a blocking build. This is the shape of
-- 20261102130000_normalized_events_v1_transfer_node_index.sql.
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
    checked_index text := 'normalized_events_registry_roles_idx';
    expected_definition text := $def$CREATE INDEX normalized_events_registry_roles_idx ON bigname_phase.normalized_events USING btree (chain_id, lower((raw_fact_ref ->> 'emitting_address'::text)), resource_id, lower((after_state ->> 'subject'::text)), block_number DESC, transaction_index DESC NULLS LAST, log_index DESC NULLS LAST, normalized_event_id DESC) WHERE ((source_family = ANY (ARRAY['ens_v2_root_l1'::text, 'ens_v2_registry_l1'::text])) AND (event_kind = ANY (ARRAY['PermissionChanged'::text, 'RootPermissionChanged'::text])) AND ((after_state ->> 'source_event'::text) = 'EACRolesChanged'::text) AND (resource_id IS NOT NULL) AND ((after_state ->> 'subject'::text) IS NOT NULL) AND (consumer_visibility = 'activated'::text) AND (canonicality_state = ANY (ARRAY['canonical'::bigname_phase.canonicality_state, 'safe'::bigname_phase.canonicality_state, 'finalized'::bigname_phase.canonicality_state])))$def$;
    found_definition text;
    found_kind text;
    previous_search_path text;
    previous_quote_all_identifiers text;
BEGIN
    IF to_regclass('bigname_phase.normalized_events') IS NULL THEN
        RETURN;
    END IF;

    IF to_regclass('bigname_phase.normalized_events_registry_roles_idx') IS NULL THEN
        BEGIN
            LOCK TABLE bigname_phase.normalized_events IN SHARE MODE NOWAIT;
        EXCEPTION WHEN lock_not_available THEN
            RAISE EXCEPTION 'normalized_events is busy; prebuild normalized_events_registry_roles_idx as docs/deployment.md describes before this schema-migration';
        END;
        IF EXISTS (SELECT 1 FROM bigname_phase.normalized_events) THEN
            RAISE EXCEPTION 'missing prebuilt index normalized_events_registry_roles_idx on populated normalized_events; prebuild it as docs/deployment.md describes, or with ops/walk-index-set/install.sql, before this schema-migration';
        END IF;
        CREATE INDEX IF NOT EXISTS normalized_events_registry_roles_idx
            ON bigname_phase.normalized_events (
                chain_id,
                lower(raw_fact_ref ->> 'emitting_address'),
                resource_id,
                lower(after_state ->> 'subject'),
                block_number DESC,
                transaction_index DESC NULLS LAST,
                log_index DESC NULLS LAST,
                normalized_event_id DESC
            )
            WHERE source_family IN ('ens_v2_root_l1', 'ens_v2_registry_l1')
              AND event_kind IN ('PermissionChanged', 'RootPermissionChanged')
              AND after_state ->> 'source_event' = 'EACRolesChanged'
              AND resource_id IS NOT NULL
              AND after_state ->> 'subject' IS NOT NULL
              AND consumer_visibility = 'activated'
              AND canonicality_state IN ('canonical', 'safe', 'finalized');
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
            '% does not exist although bigname_phase.normalized_events does; prebuild it as docs/deployment.md describes, then run the schema-migrations again',
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
            '% exists but is not a valid and ready index on bigname_phase.normalized_events; follow the recovery steps in docs/deployment.md, then run the schema-migrations again',
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
