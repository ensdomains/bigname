-- Four normalized_events indexes that Project's resolver scoping reads
-- (crates/project/src/scope) were added to schema-v2/baseline by #415 on
-- 2026-08-14 with no schema-migration, so a database initialized before that
-- day and upgraded in place since never got them. Their predicates name
-- consumer_visibility, which 20260811120000_ens_v2_migration_slice_1.sql adds,
-- so no earlier order reaches this file with the table present and the column
-- absent.
--
-- Each index is built here when it is missing and verified when it is present:
-- an index under the right name that is invalid (an interrupted concurrent
-- build), not on this table, or not the reviewed definition (a wrong manual
-- prebuild) stops the run instead of being adopted by name. Definitions are
-- compared as PostgreSQL prints them with search_path set to pg_catalog, so
-- the table and the enum type carry the schema name; see
-- 20260917160000_discovery_edges_index_validity_check.sql for why the printed
-- text is never rewritten and why quote_all_identifiers is turned off while
-- the definitions are read. Both changes are transaction-local and put back
-- before the block returns.
--
-- On a large initialized database, prebuild concurrently with
-- ops/resolver-history-indexes/install.sql as its README and docs/deployment.md
-- describe, then run the schema-migrations; this file then verifies and builds
-- nothing. Built here, each index is an ordinary CREATE INDEX that blocks
-- writes to normalized_events for the build.
--
-- Fresh databases may not yet contain the phase baseline; that case passes,
-- and the baseline installed afterwards carries all four.
DO $migration$
DECLARE
    checked_index text;
    expected_definition text;
    found_definition text;
    found_kind text;
    previous_search_path text;
    previous_quote_all_identifiers text;
BEGIN
    IF to_regclass('bigname_phase.normalized_events') IS NULL THEN
        RETURN;
    END IF;

    previous_search_path := current_setting('search_path');
    PERFORM set_config('search_path', 'pg_catalog', true);
    previous_quote_all_identifiers := current_setting('quote_all_identifiers');
    PERFORM set_config('quote_all_identifiers', 'off', true);

    FOR checked_index, expected_definition IN
        SELECT * FROM (VALUES
            ('normalized_events_permission_after_resolver_history_idx',
             'CREATE INDEX normalized_events_permission_after_resolver_history_idx ON bigname_phase.normalized_events USING btree (chain_id, lower((after_state #>> ''{scope,resolver_address}''::text[])), block_number, block_hash) INCLUDE (resource_id) WHERE ((event_kind = ''PermissionChanged''::text) AND (consumer_visibility = ''activated''::text) AND (canonicality_state = ANY (ARRAY[''canonical''::bigname_phase.canonicality_state, ''safe''::bigname_phase.canonicality_state, ''finalized''::bigname_phase.canonicality_state])) AND ((after_state #>> ''{scope,kind}''::text[]) = ''resolver''::text) AND (resource_id IS NOT NULL))'),
            ('normalized_events_permission_before_resolver_history_idx',
             'CREATE INDEX normalized_events_permission_before_resolver_history_idx ON bigname_phase.normalized_events USING btree (chain_id, lower((before_state #>> ''{scope,resolver_address}''::text[])), block_number, block_hash) INCLUDE (resource_id) WHERE ((event_kind = ''PermissionChanged''::text) AND (consumer_visibility = ''activated''::text) AND (canonicality_state = ANY (ARRAY[''canonical''::bigname_phase.canonicality_state, ''safe''::bigname_phase.canonicality_state, ''finalized''::bigname_phase.canonicality_state])) AND ((before_state #>> ''{scope,kind}''::text[]) = ''resolver''::text) AND (resource_id IS NOT NULL))'),
            ('normalized_events_pointer_after_resolver_history_idx',
             'CREATE INDEX normalized_events_pointer_after_resolver_history_idx ON bigname_phase.normalized_events USING btree (chain_id, lower((after_state ->> ''resolver''::text)), block_number, block_hash) INCLUDE (normalized_event_id) WHERE ((event_kind = ''ResolverChanged''::text) AND (consumer_visibility = ''activated''::text) AND (canonicality_state = ANY (ARRAY[''canonical''::bigname_phase.canonicality_state, ''safe''::bigname_phase.canonicality_state, ''finalized''::bigname_phase.canonicality_state])))'),
            ('normalized_events_pointer_before_resolver_history_idx',
             'CREATE INDEX normalized_events_pointer_before_resolver_history_idx ON bigname_phase.normalized_events USING btree (chain_id, lower((before_state ->> ''resolver''::text)), block_number, block_hash) INCLUDE (normalized_event_id) WHERE ((event_kind = ''ResolverChanged''::text) AND (consumer_visibility = ''activated''::text) AND (canonicality_state = ANY (ARRAY[''canonical''::bigname_phase.canonicality_state, ''safe''::bigname_phase.canonicality_state, ''finalized''::bigname_phase.canonicality_state])))')
        ) AS reviewed(index_name, definition)
    LOOP
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
            CASE checked_index
            WHEN 'normalized_events_permission_after_resolver_history_idx' THEN
                EXECUTE $ddl$
CREATE INDEX normalized_events_permission_after_resolver_history_idx
    ON bigname_phase.normalized_events (
        chain_id,
        lower(after_state #>> '{scope,resolver_address}'),
        block_number,
        block_hash
    ) INCLUDE (resource_id)
    WHERE event_kind = 'PermissionChanged'
      AND consumer_visibility = 'activated'
      AND canonicality_state IN ('canonical', 'safe', 'finalized')
      AND after_state #>> '{scope,kind}' = 'resolver'
      AND resource_id IS NOT NULL
                $ddl$;
            WHEN 'normalized_events_permission_before_resolver_history_idx' THEN
                EXECUTE $ddl$
CREATE INDEX normalized_events_permission_before_resolver_history_idx
    ON bigname_phase.normalized_events (
        chain_id,
        lower(before_state #>> '{scope,resolver_address}'),
        block_number,
        block_hash
    ) INCLUDE (resource_id)
    WHERE event_kind = 'PermissionChanged'
      AND consumer_visibility = 'activated'
      AND canonicality_state IN ('canonical', 'safe', 'finalized')
      AND before_state #>> '{scope,kind}' = 'resolver'
      AND resource_id IS NOT NULL
                $ddl$;
            WHEN 'normalized_events_pointer_after_resolver_history_idx' THEN
                EXECUTE $ddl$
CREATE INDEX normalized_events_pointer_after_resolver_history_idx
    ON bigname_phase.normalized_events (
        chain_id,
        lower(after_state ->> 'resolver'),
        block_number,
        block_hash
    ) INCLUDE (normalized_event_id)
    WHERE event_kind = 'ResolverChanged'
      AND consumer_visibility = 'activated'
      AND canonicality_state IN ('canonical', 'safe', 'finalized')
                $ddl$;
            WHEN 'normalized_events_pointer_before_resolver_history_idx' THEN
                EXECUTE $ddl$
CREATE INDEX normalized_events_pointer_before_resolver_history_idx
    ON bigname_phase.normalized_events (
        chain_id,
        lower(before_state ->> 'resolver'),
        block_number,
        block_hash
    ) INCLUDE (normalized_event_id)
    WHERE event_kind = 'ResolverChanged'
      AND consumer_visibility = 'activated'
      AND canonicality_state IN ('canonical', 'safe', 'finalized')
                $ddl$;
            END CASE;
            CONTINUE;
        END IF;
        IF found_kind <> 'index' THEN
            RAISE EXCEPTION
                'bigname_phase.% is a %, not an index, so the index was never built; remove or rename that relation, then follow ops/resolver-history-indexes/README.md and run the schema-migrations again',
                checked_index, found_kind;
        END IF;
        IF EXISTS (
            SELECT 1
            FROM pg_index
            WHERE indexrelid = to_regclass('bigname_phase.' || checked_index)
              AND NOT (
                  indrelid = to_regclass('bigname_phase.normalized_events')
                  AND indisvalid
                  AND indisready
              )
        ) THEN
            RAISE EXCEPTION
                '% exists but is not a valid and ready index on bigname_phase.normalized_events; follow the recovery steps in ops/resolver-history-indexes/README.md, then run the schema-migrations again',
                checked_index;
        END IF;
        SELECT pg_get_indexdef(indexrelid)
        INTO found_definition
        FROM pg_index
        WHERE indexrelid = to_regclass('bigname_phase.' || checked_index);
        IF found_definition <> expected_definition THEN
            RAISE EXCEPTION
                '% exists but does not have the reviewed definition; found "%", expected "%"; follow the recovery steps in ops/resolver-history-indexes/README.md, then run the schema-migrations again',
                checked_index, found_definition, expected_definition;
        END IF;
    END LOOP;

    PERFORM set_config('search_path', previous_search_path, true);
    PERFORM set_config('quote_all_identifiers', previous_quote_all_identifiers, true);
END
$migration$;
