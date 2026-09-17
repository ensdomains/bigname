-- Checks eight indexes; builds, drops, and changes nothing.
--
-- 20260917131000_project_scoped_history_indexes.sql uses CREATE INDEX IF NOT
-- EXISTS, which matches on the name alone. An interrupted concurrent prebuild
-- (ops/project-scoped-history/install.sql) leaves an invalid index under the
-- right name, and a wrong manual prebuild leaves a valid index with other keys
-- or another predicate, or a table, view, or other relation that is not an
-- index under the right name. That file then succeeds without building
-- anything. This file stops the run instead of recording success over an index
-- Project's scoped history lookups cannot use. It is the check
-- 20260917160000_discovery_edges_index_validity_check.sql makes for the
-- discovery indexes.
--
-- The definition is compared as PostgreSQL prints it with pg_get_indexdef, so
-- key order, expressions, the included column, and the predicate are all
-- covered. PostgreSQL adds the schema name to the table always and to the enum
-- type only when the session search_path does not include it. The printed text
-- is never rewritten to even that out, because a text replacement cannot tell
-- a schema name from the same characters inside a string literal: an index on
-- after_state ->> 'bigname_phase.node' would then compare equal to the
-- reviewed one on after_state ->> 'node'. Instead search_path is set to
-- pg_catalog while the definitions are read, so PostgreSQL always prints both
-- schema names, and the expected text keeps them. Whitespace is compared as
-- printed too: PostgreSQL 16 prints each of these definitions on one line.
-- The expected text is how the fresh baseline index prints under that
-- search_path; schema-v2/apply-check.sh proves it for the baseline, the
-- earlier schema-migration, and install.sql.
--
-- The search_path change is transaction-local, and the block puts the previous
-- value back before it returns, so later statements in the same transaction
-- see the search_path they would have seen without this file. When the block
-- raises, the transaction, or the savepoint around it, rolls the change back.
--
-- To recover, follow ops/project-scoped-history/README.md: confirm no build is
-- running, drop only the named index with DROP INDEX CONCURRENTLY, rerun
-- install.sql, then run the schema-migrations again.
--
-- Fresh migration databases may not yet contain the phase baseline; that case
-- passes, and the baseline installed afterwards carries all eight indexes.
--
-- When bigname_phase.normalized_events exists, a name that resolves to nothing
-- fails too. The earlier file builds every index whenever the table exists, and
-- every source revision that carries it also carries all eight indexes in
-- schema-v2/baseline, so no supported order reaches this file with the table
-- present and an index absent. It only happens when the index was dropped, or
-- the table was installed without it after the earlier file was already
-- recorded. That file never runs again, so passing here would leave the scoped
-- history lookups unindexed for good.
DO $migration$
DECLARE
    checked_index text;
    expected_definition text;
    found_definition text;
    found_kind text;
    previous_search_path text;
BEGIN
    IF to_regclass('bigname_phase.normalized_events') IS NULL THEN
        RETURN;
    END IF;

    -- Every name below is schema-qualified or lives in pg_catalog.
    previous_search_path := current_setting('search_path');
    PERFORM set_config('search_path', 'pg_catalog', true);

    FOR checked_index, expected_definition IN
        SELECT * FROM (VALUES
            ('normalized_events_project_name_node_idx',
             $def$CREATE INDEX normalized_events_project_name_node_idx ON bigname_phase.normalized_events USING btree (chain_id, (((namespace || ':'::text) || lower((after_state ->> 'node'::text)))), block_number) INCLUDE (normalized_event_id) WHERE (((event_kind = ANY (ARRAY['SubregistryChanged'::text, 'AliasChanged'::text])) OR ((event_kind = 'AuthorityTransferred'::text) AND (source_family = ANY (ARRAY['ens_v1_registry_l1'::text, 'basenames_base_registry'::text])))) AND (canonicality_state = ANY (ARRAY['canonical'::bigname_phase.canonicality_state, 'safe'::bigname_phase.canonicality_state, 'finalized'::bigname_phase.canonicality_state])) AND (((namespace || ':'::text) || lower((after_state ->> 'node'::text))) IS NOT NULL))$def$),
            ('normalized_events_project_name_child_idx',
             $def$CREATE INDEX normalized_events_project_name_child_idx ON bigname_phase.normalized_events USING btree (chain_id, (((namespace || ':'::text) || lower((after_state ->> 'child_node'::text)))), block_number) INCLUDE (normalized_event_id) WHERE (((event_kind = ANY (ARRAY['SubregistryChanged'::text, 'AliasChanged'::text])) OR ((event_kind = 'AuthorityTransferred'::text) AND (source_family = ANY (ARRAY['ens_v1_registry_l1'::text, 'basenames_base_registry'::text])))) AND (canonicality_state = ANY (ARRAY['canonical'::bigname_phase.canonicality_state, 'safe'::bigname_phase.canonicality_state, 'finalized'::bigname_phase.canonicality_state])) AND (((namespace || ':'::text) || lower((after_state ->> 'child_node'::text))) IS NOT NULL))$def$),
            ('normalized_events_project_name_after_target_idx',
             $def$CREATE INDEX normalized_events_project_name_after_target_idx ON bigname_phase.normalized_events USING btree (chain_id, ((after_state ->> 'to_logical_name_id'::text)), block_number) INCLUDE (normalized_event_id) WHERE (((event_kind = ANY (ARRAY['SubregistryChanged'::text, 'AliasChanged'::text])) OR ((event_kind = 'AuthorityTransferred'::text) AND (source_family = ANY (ARRAY['ens_v1_registry_l1'::text, 'basenames_base_registry'::text])))) AND (canonicality_state = ANY (ARRAY['canonical'::bigname_phase.canonicality_state, 'safe'::bigname_phase.canonicality_state, 'finalized'::bigname_phase.canonicality_state])) AND ((after_state ->> 'to_logical_name_id'::text) IS NOT NULL))$def$),
            ('normalized_events_project_name_before_target_idx',
             $def$CREATE INDEX normalized_events_project_name_before_target_idx ON bigname_phase.normalized_events USING btree (chain_id, ((before_state ->> 'to_logical_name_id'::text)), block_number) INCLUDE (normalized_event_id) WHERE (((event_kind = ANY (ARRAY['SubregistryChanged'::text, 'AliasChanged'::text])) OR ((event_kind = 'AuthorityTransferred'::text) AND (source_family = ANY (ARRAY['ens_v1_registry_l1'::text, 'basenames_base_registry'::text])))) AND (canonicality_state = ANY (ARRAY['canonical'::bigname_phase.canonicality_state, 'safe'::bigname_phase.canonicality_state, 'finalized'::bigname_phase.canonicality_state])) AND ((before_state ->> 'to_logical_name_id'::text) IS NOT NULL))$def$),
            ('normalized_events_project_primary_after_idx',
             $def$CREATE INDEX normalized_events_project_primary_after_idx ON bigname_phase.normalized_events USING btree (chain_id, lower((after_state ->> 'address'::text)), ((after_state ->> 'coin_type'::text)), ((after_state ->> 'namespace'::text)), block_number) INCLUDE (normalized_event_id) WHERE ((event_kind = ANY (ARRAY['ReverseChanged'::text, 'RecordChanged'::text])) AND (canonicality_state = ANY (ARRAY['canonical'::bigname_phase.canonicality_state, 'safe'::bigname_phase.canonicality_state, 'finalized'::bigname_phase.canonicality_state])) AND (lower((after_state ->> 'address'::text)) IS NOT NULL) AND ((after_state ->> 'coin_type'::text) IS NOT NULL) AND ((after_state ->> 'namespace'::text) IS NOT NULL))$def$),
            ('normalized_events_project_primary_before_idx',
             $def$CREATE INDEX normalized_events_project_primary_before_idx ON bigname_phase.normalized_events USING btree (chain_id, lower((before_state ->> 'address'::text)), ((before_state ->> 'coin_type'::text)), ((before_state ->> 'namespace'::text)), block_number) INCLUDE (normalized_event_id) WHERE ((event_kind = ANY (ARRAY['ReverseChanged'::text, 'RecordChanged'::text])) AND (canonicality_state = ANY (ARRAY['canonical'::bigname_phase.canonicality_state, 'safe'::bigname_phase.canonicality_state, 'finalized'::bigname_phase.canonicality_state])) AND (lower((before_state ->> 'address'::text)) IS NOT NULL) AND ((before_state ->> 'coin_type'::text) IS NOT NULL) AND ((before_state ->> 'namespace'::text) IS NOT NULL))$def$),
            ('normalized_events_project_primary_after_source_idx',
             $def$CREATE INDEX normalized_events_project_primary_after_source_idx ON bigname_phase.normalized_events USING btree (chain_id, lower(((after_state -> 'primary_claim_source'::text) ->> 'address'::text)), (((after_state -> 'primary_claim_source'::text) ->> 'coin_type'::text)), (((after_state -> 'primary_claim_source'::text) ->> 'namespace'::text)), block_number) INCLUDE (normalized_event_id) WHERE ((event_kind = ANY (ARRAY['ReverseChanged'::text, 'RecordChanged'::text])) AND (canonicality_state = ANY (ARRAY['canonical'::bigname_phase.canonicality_state, 'safe'::bigname_phase.canonicality_state, 'finalized'::bigname_phase.canonicality_state])) AND (lower(((after_state -> 'primary_claim_source'::text) ->> 'address'::text)) IS NOT NULL) AND (((after_state -> 'primary_claim_source'::text) ->> 'coin_type'::text) IS NOT NULL) AND (((after_state -> 'primary_claim_source'::text) ->> 'namespace'::text) IS NOT NULL))$def$),
            ('normalized_events_project_primary_before_source_idx',
             $def$CREATE INDEX normalized_events_project_primary_before_source_idx ON bigname_phase.normalized_events USING btree (chain_id, lower(((before_state -> 'primary_claim_source'::text) ->> 'address'::text)), (((before_state -> 'primary_claim_source'::text) ->> 'coin_type'::text)), (((before_state -> 'primary_claim_source'::text) ->> 'namespace'::text)), block_number) INCLUDE (normalized_event_id) WHERE ((event_kind = ANY (ARRAY['ReverseChanged'::text, 'RecordChanged'::text])) AND (canonicality_state = ANY (ARRAY['canonical'::bigname_phase.canonicality_state, 'safe'::bigname_phase.canonicality_state, 'finalized'::bigname_phase.canonicality_state])) AND (lower(((before_state -> 'primary_claim_source'::text) ->> 'address'::text)) IS NOT NULL) AND (((before_state -> 'primary_claim_source'::text) ->> 'coin_type'::text) IS NOT NULL) AND (((before_state -> 'primary_claim_source'::text) ->> 'namespace'::text) IS NOT NULL))$def$)
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
            RAISE EXCEPTION
                '% does not exist although bigname_phase.normalized_events does; build it with ops/project-scoped-history/install.sql as ops/project-scoped-history/README.md describes, then run the schema-migrations again',
                checked_index;
        END IF;
        IF found_kind <> 'index' THEN
            RAISE EXCEPTION
                'bigname_phase.% is a %, not an index, so the index was never built; remove or rename that relation, build the index with ops/project-scoped-history/install.sql as ops/project-scoped-history/README.md describes, then run the schema-migrations again',
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
                '% exists but is not a valid and ready index on bigname_phase.normalized_events; follow the recovery steps in ops/project-scoped-history/README.md, then run the schema-migrations again',
                checked_index;
        END IF;

        SELECT pg_get_indexdef(indexrelid)
        INTO found_definition
        FROM pg_index
        WHERE indexrelid = to_regclass('bigname_phase.' || checked_index);
        IF found_definition <> expected_definition THEN
            RAISE EXCEPTION
                '% exists but does not have the reviewed definition; found "%", expected "%"; follow the recovery steps in ops/project-scoped-history/README.md, then run the schema-migrations again',
                checked_index, found_definition, expected_definition;
        END IF;
    END LOOP;

    PERFORM set_config('search_path', previous_search_path, true);
END
$migration$;
