-- Run with psql -X -v ON_ERROR_STOP=1 outside any transaction.
-- Concurrent builds can wait for existing writer transactions; inspect progress.
SET lock_timeout = '0';
SET statement_timeout = '30min';

-- IF NOT EXISTS matches on the name alone, so an interrupted concurrent build
-- leaves an invalid index that the statements below then skip, and an earlier
-- manual build can leave a valid index with other keys or another predicate,
-- or a table, view, or other relation that is not an index under one of these
-- names. This check runs twice. Before the builds it refuses any of the eight
-- names that is already taken by something other than the reviewed, valid and
-- ready index, so the operator repairs it before the other builds run; names
-- that resolve to nothing pass. After the builds it also requires every index
-- to exist, so the script fails instead of reporting success. README.md
-- describes the recovery.
-- The definition check matches the one in the schema-migration
-- 20260917161000_project_scoped_history_index_validity_check.sql. PostgreSQL
-- always prints the table's schema name, and a type's schema name only when the
-- session search_path does not include it. The printed text is not rewritten to
-- even that out, because a text replacement would also change a string literal
-- such as a JSON key. Instead the function's own SET search_path clause makes
-- search_path pg_catalog while it runs, so both schema names are always
-- printed, and the expected text keeps them. Its SET quote_all_identifiers
-- clause turns that setting off for the same read: when the session has it on,
-- PostgreSQL prints every identifier in double quotes and a healthy index would
-- be refused. The quotes are not stripped from the printed text either.
-- PostgreSQL puts the session's search_path and quote_all_identifiers back when
-- the function returns or raises, so the CREATE INDEX statements below are not
-- affected. Every name the function uses is schema-qualified or lives in
-- pg_catalog. The function lives in pg_temp and disappears with the session.
CREATE OR REPLACE FUNCTION pg_temp.check_project_scoped_history_indexes(require_built boolean)
RETURNS void
LANGUAGE plpgsql
SET search_path = pg_catalog
SET quote_all_identifiers = off
AS $check$
DECLARE
    checked_index text;
    expected_definition text;
    found_definition text;
    found_kind text;
    found_table oid;
BEGIN
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
        IF found_kind IS NULL AND NOT require_built THEN
            CONTINUE;
        END IF;
        IF found_kind <> 'index' THEN
            RAISE EXCEPTION
                'bigname_phase.% is a %, not an index, so the index was never built; remove or rename that relation, then follow ops/project-scoped-history/README.md before retrying',
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
            SELECT indrelid
            INTO found_table
            FROM pg_index
            WHERE indexrelid = to_regclass('bigname_phase.' || checked_index);
            RAISE EXCEPTION
                '% is missing from bigname_phase.normalized_events or is not valid and ready; follow the recovery steps in ops/project-scoped-history/README.md before retrying',
                checked_index
                USING HINT = CASE
                    WHEN found_table IS NULL THEN
                        'No relation has this name. Rerun this script to build the index.'
                    WHEN found_table <> to_regclass('bigname_phase.normalized_events') THEN
                        format('An index on %s holds this name. Rename or remove it, then rerun this script.', found_table::regclass)
                    ELSE
                        format('An interrupted concurrent build leaves an invalid index. Confirm in pg_stat_progress_create_index that no build is still running, run DROP INDEX CONCURRENTLY bigname_phase.%I, then rerun this script.', checked_index)
                END;
        END IF;

        SELECT pg_get_indexdef(indexrelid)
        INTO found_definition
        FROM pg_index
        WHERE indexrelid = to_regclass('bigname_phase.' || checked_index);
        IF found_definition <> expected_definition THEN
            RAISE EXCEPTION
                '% exists but does not have the reviewed definition; found "%", expected "%"; follow the recovery steps in ops/project-scoped-history/README.md before retrying',
                checked_index, found_definition, expected_definition;
        END IF;
    END LOOP;
END
$check$;

-- Refuse before building anything; see the comment above.
DO $$ BEGIN PERFORM pg_temp.check_project_scoped_history_indexes(false); END $$;

CREATE INDEX CONCURRENTLY IF NOT EXISTS normalized_events_project_name_node_idx
    ON bigname_phase.normalized_events (chain_id, (namespace || ':' || lower(after_state ->> 'node')), block_number)
    INCLUDE (normalized_event_id)
    WHERE (event_kind IN ('SubregistryChanged', 'AliasChanged')
           OR (event_kind = 'AuthorityTransferred'
               AND source_family IN ('ens_v1_registry_l1', 'basenames_base_registry')))
      AND canonicality_state IN ('canonical', 'safe', 'finalized')
      AND (namespace || ':' || lower(after_state ->> 'node')) IS NOT NULL;

CREATE INDEX CONCURRENTLY IF NOT EXISTS normalized_events_project_name_child_idx
    ON bigname_phase.normalized_events (chain_id, (namespace || ':' || lower(after_state ->> 'child_node')), block_number)
    INCLUDE (normalized_event_id)
    WHERE (event_kind IN ('SubregistryChanged', 'AliasChanged')
           OR (event_kind = 'AuthorityTransferred'
               AND source_family IN ('ens_v1_registry_l1', 'basenames_base_registry')))
      AND canonicality_state IN ('canonical', 'safe', 'finalized')
      AND (namespace || ':' || lower(after_state ->> 'child_node')) IS NOT NULL;

CREATE INDEX CONCURRENTLY IF NOT EXISTS normalized_events_project_name_after_target_idx
    ON bigname_phase.normalized_events (chain_id, (after_state ->> 'to_logical_name_id'), block_number)
    INCLUDE (normalized_event_id)
    WHERE (event_kind IN ('SubregistryChanged', 'AliasChanged')
           OR (event_kind = 'AuthorityTransferred'
               AND source_family IN ('ens_v1_registry_l1', 'basenames_base_registry')))
      AND canonicality_state IN ('canonical', 'safe', 'finalized')
      AND (after_state ->> 'to_logical_name_id') IS NOT NULL;

CREATE INDEX CONCURRENTLY IF NOT EXISTS normalized_events_project_name_before_target_idx
    ON bigname_phase.normalized_events (chain_id, (before_state ->> 'to_logical_name_id'), block_number)
    INCLUDE (normalized_event_id)
    WHERE (event_kind IN ('SubregistryChanged', 'AliasChanged')
           OR (event_kind = 'AuthorityTransferred'
               AND source_family IN ('ens_v1_registry_l1', 'basenames_base_registry')))
      AND canonicality_state IN ('canonical', 'safe', 'finalized')
      AND (before_state ->> 'to_logical_name_id') IS NOT NULL;

CREATE INDEX CONCURRENTLY IF NOT EXISTS normalized_events_project_primary_after_idx
    ON bigname_phase.normalized_events (
        chain_id, (lower(after_state ->> 'address')), (after_state ->> 'coin_type'), (after_state ->> 'namespace'), block_number
    ) INCLUDE (normalized_event_id)
    WHERE event_kind IN ('ReverseChanged', 'RecordChanged')
      AND canonicality_state IN ('canonical', 'safe', 'finalized')
      AND (lower(after_state ->> 'address')) IS NOT NULL
      AND (after_state ->> 'coin_type') IS NOT NULL
      AND (after_state ->> 'namespace') IS NOT NULL;

CREATE INDEX CONCURRENTLY IF NOT EXISTS normalized_events_project_primary_before_idx
    ON bigname_phase.normalized_events (
        chain_id, (lower(before_state ->> 'address')), (before_state ->> 'coin_type'), (before_state ->> 'namespace'), block_number
    ) INCLUDE (normalized_event_id)
    WHERE event_kind IN ('ReverseChanged', 'RecordChanged')
      AND canonicality_state IN ('canonical', 'safe', 'finalized')
      AND (lower(before_state ->> 'address')) IS NOT NULL
      AND (before_state ->> 'coin_type') IS NOT NULL
      AND (before_state ->> 'namespace') IS NOT NULL;

CREATE INDEX CONCURRENTLY IF NOT EXISTS normalized_events_project_primary_after_source_idx
    ON bigname_phase.normalized_events (
        chain_id, (lower(after_state -> 'primary_claim_source' ->> 'address')), (after_state -> 'primary_claim_source' ->> 'coin_type'), (after_state -> 'primary_claim_source' ->> 'namespace'), block_number
    ) INCLUDE (normalized_event_id)
    WHERE event_kind IN ('ReverseChanged', 'RecordChanged')
      AND canonicality_state IN ('canonical', 'safe', 'finalized')
      AND (lower(after_state -> 'primary_claim_source' ->> 'address')) IS NOT NULL
      AND (after_state -> 'primary_claim_source' ->> 'coin_type') IS NOT NULL
      AND (after_state -> 'primary_claim_source' ->> 'namespace') IS NOT NULL;

CREATE INDEX CONCURRENTLY IF NOT EXISTS normalized_events_project_primary_before_source_idx
    ON bigname_phase.normalized_events (
        chain_id, (lower(before_state -> 'primary_claim_source' ->> 'address')), (before_state -> 'primary_claim_source' ->> 'coin_type'), (before_state -> 'primary_claim_source' ->> 'namespace'), block_number
    ) INCLUDE (normalized_event_id)
    WHERE event_kind IN ('ReverseChanged', 'RecordChanged')
      AND canonicality_state IN ('canonical', 'safe', 'finalized')
      AND (lower(before_state -> 'primary_claim_source' ->> 'address')) IS NOT NULL
      AND (before_state -> 'primary_claim_source' ->> 'coin_type') IS NOT NULL
      AND (before_state -> 'primary_claim_source' ->> 'namespace') IS NOT NULL;

-- Printed first so the receipt shows the flags even when the check below fails.
SELECT indexrelid::regclass AS index_name, indisvalid, indisready,
       pg_size_pretty(pg_relation_size(indexrelid)) AS index_size,
       pg_get_indexdef(indexrelid) AS definition
FROM pg_index
WHERE indexrelid IN (
    to_regclass('bigname_phase.normalized_events_project_name_node_idx'),
    to_regclass('bigname_phase.normalized_events_project_name_child_idx'),
    to_regclass('bigname_phase.normalized_events_project_name_after_target_idx'),
    to_regclass('bigname_phase.normalized_events_project_name_before_target_idx'),
    to_regclass('bigname_phase.normalized_events_project_primary_after_idx'),
    to_regclass('bigname_phase.normalized_events_project_primary_before_idx'),
    to_regclass('bigname_phase.normalized_events_project_primary_after_source_idx'),
    to_regclass('bigname_phase.normalized_events_project_primary_before_source_idx')
) ORDER BY index_name;

-- All eight must now exist, belong to bigname_phase.normalized_events, be valid
-- and ready, and have the reviewed definition.
DO $$ BEGIN PERFORM pg_temp.check_project_scoped_history_indexes(true); END $$;
