-- Run with psql -X -v ON_ERROR_STOP=1, outside any transaction, while the existing
-- runner is still processing: the builds are concurrent and permit writes.
-- A long batch can hold the writer transaction a concurrent build waits for.
-- Bound each build, rather than aborting that expected wait after a few seconds.
SET lock_timeout = '0';
SET statement_timeout = '6h';

-- This script never drops an index. The earlier label-array indexes
-- (name_surfaces_project_labels_idx and name_surfaces_project_suffix_idx) stay in
-- place for the running binary, whose lookups can only use them; the
-- schema-migration 20260923140000_project_name_surfaces_label_indexes.sql drops them
-- in the stop/start window, and validate-after-switch.sql then requires them gone.
--
-- IF NOT EXISTS matches on the name alone, so an interrupted concurrent build leaves
-- an invalid index that the statements below would skip, and an earlier manual build
-- can leave a valid index with other keys, or a table or other relation, under one of
-- these names. This check runs twice. Before the builds it refuses any name already
-- taken by something other than a valid and ready index on the right table (and, for
-- the two label-hash indexes and the label_hashes function, the reviewed definition),
-- so the operator repairs it before anything is built; names that resolve to nothing
-- pass. After the builds it also requires every index and the function to exist, so
-- the script fails instead of reporting success. README.md describes the recovery.
-- The definitions are compared as pg_get_indexdef prints them, like the
-- schema-migration does: the function's SET search_path = pg_catalog makes
-- PostgreSQL always print schema names, and SET quote_all_identifiers = off stops it
-- quoting every identifier. PostgreSQL puts the session's settings back when the
-- function returns or raises. The function lives in pg_temp and disappears with the
-- session.
CREATE OR REPLACE FUNCTION pg_temp.check_project_progressive_indexes(require_built boolean)
RETURNS void
LANGUAGE plpgsql
SET search_path = pg_catalog
SET quote_all_identifiers = off
AS $check$
DECLARE
    checked_index text;
    checked_table text;
    expected_definition text;
    found_definition text;
    found_kind text;
    found_table oid;
    found_body text;
BEGIN
    IF require_built OR to_regprocedure('bigname_phase.label_hashes(text[])') IS NOT NULL THEN
        SELECT btrim(regexp_replace(prosrc, '\s+', ' ', 'g'))
        INTO found_body
        FROM pg_proc
        WHERE oid = to_regprocedure('bigname_phase.label_hashes(text[])')
          AND provolatile = 'i' AND proisstrict AND proparallel = 's'
          AND prorettype = 'bigint[]'::regtype;
        IF found_body IS DISTINCT FROM
            'SELECT ARRAY(SELECT pg_catalog.hashtextextended(label, 0) FROM pg_catalog.unnest(labels) AS label)'
        THEN
            RAISE EXCEPTION
                'bigname_phase.label_hashes(text[]) is missing or does not have the reviewed definition; follow the recovery steps in ops/project-progressive/README.md before retrying';
        END IF;
    END IF;

    FOR checked_index, checked_table, expected_definition IN
        SELECT * FROM (VALUES
            ('normalized_events_project_node_history_idx', 'normalized_events', NULL),
            ('normalized_events_project_v1_pointer_node_idx', 'normalized_events', NULL),
            ('name_surfaces_project_node_idx', 'name_surfaces', NULL),
            ('name_surfaces_project_suffix_hash_idx', 'name_surfaces',
             'CREATE INDEX name_surfaces_project_suffix_hash_idx ON bigname_phase.name_surfaces USING btree (namespace, hash_array_extended(raw_labels, (0)::bigint))'),
            ('name_surfaces_project_label_hashes_idx', 'name_surfaces',
             'CREATE INDEX name_surfaces_project_label_hashes_idx ON bigname_phase.name_surfaces USING gin (bigname_phase.label_hashes(raw_labels))')
        ) AS reviewed(index_name, table_name, definition)
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
                'bigname_phase.% is a %, not an index, so the index was never built; remove or rename that relation, then follow ops/project-progressive/README.md before retrying',
                checked_index, found_kind;
        END IF;

        IF NOT EXISTS (
            SELECT 1
            FROM pg_index
            WHERE indexrelid = to_regclass('bigname_phase.' || checked_index)
              AND indrelid = to_regclass('bigname_phase.' || checked_table)
              AND indisvalid
              AND indisready
        ) THEN
            SELECT indrelid
            INTO found_table
            FROM pg_index
            WHERE indexrelid = to_regclass('bigname_phase.' || checked_index);
            RAISE EXCEPTION
                '% is missing from bigname_phase.% or is not valid and ready; follow the recovery steps in ops/project-progressive/README.md before retrying',
                checked_index, checked_table
                USING HINT = CASE
                    WHEN found_table IS NULL THEN
                        'No relation has this name. Rerun this script to build the index.'
                    WHEN found_table <> to_regclass('bigname_phase.' || checked_table) THEN
                        format('An index on %s holds this name. Rename or remove it, then rerun this script.', found_table::regclass)
                    ELSE
                        format('An interrupted concurrent build leaves an invalid index. Confirm in pg_stat_progress_create_index that no build is still running, run DROP INDEX CONCURRENTLY bigname_phase.%I, then rerun this script.', checked_index)
                END;
        END IF;

        IF expected_definition IS NOT NULL THEN
            found_definition := pg_get_indexdef(to_regclass('bigname_phase.' || checked_index));
            IF found_definition <> expected_definition THEN
                RAISE EXCEPTION
                    '% exists but does not have the reviewed definition; found "%", expected "%"; follow the recovery steps in ops/project-progressive/README.md before retrying',
                    checked_index, found_definition, expected_definition;
            END IF;
        END IF;
    END LOOP;
END
$check$;

-- Refuse before building anything; see the comment above.
DO $$ BEGIN PERFORM pg_temp.check_project_progressive_indexes(false); END $$;

-- One 64-bit hash per label, in label order. The body names pg_catalog functions so it
-- does not depend on the caller's search_path. It must match the definition in
-- migrations/20260923140000_project_name_surfaces_label_indexes.sql; an existing
-- function with another definition was refused above and is never replaced.
DO $install$
BEGIN
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
END
$install$;

CREATE INDEX CONCURRENTLY IF NOT EXISTS normalized_events_project_node_history_idx
    ON bigname_phase.normalized_events (chain_id, lower(after_state ->> 'node'), block_number)
    WHERE logical_name_id IS NULL
      AND consumer_visibility = 'activated'
      AND canonicality_state IN ('canonical', 'safe', 'finalized')
      AND after_state ->> 'node' IS NOT NULL
      AND ((event_kind IN ('RecordChanged', 'RecordVersionChanged')
            AND source_family IN ('ens_v1_resolver_l1', 'ens_v2_resolver_l1', 'basenames_base_resolver'))
           OR (event_kind = 'ResolverChanged'
               AND source_family IN ('ens_v1_registry_l1', 'ens_v1_registrar_l1', 'ens_v1_wrapper_l1')));

CREATE INDEX CONCURRENTLY IF NOT EXISTS name_surfaces_project_label_hashes_idx ON bigname_phase.name_surfaces USING gin(bigname_phase.label_hashes(raw_labels));
CREATE INDEX CONCURRENTLY IF NOT EXISTS name_surfaces_project_suffix_hash_idx ON bigname_phase.name_surfaces(namespace, hash_array_extended(raw_labels, 0));
CREATE INDEX CONCURRENTLY IF NOT EXISTS name_surfaces_project_node_idx ON bigname_phase.name_surfaces(namespace, lower(namehash));
CREATE INDEX CONCURRENTLY IF NOT EXISTS normalized_events_project_v1_pointer_node_idx
    ON bigname_phase.normalized_events(chain_id, namespace, lower(after_state ->> 'node'), block_number)
    WHERE event_kind = 'ResolverChanged'
      AND source_family IN ('ens_v1_registry_l1', 'ens_v1_registrar_l1', 'ens_v1_wrapper_l1')
      AND after_state ->> 'node' IS NOT NULL
      AND consumer_visibility = 'activated'
      AND canonicality_state IN ('canonical', 'safe', 'finalized');

-- Expression indexes have no statistics until the table is analyzed, and the
-- planner needs them to choose the label-hash indexes for the mirror lookups.
ANALYZE bigname_phase.name_surfaces;

-- Printed first so the receipt shows the flags even when the check below fails.
SELECT indexrelid::regclass AS index_name, indisvalid, indisready,
       pg_size_pretty(pg_relation_size(indexrelid)) AS index_size,
       pg_get_indexdef(indexrelid) AS definition
FROM pg_index
WHERE indexrelid IN (
    to_regclass('bigname_phase.normalized_events_project_node_history_idx'),
    to_regclass('bigname_phase.normalized_events_project_v1_pointer_node_idx'),
    to_regclass('bigname_phase.name_surfaces_project_node_idx'),
    to_regclass('bigname_phase.name_surfaces_project_suffix_hash_idx'),
    to_regclass('bigname_phase.name_surfaces_project_label_hashes_idx')
) ORDER BY index_name;

-- Every index and the function must now exist and pass the check.
DO $$ BEGIN PERFORM pg_temp.check_project_progressive_indexes(true); END $$;
