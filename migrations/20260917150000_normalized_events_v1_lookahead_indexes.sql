-- Prebuild these indexes concurrently on large initialized databases using
-- ops/v1-lookahead-indexes/install.sql before applying schema-migrations.
--
-- CREATE INDEX IF NOT EXISTS matches on the name alone. An interrupted concurrent
-- prebuild leaves an invalid index under the right name, and a wrong manual
-- prebuild leaves a valid index with other keys or another predicate, or a table,
-- view, or other relation that is not an index under the right name. The
-- statements below then succeed without building anything. The check at the end
-- stops the run instead of recording success over an index the lookahead loader
-- cannot use. It is the check 20260917160000_discovery_edges_index_validity_check.sql
-- makes for the discovery indexes.
--
-- The definition is compared as PostgreSQL prints it with pg_get_indexdef, so
-- key order, expressions, ordering, operator classes, uniqueness, and the
-- predicate are all covered. PostgreSQL adds the schema name to the table
-- always and to the enum type only when the session search_path does not
-- include it, so the schema name is removed before comparing. It prints the
-- expiry CASE expression over several indented lines, so runs of whitespace are
-- collapsed to one space before comparing. The expected text is how the fresh
-- baseline index prints; schema-v2/apply-check.sh proves it for the baseline,
-- this schema-migration, and install.sql.
--
-- To recover, follow ops/v1-lookahead-indexes/README.md: confirm no build is
-- running, drop only the named index with DROP INDEX CONCURRENTLY, rerun
-- install.sql, then run the schema-migrations again.
DO $migration$
DECLARE
    checked_index text;
    expected_definition text;
    found_definition text;
    found_kind text;
BEGIN
    IF to_regclass('bigname_phase.normalized_events') IS NULL THEN
        RETURN;
    END IF;

    CREATE INDEX IF NOT EXISTS normalized_events_v1_due_probe_idx
        ON bigname_phase.normalized_events (
            chain_id,
            (CASE WHEN jsonb_typeof(after_state -> 'expiry') IN ('number','string')
                AND after_state ->> 'expiry' ~ '^[+-]?[0-9]+$'
                AND length(ltrim(after_state ->> 'expiry', '+-0')) <= 19
              THEN ((CASE WHEN left(after_state ->> 'expiry', 1) = '-' THEN '-' ELSE '' END)
                || COALESCE(NULLIF(ltrim(after_state ->> 'expiry', '+-0'), ''), '0'))::numeric
            END),
            block_number
        )
        WHERE canonicality_state IN ('canonical','safe','finalized')
          AND source_family = 'ens_v1_registrar_l1'
          AND event_kind IN ('RegistrationGranted','RegistrationRenewed','TokenControlTransferred');

    CREATE INDEX IF NOT EXISTS normalized_events_v1_direct_node_probe_idx
        ON bigname_phase.normalized_events (
            chain_id,
            (COALESCE(namespace || ':' || lower(COALESCE(after_state ->> 'child_node', after_state ->> 'namehash', after_state ->> 'node', after_state #>> '{grant_source,node}', after_state #>> '{revocation_source,node}')), logical_name_id)),
            block_number
        )
        WHERE canonicality_state IN ('canonical','safe','finalized')
          AND source_family LIKE 'ens\_v1\_%';

    FOR checked_index, expected_definition IN
        SELECT * FROM (VALUES
            ('normalized_events_v1_due_probe_idx',
             $def$CREATE INDEX normalized_events_v1_due_probe_idx ON normalized_events USING btree (chain_id, ( CASE WHEN ((jsonb_typeof((after_state -> 'expiry'::text)) = ANY (ARRAY['number'::text, 'string'::text])) AND ((after_state ->> 'expiry'::text) ~ '^[+-]?[0-9]+$'::text) AND (length(ltrim((after_state ->> 'expiry'::text), '+-0'::text)) <= 19)) THEN (( CASE WHEN ("left"((after_state ->> 'expiry'::text), 1) = '-'::text) THEN '-'::text ELSE ''::text END || COALESCE(NULLIF(ltrim((after_state ->> 'expiry'::text), '+-0'::text), ''::text), '0'::text)))::numeric ELSE NULL::numeric END), block_number) WHERE ((canonicality_state = ANY (ARRAY['canonical'::canonicality_state, 'safe'::canonicality_state, 'finalized'::canonicality_state])) AND (source_family = 'ens_v1_registrar_l1'::text) AND (event_kind = ANY (ARRAY['RegistrationGranted'::text, 'RegistrationRenewed'::text, 'TokenControlTransferred'::text])))$def$),
            ('normalized_events_v1_direct_node_probe_idx',
             $def$CREATE INDEX normalized_events_v1_direct_node_probe_idx ON normalized_events USING btree (chain_id, COALESCE(((namespace || ':'::text) || lower(COALESCE((after_state ->> 'child_node'::text), (after_state ->> 'namehash'::text), (after_state ->> 'node'::text), (after_state #>> '{grant_source,node}'::text[]), (after_state #>> '{revocation_source,node}'::text[])))), logical_name_id), block_number) WHERE ((canonicality_state = ANY (ARRAY['canonical'::canonicality_state, 'safe'::canonicality_state, 'finalized'::canonicality_state])) AND (source_family ~~ 'ens\_v1\_%'::text))$def$)
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
                '% exists but is not a valid and ready index on bigname_phase.normalized_events; follow the recovery steps in ops/v1-lookahead-indexes/README.md, then run the schema-migrations again',
                checked_index;
        END IF;

        SELECT regexp_replace(
                   replace(pg_get_indexdef(indexrelid), 'bigname_phase.', ''),
                   '\s+', ' ', 'g')
        INTO found_definition
        FROM pg_index
        WHERE indexrelid = to_regclass('bigname_phase.' || checked_index);
        IF found_definition <> expected_definition THEN
            RAISE EXCEPTION
                '% exists but does not have the reviewed definition; found "%", expected "%"; follow the recovery steps in ops/v1-lookahead-indexes/README.md, then run the schema-migrations again',
                checked_index, found_definition, expected_definition;
        END IF;
    END LOOP;
END
$migration$;
