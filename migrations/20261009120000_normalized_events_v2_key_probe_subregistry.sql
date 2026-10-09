-- The ENSv2 key index of Interpret's lookahead loader gains one array element: the registry
-- an event's `subregistry` value names, filed under that registry's ENSv2 state key with a
-- zero token. A registry is named by the parent token that points at it (TYR-277), so the
-- loader reads those tokens when it restores the registry.
--
-- Index only. No row changes. The block drops normalized_events_v2_key_probe_idx when it is
-- a valid index with the definition 20261001130000_normalized_events_v2_lookahead_indexes.sql
-- installed, then builds the new definition. The drop takes an ACCESS EXCLUSIVE lock on
-- normalized_events and holds it until the schema-migration commits, so the build blocks
-- reads as well as writes for its whole length. On a large initialized database, replace the
-- index before applying schema-migrations, as ops/v1-lookahead-indexes/README.md describes:
--
-- 1. Stop every runner that uses the lookahead loader.
-- 2. Run DROP INDEX CONCURRENTLY bigname_phase.normalized_events_v2_key_probe_idx.
-- 3. Run ops/v1-lookahead-indexes/install.sql.
--
-- This block then adopts the prebuilt index. CREATE INDEX IF NOT EXISTS matches on the name
-- alone, so the block ends with the validity and definition check of that schema-migration,
-- under the same transaction-local search_path and quote_all_identifiers settings, put back
-- before it returns. Any other relation or definition under the name is refused, not dropped.
DO $migration$
DECLARE
    checked_index constant text := 'normalized_events_v2_key_probe_idx';
    previous_definition constant text := $def$CREATE INDEX normalized_events_v2_key_probe_idx ON bigname_phase.normalized_events USING gin (array_remove(ARRAY[(((lower(split_part((raw_fact_ref ->> 'state_scope'::text), ':'::text, 1)) || ':'::text) || lower("left"(split_part((raw_fact_ref ->> 'state_scope'::text), ':'::text, 3), GREATEST((length(split_part((raw_fact_ref ->> 'state_scope'::text), ':'::text, 3)) - 8), 0)))) || '00000000'::text), (lower(split_part((raw_fact_ref ->> 'state_scope'::text), ':'::text, 1)) || ':*'::text), (((lower(split_part((raw_fact_ref ->> 'state_scope'::text), ':'::text, 1)) || ':'::text) || lower("left"((after_state ->> 'new_token_id'::text), GREATEST((length((after_state ->> 'new_token_id'::text)) - 8), 0)))) || '00000000'::text), (((lower(split_part((raw_fact_ref ->> 'state_scope'::text), ':'::text, 1)) || ':'::text) || lower("left"(COALESCE((after_state ->> 'resource'::text), (after_state ->> 'upstream_resource'::text)), GREATEST((length(COALESCE((after_state ->> 'resource'::text), (after_state ->> 'upstream_resource'::text))) - 8), 0)))) || '00000000'::text), (((lower(split_part((raw_fact_ref ->> 'state_scope'::text), ':'::text, 1)) || ':'::text) || lower("left"((after_state ->> 'labelhash'::text), GREATEST((length((after_state ->> 'labelhash'::text)) - 8), 0)))) || '00000000'::text)], NULL::text)) WHERE ((canonicality_state = ANY (ARRAY['canonical'::bigname_phase.canonicality_state, 'safe'::bigname_phase.canonicality_state, 'finalized'::bigname_phase.canonicality_state])) AND (source_family ~~ 'ens\_v2\_%'::text))$def$;
    expected_definition constant text := $def$CREATE INDEX normalized_events_v2_key_probe_idx ON bigname_phase.normalized_events USING gin (array_remove(ARRAY[(((lower(split_part((raw_fact_ref ->> 'state_scope'::text), ':'::text, 1)) || ':'::text) || lower("left"(split_part((raw_fact_ref ->> 'state_scope'::text), ':'::text, 3), GREATEST((length(split_part((raw_fact_ref ->> 'state_scope'::text), ':'::text, 3)) - 8), 0)))) || '00000000'::text), (lower(split_part((raw_fact_ref ->> 'state_scope'::text), ':'::text, 1)) || ':*'::text), (((lower(split_part((raw_fact_ref ->> 'state_scope'::text), ':'::text, 1)) || ':'::text) || lower("left"((after_state ->> 'new_token_id'::text), GREATEST((length((after_state ->> 'new_token_id'::text)) - 8), 0)))) || '00000000'::text), (((lower(split_part((raw_fact_ref ->> 'state_scope'::text), ':'::text, 1)) || ':'::text) || lower("left"(COALESCE((after_state ->> 'resource'::text), (after_state ->> 'upstream_resource'::text)), GREATEST((length(COALESCE((after_state ->> 'resource'::text), (after_state ->> 'upstream_resource'::text))) - 8), 0)))) || '00000000'::text), (((lower(split_part((raw_fact_ref ->> 'state_scope'::text), ':'::text, 1)) || ':'::text) || lower("left"((after_state ->> 'labelhash'::text), GREATEST((length((after_state ->> 'labelhash'::text)) - 8), 0)))) || '00000000'::text), (lower((after_state ->> 'subregistry'::text)) || ':00000000'::text)], NULL::text)) WHERE ((canonicality_state = ANY (ARRAY['canonical'::bigname_phase.canonicality_state, 'safe'::bigname_phase.canonicality_state, 'finalized'::bigname_phase.canonicality_state])) AND (source_family ~~ 'ens\_v2\_%'::text))$def$;
    found_definition text;
    found_kind text;
    previous_search_path text;
    previous_quote_all_identifiers text;
BEGIN
    IF to_regclass('bigname_phase.normalized_events') IS NULL THEN
        RETURN;
    END IF;

    -- Every name below is schema-qualified or lives in pg_catalog.
    previous_search_path := current_setting('search_path');
    PERFORM set_config('search_path', 'pg_catalog', true);
    -- The definitions above have no quoted identifiers.
    previous_quote_all_identifiers := current_setting('quote_all_identifiers');
    PERFORM set_config('quote_all_identifiers', 'off', true);

    IF EXISTS (
        SELECT 1
        FROM pg_index
        WHERE indexrelid = to_regclass('bigname_phase.' || checked_index)
          AND indrelid = to_regclass('bigname_phase.normalized_events')
          AND indisvalid
          AND indisready
          AND pg_get_indexdef(indexrelid) = previous_definition
    ) THEN
        DROP INDEX bigname_phase.normalized_events_v2_key_probe_idx;
    END IF;

    CREATE INDEX IF NOT EXISTS normalized_events_v2_key_probe_idx
        ON bigname_phase.normalized_events USING gin ((
            array_remove(ARRAY[
                lower(split_part(raw_fact_ref ->> 'state_scope', ':', 1)) || ':' || lower(left(split_part(raw_fact_ref ->> 'state_scope', ':', 3), greatest(length(split_part(raw_fact_ref ->> 'state_scope', ':', 3)) - 8, 0))) || '00000000',
                lower(split_part(raw_fact_ref ->> 'state_scope', ':', 1)) || ':*',
                lower(split_part(raw_fact_ref ->> 'state_scope', ':', 1)) || ':' || lower(left(after_state ->> 'new_token_id', greatest(length(after_state ->> 'new_token_id') - 8, 0))) || '00000000',
                lower(split_part(raw_fact_ref ->> 'state_scope', ':', 1)) || ':' || lower(left(COALESCE(after_state ->> 'resource', after_state ->> 'upstream_resource'), greatest(length(COALESCE(after_state ->> 'resource', after_state ->> 'upstream_resource')) - 8, 0))) || '00000000',
                lower(split_part(raw_fact_ref ->> 'state_scope', ':', 1)) || ':' || lower(left(after_state ->> 'labelhash', greatest(length(after_state ->> 'labelhash') - 8, 0))) || '00000000',
                lower(after_state ->> 'subregistry') || ':00000000'
            ]::text[], NULL)
        ))
        WHERE canonicality_state IN ('canonical','safe','finalized')
          AND source_family LIKE 'ens\_v2\_%';

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
            '% does not exist although bigname_phase.normalized_events does; build it with ops/v1-lookahead-indexes/install.sql as ops/v1-lookahead-indexes/README.md describes, then run the schema-migrations again',
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
            '% exists but is not a valid and ready index on bigname_phase.normalized_events; follow the recovery steps in ops/v1-lookahead-indexes/README.md, then run the schema-migrations again',
            checked_index;
    END IF;

    SELECT pg_get_indexdef(indexrelid)
    INTO found_definition
    FROM pg_index
    WHERE indexrelid = to_regclass('bigname_phase.' || checked_index);
    IF found_definition <> expected_definition THEN
        RAISE EXCEPTION
            '% exists but does not have the reviewed definition; found "%", expected "%"; follow the recovery steps in ops/v1-lookahead-indexes/README.md, then run the schema-migrations again',
            checked_index, found_definition, expected_definition;
    END IF;

    PERFORM set_config('search_path', previous_search_path, true);
    PERFORM set_config('quote_all_identifiers', previous_quote_all_identifiers, true);
END
$migration$;
