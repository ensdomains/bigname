-- Run with psql -X -v ON_ERROR_STOP=1 outside a transaction, before
-- 20261005200000. Final names are new; no old index needs replacing.
SET lock_timeout = '0';
SET statement_timeout = '6h';

-- A set-returning PL/pgSQL function materializes all commands before gexec runs
-- one. Thus every occupied name passes validation before any missing build starts.
CREATE OR REPLACE FUNCTION pg_temp.registry_permission_index_builds(require_built boolean)
RETURNS TABLE(command text)
LANGUAGE plpgsql
SET search_path = pg_catalog
SET quote_all_identifiers = off
AS $check$
DECLARE
    checked record;
BEGIN
    FOR checked IN SELECT * FROM (VALUES
            ('normalized_events_registry_origin_idx', $def$CREATE INDEX normalized_events_registry_origin_idx ON bigname_phase.normalized_events USING btree (chain_id, lower((after_state ->> 'proxy_address'::text)), block_number, transaction_index, log_index, event_identity COLLATE "C") WHERE ((source_family = 'ens_v2_migration_l1'::text) AND (event_kind = 'ContractDiscovered'::text) AND (canonicality_state = ANY (ARRAY['canonical'::bigname_phase.canonicality_state, 'safe'::bigname_phase.canonicality_state, 'finalized'::bigname_phase.canonicality_state])))$def$),
            ('normalized_events_wrapper_departure_idx', $def$CREATE INDEX normalized_events_wrapper_departure_idx ON bigname_phase.normalized_events USING btree (chain_id, lower((after_state ->> 'proxy_address'::text)), block_number) WHERE ((source_family = 'ens_v2_registry_l1'::text) AND (event_kind = 'Upgraded'::text) AND (canonicality_state = ANY (ARRAY['canonical'::bigname_phase.canonicality_state, 'safe'::bigname_phase.canonicality_state, 'finalized'::bigname_phase.canonicality_state])) AND (lower((after_state ->> 'implementation'::text)) IS DISTINCT FROM '0xbe768b63e5fbbfbb0ae97e9064e0002df8001880'::text))$def$),
            ('normalized_events_user_registry_departure_idx', $def$CREATE INDEX normalized_events_user_registry_departure_idx ON bigname_phase.normalized_events USING btree (chain_id, lower((after_state ->> 'proxy_address'::text)), block_number) WHERE ((source_family = 'ens_v2_registry_l1'::text) AND (event_kind = 'Upgraded'::text) AND (canonicality_state = ANY (ARRAY['canonical'::bigname_phase.canonicality_state, 'safe'::bigname_phase.canonicality_state, 'finalized'::bigname_phase.canonicality_state])) AND (lower((after_state ->> 'implementation'::text)) IS DISTINCT FROM '0x9bd8a88719068d09ecee662f36c0e3856708366a'::text))$def$)
        ) definitions(index_name, expected_definition)
    LOOP
        IF to_regclass('bigname_phase.' || checked.index_name) IS NULL THEN
            IF require_built THEN
                RAISE EXCEPTION 'missing prebuilt index %; rerun ops/registry-permission-indexes/install.sql', checked.index_name;
            END IF;
            command := replace(checked.expected_definition, 'CREATE INDEX ',
                'CREATE INDEX CONCURRENTLY IF NOT EXISTS ');
            RETURN NEXT;
        ELSIF NOT EXISTS (SELECT 1 FROM pg_index
            WHERE indexrelid = to_regclass('bigname_phase.' || checked.index_name)
              AND indrelid = to_regclass('bigname_phase.normalized_events')
              AND indisvalid AND indisready AND indislive
              AND pg_get_indexdef(indexrelid) = checked.expected_definition)
        THEN
            RAISE EXCEPTION 'bigname_phase.% is invalid or has an unexpected definition; follow ops/registry-permission-indexes/README.md before retrying', checked.index_name;
        END IF;
    END LOOP;
END
$check$;

SELECT command FROM pg_temp.registry_permission_index_builds(false)
\gexec
SELECT command FROM pg_temp.registry_permission_index_builds(true);
ANALYZE bigname_phase.normalized_events;
SELECT indexrelid::regclass AS index_name, indisvalid, indisready, indislive,
       pg_size_pretty(pg_relation_size(indexrelid)) AS index_size,
       pg_get_indexdef(indexrelid) AS definition
FROM pg_index
WHERE indexrelid IN (
    to_regclass('bigname_phase.normalized_events_registry_origin_idx'),
    to_regclass('bigname_phase.normalized_events_wrapper_departure_idx'),
    to_regclass('bigname_phase.normalized_events_user_registry_departure_idx')
)
ORDER BY indexrelid::regclass::text;
