-- Run with psql -X -v ON_ERROR_STOP=1, outside a transaction, after the
-- historical index migrations and before 20261005170000. See README.md.
-- Old replacement indexes remain available throughout concurrent builds.
SET lock_timeout = '0';
SET statement_timeout = '6h';

-- The function validates every occupied name before returning any build command.
-- PostgreSQL materializes its result before psql executes the commands with gexec,
-- so a later invalid candidate cannot cause an earlier build to start.
CREATE OR REPLACE FUNCTION pg_temp.address_history_catalogue_index_builds(require_built boolean)
RETURNS TABLE(command text)
LANGUAGE plpgsql
SET search_path = pg_catalog
SET quote_all_identifiers = off
AS $check$
DECLARE
    reviewed record;
    final_oid oid;
    candidate_oid oid;
    final_matches boolean;
BEGIN
    FOR reviewed IN
        SELECT * FROM (VALUES
            ('normalized_events_name_history_idx', 'ahc_name_prebuild_idx',
             $def$ON bigname_phase.normalized_events USING btree (logical_name_id, block_number DESC NULLS LAST, chain_id, block_hash DESC NULLS LAST, transaction_index DESC NULLS LAST, log_index DESC NULLS LAST, event_identity DESC) WHERE ((logical_name_id IS NOT NULL) AND (canonicality_state = ANY (ARRAY['canonical'::bigname_phase.canonicality_state, 'safe'::bigname_phase.canonicality_state, 'finalized'::bigname_phase.canonicality_state])))$def$),
            ('normalized_events_resource_history_idx', 'ahc_resource_prebuild_idx',
             $def$ON bigname_phase.normalized_events USING btree (resource_id, block_number DESC NULLS LAST, chain_id, block_hash DESC NULLS LAST, transaction_index DESC NULLS LAST, log_index DESC NULLS LAST, event_identity DESC) WHERE ((resource_id IS NOT NULL) AND (canonicality_state = ANY (ARRAY['canonical'::bigname_phase.canonicality_state, 'safe'::bigname_phase.canonicality_state, 'finalized'::bigname_phase.canonicality_state])))$def$),
            ('normalized_events_project_node_history_idx', 'ahc_node_prebuild_idx',
             $def$ON bigname_phase.normalized_events USING btree (chain_id, lower((after_state ->> 'node'::text)), block_number DESC NULLS LAST, block_hash DESC NULLS LAST, transaction_index DESC NULLS LAST, log_index DESC NULLS LAST, event_identity DESC) WHERE ((logical_name_id IS NULL) AND (consumer_visibility = 'activated'::text) AND (canonicality_state = ANY (ARRAY['canonical'::bigname_phase.canonicality_state, 'safe'::bigname_phase.canonicality_state, 'finalized'::bigname_phase.canonicality_state])) AND ((after_state ->> 'node'::text) IS NOT NULL) AND (((event_kind = ANY (ARRAY['RecordChanged'::text, 'RecordVersionChanged'::text])) AND (source_family = ANY (ARRAY['ens_v1_resolver_l1'::text, 'ens_v2_resolver_l1'::text, 'basenames_base_resolver'::text]))) OR ((event_kind = 'ResolverChanged'::text) AND (source_family = ANY (ARRAY['ens_v1_registry_l1'::text, 'ens_v1_registrar_l1'::text, 'ens_v1_wrapper_l1'::text])))))$def$),
            ('normalized_events_record_id_write_idx', 'ahc_record_prebuild_idx',
             $def$ON bigname_phase.normalized_events USING btree (chain_id, lower((after_state ->> 'resolver'::text)), ((after_state ->> 'resolver_record_id'::text)), block_number DESC NULLS LAST, block_hash DESC NULLS LAST, transaction_index DESC NULLS LAST, log_index DESC NULLS LAST, event_identity DESC) WHERE ((event_kind = 'RecordChanged'::text) AND ((after_state ->> 'storage_model'::text) = 'resolver_record_id'::text) AND (consumer_visibility = 'activated'::text) AND (canonicality_state = ANY (ARRAY['canonical'::bigname_phase.canonicality_state, 'safe'::bigname_phase.canonicality_state, 'finalized'::bigname_phase.canonicality_state])))$def$),
            ('normalized_events_history_discovery_name_idx', 'normalized_events_history_discovery_name_idx',
             $def$ON bigname_phase.normalized_events USING btree (chain_id, logical_name_id, block_number) WHERE ((logical_name_id IS NOT NULL) AND (resource_id IS NOT NULL) AND (canonicality_state <> ALL (ARRAY['canonical'::bigname_phase.canonicality_state, 'safe'::bigname_phase.canonicality_state, 'finalized'::bigname_phase.canonicality_state])))$def$),
            ('normalized_events_history_discovery_resource_idx', 'normalized_events_history_discovery_resource_idx',
             $def$ON bigname_phase.normalized_events USING btree (chain_id, resource_id, block_number) WHERE ((logical_name_id IS NOT NULL) AND (resource_id IS NOT NULL) AND (canonicality_state <> ALL (ARRAY['canonical'::bigname_phase.canonicality_state, 'safe'::bigname_phase.canonicality_state, 'finalized'::bigname_phase.canonicality_state])))$def$)
        ) AS definitions(index_name, candidate_name, definition)
    LOOP
        final_oid := to_regclass('bigname_phase.' || reviewed.index_name);
        candidate_oid := to_regclass('bigname_phase.' || reviewed.candidate_name);
        final_matches := false;
        IF final_oid IS NOT NULL THEN
            IF NOT EXISTS (
                SELECT 1 FROM pg_index
                WHERE indexrelid = final_oid
                  AND indrelid = 'bigname_phase.normalized_events'::regclass
            ) THEN
                RAISE EXCEPTION '% is not an index on bigname_phase.normalized_events; follow ops/address-history-catalogue-indexes/README.md', reviewed.index_name;
            END IF;
            SELECT indisvalid AND indisready
                   AND pg_get_indexdef(indexrelid) = format('CREATE INDEX %I %s', reviewed.index_name, reviewed.definition)
            INTO final_matches FROM pg_index WHERE indexrelid = final_oid;
        END IF;
        -- A replacement has a separate temporary name. The two new discovery
        -- indexes are prebuilt under their final names and must already be exact.
        IF candidate_oid IS NOT NULL THEN
            IF NOT EXISTS (
                SELECT 1 FROM pg_index
                WHERE indexrelid = candidate_oid
                  AND indrelid = 'bigname_phase.normalized_events'::regclass
                  AND indisvalid AND indisready
                  AND pg_get_indexdef(indexrelid) = format('CREATE INDEX %I %s', reviewed.candidate_name, reviewed.definition)
            ) THEN
                RAISE EXCEPTION 'prebuilt index % is not the valid, ready reviewed index on bigname_phase.normalized_events; follow ops/address-history-catalogue-indexes/README.md', reviewed.candidate_name;
            END IF;
        END IF;
        IF NOT final_matches AND candidate_oid IS NULL THEN
            IF require_built THEN
                RAISE EXCEPTION 'missing prebuilt index %; run ops/address-history-catalogue-indexes/install.sql before the catalogue migration', reviewed.candidate_name;
            END IF;
            command := format('CREATE INDEX CONCURRENTLY IF NOT EXISTS %I %s', reviewed.candidate_name, reviewed.definition);
            RETURN NEXT;
        END IF;
    END LOOP;
END
$check$;

SELECT command FROM pg_temp.address_history_catalogue_index_builds(false)
\gexec

-- A rerun only builds missing candidates; an interrupted invalid build is refused.
SELECT command FROM pg_temp.address_history_catalogue_index_builds(true);
ANALYZE bigname_phase.normalized_events;

SELECT indexrelid::regclass AS index_name, indisvalid, indisready,
       pg_size_pretty(pg_relation_size(indexrelid)) AS index_size,
       pg_get_indexdef(indexrelid) AS definition
FROM pg_index
WHERE indexrelid IN (
    to_regclass('bigname_phase.normalized_events_name_history_idx'),
    to_regclass('bigname_phase.ahc_name_prebuild_idx'),
    to_regclass('bigname_phase.normalized_events_resource_history_idx'),
    to_regclass('bigname_phase.ahc_resource_prebuild_idx'),
    to_regclass('bigname_phase.normalized_events_project_node_history_idx'),
    to_regclass('bigname_phase.ahc_node_prebuild_idx'),
    to_regclass('bigname_phase.normalized_events_record_id_write_idx'),
    to_regclass('bigname_phase.ahc_record_prebuild_idx'),
    to_regclass('bigname_phase.normalized_events_history_discovery_name_idx'),
    to_regclass('bigname_phase.normalized_events_history_discovery_resource_idx')
)
ORDER BY indexrelid::regclass::text;
