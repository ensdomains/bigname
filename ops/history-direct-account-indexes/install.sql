-- psql -X -v ON_ERROR_STOP=1; outside any transaction.
SET lock_timeout = '0';
SET statement_timeout = '6h';
-- Session-local validation shared by the concurrent installer and read-only verifier.
CREATE OR REPLACE FUNCTION pg_temp.check_history_direct_account_indexes(require_built boolean)
RETURNS void LANGUAGE plpgsql SET search_path = pg_catalog SET quote_all_identifiers = off
AS $check$
DECLARE item record; found_kind text; found_definition text;
BEGIN
    FOR item IN SELECT * FROM (VALUES
            ('normalized_events_history_account_owner_idx', $def$CREATE INDEX normalized_events_history_account_owner_idx ON bigname_phase.normalized_events USING btree (lower((after_state #>> '{scope,owner}'::text[])), block_number DESC NULLS LAST, log_index DESC NULLS LAST, normalized_event_id DESC) WHERE ((event_kind = 'AccountPermissionChanged'::text) AND (consumer_visibility = 'activated'::text) AND (canonicality_state = ANY (ARRAY['canonical'::bigname_phase.canonicality_state, 'safe'::bigname_phase.canonicality_state, 'finalized'::bigname_phase.canonicality_state])) AND ((after_state #>> '{scope,kind}'::text[]) = 'account'::text) AND ((after_state ->> 'relation_kind'::text) = 'operator'::text))$def$),
            ('normalized_events_history_account_subject_idx', $def$CREATE INDEX normalized_events_history_account_subject_idx ON bigname_phase.normalized_events USING btree (lower((after_state ->> 'subject'::text)), block_number DESC NULLS LAST, log_index DESC NULLS LAST, normalized_event_id DESC) WHERE ((event_kind = 'AccountPermissionChanged'::text) AND (consumer_visibility = 'activated'::text) AND (canonicality_state = ANY (ARRAY['canonical'::bigname_phase.canonicality_state, 'safe'::bigname_phase.canonicality_state, 'finalized'::bigname_phase.canonicality_state])) AND ((after_state #>> '{scope,kind}'::text[]) = 'account'::text) AND ((after_state ->> 'relation_kind'::text) = 'operator'::text))$def$),
            ('normalized_events_history_reverse_address_idx', $def$CREATE INDEX normalized_events_history_reverse_address_idx ON bigname_phase.normalized_events USING btree (lower((after_state ->> 'address'::text)), block_number DESC NULLS LAST, log_index DESC NULLS LAST, normalized_event_id DESC) WHERE ((event_kind = 'ReverseChanged'::text) AND (consumer_visibility = 'activated'::text) AND (canonicality_state = ANY (ARRAY['canonical'::bigname_phase.canonicality_state, 'safe'::bigname_phase.canonicality_state, 'finalized'::bigname_phase.canonicality_state])))$def$)
    ) expected(index_name, definition) LOOP
        SELECT relkind::text INTO found_kind FROM pg_class
        WHERE oid = to_regclass('bigname_phase.' || item.index_name);
        IF found_kind IS NULL AND NOT require_built THEN CONTINUE; END IF;
        IF found_kind IS DISTINCT FROM 'i' THEN
            RAISE EXCEPTION '% is missing or is not an index; see ops/history-direct-account-indexes/README.md', item.index_name;
        END IF;
        IF NOT EXISTS (SELECT 1 FROM pg_index
            WHERE indexrelid = to_regclass('bigname_phase.' || item.index_name)
              AND indrelid = to_regclass('bigname_phase.normalized_events') AND indisvalid AND indisready) THEN
            RAISE EXCEPTION '% is not a valid and ready index on normalized_events; see ops/history-direct-account-indexes/README.md', item.index_name;
        END IF;
        SELECT pg_get_indexdef(to_regclass('bigname_phase.' || item.index_name)) INTO found_definition;
        IF found_definition <> item.definition THEN
            RAISE EXCEPTION '% has an unexpected definition: %, expected %; see ops/history-direct-account-indexes/README.md', item.index_name, found_definition, item.definition;
        END IF;
    END LOOP;
END
$check$;

SELECT pg_temp.check_history_direct_account_indexes(false);

CREATE INDEX CONCURRENTLY IF NOT EXISTS normalized_events_history_account_owner_idx
    ON bigname_phase.normalized_events (
        lower(after_state #>> '{scope,owner}'), block_number DESC NULLS LAST,
        log_index DESC NULLS LAST, normalized_event_id DESC
    )
    WHERE event_kind = 'AccountPermissionChanged' AND consumer_visibility = 'activated'
      AND canonicality_state IN ('canonical', 'safe', 'finalized')
      AND after_state #>> '{scope,kind}' = 'account'
      AND after_state ->> 'relation_kind' = 'operator';

CREATE INDEX CONCURRENTLY IF NOT EXISTS normalized_events_history_account_subject_idx
    ON bigname_phase.normalized_events (
        lower(after_state ->> 'subject'), block_number DESC NULLS LAST,
        log_index DESC NULLS LAST, normalized_event_id DESC
    )
    WHERE event_kind = 'AccountPermissionChanged' AND consumer_visibility = 'activated'
      AND canonicality_state IN ('canonical', 'safe', 'finalized')
      AND after_state #>> '{scope,kind}' = 'account'
      AND after_state ->> 'relation_kind' = 'operator';

CREATE INDEX CONCURRENTLY IF NOT EXISTS normalized_events_history_reverse_address_idx
    ON bigname_phase.normalized_events (
        lower(after_state ->> 'address'), block_number DESC NULLS LAST,
        log_index DESC NULLS LAST, normalized_event_id DESC
    )
    WHERE event_kind = 'ReverseChanged' AND consumer_visibility = 'activated'
      AND canonicality_state IN ('canonical', 'safe', 'finalized');

-- Read-only validation; reports allocated index bytes and exact definitions.

SELECT indexrelid::regclass AS index_name, indisvalid, indisready,
       pg_relation_size(indexrelid) AS index_bytes, pg_get_indexdef(indexrelid) AS definition
FROM pg_index WHERE indexrelid IN (
    to_regclass('bigname_phase.normalized_events_history_account_owner_idx'),
    to_regclass('bigname_phase.normalized_events_history_account_subject_idx'),
    to_regclass('bigname_phase.normalized_events_history_reverse_address_idx')
);
SELECT pg_temp.check_history_direct_account_indexes(true);
