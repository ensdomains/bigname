-- API/storage history read-only participant indexes. No rows, phase markers, catalogue
-- version or content-hash input changes. Populated tables require the exact concurrent
-- prebuild in ops/history-direct-account-indexes/install.sql. No replay is required.
DO $migration$
DECLARE
    item record; found_kind text; found_definition text;
    previous_search_path text := current_setting('search_path');
    previous_quote_all_identifiers text := current_setting('quote_all_identifiers');
    require_built boolean := true;
BEGIN
    IF to_regclass('bigname_phase.normalized_events') IS NULL THEN RETURN; END IF;
    PERFORM set_config('search_path', 'pg_catalog', true);
    PERFORM set_config('quote_all_identifiers', 'off', true);
    FOR item IN SELECT * FROM (VALUES
            ('normalized_events_history_account_owner_idx', $def$CREATE INDEX normalized_events_history_account_owner_idx ON bigname_phase.normalized_events USING btree (lower((after_state #>> '{scope,owner}'::text[])), block_number DESC NULLS LAST, log_index DESC NULLS LAST, normalized_event_id DESC) WHERE ((event_kind = 'AccountPermissionChanged'::text) AND (consumer_visibility = 'activated'::text) AND (canonicality_state = ANY (ARRAY['canonical'::bigname_phase.canonicality_state, 'safe'::bigname_phase.canonicality_state, 'finalized'::bigname_phase.canonicality_state])) AND ((after_state #>> '{scope,kind}'::text[]) = 'account'::text) AND ((after_state ->> 'relation_kind'::text) = 'operator'::text))$def$),
            ('normalized_events_history_account_subject_idx', $def$CREATE INDEX normalized_events_history_account_subject_idx ON bigname_phase.normalized_events USING btree (lower((after_state ->> 'subject'::text)), block_number DESC NULLS LAST, log_index DESC NULLS LAST, normalized_event_id DESC) WHERE ((event_kind = 'AccountPermissionChanged'::text) AND (consumer_visibility = 'activated'::text) AND (canonicality_state = ANY (ARRAY['canonical'::bigname_phase.canonicality_state, 'safe'::bigname_phase.canonicality_state, 'finalized'::bigname_phase.canonicality_state])) AND ((after_state #>> '{scope,kind}'::text[]) = 'account'::text) AND ((after_state ->> 'relation_kind'::text) = 'operator'::text))$def$),
            ('normalized_events_history_reverse_address_idx', $def$CREATE INDEX normalized_events_history_reverse_address_idx ON bigname_phase.normalized_events USING btree (lower((after_state ->> 'address'::text)), block_number DESC NULLS LAST, log_index DESC NULLS LAST, normalized_event_id DESC) WHERE ((event_kind = 'ReverseChanged'::text) AND (consumer_visibility = 'activated'::text) AND (canonicality_state = ANY (ARRAY['canonical'::bigname_phase.canonicality_state, 'safe'::bigname_phase.canonicality_state, 'finalized'::bigname_phase.canonicality_state])))$def$)
    ) expected(index_name, definition) LOOP
        IF to_regclass('bigname_phase.' || item.index_name) IS NULL THEN
            BEGIN
                LOCK TABLE bigname_phase.normalized_events IN SHARE MODE NOWAIT;
            EXCEPTION WHEN lock_not_available THEN
                RAISE EXCEPTION 'normalized_events is busy; run ops/history-direct-account-indexes/install.sql before this schema-migration';
            END;
            IF EXISTS (SELECT 1 FROM bigname_phase.normalized_events) THEN
                RAISE EXCEPTION 'missing prebuilt index % on populated normalized_events; run ops/history-direct-account-indexes/install.sql first', item.index_name;
            END IF;
            EXECUTE item.definition;
        END IF;
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
    PERFORM set_config('search_path', previous_search_path, true);
    PERFORM set_config('quote_all_identifiers', previous_quote_all_identifiers, true);
END
$migration$;
