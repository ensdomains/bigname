-- Permission-reader indexes only: no fact rewrite or serving hash change here.
-- Populated upgrades require ops/registry-permission-indexes/install.sql first.
-- Validate the whole set before building anything. A prebuilt set is adopted in place;
-- only an empty target table may build its missing indexes in this transaction.
DO $migration$
DECLARE
    checked record;
    has_events boolean;
    has_grants boolean := false;
    commands text[] := ARRAY[]::text[];
    command text;
    previous_search_path text;
    previous_quote_all_identifiers text;
BEGIN
    IF to_regclass('bigname_phase.normalized_events') IS NULL THEN RETURN; END IF;
    LOCK TABLE bigname_phase.normalized_events IN SHARE MODE;
    SELECT EXISTS (SELECT 1 FROM bigname_phase.normalized_events) INTO has_events;
    IF to_regclass('bigname_phase.project_grant') IS NOT NULL THEN
        LOCK TABLE bigname_phase.project_grant IN SHARE MODE;
        SELECT EXISTS (SELECT 1 FROM bigname_phase.project_grant) INTO has_grants;
    END IF;
    previous_search_path := current_setting('search_path');
    previous_quote_all_identifiers := current_setting('quote_all_identifiers');
    PERFORM set_config('search_path', 'pg_catalog', true);
    PERFORM set_config('quote_all_identifiers', 'off', true);
    FOR checked IN SELECT * FROM (VALUES
            ('normalized_events_registry_origin_idx', 'normalized_events', $def$CREATE INDEX normalized_events_registry_origin_idx ON bigname_phase.normalized_events USING btree (chain_id, lower((after_state ->> 'proxy_address'::text)), block_number, transaction_index, log_index, event_identity COLLATE "C") WHERE ((source_family = 'ens_v2_migration_l1'::text) AND (event_kind = 'ContractDiscovered'::text) AND (canonicality_state = ANY (ARRAY['canonical'::bigname_phase.canonicality_state, 'safe'::bigname_phase.canonicality_state, 'finalized'::bigname_phase.canonicality_state])))$def$),
            ('normalized_events_registry_announcement_idx', 'normalized_events', $def$CREATE INDEX normalized_events_registry_announcement_idx ON bigname_phase.normalized_events USING btree (chain_id, lower((raw_fact_ref ->> 'emitting_address'::text)), block_number, log_index, normalized_event_id) WHERE ((source_family = 'ens_v2_registry_l1'::text) AND (event_kind = 'RegistryCreated'::text) AND (consumer_visibility = 'activated'::text) AND (canonicality_state = ANY (ARRAY['canonical'::bigname_phase.canonicality_state, 'safe'::bigname_phase.canonicality_state, 'finalized'::bigname_phase.canonicality_state])))$def$),
            ('normalized_events_wrapper_departure_idx', 'normalized_events', $def$CREATE INDEX normalized_events_wrapper_departure_idx ON bigname_phase.normalized_events USING btree (chain_id, lower((after_state ->> 'proxy_address'::text)), block_number) WHERE ((source_family = 'ens_v2_registry_l1'::text) AND (event_kind = 'Upgraded'::text) AND (canonicality_state = ANY (ARRAY['canonical'::bigname_phase.canonicality_state, 'safe'::bigname_phase.canonicality_state, 'finalized'::bigname_phase.canonicality_state])) AND (lower((after_state ->> 'implementation'::text)) IS DISTINCT FROM '0xbe768b63e5fbbfbb0ae97e9064e0002df8001880'::text))$def$),
            ('normalized_events_user_registry_departure_idx', 'normalized_events', $def$CREATE INDEX normalized_events_user_registry_departure_idx ON bigname_phase.normalized_events USING btree (chain_id, lower((after_state ->> 'proxy_address'::text)), block_number) WHERE ((source_family = 'ens_v2_registry_l1'::text) AND (event_kind = 'Upgraded'::text) AND (canonicality_state = ANY (ARRAY['canonical'::bigname_phase.canonicality_state, 'safe'::bigname_phase.canonicality_state, 'finalized'::bigname_phase.canonicality_state])) AND (lower((after_state ->> 'implementation'::text)) IS DISTINCT FROM '0x9bd8a88719068d09ecee662f36c0e3856708366a'::text))$def$),
            ('project_grant_registry_parent_idx', 'project_grant', $def$CREATE INDEX project_grant_registry_parent_idx ON bigname_phase.project_grant USING btree (chain_id, subject, ((scope_detail ->> 'registry_address'::text))) WHERE (scope = 'root'::text)$def$)
        ) definitions(index_name, table_name, expected_definition)
    LOOP
        IF to_regclass('bigname_phase.' || checked.table_name) IS NULL THEN CONTINUE; END IF;
        IF to_regclass('bigname_phase.' || checked.index_name) IS NULL THEN
            IF (checked.table_name = 'normalized_events' AND has_events)
                OR (checked.table_name = 'project_grant' AND has_grants) THEN
                RAISE EXCEPTION 'missing prebuilt index % on populated %; run ops/registry-permission-indexes/install.sql before this schema-migration', checked.index_name, checked.table_name;
            END IF;
            commands := array_append(commands, checked.expected_definition);
        ELSIF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_index
            WHERE indexrelid = to_regclass('bigname_phase.' || checked.index_name)
              AND indrelid = to_regclass('bigname_phase.' || checked.table_name)
              AND indisvalid AND indisready AND indislive
              AND pg_catalog.pg_get_indexdef(indexrelid) = checked.expected_definition)
        THEN
            RAISE EXCEPTION 'bigname_phase.% is invalid or has an unexpected definition; follow ops/registry-permission-indexes/README.md before retrying', checked.index_name;
        END IF;
    END LOOP;
    FOREACH command IN ARRAY commands LOOP
        EXECUTE command;
    END LOOP;
COMMENT ON INDEX bigname_phase.normalized_events_registry_origin_idx IS
    'This index seeks at most two canonical factory origins for one registry address at the served publication.';
COMMENT ON INDEX bigname_phase.normalized_events_registry_announcement_idx IS
    'This index seeks at most two activated canonical registry announcements for one address at the served publication.';
COMMENT ON INDEX bigname_phase.normalized_events_wrapper_departure_idx IS
    'This sparse index checks for a canonical departure from the supported WrapperRegistry implementation without visiting benign same-code upgrades.';
COMMENT ON INDEX bigname_phase.normalized_events_user_registry_departure_idx IS
    'This sparse index checks for a canonical departure from the supported UserRegistry implementation without visiting benign same-code upgrades.';
    IF to_regclass('bigname_phase.project_grant') IS NOT NULL THEN
        COMMENT ON INDEX bigname_phase.project_grant_registry_parent_idx IS
            'This index finds a virtual parent root grant for one registry without joining all of the parent subject’s registry grants.';
    END IF;
    PERFORM set_config('search_path', previous_search_path, true);
    PERFORM set_config('quote_all_identifiers', previous_quote_all_identifiers, true);
END
$migration$;
