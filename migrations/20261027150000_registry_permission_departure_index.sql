-- Permission-reader index only: no fact rewrite or serving hash change here.
-- The departure index the registry permission reader probes for every declared registry
-- implementation. The implementation is a key column, so the probe reads either side of the
-- origin's implementation and never visits a same-code upgrade.
-- Dead indexes: normalized_events_wrapper_departure_idx,
-- normalized_events_user_registry_departure_idx, normalized_events_mainnet_wrapper_departure_idx
-- and normalized_events_mainnet_user_registry_departure_idx have no reader from this release on.
-- They stay installed so earlier migrations keep their proofs. Drop them in later shrink work.
-- Populated upgrades require ops/registry-permission-indexes/install.sql first.
-- Validate the whole set before building anything. A prebuilt index is adopted in place.
-- Only an empty target table may build its missing index in this transaction.
DO $migration$
DECLARE
    checked record;
    has_events boolean;
    commands text[] := ARRAY[]::text[];
    command text;
    previous_search_path text;
    previous_quote_all_identifiers text;
BEGIN
    IF to_regclass('bigname_phase.normalized_events') IS NULL THEN RETURN; END IF;
    LOCK TABLE bigname_phase.normalized_events IN SHARE MODE;
    SELECT EXISTS (SELECT 1 FROM bigname_phase.normalized_events) INTO has_events;
    previous_search_path := current_setting('search_path');
    previous_quote_all_identifiers := current_setting('quote_all_identifiers');
    PERFORM set_config('search_path', 'pg_catalog', true);
    PERFORM set_config('quote_all_identifiers', 'off', true);
    FOR checked IN SELECT * FROM (VALUES
            ('normalized_events_registry_departure_idx', 'normalized_events', $def$CREATE INDEX normalized_events_registry_departure_idx ON bigname_phase.normalized_events USING btree (chain_id, lower((after_state ->> 'proxy_address'::text)), lower((after_state ->> 'implementation'::text)), block_number) WHERE ((source_family = 'ens_v2_registry_l1'::text) AND (event_kind = 'Upgraded'::text) AND (canonicality_state = ANY (ARRAY['canonical'::bigname_phase.canonicality_state, 'safe'::bigname_phase.canonicality_state, 'finalized'::bigname_phase.canonicality_state])))$def$)
        ) definitions(index_name, table_name, expected_definition)
    LOOP
        IF to_regclass('bigname_phase.' || checked.index_name) IS NULL THEN
            IF has_events THEN
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
COMMENT ON INDEX bigname_phase.normalized_events_registry_departure_idx IS
    'This index checks for a canonical departure from a supported registry implementation without visiting same-code upgrades.';
    PERFORM set_config('search_path', previous_search_path, true);
    PERFORM set_config('quote_all_identifiers', previous_quote_all_identifiers, true);
END
$migration$;
