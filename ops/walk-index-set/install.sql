-- Run with psql -X -v ON_ERROR_STOP=1, outside any transaction, after drop.sql and before
-- Project runs. It rebuilds every index drop.sql dropped, then analyzes the table.
-- A running Interpret batch can hold the writer transaction a concurrent build waits for.
-- Bound each build, rather than aborting that expected wait after a few seconds.
SET lock_timeout = '0';
SET statement_timeout = '6h';

-- IF NOT EXISTS matches on the name alone, so an interrupted concurrent build leaves an
-- invalid index that the statements below then skip, and an earlier manual build can leave a
-- valid index with other keys or another predicate, or a relation that is not an index,
-- under one of these names. This check runs twice. Before the builds it refuses any name
-- that is already taken by something other than the reviewed, valid and ready index; names
-- that resolve to nothing pass. After the builds it also requires every index to exist.
-- Each expected definition is the fresh baseline's, as pg_get_indexdef prints it with
-- search_path set to pg_catalog and quote_all_identifiers off, which the function sets for
-- its own reads only, as in ops/v1-lookahead-indexes/install.sql. README.md describes the
-- recovery.
CREATE OR REPLACE FUNCTION pg_temp.check_walk_index_set(require_built boolean)
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
            ('normalized_events_history_registrar_lease_idx',
             $def$CREATE INDEX normalized_events_history_registrar_lease_idx ON bigname_phase.normalized_events USING btree (resource_id) WHERE ((source_family = 'ens_v1_registrar_l1'::text) AND (canonicality_state <> 'orphaned'::bigname_phase.canonicality_state))$def$),
            ('normalized_events_registry_token_idx',
             $def$CREATE INDEX normalized_events_registry_token_idx ON bigname_phase.normalized_events USING btree (chain_id, resource_id, block_number DESC, transaction_index DESC, log_index DESC) WHERE ((source_family = ANY (ARRAY['ens_v2_registry_l1'::text, 'ens_v2_root_l1'::text])) AND (event_kind = ANY (ARRAY['TokenResourceLinked'::text, 'TokenRegenerated'::text])) AND (consumer_visibility = 'activated'::text) AND (canonicality_state = ANY (ARRAY['canonical'::bigname_phase.canonicality_state, 'safe'::bigname_phase.canonicality_state, 'finalized'::bigname_phase.canonicality_state])) AND (resource_id IS NOT NULL) AND (block_number IS NOT NULL) AND (transaction_index IS NOT NULL) AND (log_index IS NOT NULL))$def$),
            ('normalized_events_v1_subregistry_after_node_scope_idx',
             $def$CREATE INDEX normalized_events_v1_subregistry_after_node_scope_idx ON bigname_phase.normalized_events USING btree (chain_id, (((namespace || ':'::text) || lower((after_state ->> 'node'::text)))), block_number) WHERE ((event_kind = 'SubregistryChanged'::text) AND (source_family = ANY (ARRAY['ens_v1_registry_l1'::text, 'basenames_base_registry'::text])) AND (consumer_visibility = 'activated'::text) AND (canonicality_state = ANY (ARRAY['canonical'::bigname_phase.canonicality_state, 'safe'::bigname_phase.canonicality_state, 'finalized'::bigname_phase.canonicality_state])) AND ((after_state ->> 'node'::text) IS NOT NULL) AND (btrim((after_state ->> 'node'::text)) <> ''::text) AND ((after_state ->> 'child_node'::text) IS NOT NULL) AND (btrim((after_state ->> 'child_node'::text)) <> ''::text))$def$),
            ('normalized_events_v1_subregistry_after_child_scope_idx',
             $def$CREATE INDEX normalized_events_v1_subregistry_after_child_scope_idx ON bigname_phase.normalized_events USING btree (chain_id, (((namespace || ':'::text) || lower((after_state ->> 'child_node'::text)))), block_number) WHERE ((event_kind = 'SubregistryChanged'::text) AND (source_family = ANY (ARRAY['ens_v1_registry_l1'::text, 'basenames_base_registry'::text])) AND (consumer_visibility = 'activated'::text) AND (canonicality_state = ANY (ARRAY['canonical'::bigname_phase.canonicality_state, 'safe'::bigname_phase.canonicality_state, 'finalized'::bigname_phase.canonicality_state])) AND ((after_state ->> 'node'::text) IS NOT NULL) AND (btrim((after_state ->> 'node'::text)) <> ''::text) AND ((after_state ->> 'child_node'::text) IS NOT NULL) AND (btrim((after_state ->> 'child_node'::text)) <> ''::text))$def$),
            ('normalized_events_v1_subregistry_before_node_scope_idx',
             $def$CREATE INDEX normalized_events_v1_subregistry_before_node_scope_idx ON bigname_phase.normalized_events USING btree (chain_id, (((namespace || ':'::text) || lower((before_state ->> 'node'::text)))), block_number) WHERE ((event_kind = 'SubregistryChanged'::text) AND (source_family = ANY (ARRAY['ens_v1_registry_l1'::text, 'basenames_base_registry'::text])) AND (consumer_visibility = 'activated'::text) AND (canonicality_state = ANY (ARRAY['canonical'::bigname_phase.canonicality_state, 'safe'::bigname_phase.canonicality_state, 'finalized'::bigname_phase.canonicality_state])) AND ((before_state ->> 'node'::text) IS NOT NULL) AND (btrim((before_state ->> 'node'::text)) <> ''::text) AND ((before_state ->> 'child_node'::text) IS NOT NULL) AND (btrim((before_state ->> 'child_node'::text)) <> ''::text))$def$),
            ('normalized_events_v2_subregistry_pointer_scope_idx',
             $def$CREATE INDEX normalized_events_v2_subregistry_pointer_scope_idx ON bigname_phase.normalized_events USING gin ((ARRAY[lower((after_state ->> 'subregistry'::text)), lower((before_state ->> 'subregistry'::text))])) WHERE ((event_kind = 'SubregistryChanged'::text) AND (source_family = ANY (ARRAY['ens_v2_root_l1'::text, 'ens_v2_registry_l1'::text])) AND (consumer_visibility = 'activated'::text) AND (canonicality_state = ANY (ARRAY['canonical'::bigname_phase.canonicality_state, 'safe'::bigname_phase.canonicality_state, 'finalized'::bigname_phase.canonicality_state])) AND (logical_name_id IS NOT NULL))$def$),
            ('normalized_events_v1_subregistry_before_child_scope_idx',
             $def$CREATE INDEX normalized_events_v1_subregistry_before_child_scope_idx ON bigname_phase.normalized_events USING btree (chain_id, (((namespace || ':'::text) || lower((before_state ->> 'child_node'::text)))), block_number) WHERE ((event_kind = 'SubregistryChanged'::text) AND (source_family = ANY (ARRAY['ens_v1_registry_l1'::text, 'basenames_base_registry'::text])) AND (consumer_visibility = 'activated'::text) AND (canonicality_state = ANY (ARRAY['canonical'::bigname_phase.canonicality_state, 'safe'::bigname_phase.canonicality_state, 'finalized'::bigname_phase.canonicality_state])) AND ((before_state ->> 'node'::text) IS NOT NULL) AND (btrim((before_state ->> 'node'::text)) <> ''::text) AND ((before_state ->> 'child_node'::text) IS NOT NULL) AND (btrim((before_state ->> 'child_node'::text)) <> ''::text))$def$),
            ('normalized_events_block_idx',
             $def$CREATE INDEX normalized_events_block_idx ON bigname_phase.normalized_events USING btree (chain_id, block_hash, transaction_index, log_index, normalized_event_id) WHERE (block_hash IS NOT NULL)$def$),
            ('normalized_events_emitter_history_idx',
             $def$CREATE INDEX normalized_events_emitter_history_idx ON bigname_phase.normalized_events USING btree (lower((raw_fact_ref ->> 'emitting_address'::text)), block_number DESC NULLS LAST, log_index DESC NULLS LAST, normalized_event_id DESC) WHERE (((raw_fact_ref ->> 'emitting_address'::text) IS NOT NULL) AND (canonicality_state = ANY (ARRAY['canonical'::bigname_phase.canonicality_state, 'safe'::bigname_phase.canonicality_state, 'finalized'::bigname_phase.canonicality_state])))$def$),
            ('normalized_events_v2_expiry_scope_idx',
             $def$CREATE INDEX normalized_events_v2_expiry_scope_idx ON bigname_phase.normalized_events USING btree (chain_id, (((after_state ->> 'expiry'::text))::numeric), block_number, logical_name_id) WHERE ((logical_name_id IS NOT NULL) AND (source_family = ANY (ARRAY['ens_v2_root_l1'::text, 'ens_v2_registry_l1'::text])) AND (event_kind = ANY (ARRAY['RegistrationGranted'::text, 'RegistrationReserved'::text, 'RegistrationRenewed'::text, 'RegistrationReleased'::text, 'ExpiryChanged'::text])) AND (canonicality_state = ANY (ARRAY['canonical'::bigname_phase.canonicality_state, 'safe'::bigname_phase.canonicality_state, 'finalized'::bigname_phase.canonicality_state])) AND (jsonb_typeof((after_state -> 'expiry'::text)) = 'number'::text))$def$),
            ('normalized_events_ens_v1_record_node_resolver_idx',
             $def$CREATE INDEX normalized_events_ens_v1_record_node_resolver_idx ON bigname_phase.normalized_events USING btree (chain_id, lower((after_state ->> 'node'::text)), lower(COALESCE(NULLIF((after_state ->> 'resolver'::text), ''::text), NULLIF((raw_fact_ref ->> 'emitting_address'::text), ''::text))), block_number, transaction_index, log_index, normalized_event_id) WHERE ((logical_name_id IS NULL) AND (source_family = 'ens_v1_resolver_l1'::text) AND (event_kind = ANY (ARRAY['RecordChanged'::text, 'RecordVersionChanged'::text])) AND (consumer_visibility = 'activated'::text) AND (canonicality_state = ANY (ARRAY['canonical'::bigname_phase.canonicality_state, 'safe'::bigname_phase.canonicality_state, 'finalized'::bigname_phase.canonicality_state])))$def$),
            ('normalized_events_basenames_record_node_resolver_idx',
             $def$CREATE INDEX normalized_events_basenames_record_node_resolver_idx ON bigname_phase.normalized_events USING btree (chain_id, lower((after_state ->> 'node'::text)), lower(COALESCE(NULLIF((after_state ->> 'resolver'::text), ''::text), NULLIF((raw_fact_ref ->> 'emitting_address'::text), ''::text))), block_number, transaction_index, log_index, normalized_event_id) WHERE ((logical_name_id IS NULL) AND (source_family = 'basenames_base_resolver'::text) AND (event_kind = ANY (ARRAY['RecordChanged'::text, 'RecordVersionChanged'::text])) AND (consumer_visibility = 'activated'::text) AND (canonicality_state = ANY (ARRAY['canonical'::bigname_phase.canonicality_state, 'safe'::bigname_phase.canonicality_state, 'finalized'::bigname_phase.canonicality_state])))$def$),
            ('normalized_events_record_id_write_idx',
             $def$CREATE INDEX normalized_events_record_id_write_idx ON bigname_phase.normalized_events USING btree (chain_id, lower((after_state ->> 'resolver'::text)), ((after_state ->> 'resolver_record_id'::text)), block_number DESC NULLS LAST, block_hash DESC NULLS LAST, transaction_index DESC NULLS LAST, log_index DESC NULLS LAST, event_identity DESC) WHERE ((event_kind = 'RecordChanged'::text) AND ((after_state ->> 'storage_model'::text) = 'resolver_record_id'::text) AND (consumer_visibility = 'activated'::text) AND (canonicality_state = ANY (ARRAY['canonical'::bigname_phase.canonicality_state, 'safe'::bigname_phase.canonicality_state, 'finalized'::bigname_phase.canonicality_state])))$def$),
            ('normalized_events_record_id_link_idx',
             $def$CREATE INDEX normalized_events_record_id_link_idx ON bigname_phase.normalized_events USING btree (chain_id, lower((after_state ->> 'resolver'::text)), lower((after_state ->> 'node'::text))) WHERE ((event_kind = 'ResolverRecordLinked'::text) AND ((after_state ->> 'storage_model'::text) = 'resolver_record_id'::text) AND (consumer_visibility = 'activated'::text) AND (canonicality_state = ANY (ARRAY['canonical'::bigname_phase.canonicality_state, 'safe'::bigname_phase.canonicality_state, 'finalized'::bigname_phase.canonicality_state])))$def$),
            ('normalized_events_resolver_alias_history_idx',
             $def$CREATE INDEX normalized_events_resolver_alias_history_idx ON bigname_phase.normalized_events USING btree (chain_id, lower(COALESCE((after_state ->> 'resolver'::text), (before_state ->> 'resolver'::text), (raw_fact_ref ->> 'emitting_address'::text))), block_number DESC, normalized_event_id DESC) WHERE ((event_kind = 'AliasChanged'::text) AND (canonicality_state = ANY (ARRAY['canonical'::bigname_phase.canonicality_state, 'safe'::bigname_phase.canonicality_state, 'finalized'::bigname_phase.canonicality_state])))$def$),
            ('normalized_events_registry_origin_idx',
             $def$CREATE INDEX normalized_events_registry_origin_idx ON bigname_phase.normalized_events USING btree (chain_id, lower((after_state ->> 'proxy_address'::text)), block_number, transaction_index, log_index, event_identity COLLATE "C") WHERE ((source_family = 'ens_v2_migration_l1'::text) AND (event_kind = 'ContractDiscovered'::text) AND (canonicality_state = ANY (ARRAY['canonical'::bigname_phase.canonicality_state, 'safe'::bigname_phase.canonicality_state, 'finalized'::bigname_phase.canonicality_state])))$def$),
            ('normalized_events_registry_announcement_idx',
             $def$CREATE INDEX normalized_events_registry_announcement_idx ON bigname_phase.normalized_events USING btree (chain_id, lower((raw_fact_ref ->> 'emitting_address'::text)), block_number, log_index, normalized_event_id) WHERE ((source_family = 'ens_v2_registry_l1'::text) AND (event_kind = 'RegistryCreated'::text) AND (consumer_visibility = 'activated'::text) AND (canonicality_state = ANY (ARRAY['canonical'::bigname_phase.canonicality_state, 'safe'::bigname_phase.canonicality_state, 'finalized'::bigname_phase.canonicality_state])))$def$),
            ('normalized_events_wrapper_departure_idx',
             $def$CREATE INDEX normalized_events_wrapper_departure_idx ON bigname_phase.normalized_events USING btree (chain_id, lower((after_state ->> 'proxy_address'::text)), block_number) WHERE ((source_family = 'ens_v2_registry_l1'::text) AND (event_kind = 'Upgraded'::text) AND (canonicality_state = ANY (ARRAY['canonical'::bigname_phase.canonicality_state, 'safe'::bigname_phase.canonicality_state, 'finalized'::bigname_phase.canonicality_state])) AND (lower((after_state ->> 'implementation'::text)) IS DISTINCT FROM '0xbe768b63e5fbbfbb0ae97e9064e0002df8001880'::text))$def$),
            ('normalized_events_user_registry_departure_idx',
             $def$CREATE INDEX normalized_events_user_registry_departure_idx ON bigname_phase.normalized_events USING btree (chain_id, lower((after_state ->> 'proxy_address'::text)), block_number) WHERE ((source_family = 'ens_v2_registry_l1'::text) AND (event_kind = 'Upgraded'::text) AND (canonicality_state = ANY (ARRAY['canonical'::bigname_phase.canonicality_state, 'safe'::bigname_phase.canonicality_state, 'finalized'::bigname_phase.canonicality_state])) AND (lower((after_state ->> 'implementation'::text)) IS DISTINCT FROM '0x9bd8a88719068d09ecee662f36c0e3856708366a'::text))$def$),
            ('normalized_events_resolver_upgrade_history_idx',
             $def$CREATE INDEX normalized_events_resolver_upgrade_history_idx ON bigname_phase.normalized_events USING btree (chain_id, lower((after_state ->> 'proxy_address'::text)), block_number DESC, normalized_event_id DESC) WHERE ((event_kind = 'Upgraded'::text) AND (canonicality_state = ANY (ARRAY['canonical'::bigname_phase.canonicality_state, 'safe'::bigname_phase.canonicality_state, 'finalized'::bigname_phase.canonicality_state])))$def$),
            ('normalized_events_pointer_after_resolver_history_idx',
             $def$CREATE INDEX normalized_events_pointer_after_resolver_history_idx ON bigname_phase.normalized_events USING btree (chain_id, lower((after_state ->> 'resolver'::text)), block_number, block_hash) INCLUDE (normalized_event_id) WHERE ((event_kind = 'ResolverChanged'::text) AND (consumer_visibility = 'activated'::text) AND (canonicality_state = ANY (ARRAY['canonical'::bigname_phase.canonicality_state, 'safe'::bigname_phase.canonicality_state, 'finalized'::bigname_phase.canonicality_state])))$def$),
            ('normalized_events_pointer_before_resolver_history_idx',
             $def$CREATE INDEX normalized_events_pointer_before_resolver_history_idx ON bigname_phase.normalized_events USING btree (chain_id, lower((before_state ->> 'resolver'::text)), block_number, block_hash) INCLUDE (normalized_event_id) WHERE ((event_kind = 'ResolverChanged'::text) AND (consumer_visibility = 'activated'::text) AND (canonicality_state = ANY (ARRAY['canonical'::bigname_phase.canonicality_state, 'safe'::bigname_phase.canonicality_state, 'finalized'::bigname_phase.canonicality_state])))$def$),
            ('normalized_events_permission_after_resolver_history_idx',
             $def$CREATE INDEX normalized_events_permission_after_resolver_history_idx ON bigname_phase.normalized_events USING btree (chain_id, lower((after_state #>> '{scope,resolver_address}'::text[])), block_number, block_hash) INCLUDE (resource_id) WHERE ((event_kind = 'PermissionChanged'::text) AND (consumer_visibility = 'activated'::text) AND (canonicality_state = ANY (ARRAY['canonical'::bigname_phase.canonicality_state, 'safe'::bigname_phase.canonicality_state, 'finalized'::bigname_phase.canonicality_state])) AND ((after_state #>> '{scope,kind}'::text[]) = 'resolver'::text) AND (resource_id IS NOT NULL))$def$),
            ('normalized_events_permission_before_resolver_history_idx',
             $def$CREATE INDEX normalized_events_permission_before_resolver_history_idx ON bigname_phase.normalized_events USING btree (chain_id, lower((before_state #>> '{scope,resolver_address}'::text[])), block_number, block_hash) INCLUDE (resource_id) WHERE ((event_kind = 'PermissionChanged'::text) AND (consumer_visibility = 'activated'::text) AND (canonicality_state = ANY (ARRAY['canonical'::bigname_phase.canonicality_state, 'safe'::bigname_phase.canonicality_state, 'finalized'::bigname_phase.canonicality_state])) AND ((before_state #>> '{scope,kind}'::text[]) = 'resolver'::text) AND (resource_id IS NOT NULL))$def$),
            ('normalized_events_subregistry_registration_history_idx',
             $def$CREATE INDEX normalized_events_subregistry_registration_history_idx ON bigname_phase.normalized_events USING btree (chain_id, ((after_state ->> 'registry_contract_instance_id'::text)), block_number DESC, normalized_event_id DESC, logical_name_id) WHERE ((event_kind = ANY (ARRAY['RegistrationGranted'::text, 'RegistrationReserved'::text, 'RegistrationRenewed'::text, 'RegistrationReleased'::text])) AND (source_family = ANY (ARRAY['ens_v2_root_l1'::text, 'ens_v2_registry_l1'::text])) AND (canonicality_state = ANY (ARRAY['canonical'::bigname_phase.canonicality_state, 'safe'::bigname_phase.canonicality_state, 'finalized'::bigname_phase.canonicality_state])) AND (logical_name_id IS NOT NULL) AND ((after_state ->> 'registry_contract_instance_id'::text) IS NOT NULL))$def$),
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
             $def$CREATE INDEX normalized_events_project_primary_before_source_idx ON bigname_phase.normalized_events USING btree (chain_id, lower(((before_state -> 'primary_claim_source'::text) ->> 'address'::text)), (((before_state -> 'primary_claim_source'::text) ->> 'coin_type'::text)), (((before_state -> 'primary_claim_source'::text) ->> 'namespace'::text)), block_number) INCLUDE (normalized_event_id) WHERE ((event_kind = ANY (ARRAY['ReverseChanged'::text, 'RecordChanged'::text])) AND (canonicality_state = ANY (ARRAY['canonical'::bigname_phase.canonicality_state, 'safe'::bigname_phase.canonicality_state, 'finalized'::bigname_phase.canonicality_state])) AND (lower(((before_state -> 'primary_claim_source'::text) ->> 'address'::text)) IS NOT NULL) AND (((before_state -> 'primary_claim_source'::text) ->> 'coin_type'::text) IS NOT NULL) AND (((before_state -> 'primary_claim_source'::text) ->> 'namespace'::text) IS NOT NULL))$def$),
            ('normalized_events_address_registrant_match_idx',
             $def$CREATE INDEX normalized_events_address_registrant_match_idx ON bigname_phase.normalized_events USING btree (lower(COALESCE((after_state ->> 'registrant'::text), ''::text))) WHERE ((event_kind = 'RegistrationGranted'::text) AND (consumer_visibility = 'activated'::text) AND (canonicality_state = ANY (ARRAY['canonical'::bigname_phase.canonicality_state, 'safe'::bigname_phase.canonicality_state, 'finalized'::bigname_phase.canonicality_state])))$def$),
            ('normalized_events_address_token_holder_match_idx',
             $def$CREATE INDEX normalized_events_address_token_holder_match_idx ON bigname_phase.normalized_events USING btree (lower(COALESCE((after_state ->> 'to'::text), ''::text))) WHERE ((event_kind = 'TokenControlTransferred'::text) AND (consumer_visibility = 'activated'::text) AND (canonicality_state = ANY (ARRAY['canonical'::bigname_phase.canonicality_state, 'safe'::bigname_phase.canonicality_state, 'finalized'::bigname_phase.canonicality_state])))$def$),
            ('normalized_events_address_registry_owner_match_idx',
             $def$CREATE INDEX normalized_events_address_registry_owner_match_idx ON bigname_phase.normalized_events USING btree (lower(COALESCE((after_state ->> 'owner'::text), ''::text))) WHERE ((event_kind = 'AuthorityTransferred'::text) AND (consumer_visibility = 'activated'::text) AND (canonicality_state = ANY (ARRAY['canonical'::bigname_phase.canonicality_state, 'safe'::bigname_phase.canonicality_state, 'finalized'::bigname_phase.canonicality_state])))$def$),
            ('normalized_events_address_root_permission_idx',
             $def$CREATE INDEX normalized_events_address_root_permission_idx ON bigname_phase.normalized_events USING btree (lower((after_state ->> 'subject'::text)), block_number DESC NULLS LAST, log_index DESC NULLS LAST, normalized_event_id DESC) WHERE ((event_kind = 'RootPermissionChanged'::text) AND (consumer_visibility = 'activated'::text) AND (canonicality_state = ANY (ARRAY['canonical'::bigname_phase.canonicality_state, 'safe'::bigname_phase.canonicality_state, 'finalized'::bigname_phase.canonicality_state])))$def$),
            ('normalized_events_project_node_history_idx',
             $def$CREATE INDEX normalized_events_project_node_history_idx ON bigname_phase.normalized_events USING btree (chain_id, lower((after_state ->> 'node'::text)), block_number DESC NULLS LAST, block_hash DESC NULLS LAST, transaction_index DESC NULLS LAST, log_index DESC NULLS LAST, event_identity DESC) WHERE ((logical_name_id IS NULL) AND (consumer_visibility = 'activated'::text) AND (canonicality_state = ANY (ARRAY['canonical'::bigname_phase.canonicality_state, 'safe'::bigname_phase.canonicality_state, 'finalized'::bigname_phase.canonicality_state])) AND ((after_state ->> 'node'::text) IS NOT NULL) AND (((event_kind = ANY (ARRAY['RecordChanged'::text, 'RecordVersionChanged'::text])) AND (source_family = ANY (ARRAY['ens_v1_resolver_l1'::text, 'ens_v2_resolver_l1'::text, 'basenames_base_resolver'::text]))) OR ((event_kind = 'ResolverChanged'::text) AND (source_family = ANY (ARRAY['ens_v1_registry_l1'::text, 'ens_v1_registrar_l1'::text, 'ens_v1_wrapper_l1'::text])))))$def$),
            ('normalized_events_project_v1_pointer_node_idx',
             $def$CREATE INDEX normalized_events_project_v1_pointer_node_idx ON bigname_phase.normalized_events USING btree (chain_id, namespace, lower((after_state ->> 'node'::text)), block_number) WHERE ((event_kind = 'ResolverChanged'::text) AND (source_family = ANY (ARRAY['ens_v1_registry_l1'::text, 'ens_v1_registrar_l1'::text, 'ens_v1_wrapper_l1'::text])) AND ((after_state ->> 'node'::text) IS NOT NULL) AND (consumer_visibility = 'activated'::text) AND (canonicality_state = ANY (ARRAY['canonical'::bigname_phase.canonicality_state, 'safe'::bigname_phase.canonicality_state, 'finalized'::bigname_phase.canonicality_state])))$def$),
            ('normalized_events_project_v1_pointer_addressed_node_idx',
             $def$CREATE INDEX normalized_events_project_v1_pointer_addressed_node_idx ON bigname_phase.normalized_events USING btree (chain_id, namespace, lower(COALESCE((after_state ->> 'child_node'::text), (after_state ->> 'namehash'::text), (after_state ->> 'node'::text))), block_number) WHERE ((event_kind = 'ResolverChanged'::text) AND (source_family = ANY (ARRAY['ens_v1_registry_l1'::text, 'ens_v1_registrar_l1'::text, 'ens_v1_wrapper_l1'::text])) AND (COALESCE((after_state ->> 'child_node'::text), (after_state ->> 'namehash'::text), (after_state ->> 'node'::text)) IS NOT NULL) AND (consumer_visibility = 'activated'::text) AND (canonicality_state = ANY (ARRAY['canonical'::bigname_phase.canonicality_state, 'safe'::bigname_phase.canonicality_state, 'finalized'::bigname_phase.canonicality_state])))$def$),
            ('normalized_events_history_discovery_name_idx', $def$CREATE INDEX normalized_events_history_discovery_name_idx ON bigname_phase.normalized_events USING btree (chain_id, logical_name_id, block_number) WHERE ((logical_name_id IS NOT NULL) AND (resource_id IS NOT NULL) AND (canonicality_state <> ALL (ARRAY['canonical'::bigname_phase.canonicality_state, 'safe'::bigname_phase.canonicality_state, 'finalized'::bigname_phase.canonicality_state])))$def$),
            ('normalized_events_history_discovery_resource_idx', $def$CREATE INDEX normalized_events_history_discovery_resource_idx ON bigname_phase.normalized_events USING btree (chain_id, resource_id, block_number) WHERE ((logical_name_id IS NOT NULL) AND (resource_id IS NOT NULL) AND (canonicality_state <> ALL (ARRAY['canonical'::bigname_phase.canonicality_state, 'safe'::bigname_phase.canonicality_state, 'finalized'::bigname_phase.canonicality_state])))$def$)
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
                'bigname_phase.% is a %, not an index, so the index was never built; remove or rename that relation, then follow ops/walk-index-set/README.md before retrying',
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
                '% is missing from bigname_phase.normalized_events or is not valid and ready; follow the recovery steps in ops/walk-index-set/README.md before retrying',
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
                '% exists but does not have the reviewed definition; found "%", expected "%"; follow the recovery steps in ops/walk-index-set/README.md before retrying',
                checked_index, found_definition, expected_definition;
        END IF;
    END LOOP;
END
$check$;

-- Refuse before building anything; see the comment above.
DO $$ BEGIN PERFORM pg_temp.check_walk_index_set(false); END $$;

CREATE INDEX CONCURRENTLY IF NOT EXISTS normalized_events_registry_token_idx
ON bigname_phase.normalized_events
    (chain_id, resource_id, block_number DESC, transaction_index DESC, log_index DESC)
WHERE source_family IN ('ens_v2_registry_l1', 'ens_v2_root_l1')
  AND event_kind IN ('TokenResourceLinked', 'TokenRegenerated')
  AND consumer_visibility = 'activated'
  AND canonicality_state IN ('canonical', 'safe', 'finalized')
  AND resource_id IS NOT NULL AND block_number IS NOT NULL
  AND transaction_index IS NOT NULL AND log_index IS NOT NULL;

CREATE INDEX CONCURRENTLY IF NOT EXISTS normalized_events_v1_subregistry_after_node_scope_idx
    ON bigname_phase.normalized_events (
        chain_id,
        (namespace || ':' || lower(after_state ->> 'node')),
        block_number
    )
WHERE event_kind = 'SubregistryChanged'
  AND source_family IN ('ens_v1_registry_l1', 'basenames_base_registry')
  AND consumer_visibility = 'activated'
  AND canonicality_state IN ('canonical', 'safe', 'finalized')
  AND after_state ->> 'node' IS NOT NULL
  AND btrim(after_state ->> 'node') <> ''
  AND after_state ->> 'child_node' IS NOT NULL
  AND btrim(after_state ->> 'child_node') <> '';

CREATE INDEX CONCURRENTLY IF NOT EXISTS normalized_events_v1_subregistry_after_child_scope_idx
    ON bigname_phase.normalized_events (
        chain_id,
        (namespace || ':' || lower(after_state ->> 'child_node')),
        block_number
    )
WHERE event_kind = 'SubregistryChanged'
  AND source_family IN ('ens_v1_registry_l1', 'basenames_base_registry')
  AND consumer_visibility = 'activated'
  AND canonicality_state IN ('canonical', 'safe', 'finalized')
  AND after_state ->> 'node' IS NOT NULL
  AND btrim(after_state ->> 'node') <> ''
  AND after_state ->> 'child_node' IS NOT NULL
  AND btrim(after_state ->> 'child_node') <> '';

CREATE INDEX CONCURRENTLY IF NOT EXISTS normalized_events_v1_subregistry_before_node_scope_idx
    ON bigname_phase.normalized_events (
        chain_id,
        (namespace || ':' || lower(before_state ->> 'node')),
        block_number
    )
WHERE event_kind = 'SubregistryChanged'
  AND source_family IN ('ens_v1_registry_l1', 'basenames_base_registry')
  AND consumer_visibility = 'activated'
  AND canonicality_state IN ('canonical', 'safe', 'finalized')
  AND before_state ->> 'node' IS NOT NULL
  AND btrim(before_state ->> 'node') <> ''
  AND before_state ->> 'child_node' IS NOT NULL
  AND btrim(before_state ->> 'child_node') <> '';

CREATE INDEX CONCURRENTLY IF NOT EXISTS normalized_events_v2_subregistry_pointer_scope_idx
    ON bigname_phase.normalized_events USING gin ((ARRAY[
        lower(after_state ->> 'subregistry'),
        lower(before_state ->> 'subregistry')
    ]))
    WHERE event_kind = 'SubregistryChanged'
      AND source_family IN ('ens_v2_root_l1', 'ens_v2_registry_l1')
      AND consumer_visibility = 'activated'
      AND canonicality_state IN ('canonical', 'safe', 'finalized')
      AND logical_name_id IS NOT NULL;

CREATE INDEX CONCURRENTLY IF NOT EXISTS normalized_events_v1_subregistry_before_child_scope_idx
    ON bigname_phase.normalized_events (
        chain_id,
        (namespace || ':' || lower(before_state ->> 'child_node')),
        block_number
    )
WHERE event_kind = 'SubregistryChanged'
  AND source_family IN ('ens_v1_registry_l1', 'basenames_base_registry')
  AND consumer_visibility = 'activated'
  AND canonicality_state IN ('canonical', 'safe', 'finalized')
  AND before_state ->> 'node' IS NOT NULL
  AND btrim(before_state ->> 'node') <> ''
  AND before_state ->> 'child_node' IS NOT NULL
  AND btrim(before_state ->> 'child_node') <> '';

CREATE INDEX CONCURRENTLY IF NOT EXISTS normalized_events_block_idx
    ON bigname_phase.normalized_events (
        chain_id,
        block_hash,
        transaction_index,
        log_index,
        normalized_event_id
    )
    WHERE block_hash IS NOT NULL;

CREATE INDEX CONCURRENTLY IF NOT EXISTS normalized_events_emitter_history_idx
    ON bigname_phase.normalized_events (
        lower(raw_fact_ref ->> 'emitting_address'),
        block_number DESC NULLS LAST,
        log_index DESC NULLS LAST,
        normalized_event_id DESC
    )
    WHERE raw_fact_ref ->> 'emitting_address' IS NOT NULL
      AND canonicality_state IN ('canonical', 'safe', 'finalized');

CREATE INDEX CONCURRENTLY IF NOT EXISTS normalized_events_v2_expiry_scope_idx
    ON bigname_phase.normalized_events (
        chain_id,
        ((after_state ->> 'expiry')::numeric),
        block_number,
        logical_name_id
    )
    WHERE logical_name_id IS NOT NULL
      AND source_family IN ('ens_v2_root_l1', 'ens_v2_registry_l1')
      AND event_kind IN (
          'RegistrationGranted', 'RegistrationReserved',
          'RegistrationRenewed', 'RegistrationReleased', 'ExpiryChanged'
      )
      AND canonicality_state IN ('canonical', 'safe', 'finalized')
      AND jsonb_typeof(after_state -> 'expiry') = 'number';

CREATE INDEX CONCURRENTLY IF NOT EXISTS normalized_events_ens_v1_record_node_resolver_idx
    ON bigname_phase.normalized_events (
        chain_id,
        lower(after_state ->> 'node'),
        lower(COALESCE(
            NULLIF(after_state ->> 'resolver', ''),
            NULLIF(raw_fact_ref ->> 'emitting_address', '')
        )),
        block_number,
        transaction_index,
        log_index,
        normalized_event_id
    )
    WHERE logical_name_id IS NULL
      AND source_family = 'ens_v1_resolver_l1'
      AND event_kind IN ('RecordChanged', 'RecordVersionChanged')
      AND consumer_visibility = 'activated'
      AND canonicality_state IN ('canonical', 'safe', 'finalized');

CREATE INDEX CONCURRENTLY IF NOT EXISTS normalized_events_basenames_record_node_resolver_idx
    ON bigname_phase.normalized_events (
        chain_id,
        lower(after_state ->> 'node'),
        lower(COALESCE(
            NULLIF(after_state ->> 'resolver', ''),
            NULLIF(raw_fact_ref ->> 'emitting_address', '')
        )),
        block_number,
        transaction_index,
        log_index,
        normalized_event_id
    )
    WHERE logical_name_id IS NULL
      AND source_family = 'basenames_base_resolver'
      AND event_kind IN ('RecordChanged', 'RecordVersionChanged')
      AND consumer_visibility = 'activated'
      AND canonicality_state IN ('canonical', 'safe', 'finalized');

CREATE INDEX CONCURRENTLY IF NOT EXISTS normalized_events_record_id_write_idx
    ON bigname_phase.normalized_events (
        chain_id,
        lower(after_state ->> 'resolver'),
        (after_state ->> 'resolver_record_id'),
        block_number DESC NULLS LAST, block_hash DESC NULLS LAST,
        transaction_index DESC NULLS LAST, log_index DESC NULLS LAST, event_identity DESC
    )
    WHERE event_kind = 'RecordChanged'
      AND after_state ->> 'storage_model' = 'resolver_record_id'
      AND consumer_visibility = 'activated'
      AND canonicality_state IN ('canonical', 'safe', 'finalized');

CREATE INDEX CONCURRENTLY IF NOT EXISTS normalized_events_record_id_link_idx
    ON bigname_phase.normalized_events (
        chain_id,
        lower(after_state ->> 'resolver'),
        lower(after_state ->> 'node')
    )
    WHERE event_kind = 'ResolverRecordLinked'
      AND after_state ->> 'storage_model' = 'resolver_record_id'
      AND consumer_visibility = 'activated'
      AND canonicality_state IN ('canonical', 'safe', 'finalized');

CREATE INDEX CONCURRENTLY IF NOT EXISTS normalized_events_resolver_alias_history_idx
    ON bigname_phase.normalized_events (
        chain_id,
        lower(COALESCE(
            after_state ->> 'resolver',
            before_state ->> 'resolver',
            raw_fact_ref ->> 'emitting_address'
        )),
        block_number DESC,
        normalized_event_id DESC
    )
    WHERE event_kind = 'AliasChanged'
      AND canonicality_state IN ('canonical', 'safe', 'finalized');

CREATE INDEX CONCURRENTLY IF NOT EXISTS normalized_events_registry_origin_idx
    ON bigname_phase.normalized_events (chain_id, lower(after_state ->> 'proxy_address'),
        block_number, transaction_index, log_index, event_identity COLLATE "C")
    WHERE source_family = 'ens_v2_migration_l1' AND event_kind = 'ContractDiscovered'
      AND canonicality_state IN ('canonical', 'safe', 'finalized');

CREATE INDEX CONCURRENTLY IF NOT EXISTS normalized_events_registry_announcement_idx
ON bigname_phase.normalized_events
    (chain_id, lower(raw_fact_ref ->> 'emitting_address'), block_number, log_index, normalized_event_id)
WHERE source_family = 'ens_v2_registry_l1' AND event_kind = 'RegistryCreated'
  AND consumer_visibility = 'activated'
  AND canonicality_state IN ('canonical', 'safe', 'finalized');

CREATE INDEX CONCURRENTLY IF NOT EXISTS normalized_events_wrapper_departure_idx
    ON bigname_phase.normalized_events (chain_id, lower(after_state ->> 'proxy_address'), block_number)
    WHERE source_family = 'ens_v2_registry_l1' AND event_kind = 'Upgraded'
      AND canonicality_state IN ('canonical', 'safe', 'finalized')
      AND lower(after_state ->> 'implementation') IS DISTINCT FROM '0xbe768b63e5fbbfbb0ae97e9064e0002df8001880';

CREATE INDEX CONCURRENTLY IF NOT EXISTS normalized_events_user_registry_departure_idx
    ON bigname_phase.normalized_events (chain_id, lower(after_state ->> 'proxy_address'), block_number)
    WHERE source_family = 'ens_v2_registry_l1' AND event_kind = 'Upgraded'
      AND canonicality_state IN ('canonical', 'safe', 'finalized')
      AND lower(after_state ->> 'implementation') IS DISTINCT FROM '0x9bd8a88719068d09ecee662f36c0e3856708366a';

CREATE INDEX CONCURRENTLY IF NOT EXISTS normalized_events_resolver_upgrade_history_idx
    ON bigname_phase.normalized_events (
        chain_id,
        lower(after_state ->> 'proxy_address'),
        block_number DESC,
        normalized_event_id DESC
    )
    WHERE event_kind = 'Upgraded'
      AND canonicality_state IN ('canonical', 'safe', 'finalized');

CREATE INDEX CONCURRENTLY IF NOT EXISTS normalized_events_pointer_after_resolver_history_idx
    ON bigname_phase.normalized_events (
        chain_id,
        lower(after_state ->> 'resolver'),
        block_number,
        block_hash
    ) INCLUDE (normalized_event_id)
    WHERE event_kind = 'ResolverChanged'
      AND consumer_visibility = 'activated'
      AND canonicality_state IN ('canonical', 'safe', 'finalized');

CREATE INDEX CONCURRENTLY IF NOT EXISTS normalized_events_pointer_before_resolver_history_idx
    ON bigname_phase.normalized_events (
        chain_id,
        lower(before_state ->> 'resolver'),
        block_number,
        block_hash
    ) INCLUDE (normalized_event_id)
    WHERE event_kind = 'ResolverChanged'
      AND consumer_visibility = 'activated'
      AND canonicality_state IN ('canonical', 'safe', 'finalized');

CREATE INDEX CONCURRENTLY IF NOT EXISTS normalized_events_permission_after_resolver_history_idx
    ON bigname_phase.normalized_events (
        chain_id,
        lower(after_state #>> '{scope,resolver_address}'),
        block_number,
        block_hash
    ) INCLUDE (resource_id)
    WHERE event_kind = 'PermissionChanged'
      AND consumer_visibility = 'activated'
      AND canonicality_state IN ('canonical', 'safe', 'finalized')
      AND after_state #>> '{scope,kind}' = 'resolver'
      AND resource_id IS NOT NULL;

CREATE INDEX CONCURRENTLY IF NOT EXISTS normalized_events_permission_before_resolver_history_idx
    ON bigname_phase.normalized_events (
        chain_id,
        lower(before_state #>> '{scope,resolver_address}'),
        block_number,
        block_hash
    ) INCLUDE (resource_id)
    WHERE event_kind = 'PermissionChanged'
      AND consumer_visibility = 'activated'
      AND canonicality_state IN ('canonical', 'safe', 'finalized')
      AND before_state #>> '{scope,kind}' = 'resolver'
      AND resource_id IS NOT NULL;

CREATE INDEX CONCURRENTLY IF NOT EXISTS normalized_events_subregistry_registration_history_idx
    ON bigname_phase.normalized_events (
        chain_id,
        (after_state ->> 'registry_contract_instance_id'),
        block_number DESC,
        normalized_event_id DESC,
        logical_name_id
    )
    WHERE event_kind IN (
              'RegistrationGranted', 'RegistrationReserved',
              'RegistrationRenewed', 'RegistrationReleased'
          )
      AND source_family IN ('ens_v2_root_l1', 'ens_v2_registry_l1')
      AND canonicality_state IN ('canonical', 'safe', 'finalized')
      AND logical_name_id IS NOT NULL
      AND after_state ->> 'registry_contract_instance_id' IS NOT NULL;

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

CREATE INDEX CONCURRENTLY IF NOT EXISTS normalized_events_address_registrant_match_idx
    ON bigname_phase.normalized_events (lower(COALESCE(after_state ->> 'registrant', '')))
    WHERE event_kind = 'RegistrationGranted'
      AND consumer_visibility = 'activated'
      AND canonicality_state IN ('canonical', 'safe', 'finalized');

CREATE INDEX CONCURRENTLY IF NOT EXISTS normalized_events_address_token_holder_match_idx
    ON bigname_phase.normalized_events (lower(COALESCE(after_state ->> 'to', '')))
    WHERE event_kind = 'TokenControlTransferred'
      AND consumer_visibility = 'activated'
      AND canonicality_state IN ('canonical', 'safe', 'finalized');

CREATE INDEX CONCURRENTLY IF NOT EXISTS normalized_events_address_registry_owner_match_idx
    ON bigname_phase.normalized_events (lower(COALESCE(after_state ->> 'owner', '')))
    WHERE event_kind = 'AuthorityTransferred'
      AND consumer_visibility = 'activated'
      AND canonicality_state IN ('canonical', 'safe', 'finalized');

CREATE INDEX CONCURRENTLY IF NOT EXISTS normalized_events_address_root_permission_idx
    ON bigname_phase.normalized_events (
        lower(after_state ->> 'subject'),
        block_number DESC NULLS LAST,
        log_index DESC NULLS LAST,
        normalized_event_id DESC
    )
    WHERE event_kind = 'RootPermissionChanged'
      AND consumer_visibility = 'activated'
      AND canonicality_state IN ('canonical', 'safe', 'finalized');

CREATE INDEX CONCURRENTLY IF NOT EXISTS normalized_events_project_node_history_idx
    ON bigname_phase.normalized_events (
        chain_id, lower(after_state ->> 'node'),
        block_number DESC NULLS LAST, block_hash DESC NULLS LAST,
        transaction_index DESC NULLS LAST, log_index DESC NULLS LAST, event_identity DESC
    )
    WHERE logical_name_id IS NULL
      AND consumer_visibility = 'activated'
      AND canonicality_state IN ('canonical', 'safe', 'finalized')
      AND after_state ->> 'node' IS NOT NULL
      AND ((event_kind IN ('RecordChanged', 'RecordVersionChanged')
            AND source_family IN ('ens_v1_resolver_l1', 'ens_v2_resolver_l1', 'basenames_base_resolver'))
           OR (event_kind = 'ResolverChanged'
               AND source_family IN ('ens_v1_registry_l1', 'ens_v1_registrar_l1', 'ens_v1_wrapper_l1')));

CREATE INDEX CONCURRENTLY IF NOT EXISTS normalized_events_project_v1_pointer_node_idx
    ON bigname_phase.normalized_events(chain_id, namespace, lower(after_state ->> 'node'), block_number)
    WHERE event_kind = 'ResolverChanged'
      AND source_family IN ('ens_v1_registry_l1', 'ens_v1_registrar_l1', 'ens_v1_wrapper_l1')
      AND after_state ->> 'node' IS NOT NULL
      AND consumer_visibility = 'activated'
      AND canonicality_state IN ('canonical', 'safe', 'finalized');

CREATE INDEX CONCURRENTLY IF NOT EXISTS normalized_events_project_v1_pointer_addressed_node_idx
    ON bigname_phase.normalized_events (
        chain_id,
        namespace,
        lower(COALESCE(after_state ->> 'child_node', after_state ->> 'namehash', after_state ->> 'node')),
        block_number
    )
    WHERE event_kind = 'ResolverChanged'
      AND source_family IN ('ens_v1_registry_l1', 'ens_v1_registrar_l1', 'ens_v1_wrapper_l1')
      AND COALESCE(after_state ->> 'child_node', after_state ->> 'namehash', after_state ->> 'node') IS NOT NULL
      AND consumer_visibility = 'activated'
      AND canonicality_state IN ('canonical', 'safe', 'finalized');

CREATE INDEX CONCURRENTLY IF NOT EXISTS normalized_events_history_discovery_name_idx
    ON bigname_phase.normalized_events (chain_id, logical_name_id, block_number)
    WHERE logical_name_id IS NOT NULL AND resource_id IS NOT NULL
      AND canonicality_state NOT IN (
          'canonical'::bigname_phase.canonicality_state,
          'safe'::bigname_phase.canonicality_state,
          'finalized'::bigname_phase.canonicality_state);

CREATE INDEX CONCURRENTLY IF NOT EXISTS normalized_events_history_discovery_resource_idx
    ON bigname_phase.normalized_events (chain_id, resource_id, block_number)
    WHERE logical_name_id IS NOT NULL AND resource_id IS NOT NULL
      AND canonicality_state NOT IN (
          'canonical'::bigname_phase.canonicality_state,
          'safe'::bigname_phase.canonicality_state,
          'finalized'::bigname_phase.canonicality_state);

CREATE INDEX CONCURRENTLY IF NOT EXISTS normalized_events_history_registrar_lease_idx
    ON bigname_phase.normalized_events (resource_id)
    WHERE source_family = 'ens_v1_registrar_l1'
      AND canonicality_state <> 'orphaned'::bigname_phase.canonicality_state;

-- Printed first so the receipt shows the flags even when the check below fails.
SELECT indexrelid::regclass AS index_name, indisvalid, indisready,
       pg_size_pretty(pg_relation_size(indexrelid)) AS index_size
FROM pg_index
WHERE indexrelid IN (
    to_regclass('bigname_phase.normalized_events_history_registrar_lease_idx'),
    to_regclass('bigname_phase.normalized_events_v1_subregistry_after_node_scope_idx'),
    to_regclass('bigname_phase.normalized_events_v1_subregistry_after_child_scope_idx'),
    to_regclass('bigname_phase.normalized_events_v1_subregistry_before_node_scope_idx'),
    to_regclass('bigname_phase.normalized_events_v2_subregistry_pointer_scope_idx'),
    to_regclass('bigname_phase.normalized_events_v1_subregistry_before_child_scope_idx'),
    to_regclass('bigname_phase.normalized_events_block_idx'),
    to_regclass('bigname_phase.normalized_events_emitter_history_idx'),
    to_regclass('bigname_phase.normalized_events_v2_expiry_scope_idx'),
    to_regclass('bigname_phase.normalized_events_ens_v1_record_node_resolver_idx'),
    to_regclass('bigname_phase.normalized_events_basenames_record_node_resolver_idx'),
    to_regclass('bigname_phase.normalized_events_history_discovery_name_idx'),
    to_regclass('bigname_phase.normalized_events_history_discovery_resource_idx'),
    to_regclass('bigname_phase.normalized_events_record_id_write_idx'),
    to_regclass('bigname_phase.normalized_events_record_id_link_idx'),
    to_regclass('bigname_phase.normalized_events_resolver_alias_history_idx'),
    to_regclass('bigname_phase.normalized_events_resolver_upgrade_history_idx'),
    to_regclass('bigname_phase.normalized_events_registry_origin_idx'),
    to_regclass('bigname_phase.normalized_events_registry_announcement_idx'),
    to_regclass('bigname_phase.normalized_events_wrapper_departure_idx'),
    to_regclass('bigname_phase.normalized_events_user_registry_departure_idx'),
    to_regclass('bigname_phase.normalized_events_pointer_after_resolver_history_idx'),
    to_regclass('bigname_phase.normalized_events_pointer_before_resolver_history_idx'),
    to_regclass('bigname_phase.normalized_events_permission_after_resolver_history_idx'),
    to_regclass('bigname_phase.normalized_events_permission_before_resolver_history_idx'),
    to_regclass('bigname_phase.normalized_events_subregistry_registration_history_idx'),
    to_regclass('bigname_phase.normalized_events_project_name_node_idx'),
    to_regclass('bigname_phase.normalized_events_project_name_child_idx'),
    to_regclass('bigname_phase.normalized_events_project_name_after_target_idx'),
    to_regclass('bigname_phase.normalized_events_project_name_before_target_idx'),
    to_regclass('bigname_phase.normalized_events_project_primary_after_idx'),
    to_regclass('bigname_phase.normalized_events_project_primary_before_idx'),
    to_regclass('bigname_phase.normalized_events_project_primary_after_source_idx'),
    to_regclass('bigname_phase.normalized_events_project_primary_before_source_idx'),
    to_regclass('bigname_phase.normalized_events_address_registrant_match_idx'),
    to_regclass('bigname_phase.normalized_events_address_token_holder_match_idx'),
    to_regclass('bigname_phase.normalized_events_address_registry_owner_match_idx'),
    to_regclass('bigname_phase.normalized_events_address_root_permission_idx'),
    to_regclass('bigname_phase.normalized_events_project_node_history_idx'),
    to_regclass('bigname_phase.normalized_events_project_v1_pointer_node_idx'),
    to_regclass('bigname_phase.normalized_events_project_v1_pointer_addressed_node_idx')
) ORDER BY index_name;

-- Every index must now exist, belong to bigname_phase.normalized_events, be valid and
-- ready, and have the reviewed definition.
DO $$ BEGIN PERFORM pg_temp.check_walk_index_set(true); END $$;

-- Expression and partial indexes have no statistics until the table is analyzed.
ANALYZE bigname_phase.normalized_events;
