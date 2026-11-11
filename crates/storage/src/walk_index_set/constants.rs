//! The indexes `ops/walk-index-set/drop.sql` drops, in its order, with the statements
//! `ops/walk-index-set/install.sql` builds them with and the definitions it checks, the
//! indexes Interpret keeps, and the advisory indexes. `tests.rs` holds every entry to the scripts and the docs, and the
//! phase-runner tests hold every definition to the fresh baseline.
use super::DroppedIndex;

/// The indexes on `normalized_events` Interpret keeps, as `docs/storage.md` § Walk index set
/// lists them.
pub const KEPT: &[&str] = &[
    "normalized_events_pkey",
    "normalized_events_event_identity_key",
    "normalized_events_interpreter_state_history_idx",
    "normalized_events_resource_history_idx",
    "normalized_events_name_history_idx",
    "normalized_events_chain_block_number_idx",
    "normalized_events_chain_block_number_desc_idx",
    "normalized_events_projection_idx",
    "normalized_events_manifest_idx",
    "normalized_events_v1_direct_node_probe_idx",
    "normalized_events_v1_due_probe_idx",
    "normalized_events_basenames_direct_node_probe_idx",
    "normalized_events_basenames_due_probe_idx",
    "normalized_events_v2_direct_node_probe_idx",
    "normalized_events_v2_key_probe_idx",
    "normalized_events_v2_due_probe_idx",
    "normalized_events_v2_lookahead_probe_idx",
];

/// The indexes on `normalized_events` `drop.sql` drops, in its order.
pub const DROP_LIST: &[DroppedIndex] = &[
    DroppedIndex {
        name: "normalized_events_history_account_owner_idx",
        create: r#"CREATE INDEX CONCURRENTLY IF NOT EXISTS normalized_events_history_account_owner_idx
    ON bigname_phase.normalized_events (
        lower(after_state #>> '{scope,owner}'), block_number DESC NULLS LAST,
        log_index DESC NULLS LAST, normalized_event_id DESC
    )
    WHERE event_kind = 'AccountPermissionChanged' AND consumer_visibility = 'activated'
      AND canonicality_state IN ('canonical', 'safe', 'finalized')
      AND after_state #>> '{scope,kind}' = 'account'
      AND after_state ->> 'relation_kind' = 'operator'"#,
        definition: r#"CREATE INDEX normalized_events_history_account_owner_idx ON bigname_phase.normalized_events USING btree (lower((after_state #>> '{scope,owner}'::text[])), block_number DESC NULLS LAST, log_index DESC NULLS LAST, normalized_event_id DESC) WHERE ((event_kind = 'AccountPermissionChanged'::text) AND (consumer_visibility = 'activated'::text) AND (canonicality_state = ANY (ARRAY['canonical'::bigname_phase.canonicality_state, 'safe'::bigname_phase.canonicality_state, 'finalized'::bigname_phase.canonicality_state])) AND ((after_state #>> '{scope,kind}'::text[]) = 'account'::text) AND ((after_state ->> 'relation_kind'::text) = 'operator'::text))"#,
    },
    DroppedIndex {
        name: "normalized_events_history_account_subject_idx",
        create: r#"CREATE INDEX CONCURRENTLY IF NOT EXISTS normalized_events_history_account_subject_idx
    ON bigname_phase.normalized_events (
        lower(after_state ->> 'subject'), block_number DESC NULLS LAST,
        log_index DESC NULLS LAST, normalized_event_id DESC
    )
    WHERE event_kind = 'AccountPermissionChanged' AND consumer_visibility = 'activated'
      AND canonicality_state IN ('canonical', 'safe', 'finalized')
      AND after_state #>> '{scope,kind}' = 'account'
      AND after_state ->> 'relation_kind' = 'operator'"#,
        definition: r#"CREATE INDEX normalized_events_history_account_subject_idx ON bigname_phase.normalized_events USING btree (lower((after_state ->> 'subject'::text)), block_number DESC NULLS LAST, log_index DESC NULLS LAST, normalized_event_id DESC) WHERE ((event_kind = 'AccountPermissionChanged'::text) AND (consumer_visibility = 'activated'::text) AND (canonicality_state = ANY (ARRAY['canonical'::bigname_phase.canonicality_state, 'safe'::bigname_phase.canonicality_state, 'finalized'::bigname_phase.canonicality_state])) AND ((after_state #>> '{scope,kind}'::text[]) = 'account'::text) AND ((after_state ->> 'relation_kind'::text) = 'operator'::text))"#,
    },
    DroppedIndex {
        name: "normalized_events_history_reverse_address_idx",
        create: r#"CREATE INDEX CONCURRENTLY IF NOT EXISTS normalized_events_history_reverse_address_idx
    ON bigname_phase.normalized_events (
        lower(after_state ->> 'address'), block_number DESC NULLS LAST,
        log_index DESC NULLS LAST, normalized_event_id DESC
    )
    WHERE event_kind = 'ReverseChanged' AND consumer_visibility = 'activated'
      AND canonicality_state IN ('canonical', 'safe', 'finalized')"#,
        definition: r#"CREATE INDEX normalized_events_history_reverse_address_idx ON bigname_phase.normalized_events USING btree (lower((after_state ->> 'address'::text)), block_number DESC NULLS LAST, log_index DESC NULLS LAST, normalized_event_id DESC) WHERE ((event_kind = 'ReverseChanged'::text) AND (consumer_visibility = 'activated'::text) AND (canonicality_state = ANY (ARRAY['canonical'::bigname_phase.canonicality_state, 'safe'::bigname_phase.canonicality_state, 'finalized'::bigname_phase.canonicality_state])))"#,
    },
    DroppedIndex {
        name: "normalized_events_history_registrar_lease_idx",
        create: r#"CREATE INDEX CONCURRENTLY IF NOT EXISTS normalized_events_history_registrar_lease_idx
    ON bigname_phase.normalized_events (resource_id)
    WHERE source_family = 'ens_v1_registrar_l1'
      AND canonicality_state <> 'orphaned'::bigname_phase.canonicality_state"#,
        definition: r#"CREATE INDEX normalized_events_history_registrar_lease_idx ON bigname_phase.normalized_events USING btree (resource_id) WHERE ((source_family = 'ens_v1_registrar_l1'::text) AND (canonicality_state <> 'orphaned'::bigname_phase.canonicality_state))"#,
    },
    DroppedIndex {
        name: "normalized_events_registry_token_idx",
        create: r#"CREATE INDEX CONCURRENTLY IF NOT EXISTS normalized_events_registry_token_idx
ON bigname_phase.normalized_events
    (chain_id, resource_id, block_number DESC, transaction_index DESC, log_index DESC)
WHERE source_family IN ('ens_v2_registry_l1', 'ens_v2_root_l1')
  AND event_kind IN ('TokenResourceLinked', 'TokenRegenerated')
  AND consumer_visibility = 'activated'
  AND canonicality_state IN ('canonical', 'safe', 'finalized')
  AND resource_id IS NOT NULL AND block_number IS NOT NULL
  AND transaction_index IS NOT NULL AND log_index IS NOT NULL"#,
        definition: r#"CREATE INDEX normalized_events_registry_token_idx ON bigname_phase.normalized_events USING btree (chain_id, resource_id, block_number DESC, transaction_index DESC, log_index DESC) WHERE ((source_family = ANY (ARRAY['ens_v2_registry_l1'::text, 'ens_v2_root_l1'::text])) AND (event_kind = ANY (ARRAY['TokenResourceLinked'::text, 'TokenRegenerated'::text])) AND (consumer_visibility = 'activated'::text) AND (canonicality_state = ANY (ARRAY['canonical'::bigname_phase.canonicality_state, 'safe'::bigname_phase.canonicality_state, 'finalized'::bigname_phase.canonicality_state])) AND (resource_id IS NOT NULL) AND (block_number IS NOT NULL) AND (transaction_index IS NOT NULL) AND (log_index IS NOT NULL))"#,
    },
    DroppedIndex {
        name: "normalized_events_v1_subregistry_after_child_scope_idx",
        create: r#"CREATE INDEX CONCURRENTLY IF NOT EXISTS normalized_events_v1_subregistry_after_child_scope_idx
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
  AND btrim(after_state ->> 'child_node') <> ''"#,
        definition: r#"CREATE INDEX normalized_events_v1_subregistry_after_child_scope_idx ON bigname_phase.normalized_events USING btree (chain_id, (((namespace || ':'::text) || lower((after_state ->> 'child_node'::text)))), block_number) WHERE ((event_kind = 'SubregistryChanged'::text) AND (source_family = ANY (ARRAY['ens_v1_registry_l1'::text, 'basenames_base_registry'::text])) AND (consumer_visibility = 'activated'::text) AND (canonicality_state = ANY (ARRAY['canonical'::bigname_phase.canonicality_state, 'safe'::bigname_phase.canonicality_state, 'finalized'::bigname_phase.canonicality_state])) AND ((after_state ->> 'node'::text) IS NOT NULL) AND (btrim((after_state ->> 'node'::text)) <> ''::text) AND ((after_state ->> 'child_node'::text) IS NOT NULL) AND (btrim((after_state ->> 'child_node'::text)) <> ''::text))"#,
    },
    DroppedIndex {
        name: "normalized_events_v2_subregistry_pointer_scope_idx",
        create: r#"CREATE INDEX CONCURRENTLY IF NOT EXISTS normalized_events_v2_subregistry_pointer_scope_idx
    ON bigname_phase.normalized_events USING gin ((ARRAY[
        lower(after_state ->> 'subregistry'),
        lower(before_state ->> 'subregistry')
    ]))
    WHERE event_kind = 'SubregistryChanged'
      AND source_family IN ('ens_v2_root_l1', 'ens_v2_registry_l1')
      AND consumer_visibility = 'activated'
      AND canonicality_state IN ('canonical', 'safe', 'finalized')
      AND logical_name_id IS NOT NULL"#,
        definition: r#"CREATE INDEX normalized_events_v2_subregistry_pointer_scope_idx ON bigname_phase.normalized_events USING gin ((ARRAY[lower((after_state ->> 'subregistry'::text)), lower((before_state ->> 'subregistry'::text))])) WHERE ((event_kind = 'SubregistryChanged'::text) AND (source_family = ANY (ARRAY['ens_v2_root_l1'::text, 'ens_v2_registry_l1'::text])) AND (consumer_visibility = 'activated'::text) AND (canonicality_state = ANY (ARRAY['canonical'::bigname_phase.canonicality_state, 'safe'::bigname_phase.canonicality_state, 'finalized'::bigname_phase.canonicality_state])) AND (logical_name_id IS NOT NULL))"#,
    },
    DroppedIndex {
        name: "normalized_events_block_idx",
        create: r#"CREATE INDEX CONCURRENTLY IF NOT EXISTS normalized_events_block_idx
    ON bigname_phase.normalized_events (
        chain_id,
        block_hash,
        transaction_index,
        log_index,
        normalized_event_id
    )
    WHERE block_hash IS NOT NULL"#,
        definition: r#"CREATE INDEX normalized_events_block_idx ON bigname_phase.normalized_events USING btree (chain_id, block_hash, transaction_index, log_index, normalized_event_id) WHERE (block_hash IS NOT NULL)"#,
    },
    DroppedIndex {
        name: "normalized_events_emitter_history_idx",
        create: r#"CREATE INDEX CONCURRENTLY IF NOT EXISTS normalized_events_emitter_history_idx
    ON bigname_phase.normalized_events (
        lower(raw_fact_ref ->> 'emitting_address'),
        block_number DESC NULLS LAST,
        log_index DESC NULLS LAST,
        normalized_event_id DESC
    )
    WHERE raw_fact_ref ->> 'emitting_address' IS NOT NULL
      AND canonicality_state IN ('canonical', 'safe', 'finalized')"#,
        definition: r#"CREATE INDEX normalized_events_emitter_history_idx ON bigname_phase.normalized_events USING btree (lower((raw_fact_ref ->> 'emitting_address'::text)), block_number DESC NULLS LAST, log_index DESC NULLS LAST, normalized_event_id DESC) WHERE (((raw_fact_ref ->> 'emitting_address'::text) IS NOT NULL) AND (canonicality_state = ANY (ARRAY['canonical'::bigname_phase.canonicality_state, 'safe'::bigname_phase.canonicality_state, 'finalized'::bigname_phase.canonicality_state])))"#,
    },
    DroppedIndex {
        name: "normalized_events_ens_v1_record_node_resolver_idx",
        create: r#"CREATE INDEX CONCURRENTLY IF NOT EXISTS normalized_events_ens_v1_record_node_resolver_idx
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
      AND canonicality_state IN ('canonical', 'safe', 'finalized')"#,
        definition: r#"CREATE INDEX normalized_events_ens_v1_record_node_resolver_idx ON bigname_phase.normalized_events USING btree (chain_id, lower((after_state ->> 'node'::text)), lower(COALESCE(NULLIF((after_state ->> 'resolver'::text), ''::text), NULLIF((raw_fact_ref ->> 'emitting_address'::text), ''::text))), block_number, transaction_index, log_index, normalized_event_id) WHERE ((logical_name_id IS NULL) AND (source_family = 'ens_v1_resolver_l1'::text) AND (event_kind = ANY (ARRAY['RecordChanged'::text, 'RecordVersionChanged'::text])) AND (consumer_visibility = 'activated'::text) AND (canonicality_state = ANY (ARRAY['canonical'::bigname_phase.canonicality_state, 'safe'::bigname_phase.canonicality_state, 'finalized'::bigname_phase.canonicality_state])))"#,
    },
    DroppedIndex {
        name: "normalized_events_basenames_record_node_resolver_idx",
        create: r#"CREATE INDEX CONCURRENTLY IF NOT EXISTS normalized_events_basenames_record_node_resolver_idx
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
      AND canonicality_state IN ('canonical', 'safe', 'finalized')"#,
        definition: r#"CREATE INDEX normalized_events_basenames_record_node_resolver_idx ON bigname_phase.normalized_events USING btree (chain_id, lower((after_state ->> 'node'::text)), lower(COALESCE(NULLIF((after_state ->> 'resolver'::text), ''::text), NULLIF((raw_fact_ref ->> 'emitting_address'::text), ''::text))), block_number, transaction_index, log_index, normalized_event_id) WHERE ((logical_name_id IS NULL) AND (source_family = 'basenames_base_resolver'::text) AND (event_kind = ANY (ARRAY['RecordChanged'::text, 'RecordVersionChanged'::text])) AND (consumer_visibility = 'activated'::text) AND (canonicality_state = ANY (ARRAY['canonical'::bigname_phase.canonicality_state, 'safe'::bigname_phase.canonicality_state, 'finalized'::bigname_phase.canonicality_state])))"#,
    },
    DroppedIndex {
        name: "normalized_events_history_discovery_name_idx",
        create: r#"CREATE INDEX CONCURRENTLY IF NOT EXISTS normalized_events_history_discovery_name_idx
    ON bigname_phase.normalized_events (chain_id, logical_name_id, block_number)
    WHERE logical_name_id IS NOT NULL AND resource_id IS NOT NULL
      AND canonicality_state NOT IN (
          'canonical'::bigname_phase.canonicality_state,
          'safe'::bigname_phase.canonicality_state,
          'finalized'::bigname_phase.canonicality_state)"#,
        definition: r#"CREATE INDEX normalized_events_history_discovery_name_idx ON bigname_phase.normalized_events USING btree (chain_id, logical_name_id, block_number) WHERE ((logical_name_id IS NOT NULL) AND (resource_id IS NOT NULL) AND (canonicality_state <> ALL (ARRAY['canonical'::bigname_phase.canonicality_state, 'safe'::bigname_phase.canonicality_state, 'finalized'::bigname_phase.canonicality_state])))"#,
    },
    DroppedIndex {
        name: "normalized_events_history_discovery_resource_idx",
        create: r#"CREATE INDEX CONCURRENTLY IF NOT EXISTS normalized_events_history_discovery_resource_idx
    ON bigname_phase.normalized_events (chain_id, resource_id, block_number)
    WHERE logical_name_id IS NOT NULL AND resource_id IS NOT NULL
      AND canonicality_state NOT IN (
          'canonical'::bigname_phase.canonicality_state,
          'safe'::bigname_phase.canonicality_state,
          'finalized'::bigname_phase.canonicality_state)"#,
        definition: r#"CREATE INDEX normalized_events_history_discovery_resource_idx ON bigname_phase.normalized_events USING btree (chain_id, resource_id, block_number) WHERE ((logical_name_id IS NOT NULL) AND (resource_id IS NOT NULL) AND (canonicality_state <> ALL (ARRAY['canonical'::bigname_phase.canonicality_state, 'safe'::bigname_phase.canonicality_state, 'finalized'::bigname_phase.canonicality_state])))"#,
    },
    DroppedIndex {
        name: "normalized_events_record_id_write_idx",
        create: r#"CREATE INDEX CONCURRENTLY IF NOT EXISTS normalized_events_record_id_write_idx
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
      AND canonicality_state IN ('canonical', 'safe', 'finalized')"#,
        definition: r#"CREATE INDEX normalized_events_record_id_write_idx ON bigname_phase.normalized_events USING btree (chain_id, lower((after_state ->> 'resolver'::text)), ((after_state ->> 'resolver_record_id'::text)), block_number DESC NULLS LAST, block_hash DESC NULLS LAST, transaction_index DESC NULLS LAST, log_index DESC NULLS LAST, event_identity DESC) WHERE ((event_kind = 'RecordChanged'::text) AND ((after_state ->> 'storage_model'::text) = 'resolver_record_id'::text) AND (consumer_visibility = 'activated'::text) AND (canonicality_state = ANY (ARRAY['canonical'::bigname_phase.canonicality_state, 'safe'::bigname_phase.canonicality_state, 'finalized'::bigname_phase.canonicality_state])))"#,
    },
    DroppedIndex {
        name: "normalized_events_record_id_link_idx",
        create: r#"CREATE INDEX CONCURRENTLY IF NOT EXISTS normalized_events_record_id_link_idx
    ON bigname_phase.normalized_events (
        chain_id,
        lower(after_state ->> 'resolver'),
        lower(after_state ->> 'node')
    )
    WHERE event_kind = 'ResolverRecordLinked'
      AND after_state ->> 'storage_model' = 'resolver_record_id'
      AND consumer_visibility = 'activated'
      AND canonicality_state IN ('canonical', 'safe', 'finalized')"#,
        definition: r#"CREATE INDEX normalized_events_record_id_link_idx ON bigname_phase.normalized_events USING btree (chain_id, lower((after_state ->> 'resolver'::text)), lower((after_state ->> 'node'::text))) WHERE ((event_kind = 'ResolverRecordLinked'::text) AND ((after_state ->> 'storage_model'::text) = 'resolver_record_id'::text) AND (consumer_visibility = 'activated'::text) AND (canonicality_state = ANY (ARRAY['canonical'::bigname_phase.canonicality_state, 'safe'::bigname_phase.canonicality_state, 'finalized'::bigname_phase.canonicality_state])))"#,
    },
    DroppedIndex {
        name: "normalized_events_resolver_upgrade_history_idx",
        create: r#"CREATE INDEX CONCURRENTLY IF NOT EXISTS normalized_events_resolver_upgrade_history_idx
    ON bigname_phase.normalized_events (
        chain_id,
        lower(after_state ->> 'proxy_address'),
        block_number DESC,
        normalized_event_id DESC
    )
    WHERE event_kind = 'Upgraded'
      AND canonicality_state IN ('canonical', 'safe', 'finalized')"#,
        definition: r#"CREATE INDEX normalized_events_resolver_upgrade_history_idx ON bigname_phase.normalized_events USING btree (chain_id, lower((after_state ->> 'proxy_address'::text)), block_number DESC, normalized_event_id DESC) WHERE ((event_kind = 'Upgraded'::text) AND (canonicality_state = ANY (ARRAY['canonical'::bigname_phase.canonicality_state, 'safe'::bigname_phase.canonicality_state, 'finalized'::bigname_phase.canonicality_state])))"#,
    },
    DroppedIndex {
        name: "normalized_events_registry_origin_idx",
        create: r#"CREATE INDEX CONCURRENTLY IF NOT EXISTS normalized_events_registry_origin_idx
    ON bigname_phase.normalized_events (chain_id, lower(after_state ->> 'proxy_address'),
        block_number, transaction_index, log_index, event_identity COLLATE "C")
    WHERE source_family = 'ens_v2_migration_l1' AND event_kind = 'ContractDiscovered'
      AND canonicality_state IN ('canonical', 'safe', 'finalized')"#,
        definition: r#"CREATE INDEX normalized_events_registry_origin_idx ON bigname_phase.normalized_events USING btree (chain_id, lower((after_state ->> 'proxy_address'::text)), block_number, transaction_index, log_index, event_identity COLLATE "C") WHERE ((source_family = 'ens_v2_migration_l1'::text) AND (event_kind = 'ContractDiscovered'::text) AND (canonicality_state = ANY (ARRAY['canonical'::bigname_phase.canonicality_state, 'safe'::bigname_phase.canonicality_state, 'finalized'::bigname_phase.canonicality_state])))"#,
    },
    DroppedIndex {
        name: "normalized_events_registry_announcement_idx",
        create: r#"CREATE INDEX CONCURRENTLY IF NOT EXISTS normalized_events_registry_announcement_idx
ON bigname_phase.normalized_events
    (chain_id, lower(raw_fact_ref ->> 'emitting_address'), block_number, log_index, normalized_event_id)
WHERE source_family = 'ens_v2_registry_l1' AND event_kind = 'RegistryCreated'
  AND consumer_visibility = 'activated'
  AND canonicality_state IN ('canonical', 'safe', 'finalized')"#,
        definition: r#"CREATE INDEX normalized_events_registry_announcement_idx ON bigname_phase.normalized_events USING btree (chain_id, lower((raw_fact_ref ->> 'emitting_address'::text)), block_number, log_index, normalized_event_id) WHERE ((source_family = 'ens_v2_registry_l1'::text) AND (event_kind = 'RegistryCreated'::text) AND (consumer_visibility = 'activated'::text) AND (canonicality_state = ANY (ARRAY['canonical'::bigname_phase.canonicality_state, 'safe'::bigname_phase.canonicality_state, 'finalized'::bigname_phase.canonicality_state])))"#,
    },
    DroppedIndex {
        name: "normalized_events_wrapper_departure_idx",
        create: r#"CREATE INDEX CONCURRENTLY IF NOT EXISTS normalized_events_wrapper_departure_idx
    ON bigname_phase.normalized_events (chain_id, lower(after_state ->> 'proxy_address'), block_number)
    WHERE source_family = 'ens_v2_registry_l1' AND event_kind = 'Upgraded'
      AND canonicality_state IN ('canonical', 'safe', 'finalized')
      AND lower(after_state ->> 'implementation') IS DISTINCT FROM '0xbe768b63e5fbbfbb0ae97e9064e0002df8001880'"#,
        definition: r#"CREATE INDEX normalized_events_wrapper_departure_idx ON bigname_phase.normalized_events USING btree (chain_id, lower((after_state ->> 'proxy_address'::text)), block_number) WHERE ((source_family = 'ens_v2_registry_l1'::text) AND (event_kind = 'Upgraded'::text) AND (canonicality_state = ANY (ARRAY['canonical'::bigname_phase.canonicality_state, 'safe'::bigname_phase.canonicality_state, 'finalized'::bigname_phase.canonicality_state])) AND (lower((after_state ->> 'implementation'::text)) IS DISTINCT FROM '0xbe768b63e5fbbfbb0ae97e9064e0002df8001880'::text))"#,
    },
    DroppedIndex {
        name: "normalized_events_user_registry_departure_idx",
        create: r#"CREATE INDEX CONCURRENTLY IF NOT EXISTS normalized_events_user_registry_departure_idx
    ON bigname_phase.normalized_events (chain_id, lower(after_state ->> 'proxy_address'), block_number)
    WHERE source_family = 'ens_v2_registry_l1' AND event_kind = 'Upgraded'
      AND canonicality_state IN ('canonical', 'safe', 'finalized')
      AND lower(after_state ->> 'implementation') IS DISTINCT FROM '0x9bd8a88719068d09ecee662f36c0e3856708366a'"#,
        definition: r#"CREATE INDEX normalized_events_user_registry_departure_idx ON bigname_phase.normalized_events USING btree (chain_id, lower((after_state ->> 'proxy_address'::text)), block_number) WHERE ((source_family = 'ens_v2_registry_l1'::text) AND (event_kind = 'Upgraded'::text) AND (canonicality_state = ANY (ARRAY['canonical'::bigname_phase.canonicality_state, 'safe'::bigname_phase.canonicality_state, 'finalized'::bigname_phase.canonicality_state])) AND (lower((after_state ->> 'implementation'::text)) IS DISTINCT FROM '0x9bd8a88719068d09ecee662f36c0e3856708366a'::text))"#,
    },
    DroppedIndex {
        name: "normalized_events_mainnet_wrapper_departure_idx",
        create: r#"CREATE INDEX CONCURRENTLY IF NOT EXISTS normalized_events_mainnet_wrapper_departure_idx
    ON bigname_phase.normalized_events (chain_id, lower(after_state ->> 'proxy_address'), block_number)
    WHERE source_family = 'ens_v2_registry_l1' AND event_kind = 'Upgraded'
      AND canonicality_state IN ('canonical', 'safe', 'finalized')
      AND lower(after_state ->> 'implementation') IS DISTINCT FROM '0xcf3edb3f5501de63b57254d910b6782ee370039b'"#,
        definition: r#"CREATE INDEX normalized_events_mainnet_wrapper_departure_idx ON bigname_phase.normalized_events USING btree (chain_id, lower((after_state ->> 'proxy_address'::text)), block_number) WHERE ((source_family = 'ens_v2_registry_l1'::text) AND (event_kind = 'Upgraded'::text) AND (canonicality_state = ANY (ARRAY['canonical'::bigname_phase.canonicality_state, 'safe'::bigname_phase.canonicality_state, 'finalized'::bigname_phase.canonicality_state])) AND (lower((after_state ->> 'implementation'::text)) IS DISTINCT FROM '0xcf3edb3f5501de63b57254d910b6782ee370039b'::text))"#,
    },
    DroppedIndex {
        name: "normalized_events_mainnet_user_registry_departure_idx",
        create: r#"CREATE INDEX CONCURRENTLY IF NOT EXISTS normalized_events_mainnet_user_registry_departure_idx
    ON bigname_phase.normalized_events (chain_id, lower(after_state ->> 'proxy_address'), block_number)
    WHERE source_family = 'ens_v2_registry_l1' AND event_kind = 'Upgraded'
      AND canonicality_state IN ('canonical', 'safe', 'finalized')
      AND lower(after_state ->> 'implementation') IS DISTINCT FROM '0x75ff9adbcfce2469b097ac68f6ae650eae1351bb'"#,
        definition: r#"CREATE INDEX normalized_events_mainnet_user_registry_departure_idx ON bigname_phase.normalized_events USING btree (chain_id, lower((after_state ->> 'proxy_address'::text)), block_number) WHERE ((source_family = 'ens_v2_registry_l1'::text) AND (event_kind = 'Upgraded'::text) AND (canonicality_state = ANY (ARRAY['canonical'::bigname_phase.canonicality_state, 'safe'::bigname_phase.canonicality_state, 'finalized'::bigname_phase.canonicality_state])) AND (lower((after_state ->> 'implementation'::text)) IS DISTINCT FROM '0x75ff9adbcfce2469b097ac68f6ae650eae1351bb'::text))"#,
    },
    DroppedIndex {
        name: "normalized_events_pointer_after_resolver_history_idx",
        create: r#"CREATE INDEX CONCURRENTLY IF NOT EXISTS normalized_events_pointer_after_resolver_history_idx
    ON bigname_phase.normalized_events (
        chain_id,
        lower(after_state ->> 'resolver'),
        block_number,
        block_hash
    ) INCLUDE (normalized_event_id)
    WHERE event_kind = 'ResolverChanged'
      AND consumer_visibility = 'activated'
      AND canonicality_state IN ('canonical', 'safe', 'finalized')"#,
        definition: r#"CREATE INDEX normalized_events_pointer_after_resolver_history_idx ON bigname_phase.normalized_events USING btree (chain_id, lower((after_state ->> 'resolver'::text)), block_number, block_hash) INCLUDE (normalized_event_id) WHERE ((event_kind = 'ResolverChanged'::text) AND (consumer_visibility = 'activated'::text) AND (canonicality_state = ANY (ARRAY['canonical'::bigname_phase.canonicality_state, 'safe'::bigname_phase.canonicality_state, 'finalized'::bigname_phase.canonicality_state])))"#,
    },
    DroppedIndex {
        name: "normalized_events_pointer_before_resolver_history_idx",
        create: r#"CREATE INDEX CONCURRENTLY IF NOT EXISTS normalized_events_pointer_before_resolver_history_idx
    ON bigname_phase.normalized_events (
        chain_id,
        lower(before_state ->> 'resolver'),
        block_number,
        block_hash
    ) INCLUDE (normalized_event_id)
    WHERE event_kind = 'ResolverChanged'
      AND consumer_visibility = 'activated'
      AND canonicality_state IN ('canonical', 'safe', 'finalized')"#,
        definition: r#"CREATE INDEX normalized_events_pointer_before_resolver_history_idx ON bigname_phase.normalized_events USING btree (chain_id, lower((before_state ->> 'resolver'::text)), block_number, block_hash) INCLUDE (normalized_event_id) WHERE ((event_kind = 'ResolverChanged'::text) AND (consumer_visibility = 'activated'::text) AND (canonicality_state = ANY (ARRAY['canonical'::bigname_phase.canonicality_state, 'safe'::bigname_phase.canonicality_state, 'finalized'::bigname_phase.canonicality_state])))"#,
    },
    DroppedIndex {
        name: "normalized_events_project_name_node_idx",
        create: r#"CREATE INDEX CONCURRENTLY IF NOT EXISTS normalized_events_project_name_node_idx
    ON bigname_phase.normalized_events (chain_id, (namespace || ':' || lower(after_state ->> 'node')), block_number)
    INCLUDE (normalized_event_id)
    WHERE (event_kind IN ('SubregistryChanged', 'AliasChanged')
           OR (event_kind = 'AuthorityTransferred'
               AND source_family IN ('ens_v1_registry_l1', 'basenames_base_registry')))
      AND canonicality_state IN ('canonical', 'safe', 'finalized')
      AND (namespace || ':' || lower(after_state ->> 'node')) IS NOT NULL"#,
        definition: r#"CREATE INDEX normalized_events_project_name_node_idx ON bigname_phase.normalized_events USING btree (chain_id, (((namespace || ':'::text) || lower((after_state ->> 'node'::text)))), block_number) INCLUDE (normalized_event_id) WHERE (((event_kind = ANY (ARRAY['SubregistryChanged'::text, 'AliasChanged'::text])) OR ((event_kind = 'AuthorityTransferred'::text) AND (source_family = ANY (ARRAY['ens_v1_registry_l1'::text, 'basenames_base_registry'::text])))) AND (canonicality_state = ANY (ARRAY['canonical'::bigname_phase.canonicality_state, 'safe'::bigname_phase.canonicality_state, 'finalized'::bigname_phase.canonicality_state])) AND (((namespace || ':'::text) || lower((after_state ->> 'node'::text))) IS NOT NULL))"#,
    },
    DroppedIndex {
        name: "normalized_events_project_name_child_idx",
        create: r#"CREATE INDEX CONCURRENTLY IF NOT EXISTS normalized_events_project_name_child_idx
    ON bigname_phase.normalized_events (chain_id, (namespace || ':' || lower(after_state ->> 'child_node')), block_number)
    INCLUDE (normalized_event_id)
    WHERE (event_kind IN ('SubregistryChanged', 'AliasChanged')
           OR (event_kind = 'AuthorityTransferred'
               AND source_family IN ('ens_v1_registry_l1', 'basenames_base_registry')))
      AND canonicality_state IN ('canonical', 'safe', 'finalized')
      AND (namespace || ':' || lower(after_state ->> 'child_node')) IS NOT NULL"#,
        definition: r#"CREATE INDEX normalized_events_project_name_child_idx ON bigname_phase.normalized_events USING btree (chain_id, (((namespace || ':'::text) || lower((after_state ->> 'child_node'::text)))), block_number) INCLUDE (normalized_event_id) WHERE (((event_kind = ANY (ARRAY['SubregistryChanged'::text, 'AliasChanged'::text])) OR ((event_kind = 'AuthorityTransferred'::text) AND (source_family = ANY (ARRAY['ens_v1_registry_l1'::text, 'basenames_base_registry'::text])))) AND (canonicality_state = ANY (ARRAY['canonical'::bigname_phase.canonicality_state, 'safe'::bigname_phase.canonicality_state, 'finalized'::bigname_phase.canonicality_state])) AND (((namespace || ':'::text) || lower((after_state ->> 'child_node'::text))) IS NOT NULL))"#,
    },
    DroppedIndex {
        name: "normalized_events_v1_transfer_node_idx",
        create: r#"CREATE INDEX CONCURRENTLY IF NOT EXISTS normalized_events_v1_transfer_node_idx
    ON bigname_phase.normalized_events (chain_id, (namespace || ':' || lower(after_state ->> 'node')), block_number)
    INCLUDE (normalized_event_id)
    WHERE event_kind = 'AuthorityTransferred'
      AND source_family IN ('ens_v1_registry_l1', 'basenames_base_registry')
      AND after_state ->> 'source_event' = 'Transfer'
      AND canonicality_state IN ('canonical', 'safe', 'finalized')"#,
        definition: r#"CREATE INDEX normalized_events_v1_transfer_node_idx ON bigname_phase.normalized_events USING btree (chain_id, (((namespace || ':'::text) || lower((after_state ->> 'node'::text)))), block_number) INCLUDE (normalized_event_id) WHERE ((event_kind = 'AuthorityTransferred'::text) AND (source_family = ANY (ARRAY['ens_v1_registry_l1'::text, 'basenames_base_registry'::text])) AND ((after_state ->> 'source_event'::text) = 'Transfer'::text) AND (canonicality_state = ANY (ARRAY['canonical'::bigname_phase.canonicality_state, 'safe'::bigname_phase.canonicality_state, 'finalized'::bigname_phase.canonicality_state])))"#,
    },
    DroppedIndex {
        name: "normalized_events_address_registrant_match_idx",
        create: r#"CREATE INDEX CONCURRENTLY IF NOT EXISTS normalized_events_address_registrant_match_idx
    ON bigname_phase.normalized_events (lower(COALESCE(after_state ->> 'registrant', '')))
    WHERE event_kind = 'RegistrationGranted'
      AND consumer_visibility = 'activated'
      AND canonicality_state IN ('canonical', 'safe', 'finalized')"#,
        definition: r#"CREATE INDEX normalized_events_address_registrant_match_idx ON bigname_phase.normalized_events USING btree (lower(COALESCE((after_state ->> 'registrant'::text), ''::text))) WHERE ((event_kind = 'RegistrationGranted'::text) AND (consumer_visibility = 'activated'::text) AND (canonicality_state = ANY (ARRAY['canonical'::bigname_phase.canonicality_state, 'safe'::bigname_phase.canonicality_state, 'finalized'::bigname_phase.canonicality_state])))"#,
    },
    DroppedIndex {
        name: "normalized_events_address_token_holder_match_idx",
        create: r#"CREATE INDEX CONCURRENTLY IF NOT EXISTS normalized_events_address_token_holder_match_idx
    ON bigname_phase.normalized_events (lower(COALESCE(after_state ->> 'to', '')))
    WHERE event_kind = 'TokenControlTransferred'
      AND consumer_visibility = 'activated'
      AND canonicality_state IN ('canonical', 'safe', 'finalized')"#,
        definition: r#"CREATE INDEX normalized_events_address_token_holder_match_idx ON bigname_phase.normalized_events USING btree (lower(COALESCE((after_state ->> 'to'::text), ''::text))) WHERE ((event_kind = 'TokenControlTransferred'::text) AND (consumer_visibility = 'activated'::text) AND (canonicality_state = ANY (ARRAY['canonical'::bigname_phase.canonicality_state, 'safe'::bigname_phase.canonicality_state, 'finalized'::bigname_phase.canonicality_state])))"#,
    },
    DroppedIndex {
        name: "normalized_events_address_registry_owner_match_idx",
        create: r#"CREATE INDEX CONCURRENTLY IF NOT EXISTS normalized_events_address_registry_owner_match_idx
    ON bigname_phase.normalized_events (lower(COALESCE(after_state ->> 'owner', '')))
    WHERE event_kind = 'AuthorityTransferred'
      AND consumer_visibility = 'activated'
      AND canonicality_state IN ('canonical', 'safe', 'finalized')"#,
        definition: r#"CREATE INDEX normalized_events_address_registry_owner_match_idx ON bigname_phase.normalized_events USING btree (lower(COALESCE((after_state ->> 'owner'::text), ''::text))) WHERE ((event_kind = 'AuthorityTransferred'::text) AND (consumer_visibility = 'activated'::text) AND (canonicality_state = ANY (ARRAY['canonical'::bigname_phase.canonicality_state, 'safe'::bigname_phase.canonicality_state, 'finalized'::bigname_phase.canonicality_state])))"#,
    },
    DroppedIndex {
        name: "normalized_events_address_root_permission_idx",
        create: r#"CREATE INDEX CONCURRENTLY IF NOT EXISTS normalized_events_address_root_permission_idx
    ON bigname_phase.normalized_events (
        lower(after_state ->> 'subject'),
        block_number DESC NULLS LAST,
        log_index DESC NULLS LAST,
        normalized_event_id DESC
    )
    WHERE event_kind = 'RootPermissionChanged'
      AND consumer_visibility = 'activated'
      AND canonicality_state IN ('canonical', 'safe', 'finalized')"#,
        definition: r#"CREATE INDEX normalized_events_address_root_permission_idx ON bigname_phase.normalized_events USING btree (lower((after_state ->> 'subject'::text)), block_number DESC NULLS LAST, log_index DESC NULLS LAST, normalized_event_id DESC) WHERE ((event_kind = 'RootPermissionChanged'::text) AND (consumer_visibility = 'activated'::text) AND (canonicality_state = ANY (ARRAY['canonical'::bigname_phase.canonicality_state, 'safe'::bigname_phase.canonicality_state, 'finalized'::bigname_phase.canonicality_state])))"#,
    },
    DroppedIndex {
        name: "normalized_events_registry_roles_idx",
        create: r#"CREATE INDEX CONCURRENTLY IF NOT EXISTS normalized_events_registry_roles_idx
    ON bigname_phase.normalized_events (
        chain_id,
        lower(raw_fact_ref ->> 'emitting_address'),
        resource_id,
        lower(after_state ->> 'subject'),
        block_number DESC,
        transaction_index DESC NULLS LAST,
        log_index DESC NULLS LAST,
        normalized_event_id DESC
    )
    WHERE source_family IN ('ens_v2_root_l1', 'ens_v2_registry_l1')
      AND event_kind IN ('PermissionChanged', 'RootPermissionChanged')
      AND after_state ->> 'source_event' = 'EACRolesChanged'
      AND resource_id IS NOT NULL
      AND after_state ->> 'subject' IS NOT NULL
      AND consumer_visibility = 'activated'
      AND canonicality_state IN ('canonical', 'safe', 'finalized')"#,
        definition: r#"CREATE INDEX normalized_events_registry_roles_idx ON bigname_phase.normalized_events USING btree (chain_id, lower((raw_fact_ref ->> 'emitting_address'::text)), resource_id, lower((after_state ->> 'subject'::text)), block_number DESC, transaction_index DESC NULLS LAST, log_index DESC NULLS LAST, normalized_event_id DESC) WHERE ((source_family = ANY (ARRAY['ens_v2_root_l1'::text, 'ens_v2_registry_l1'::text])) AND (event_kind = ANY (ARRAY['PermissionChanged'::text, 'RootPermissionChanged'::text])) AND ((after_state ->> 'source_event'::text) = 'EACRolesChanged'::text) AND (resource_id IS NOT NULL) AND ((after_state ->> 'subject'::text) IS NOT NULL) AND (consumer_visibility = 'activated'::text) AND (canonicality_state = ANY (ARRAY['canonical'::bigname_phase.canonicality_state, 'safe'::bigname_phase.canonicality_state, 'finalized'::bigname_phase.canonicality_state])))"#,
    },
    DroppedIndex {
        name: "normalized_events_project_node_history_idx",
        create: r#"CREATE INDEX CONCURRENTLY IF NOT EXISTS normalized_events_project_node_history_idx
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
               AND source_family IN ('ens_v1_registry_l1', 'ens_v1_registrar_l1', 'ens_v1_wrapper_l1')))"#,
        definition: r#"CREATE INDEX normalized_events_project_node_history_idx ON bigname_phase.normalized_events USING btree (chain_id, lower((after_state ->> 'node'::text)), block_number DESC NULLS LAST, block_hash DESC NULLS LAST, transaction_index DESC NULLS LAST, log_index DESC NULLS LAST, event_identity DESC) WHERE ((logical_name_id IS NULL) AND (consumer_visibility = 'activated'::text) AND (canonicality_state = ANY (ARRAY['canonical'::bigname_phase.canonicality_state, 'safe'::bigname_phase.canonicality_state, 'finalized'::bigname_phase.canonicality_state])) AND ((after_state ->> 'node'::text) IS NOT NULL) AND (((event_kind = ANY (ARRAY['RecordChanged'::text, 'RecordVersionChanged'::text])) AND (source_family = ANY (ARRAY['ens_v1_resolver_l1'::text, 'ens_v2_resolver_l1'::text, 'basenames_base_resolver'::text]))) OR ((event_kind = 'ResolverChanged'::text) AND (source_family = ANY (ARRAY['ens_v1_registry_l1'::text, 'ens_v1_registrar_l1'::text, 'ens_v1_wrapper_l1'::text])))))"#,
    },
    DroppedIndex {
        name: "normalized_events_project_v1_pointer_addressed_node_idx",
        create: r#"CREATE INDEX CONCURRENTLY IF NOT EXISTS normalized_events_project_v1_pointer_addressed_node_idx
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
      AND canonicality_state IN ('canonical', 'safe', 'finalized')"#,
        definition: r#"CREATE INDEX normalized_events_project_v1_pointer_addressed_node_idx ON bigname_phase.normalized_events USING btree (chain_id, namespace, lower(COALESCE((after_state ->> 'child_node'::text), (after_state ->> 'namehash'::text), (after_state ->> 'node'::text))), block_number) WHERE ((event_kind = 'ResolverChanged'::text) AND (source_family = ANY (ARRAY['ens_v1_registry_l1'::text, 'ens_v1_registrar_l1'::text, 'ens_v1_wrapper_l1'::text])) AND (COALESCE((after_state ->> 'child_node'::text), (after_state ->> 'namehash'::text), (after_state ->> 'node'::text)) IS NOT NULL) AND (consumer_visibility = 'activated'::text) AND (canonicality_state = ANY (ARRAY['canonical'::bigname_phase.canonicality_state, 'safe'::bigname_phase.canonicality_state, 'finalized'::bigname_phase.canonicality_state])))"#,
    },
];

/// The advisory indexes on `normalized_events`: API read paths that are outside the walk index
/// set. `drop.sql` leaves them, so Interpret keeps them current through a walk. An operator
/// builds them with their own install script before the schema-migration that adopts them, the
/// phase runner never builds them, and the API starts and serves without them, with a WARN
/// naming each one that is not in place. Each holds the statement and the definition of
/// `ops/record-history-indexes/install.sql`.
pub const ADVISORY: &[DroppedIndex] = &[
    DroppedIndex {
        name: "normalized_events_record_key_history_idx",
        create: r#"CREATE INDEX CONCURRENTLY IF NOT EXISTS normalized_events_record_key_history_idx
    ON bigname_phase.normalized_events (
        chain_id,
        left(after_state ->> 'record_key', 256),
        block_number DESC NULLS LAST
    )
    WHERE event_kind = 'RecordChanged'
      AND consumer_visibility = 'activated'
      AND canonicality_state IN ('canonical', 'safe', 'finalized')"#,
        definition: r#"CREATE INDEX normalized_events_record_key_history_idx ON bigname_phase.normalized_events USING btree (chain_id, "left"((after_state ->> 'record_key'::text), 256), block_number DESC NULLS LAST) WHERE ((event_kind = 'RecordChanged'::text) AND (consumer_visibility = 'activated'::text) AND (canonicality_state = ANY (ARRAY['canonical'::bigname_phase.canonicality_state, 'safe'::bigname_phase.canonicality_state, 'finalized'::bigname_phase.canonicality_state])))"#,
    },
    DroppedIndex {
        name: "normalized_events_record_emitter_key_history_idx",
        create: r#"CREATE INDEX CONCURRENTLY IF NOT EXISTS normalized_events_record_emitter_key_history_idx
    ON bigname_phase.normalized_events (
        chain_id,
        lower(raw_fact_ref ->> 'emitting_address'),
        left(after_state ->> 'record_key', 256),
        block_number DESC NULLS LAST
    )
    WHERE event_kind = 'RecordChanged'
      AND consumer_visibility = 'activated'
      AND canonicality_state IN ('canonical', 'safe', 'finalized')"#,
        definition: r#"CREATE INDEX normalized_events_record_emitter_key_history_idx ON bigname_phase.normalized_events USING btree (chain_id, lower((raw_fact_ref ->> 'emitting_address'::text)), "left"((after_state ->> 'record_key'::text), 256), block_number DESC NULLS LAST) WHERE ((event_kind = 'RecordChanged'::text) AND (consumer_visibility = 'activated'::text) AND (canonicality_state = ANY (ARRAY['canonical'::bigname_phase.canonicality_state, 'safe'::bigname_phase.canonicality_state, 'finalized'::bigname_phase.canonicality_state])))"#,
    },
];
