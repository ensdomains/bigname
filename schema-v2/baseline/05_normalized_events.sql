CREATE OR REPLACE FUNCTION migration_correlation_ids_valid(correlation_ids text[])
RETURNS boolean
LANGUAGE sql
IMMUTABLE
STRICT
AS $$
    SELECT array_position(correlation_ids, NULL) IS NULL
       AND correlation_ids = COALESCE(
           ARRAY(
               SELECT DISTINCT value
               FROM unnest(correlation_ids) AS value
               WHERE btrim(value) <> ''
               ORDER BY value
           ),
           ARRAY[]::text[]
       )
$$;

CREATE TABLE IF NOT EXISTS normalized_events (
    normalized_event_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    event_identity text NOT NULL UNIQUE,
    namespace text NOT NULL,
    logical_name_id text,
    resource_id uuid,
    event_kind text NOT NULL,
    source_family text NOT NULL,
    manifest_version bigint NOT NULL,
    source_manifest_id bigint,
    chain_id text NOT NULL,
    block_number bigint,
    block_hash text,
    transaction_hash text,
    transaction_index bigint,
    log_index bigint,
    raw_fact_ref jsonb NOT NULL DEFAULT '{}'::jsonb,
    derivation_kind text NOT NULL,
    canonicality_state canonicality_state NOT NULL DEFAULT 'observed',
    before_state jsonb NOT NULL DEFAULT '{}'::jsonb,
    after_state jsonb NOT NULL DEFAULT '{}'::jsonb,
    migration_correlation_ids text[] NOT NULL DEFAULT ARRAY[]::text[],
    consumer_visibility text NOT NULL DEFAULT 'activated',
    observed_at timestamptz NOT NULL DEFAULT now(),
    FOREIGN KEY (chain_id, logical_name_id)
        REFERENCES name_surfaces (chain_id, logical_name_id),
    FOREIGN KEY (chain_id, resource_id)
        REFERENCES resources (chain_id, resource_id),
    FOREIGN KEY (
        source_manifest_id,
        namespace,
        source_family,
        manifest_version,
        chain_id
    ) REFERENCES manifest_versions (
        manifest_id,
        namespace,
        source_family,
        manifest_version,
        chain_id
    )
        ON DELETE SET NULL (source_manifest_id),
    FOREIGN KEY (chain_id, block_hash, block_number)
        REFERENCES chain_lineage (chain_id, block_hash, block_number),
    CHECK (btrim(event_identity) <> ''),
    CHECK (btrim(namespace) <> ''),
    CONSTRAINT normalized_events_event_kind_check
        CHECK (
            event_kind IN (
                'AliasChanged',
                'AuthorityEpochChanged',
                'AuthorityTransferred',
                'ContractDiscovered',
                'ExpiryChanged',
                'MigrationApplied',
                'ParentChanged',
                'PermissionChanged',
                'PermissionScopeChanged',
                'PreimageObserved',
                'RecordChanged',
                'RecordVersionChanged',
                'RegistrarNameRegistered',
                'RegistrationGranted',
                'RegistrationReleased',
                'RegistrationRenewed',
                'RegistrationReserved',
                'RegistryCreated',
                'ResolverChanged',
                'ReverseChanged',
                'RootPermissionChanged',
                'SourceManifestUpdated',
                'SubregistryChanged',
                'SurfaceBound',
                'SurfaceUnbound',
                'TokenControlTransferred',
                'TokenRegenerated',
                'TokenResourceLinked',
                'Upgraded'
            )
        ),
    CHECK (btrim(source_family) <> ''),
    CHECK (manifest_version > 0),
    CHECK (btrim(chain_id) <> ''),
    CHECK ((block_hash IS NULL) = (block_number IS NULL)),
    CHECK (block_number IS NULL OR block_number >= 0),
    CHECK (
        transaction_hash IS NULL
        OR (
            block_hash IS NOT NULL
            AND btrim(transaction_hash) <> ''
        )
    ),
    CHECK (
        (transaction_index IS NULL AND log_index IS NULL)
        OR (
            transaction_hash IS NOT NULL
            AND transaction_index IS NOT NULL
            AND transaction_index >= 0
            AND log_index IS NOT NULL
            AND log_index >= 0
        )
    ),
    CHECK (jsonb_typeof(raw_fact_ref) = 'object'),
    CONSTRAINT normalized_events_derivation_kind_check
        CHECK (
            derivation_kind IN (
                'ens_v1_reverse_claim',
                'ens_v1_unwrapped_authority',
                'ens_v2_migration',
                'ens_v2_permissions',
                'ens_v2_registrar',
                'ens_v2_registry_resource_surface',
                'ens_v2_resolver',
                'manifest_sync',
                'proxy_upgrade',
                'raw_block_preimage_observation',
                'raw_log_preimage_observation'
            )
        ),
    CHECK (jsonb_typeof(before_state) = 'object'),
    CHECK (jsonb_typeof(after_state) = 'object'),
    CONSTRAINT normalized_events_migration_correlation_ids_check
        CHECK (migration_correlation_ids_valid(migration_correlation_ids)),
    CONSTRAINT normalized_events_consumer_visibility_check
        CHECK (consumer_visibility IN ('candidate', 'activated')),
    CONSTRAINT normalized_events_candidate_correlation_check
        CHECK (
        consumer_visibility = 'activated'
        OR cardinality(migration_correlation_ids) > 0
    )
);

CREATE TABLE IF NOT EXISTS project_redo_resolver_evidence (
    chain_id text NOT NULL,
    event_identity text NOT NULL,
    block_number bigint NOT NULL,
    event_kind text NOT NULL,
    source_family text NOT NULL,
    resource_id uuid,
    before_resolver_address text,
    after_resolver_address text,
    recorded_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (chain_id, event_identity),
    CHECK (block_number >= 0),
    CHECK (event_kind IN ('PermissionChanged', 'ResolverChanged', 'AliasChanged')),
    CHECK (
        before_resolver_address IS NOT NULL
        OR after_resolver_address IS NOT NULL
    )
);

CREATE INDEX IF NOT EXISTS project_redo_resolver_evidence_range_idx
    ON project_redo_resolver_evidence (chain_id, block_number);

CREATE TABLE IF NOT EXISTS project_redo_expiry_roots (
    chain_id text NOT NULL,
    event_identity text NOT NULL,
    block_number bigint NOT NULL,
    logical_name_id text,
    recorded_at timestamptz NOT NULL DEFAULT now(),
    resource_id uuid,
    PRIMARY KEY (chain_id, event_identity),
    CHECK (block_number >= 0),
    CHECK (btrim(logical_name_id) <> ''),
    CONSTRAINT project_redo_expiry_roots_scope_check
        CHECK (logical_name_id IS NOT NULL OR resource_id IS NOT NULL)
);

CREATE INDEX IF NOT EXISTS project_redo_expiry_roots_range_idx
    ON project_redo_expiry_roots (chain_id, block_number);

CREATE TABLE IF NOT EXISTS project_redo_child_registration_history (
    chain_id text NOT NULL,
    event_identity text NOT NULL,
    block_number bigint NOT NULL,
    event_kind text NOT NULL,
    logical_name_id text NOT NULL,
    registry_contract_instance_id uuid NOT NULL,
    recorded_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (chain_id, event_identity),
    CHECK (block_number >= 0),
    CHECK (event_kind IN (
        'RegistrationReserved', 'RegistrationGranted', 'RegistrationRenewed'
    )),
    CHECK (btrim(logical_name_id) <> '')
);

CREATE INDEX IF NOT EXISTS project_redo_child_registration_history_range_idx
    ON project_redo_child_registration_history (chain_id, block_number);

CREATE TABLE IF NOT EXISTS migration_event_associations (
    event_identity text NOT NULL,
    migration_correlation_id text NOT NULL,
    correlation_kind text NOT NULL,
    evidence_refs jsonb NOT NULL,
    chain_id text NOT NULL,
    block_number bigint NOT NULL,
    block_hash text NOT NULL,
    transaction_hash text NOT NULL,
    transaction_index bigint NOT NULL,
    log_index bigint NOT NULL,
    canonicality_state canonicality_state NOT NULL,
    consumer_visibility text NOT NULL,
    interpreter_content_hash text NOT NULL,
    observed_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (event_identity, migration_correlation_id),
    FOREIGN KEY (chain_id, block_hash, block_number)
        REFERENCES chain_lineage (chain_id, block_hash, block_number),
    CHECK (btrim(event_identity) <> ''),
    CHECK (btrim(migration_correlation_id) <> ''),
    CHECK (
        correlation_kind IN (
            'authority_transition',
            'synchronized_renewal',
            'graveyard_cleanup',
            'migration_registry_creation',
            'controller_configuration'
        )
    ),
    CHECK (jsonb_typeof(evidence_refs) = 'array'),
    CHECK (block_number >= 0),
    CHECK (transaction_index >= 0),
    CHECK (log_index >= 0),
    CHECK (consumer_visibility IN ('candidate', 'activated')),
    CHECK (btrim(interpreter_content_hash) <> '')
);

CREATE INDEX IF NOT EXISTS migration_event_associations_position_idx
    ON migration_event_associations (chain_id, block_number, transaction_index, log_index);

CREATE TABLE IF NOT EXISTS migration_discovery_associations (
    logical_edge_identity text NOT NULL,
    migration_correlation_id text NOT NULL,
    correlation_kind text NOT NULL,
    registry_contract_instance_id uuid NOT NULL,
    registry_address text NOT NULL,
    source_manifest_id bigint NOT NULL,
    evidence_refs jsonb NOT NULL,
    chain_id text NOT NULL,
    block_number bigint NOT NULL,
    block_hash text NOT NULL,
    transaction_hash text NOT NULL,
    transaction_index bigint NOT NULL,
    log_index bigint NOT NULL,
    canonicality_state canonicality_state NOT NULL,
    consumer_visibility text NOT NULL,
    interpreter_content_hash text NOT NULL,
    observed_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (logical_edge_identity, migration_correlation_id),
    FOREIGN KEY (chain_id, registry_contract_instance_id)
        REFERENCES contract_instances (chain_id, contract_instance_id),
    FOREIGN KEY (source_manifest_id, chain_id)
        REFERENCES manifest_versions (manifest_id, chain_id),
    FOREIGN KEY (chain_id, block_hash, block_number)
        REFERENCES chain_lineage (chain_id, block_hash, block_number),
    CHECK (btrim(logical_edge_identity) <> ''),
    CHECK (btrim(migration_correlation_id) <> ''),
    CHECK (correlation_kind = 'migration_registry_creation'),
    CHECK (btrim(registry_address) <> ''),
    CHECK (jsonb_typeof(evidence_refs) = 'array'),
    CHECK (block_number >= 0),
    CHECK (transaction_index >= 0),
    CHECK (log_index >= 0),
    CHECK (consumer_visibility IN ('candidate', 'activated')),
    CHECK (btrim(interpreter_content_hash) <> '')
);

CREATE INDEX IF NOT EXISTS migration_discovery_associations_registry_idx
    ON migration_discovery_associations (
        chain_id,
        registry_contract_instance_id,
        block_number,
        log_index
    );

CREATE TABLE IF NOT EXISTS migration_candidate_identity_effects (
    effect_identity text PRIMARY KEY,
    migration_correlation_ids text[] NOT NULL,
    correlation_kind text NOT NULL,
    effect_kind text NOT NULL,
    proposed_effect jsonb NOT NULL,
    evidence_refs jsonb NOT NULL,
    chain_id text NOT NULL,
    block_number bigint NOT NULL,
    block_hash text NOT NULL,
    transaction_hash text NOT NULL,
    transaction_index bigint NOT NULL,
    log_index bigint NOT NULL,
    canonicality_state canonicality_state NOT NULL,
    consumer_visibility text NOT NULL,
    interpreter_content_hash text NOT NULL,
    observed_at timestamptz NOT NULL DEFAULT now(),
    FOREIGN KEY (chain_id, block_hash, block_number)
        REFERENCES chain_lineage (chain_id, block_hash, block_number),
    CHECK (btrim(effect_identity) <> ''),
    CHECK (migration_correlation_ids_valid(migration_correlation_ids)),
    CHECK (cardinality(migration_correlation_ids) > 0),
    CHECK (correlation_kind = 'authority_transition'),
    CHECK (btrim(effect_kind) <> ''),
    CHECK (jsonb_typeof(proposed_effect) = 'object'),
    CHECK (jsonb_typeof(evidence_refs) = 'array'),
    CHECK (block_number >= 0),
    CHECK (transaction_index >= 0),
    CHECK (log_index >= 0),
    CHECK (consumer_visibility = 'candidate'),
    CHECK (btrim(interpreter_content_hash) <> '')
);

CREATE INDEX IF NOT EXISTS migration_candidate_identity_effects_position_idx
    ON migration_candidate_identity_effects (chain_id, block_number, transaction_index, log_index);

CREATE TABLE IF NOT EXISTS migration_candidate_discovery_effects (
    effect_identity text PRIMARY KEY,
    migration_correlation_ids text[] NOT NULL,
    correlation_kind text NOT NULL,
    effect_kind text NOT NULL,
    proposed_effect jsonb NOT NULL,
    evidence_refs jsonb NOT NULL,
    chain_id text NOT NULL,
    block_number bigint NOT NULL,
    block_hash text NOT NULL,
    transaction_hash text NOT NULL,
    transaction_index bigint NOT NULL,
    log_index bigint NOT NULL,
    canonicality_state canonicality_state NOT NULL,
    consumer_visibility text NOT NULL,
    interpreter_content_hash text NOT NULL,
    observed_at timestamptz NOT NULL DEFAULT now(),
    FOREIGN KEY (chain_id, block_hash, block_number)
        REFERENCES chain_lineage (chain_id, block_hash, block_number),
    CHECK (btrim(effect_identity) <> ''),
    CHECK (migration_correlation_ids_valid(migration_correlation_ids)),
    CHECK (cardinality(migration_correlation_ids) > 0),
    CHECK (btrim(correlation_kind) <> ''),
    CHECK (btrim(effect_kind) <> ''),
    CHECK (jsonb_typeof(proposed_effect) = 'object'),
    CHECK (jsonb_typeof(evidence_refs) = 'array'),
    CHECK (block_number >= 0),
    CHECK (transaction_index >= 0),
    CHECK (log_index >= 0),
    CHECK (consumer_visibility = 'candidate'),
    CHECK (btrim(interpreter_content_hash) <> '')
);

CREATE INDEX IF NOT EXISTS migration_candidate_discovery_effects_position_idx
    ON migration_candidate_discovery_effects (chain_id, block_number, transaction_index, log_index);

CREATE INDEX IF NOT EXISTS normalized_events_name_history_idx
    ON normalized_events (
        logical_name_id,
        block_number DESC,
        transaction_index DESC,
        log_index DESC,
        normalized_event_id DESC
    )
    WHERE logical_name_id IS NOT NULL
      AND canonicality_state IN ('canonical', 'safe', 'finalized');

CREATE INDEX IF NOT EXISTS normalized_events_resource_history_idx
    ON normalized_events (
        resource_id,
        block_number DESC,
        transaction_index DESC,
        log_index DESC,
        normalized_event_id DESC
    )
    WHERE resource_id IS NOT NULL
      AND canonicality_state IN ('canonical', 'safe', 'finalized');

CREATE INDEX IF NOT EXISTS normalized_events_v1_subregistry_after_node_scope_idx
    ON normalized_events (
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

CREATE INDEX IF NOT EXISTS normalized_events_v1_subregistry_after_child_scope_idx
    ON normalized_events (
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

CREATE INDEX IF NOT EXISTS normalized_events_v1_subregistry_before_node_scope_idx
    ON normalized_events (
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

CREATE INDEX IF NOT EXISTS normalized_events_v2_subregistry_pointer_scope_idx
    ON normalized_events USING gin ((ARRAY[
        lower(after_state ->> 'subregistry'),
        lower(before_state ->> 'subregistry')
    ]))
    WHERE event_kind = 'SubregistryChanged'
      AND source_family IN ('ens_v2_root_l1', 'ens_v2_registry_l1')
      AND consumer_visibility = 'activated'
      AND canonicality_state IN ('canonical', 'safe', 'finalized')
      AND logical_name_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS normalized_events_v1_subregistry_before_child_scope_idx
    ON normalized_events (
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

CREATE INDEX IF NOT EXISTS normalized_events_interpreter_state_history_idx
    ON normalized_events (
        chain_id,
        (raw_fact_ref ? 'interpreter_state_key'),
        (public.digest(
            COALESCE(
                raw_fact_ref ->> 'interpreter_state_key',
                event_identity
            ),
            'sha256'
        )),
        block_number DESC,
        transaction_index DESC,
        log_index DESC,
        normalized_event_id DESC
    )
    WHERE canonicality_state IN ('canonical', 'safe', 'finalized');

COMMENT ON INDEX normalized_events_interpreter_state_history_idx IS
    'This bounded history index groups adapter state by SHA-256 of its unbounded state key. Prior-state reads also compare the original key, preserving exact grouping if digests collide.';

CREATE INDEX IF NOT EXISTS normalized_events_block_idx
    ON normalized_events (
        chain_id,
        block_hash,
        transaction_index,
        log_index,
        normalized_event_id
    )
    WHERE block_hash IS NOT NULL;

CREATE INDEX IF NOT EXISTS normalized_events_chain_block_number_idx
    ON normalized_events (chain_id, block_number);

CREATE INDEX IF NOT EXISTS normalized_events_v2_expiry_scope_idx
    ON normalized_events (
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

CREATE INDEX IF NOT EXISTS normalized_events_ens_v1_record_node_resolver_idx
    ON normalized_events (
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

CREATE INDEX IF NOT EXISTS normalized_events_basenames_record_node_resolver_idx
    ON normalized_events (
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

CREATE INDEX IF NOT EXISTS normalized_events_resolver_alias_history_idx
    ON normalized_events (
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

CREATE INDEX IF NOT EXISTS normalized_events_resolver_upgrade_history_idx
    ON normalized_events (
        chain_id,
        lower(after_state ->> 'proxy_address'),
        block_number DESC,
        normalized_event_id DESC
    )
    WHERE event_kind = 'Upgraded'
      AND canonicality_state IN ('canonical', 'safe', 'finalized');

CREATE INDEX IF NOT EXISTS normalized_events_pointer_after_resolver_history_idx
    ON normalized_events (
        chain_id,
        lower(after_state ->> 'resolver'),
        block_number,
        block_hash
    ) INCLUDE (normalized_event_id)
    WHERE event_kind = 'ResolverChanged'
      AND consumer_visibility = 'activated'
      AND canonicality_state IN ('canonical', 'safe', 'finalized');

CREATE INDEX IF NOT EXISTS normalized_events_pointer_before_resolver_history_idx
    ON normalized_events (
        chain_id,
        lower(before_state ->> 'resolver'),
        block_number,
        block_hash
    ) INCLUDE (normalized_event_id)
    WHERE event_kind = 'ResolverChanged'
      AND consumer_visibility = 'activated'
      AND canonicality_state IN ('canonical', 'safe', 'finalized');

CREATE INDEX IF NOT EXISTS normalized_events_permission_after_resolver_history_idx
    ON normalized_events (
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

CREATE INDEX IF NOT EXISTS normalized_events_permission_before_resolver_history_idx
    ON normalized_events (
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

CREATE INDEX IF NOT EXISTS normalized_events_subregistry_registration_history_idx
    ON normalized_events (
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

CREATE INDEX IF NOT EXISTS normalized_events_projection_idx
    ON normalized_events (
        event_kind,
        canonicality_state,
        chain_id,
        block_number,
        normalized_event_id
    );

COMMENT ON TABLE normalized_events IS
    'This table stores plain protocol events from the interpreter.';
COMMENT ON COLUMN normalized_events.normalized_event_id IS
    'This value identifies the stored event.';
COMMENT ON COLUMN normalized_events.event_identity IS
    'This value is the stable event key.';
COMMENT ON COLUMN normalized_events.namespace IS
    'This value identifies the name system.';
COMMENT ON COLUMN normalized_events.logical_name_id IS
    'This value identifies the affected name.';
COMMENT ON COLUMN normalized_events.resource_id IS
    'This value identifies the affected authority object.';
COMMENT ON COLUMN normalized_events.event_kind IS
    'This value states the protocol event kind.';
COMMENT ON COLUMN normalized_events.source_family IS
    'This value identifies the declared source group.';
COMMENT ON COLUMN normalized_events.manifest_version IS
    'This value is the source manifest version.';
COMMENT ON COLUMN normalized_events.source_manifest_id IS
    'This value identifies the source manifest.';
COMMENT ON COLUMN normalized_events.chain_id IS
    'This value identifies the chain.';
COMMENT ON COLUMN normalized_events.block_number IS
    'This value is the source block height.';
COMMENT ON COLUMN normalized_events.block_hash IS
    'This value identifies the source block.';
COMMENT ON COLUMN normalized_events.transaction_hash IS
    'This value identifies the source transaction.';
COMMENT ON COLUMN normalized_events.transaction_index IS
    'This value orders the transaction in the block.';
COMMENT ON COLUMN normalized_events.log_index IS
    'This value orders the log in the block.';
COMMENT ON COLUMN normalized_events.raw_fact_ref IS
    'This object identifies the source fact.';
COMMENT ON COLUMN normalized_events.derivation_kind IS
    'This value identifies the interpreter rule.';
COMMENT ON COLUMN normalized_events.canonicality_state IS
    'This value states how the chain treats the event.';
COMMENT ON COLUMN normalized_events.before_state IS
    'This object stores the state before the event.';
COMMENT ON COLUMN normalized_events.after_state IS
    'This object stores the state after the event.';
COMMENT ON COLUMN normalized_events.migration_correlation_ids IS
    'These values identify the per-name ENSv1→ENSv2 migration correlation groups that derive this event.';
COMMENT ON COLUMN normalized_events.consumer_visibility IS
    'This value states whether product consumers may read the event.';
COMMENT ON COLUMN normalized_events.observed_at IS
    'This time records the stored observation.';

COMMENT ON TABLE project_redo_resolver_evidence IS
    'Interpret inserts this pre-delete redo handoff once and preserves it across retries; Project compares it with re-derived events and consumes it after selecting affected projection rows.';
COMMENT ON COLUMN project_redo_resolver_evidence.chain_id IS
    'This value identifies the chain whose Interpret redo replaced the event range.';
COMMENT ON COLUMN project_redo_resolver_evidence.event_identity IS
    'This value identifies the pre-redo normalized event without depending on its sequence-assigned row ID.';
COMMENT ON COLUMN project_redo_resolver_evidence.block_number IS
    'This value anchors the removed event in the active redo range.';
COMMENT ON COLUMN project_redo_resolver_evidence.event_kind IS
    'This value states whether the pre-redo row changed a permission, resolver pointer, or alias.';
COMMENT ON COLUMN project_redo_resolver_evidence.source_family IS
    'This value preserves the pre-redo event family so Project can select a replacement from the same family and event kind.';
COMMENT ON COLUMN project_redo_resolver_evidence.resource_id IS
    'This value identifies the permission resource whose current projection must be rebuilt when its event disappears.';
COMMENT ON COLUMN project_redo_resolver_evidence.before_resolver_address IS
    'This value is the resolver referenced by the pre-redo event before state.';
COMMENT ON COLUMN project_redo_resolver_evidence.after_resolver_address IS
    'This value is the resolver referenced by the pre-redo event after state.';
COMMENT ON COLUMN project_redo_resolver_evidence.recorded_at IS
    'This time records the Interpret redo that first captured the event for the pending Project repair.';

COMMENT ON TABLE project_redo_expiry_roots IS
    'Interpret preserves logical names or permission resources from deleted state-derived ENSv2 path-expiry releases here until Project publishes a covering redo.';
COMMENT ON COLUMN project_redo_expiry_roots.chain_id IS
    'This value identifies the chain whose Interpret redo replaced the event range.';
COMMENT ON COLUMN project_redo_expiry_roots.event_identity IS
    'This value identifies the pre-redo path-expiry release without depending on its sequence-assigned row ID.';
COMMENT ON COLUMN project_redo_expiry_roots.block_number IS
    'This value anchors the removed path-expiry release in the active redo range.';
COMMENT ON COLUMN project_redo_expiry_roots.logical_name_id IS
    'When present, this value seeds bounded traversal from the name whose deleted path-expiry release removed descendant projections.';
COMMENT ON COLUMN project_redo_expiry_roots.resource_id IS
    'When present, this value identifies the permission resource whose deleted path-expiry release must seed Project redo.';
COMMENT ON COLUMN project_redo_expiry_roots.recorded_at IS
    'This time records the Interpret redo that first captured the path-expiry release for pending Project repair.';

COMMENT ON TABLE project_redo_child_registration_history IS
    'Interpret preserves child identifiers from removed entry-creating events in ENSv1→ENSv2 migration registries until Project publishes a covering redo.';
COMMENT ON COLUMN project_redo_child_registration_history.chain_id IS 'This value identifies the chain whose Interpret redo replaced the event range.';
COMMENT ON COLUMN project_redo_child_registration_history.event_identity IS 'This value identifies the pre-redo normalized event without depending on its sequence-assigned row ID.';
COMMENT ON COLUMN project_redo_child_registration_history.block_number IS 'This value anchors the removed entry-creating event in the active redo range.';
COMMENT ON COLUMN project_redo_child_registration_history.event_kind IS 'This value identifies the entry-creating registry operation removed by redo.';
COMMENT ON COLUMN project_redo_child_registration_history.logical_name_id IS 'This value identifies the child whose parent reachability must be rebuilt.';
COMMENT ON COLUMN project_redo_child_registration_history.registry_contract_instance_id IS 'This value identifies the ENSv1→ENSv2 migration registry whose historical entry made the child ineligible.';
COMMENT ON COLUMN project_redo_child_registration_history.recorded_at IS 'This time records the Interpret redo that first captured the event for pending Project repair.';

COMMENT ON TABLE migration_event_associations IS
    'This table records candidate ENSv1→ENSv2 migration meaning attached to independently admitted events and retains old-fork evidence after normalized-event redo cleanup.';
COMMENT ON COLUMN migration_event_associations.event_identity IS 'This plain value identifies the independently admitted normalized event; retained old-fork evidence may outlive that event row.';
COMMENT ON COLUMN migration_event_associations.migration_correlation_id IS 'This value identifies one ENSv1→ENSv2 migration correlation group.';
COMMENT ON COLUMN migration_event_associations.correlation_kind IS 'This value states the ENSv1→ENSv2 migration correlation shape.';
COMMENT ON COLUMN migration_event_associations.evidence_refs IS 'This array stores the complete evidence references.';
COMMENT ON COLUMN migration_event_associations.chain_id IS 'This value identifies the evidence chain.';
COMMENT ON COLUMN migration_event_associations.block_number IS 'This value is the associated event block height.';
COMMENT ON COLUMN migration_event_associations.block_hash IS 'This value identifies the associated event block.';
COMMENT ON COLUMN migration_event_associations.transaction_hash IS 'This value identifies the associated event transaction.';
COMMENT ON COLUMN migration_event_associations.transaction_index IS 'This value orders the associated event transaction.';
COMMENT ON COLUMN migration_event_associations.log_index IS 'This value orders the associated event log.';
COMMENT ON COLUMN migration_event_associations.canonicality_state IS 'This value states how the chain treats the association.';
COMMENT ON COLUMN migration_event_associations.consumer_visibility IS 'This value states whether product consumers may use the association.';
COMMENT ON COLUMN migration_event_associations.interpreter_content_hash IS 'This value identifies the interpreter semantics that derived the association.';
COMMENT ON COLUMN migration_event_associations.observed_at IS 'This time records the stored association.';

COMMENT ON TABLE migration_discovery_associations IS
    'This table attaches ENSv1→ENSv2 migration provenance to ordinary registry-announcement edges.';
COMMENT ON COLUMN migration_discovery_associations.logical_edge_identity IS 'This value is the rebuild-stable identity of the ordinary discovery edge.';
COMMENT ON COLUMN migration_discovery_associations.migration_correlation_id IS 'This value identifies the ENSv1→ENSv2 migration registry-creation group.';
COMMENT ON COLUMN migration_discovery_associations.correlation_kind IS 'This value states the ENSv1→ENSv2 migration registry-creation shape.';
COMMENT ON COLUMN migration_discovery_associations.registry_contract_instance_id IS 'This value identifies the announced registry contract.';
COMMENT ON COLUMN migration_discovery_associations.registry_address IS 'This value stores the announced registry address.';
COMMENT ON COLUMN migration_discovery_associations.source_manifest_id IS 'This value identifies the ordinary registry manifest.';
COMMENT ON COLUMN migration_discovery_associations.evidence_refs IS 'This array stores the complete evidence references.';
COMMENT ON COLUMN migration_discovery_associations.chain_id IS 'This value identifies the evidence chain.';
COMMENT ON COLUMN migration_discovery_associations.block_number IS 'This value is the announcement block height.';
COMMENT ON COLUMN migration_discovery_associations.block_hash IS 'This value identifies the announcement block.';
COMMENT ON COLUMN migration_discovery_associations.transaction_hash IS 'This value identifies the announcement transaction.';
COMMENT ON COLUMN migration_discovery_associations.transaction_index IS 'This value orders the announcement transaction.';
COMMENT ON COLUMN migration_discovery_associations.log_index IS 'This value orders the announcement log.';
COMMENT ON COLUMN migration_discovery_associations.canonicality_state IS 'This value states how the chain treats the association.';
COMMENT ON COLUMN migration_discovery_associations.consumer_visibility IS 'This value states whether product consumers may use the association.';
COMMENT ON COLUMN migration_discovery_associations.interpreter_content_hash IS 'This value identifies the interpreter semantics that derived the association.';
COMMENT ON COLUMN migration_discovery_associations.observed_at IS 'This time records the stored association.';

COMMENT ON TABLE migration_candidate_identity_effects IS
    'This table stores candidate identity changes without mutating ordinary identity rows.';
COMMENT ON TABLE migration_candidate_discovery_effects IS
    'This table stores candidate discovery changes without mutating ordinary discovery rows.';

DO $$
DECLARE
    table_name text;
BEGIN
    FOREACH table_name IN ARRAY ARRAY[
        'migration_candidate_identity_effects',
        'migration_candidate_discovery_effects'
    ]
    LOOP
        EXECUTE format('COMMENT ON COLUMN %I.effect_identity IS %L', table_name, 'This value is the stable candidate-effect identity.');
        EXECUTE format('COMMENT ON COLUMN %I.migration_correlation_ids IS %L', table_name, 'These values identify the deriving ENSv1→ENSv2 migration groups.');
        EXECUTE format('COMMENT ON COLUMN %I.correlation_kind IS %L', table_name, 'This value states the ENSv1→ENSv2 migration correlation shape.');
        EXECUTE format('COMMENT ON COLUMN %I.effect_kind IS %L', table_name, 'This value states the proposed effect kind.');
        EXECUTE format('COMMENT ON COLUMN %I.proposed_effect IS %L', table_name, 'This object stores the proposed value or range change.');
        EXECUTE format('COMMENT ON COLUMN %I.evidence_refs IS %L', table_name, 'This array stores the complete evidence references.');
        EXECUTE format('COMMENT ON COLUMN %I.chain_id IS %L', table_name, 'This value identifies the evidence chain.');
        EXECUTE format('COMMENT ON COLUMN %I.block_number IS %L', table_name, 'This value is the effect anchor block height.');
        EXECUTE format('COMMENT ON COLUMN %I.block_hash IS %L', table_name, 'This value identifies the effect anchor block.');
        EXECUTE format('COMMENT ON COLUMN %I.transaction_hash IS %L', table_name, 'This value identifies the effect anchor transaction.');
        EXECUTE format('COMMENT ON COLUMN %I.transaction_index IS %L', table_name, 'This value orders the effect anchor transaction.');
        EXECUTE format('COMMENT ON COLUMN %I.log_index IS %L', table_name, 'This value orders the effect anchor log.');
        EXECUTE format('COMMENT ON COLUMN %I.canonicality_state IS %L', table_name, 'This value states how the chain treats the effect.');
        EXECUTE format('COMMENT ON COLUMN %I.consumer_visibility IS %L', table_name, 'This value states whether product consumers may use the effect.');
        EXECUTE format('COMMENT ON COLUMN %I.interpreter_content_hash IS %L', table_name, 'This value identifies the interpreter semantics that derived the effect.');
        EXECUTE format('COMMENT ON COLUMN %I.observed_at IS %L', table_name, 'This time records the stored effect.');
    END LOOP;
END
$$;
