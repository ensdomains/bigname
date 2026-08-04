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
                'ExpiryChanged',
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
                'ens_v2_permissions',
                'ens_v2_registrar',
                'ens_v2_registry_resource_surface',
                'ens_v2_resolver',
                'manifest_sync',
                'proxy_upgrade',
                'raw_log_preimage_observation'
            )
        ),
    CHECK (jsonb_typeof(before_state) = 'object'),
    CHECK (jsonb_typeof(after_state) = 'object')
);

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
COMMENT ON COLUMN normalized_events.observed_at IS
    'This time records the stored observation.';
