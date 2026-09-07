-- Admit registry-wide account approvals and their replayable Project state.
DO $migration$
BEGIN
    IF to_regclass('bigname_phase.normalized_events') IS NULL THEN
        RETURN;
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'bigname_phase.normalized_events'::regclass
          AND conname = 'normalized_events_event_kind_check_registry_operator'
    ) THEN
        ALTER TABLE bigname_phase.normalized_events
        ADD CONSTRAINT normalized_events_event_kind_check_registry_operator CHECK (event_kind IN (
            'AccountPermissionChanged', 'AliasChanged', 'AuthorityEpochChanged',
            'AuthorityTransferred', 'ContractDiscovered', 'ExpiryChanged',
            'MigrationApplied', 'ParentChanged', 'PermissionChanged',
            'PermissionScopeChanged', 'PreimageObserved', 'RecordChanged',
            'RecordVersionChanged', 'RegistrarNameRegistered', 'RegistrationGranted',
            'RegistrationReleased', 'RegistrationRenewed', 'RegistrationReserved',
            'RegistryCreated', 'ResolverChanged', 'ReverseChanged',
            'RootPermissionChanged', 'SourceManifestUpdated', 'SubregistryChanged',
            'SurfaceBound', 'SurfaceUnbound', 'TokenControlTransferred',
            'TokenRegenerated', 'TokenResourceLinked', 'Upgraded'
        )) NOT VALID;
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'bigname_phase.normalized_events'::regclass
          AND conname = 'normalized_events_derivation_kind_check_registry_operator'
    ) THEN
        ALTER TABLE bigname_phase.normalized_events
        ADD CONSTRAINT normalized_events_derivation_kind_check_registry_operator CHECK (derivation_kind IN (
            'ens_v1_reverse_claim', 'ens_v1_unwrapped_authority', 'ens_v2_migration',
            'ens_v2_permissions', 'ens_v2_registrar',
            'ens_v2_registry_resource_surface', 'ens_v2_resolver', 'manifest_sync',
            'proxy_upgrade', 'raw_block_preimage_observation',
            'raw_log_preimage_observation', 'standard_approval'
        )) NOT VALID;
    END IF;

    CREATE TABLE IF NOT EXISTS bigname_phase.account_permission_state_current (
        chain_id text NOT NULL,
        authority_kind text NOT NULL,
        authority_contract text NOT NULL,
        authority_contract_instance_id uuid NOT NULL,
        owner text NOT NULL,
        subject text NOT NULL,
        relation_kind text NOT NULL,
        approved boolean NOT NULL,
        effective_powers jsonb NOT NULL,
        grant_source jsonb NOT NULL,
        revocation_source jsonb,
        inheritance_path jsonb NOT NULL,
        transfer_behavior jsonb NOT NULL,
        provenance jsonb NOT NULL,
        chain_positions jsonb NOT NULL,
        canonicality_summary jsonb NOT NULL,
        manifest_version bigint NOT NULL,
        last_recomputed_at timestamptz NOT NULL DEFAULT now(),
        inserted_at timestamptz NOT NULL DEFAULT now(),
        PRIMARY KEY (chain_id, authority_kind, authority_contract, owner, subject, relation_kind),
        CHECK (btrim(chain_id) <> ''),
        CHECK (authority_kind = 'registry'),
        CHECK (relation_kind = 'operator'),
        CHECK (authority_contract ~ '^0x[0-9a-f]{40}$'),
        CHECK (owner ~ '^0x[0-9a-f]{40}$'),
        CHECK (subject ~ '^0x[0-9a-f]{40}$'),
        CHECK ((approved AND effective_powers = '["registry_control"]'::jsonb)
            OR (NOT approved AND effective_powers = '[]'::jsonb)),
        CHECK (jsonb_typeof(grant_source) = 'object'),
        CHECK (revocation_source IS NULL OR jsonb_typeof(revocation_source) = 'object'),
        CHECK (jsonb_typeof(inheritance_path) = 'array'),
        CHECK (jsonb_typeof(transfer_behavior) = 'object'),
        CHECK (jsonb_typeof(provenance) = 'object'),
        CHECK (jsonb_typeof(chain_positions) = 'object'),
        CHECK (jsonb_typeof(canonicality_summary) = 'object'),
        CHECK (manifest_version > 0)
    );
    CREATE INDEX IF NOT EXISTS account_permission_state_current_active_subject_idx
        ON bigname_phase.account_permission_state_current (subject, chain_id, authority_contract, owner)
        WHERE approved;
    CREATE INDEX IF NOT EXISTS account_permission_state_current_applicability_idx
        ON bigname_phase.account_permission_state_current (chain_id, authority_contract, owner, subject)
        WHERE approved;

    ALTER TABLE bigname_phase.permissions_current_resource_summary
        ADD COLUMN IF NOT EXISTS registry_owner text,
        ADD COLUMN IF NOT EXISTS registry_contract text,
        ADD COLUMN IF NOT EXISTS registry_binding_provenance jsonb,
        ADD COLUMN IF NOT EXISTS registry_binding_chain_positions jsonb;
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'bigname_phase.permissions_current_resource_summary'::regclass
          AND conname = 'permissions_current_resource_summary_registry_binding_check_v2'
    ) THEN
        ALTER TABLE bigname_phase.permissions_current_resource_summary
        ADD CONSTRAINT permissions_current_resource_summary_registry_binding_check_v2 CHECK (
            (registry_owner IS NULL AND registry_contract IS NULL
                AND registry_binding_provenance IS NULL
                AND registry_binding_chain_positions IS NULL)
            OR (registry_owner IS NOT NULL
                AND registry_contract IS NOT NULL
                AND registry_binding_provenance IS NOT NULL
                AND registry_binding_chain_positions IS NOT NULL
                AND registry_owner ~ '^0x[0-9a-f]{40}$'
                AND registry_contract ~ '^0x[0-9a-f]{40}$'
                AND jsonb_typeof(registry_binding_provenance) = 'object'
                AND jsonb_typeof(registry_binding_chain_positions) = 'object')
        ) NOT VALID;
    END IF;
    CREATE INDEX IF NOT EXISTS permissions_current_resource_registry_binding_idx
        ON bigname_phase.permissions_current_resource_summary
            (registry_contract, registry_owner, resource_id)
        WHERE registry_owner IS NOT NULL;

    COMMENT ON TABLE bigname_phase.account_permission_state_current IS
        'Latest account-wide permission states.';
    COMMENT ON COLUMN bigname_phase.account_permission_state_current.chain_id IS
        'The chain identifier.';
    COMMENT ON COLUMN bigname_phase.account_permission_state_current.authority_kind IS
        'The authority class.';
    COMMENT ON COLUMN bigname_phase.account_permission_state_current.authority_contract IS
        'The authority contract address.';
    COMMENT ON COLUMN bigname_phase.account_permission_state_current.authority_contract_instance_id IS
        'The admitted contract instance.';
    COMMENT ON COLUMN bigname_phase.account_permission_state_current.owner IS
        'The approving account.';
    COMMENT ON COLUMN bigname_phase.account_permission_state_current.subject IS
        'The approved operator.';
    COMMENT ON COLUMN bigname_phase.account_permission_state_current.relation_kind IS
        'The permission relation.';
    COMMENT ON COLUMN bigname_phase.account_permission_state_current.approved IS
        'The latest approval Boolean.';
    COMMENT ON COLUMN bigname_phase.account_permission_state_current.effective_powers IS
        'The effective powers.';
    COMMENT ON COLUMN bigname_phase.account_permission_state_current.grant_source IS
        'The grant evidence.';
    COMMENT ON COLUMN bigname_phase.account_permission_state_current.revocation_source IS
        'The revocation evidence.';
    COMMENT ON COLUMN bigname_phase.account_permission_state_current.inheritance_path IS
        'The inheritance path.';
    COMMENT ON COLUMN bigname_phase.account_permission_state_current.transfer_behavior IS
        'The owner-change behavior.';
    COMMENT ON COLUMN bigname_phase.account_permission_state_current.provenance IS
        'The source evidence.';
    COMMENT ON COLUMN bigname_phase.account_permission_state_current.chain_positions IS
        'The selected chain positions.';
    COMMENT ON COLUMN bigname_phase.account_permission_state_current.canonicality_summary IS
        'The selected block states.';
    COMMENT ON COLUMN bigname_phase.account_permission_state_current.manifest_version IS
        'The source manifest version.';
    COMMENT ON COLUMN bigname_phase.account_permission_state_current.last_recomputed_at IS
        'The latest rebuild time.';
    COMMENT ON COLUMN bigname_phase.account_permission_state_current.inserted_at IS
        'The row creation time.';
    COMMENT ON COLUMN bigname_phase.permissions_current_resource_summary.registry_owner IS
        'This value identifies the proven current registry owner.';
    COMMENT ON COLUMN bigname_phase.permissions_current_resource_summary.registry_contract IS
        'This value identifies the registry that supplied the owner.';
    COMMENT ON COLUMN bigname_phase.permissions_current_resource_summary.registry_binding_provenance IS
        'This object identifies the registry-owner evidence.';
    COMMENT ON COLUMN bigname_phase.permissions_current_resource_summary.registry_binding_chain_positions IS
        'This object identifies the registry-owner chain position.';
END
$migration$;
