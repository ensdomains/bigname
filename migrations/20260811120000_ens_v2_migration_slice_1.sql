-- An empty schema-migration database has no phase baseline yet, so this step must be
-- a no-op there; phase-runner init-schema installs the same objects afterward. On a
-- baseline-first test database every additive object already exists, so every operation
-- is idempotent. Constraint validation and the metadata swap use the next two migrations.
DO $migration$
DECLARE
    table_name text;
BEGIN
    IF to_regclass('bigname_phase.normalized_events') IS NULL THEN
        RETURN;
    END IF;

    EXECUTE $ddl$
        CREATE OR REPLACE FUNCTION bigname_phase.migration_correlation_ids_valid(
            correlation_ids text[]
        )
        RETURNS boolean
        LANGUAGE sql
        IMMUTABLE
        STRICT
        AS $function$
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
$function$
    $ddl$;

    EXECUTE $ddl$
        ALTER TABLE bigname_phase.normalized_events
            ADD COLUMN IF NOT EXISTS migration_correlation_ids text[]
                NOT NULL DEFAULT ARRAY[]::text[],
            ADD COLUMN IF NOT EXISTS consumer_visibility text
                NOT NULL DEFAULT 'activated'
    $ddl$;

    EXECUTE $ddl$
        CREATE TABLE IF NOT EXISTS bigname_phase.migration_event_associations (
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
            canonicality_state bigname_phase.canonicality_state NOT NULL,
            consumer_visibility text NOT NULL,
            interpreter_content_hash text NOT NULL,
            observed_at timestamptz NOT NULL DEFAULT now(),
            PRIMARY KEY (event_identity, migration_correlation_id),
            FOREIGN KEY (chain_id, block_hash, block_number)
                REFERENCES bigname_phase.chain_lineage (chain_id, block_hash, block_number),
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
        )
    $ddl$;
    EXECUTE $ddl$
        CREATE INDEX IF NOT EXISTS migration_event_associations_position_idx
        ON bigname_phase.migration_event_associations (
            chain_id, block_number, transaction_index, log_index
        )
    $ddl$;

    EXECUTE $ddl$
        CREATE TABLE IF NOT EXISTS bigname_phase.migration_discovery_associations (
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
            canonicality_state bigname_phase.canonicality_state NOT NULL,
            consumer_visibility text NOT NULL,
            interpreter_content_hash text NOT NULL,
            observed_at timestamptz NOT NULL DEFAULT now(),
            PRIMARY KEY (logical_edge_identity, migration_correlation_id),
            FOREIGN KEY (chain_id, registry_contract_instance_id)
                REFERENCES bigname_phase.contract_instances (chain_id, contract_instance_id),
            FOREIGN KEY (source_manifest_id, chain_id)
                REFERENCES bigname_phase.manifest_versions (manifest_id, chain_id),
            FOREIGN KEY (chain_id, block_hash, block_number)
                REFERENCES bigname_phase.chain_lineage (chain_id, block_hash, block_number),
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
        )
    $ddl$;
    EXECUTE $ddl$
        CREATE INDEX IF NOT EXISTS migration_discovery_associations_registry_idx
        ON bigname_phase.migration_discovery_associations (
            chain_id, registry_contract_instance_id, block_number, log_index
        )
    $ddl$;

    EXECUTE $ddl$
        CREATE TABLE IF NOT EXISTS bigname_phase.migration_candidate_identity_effects (
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
            canonicality_state bigname_phase.canonicality_state NOT NULL,
            consumer_visibility text NOT NULL,
            interpreter_content_hash text NOT NULL,
            observed_at timestamptz NOT NULL DEFAULT now(),
            FOREIGN KEY (chain_id, block_hash, block_number)
                REFERENCES bigname_phase.chain_lineage (chain_id, block_hash, block_number),
            CHECK (btrim(effect_identity) <> ''),
            CHECK (bigname_phase.migration_correlation_ids_valid(migration_correlation_ids)),
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
        )
    $ddl$;
    EXECUTE $ddl$
        CREATE INDEX IF NOT EXISTS migration_candidate_identity_effects_position_idx
        ON bigname_phase.migration_candidate_identity_effects (
            chain_id, block_number, transaction_index, log_index
        )
    $ddl$;

    EXECUTE $ddl$
        CREATE TABLE IF NOT EXISTS bigname_phase.migration_candidate_discovery_effects (
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
            canonicality_state bigname_phase.canonicality_state NOT NULL,
            consumer_visibility text NOT NULL,
            interpreter_content_hash text NOT NULL,
            observed_at timestamptz NOT NULL DEFAULT now(),
            FOREIGN KEY (chain_id, block_hash, block_number)
                REFERENCES bigname_phase.chain_lineage (chain_id, block_hash, block_number),
            CHECK (btrim(effect_identity) <> ''),
            CHECK (bigname_phase.migration_correlation_ids_valid(migration_correlation_ids)),
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
        )
    $ddl$;
    EXECUTE $ddl$
        CREATE INDEX IF NOT EXISTS migration_candidate_discovery_effects_position_idx
        ON bigname_phase.migration_candidate_discovery_effects (
            chain_id, block_number, transaction_index, log_index
        )
    $ddl$;

    IF NOT EXISTS (
            SELECT 1 FROM pg_constraint constraint_row
            WHERE constraint_row.conrelid = 'bigname_phase.normalized_events'::regclass
              AND constraint_row.conname = 'normalized_events_event_kind_check_v2'
    ) THEN
        EXECUTE $ddl$
            ALTER TABLE bigname_phase.normalized_events
            ADD CONSTRAINT normalized_events_event_kind_check_v2 CHECK (
                event_kind IN (
                    'AliasChanged', 'AuthorityEpochChanged', 'AuthorityTransferred',
                    'ContractDiscovered', 'ExpiryChanged', 'MigrationApplied',
                    'ParentChanged', 'PermissionChanged', 'PermissionScopeChanged',
                    'PreimageObserved', 'RecordChanged', 'RecordVersionChanged',
                    'RegistrarNameRegistered', 'RegistrationGranted',
                    'RegistrationReleased', 'RegistrationRenewed',
                    'RegistrationReserved', 'RegistryCreated', 'ResolverChanged',
                    'ReverseChanged', 'RootPermissionChanged', 'SourceManifestUpdated',
                    'SubregistryChanged', 'SurfaceBound', 'SurfaceUnbound',
                    'TokenControlTransferred', 'TokenRegenerated',
                    'TokenResourceLinked', 'Upgraded'
                )
            ) NOT VALID
        $ddl$;
    END IF;
    IF NOT EXISTS (
            SELECT 1 FROM pg_constraint constraint_row
            WHERE constraint_row.conrelid = 'bigname_phase.normalized_events'::regclass
              AND constraint_row.conname = 'normalized_events_derivation_kind_check_v2'
    ) THEN
        EXECUTE $ddl$
            ALTER TABLE bigname_phase.normalized_events
            ADD CONSTRAINT normalized_events_derivation_kind_check_v2 CHECK (
                derivation_kind IN (
                    'ens_v1_reverse_claim', 'ens_v1_unwrapped_authority',
                    'ens_v2_migration', 'ens_v2_permissions', 'ens_v2_registrar',
                    'ens_v2_registry_resource_surface', 'ens_v2_resolver',
                    'manifest_sync', 'proxy_upgrade', 'raw_log_preimage_observation'
                )
            ) NOT VALID
        $ddl$;
    END IF;
    IF NOT EXISTS (
            SELECT 1 FROM pg_constraint constraint_row
            WHERE constraint_row.conrelid = 'bigname_phase.normalized_events'::regclass
              AND constraint_row.conname = 'normalized_events_migration_correlation_ids_check_v2'
    ) THEN
        EXECUTE $ddl$
            ALTER TABLE bigname_phase.normalized_events
            ADD CONSTRAINT normalized_events_migration_correlation_ids_check_v2
            CHECK (
                bigname_phase.migration_correlation_ids_valid(migration_correlation_ids)
            ) NOT VALID
        $ddl$;
    END IF;
    IF NOT EXISTS (
            SELECT 1 FROM pg_constraint constraint_row
            WHERE constraint_row.conrelid = 'bigname_phase.normalized_events'::regclass
              AND constraint_row.conname = 'normalized_events_consumer_visibility_check_v2'
    ) THEN
        EXECUTE $ddl$
            ALTER TABLE bigname_phase.normalized_events
            ADD CONSTRAINT normalized_events_consumer_visibility_check_v2
            CHECK (consumer_visibility IN ('candidate', 'activated')) NOT VALID
        $ddl$;
    END IF;
    IF NOT EXISTS (
            SELECT 1 FROM pg_constraint constraint_row
            WHERE constraint_row.conrelid = 'bigname_phase.normalized_events'::regclass
              AND constraint_row.conname = 'normalized_events_candidate_correlation_check_v2'
    ) THEN
        EXECUTE $ddl$
            ALTER TABLE bigname_phase.normalized_events
            ADD CONSTRAINT normalized_events_candidate_correlation_check_v2 CHECK (
                consumer_visibility = 'activated'
                OR cardinality(migration_correlation_ids) > 0
            ) NOT VALID
        $ddl$;
    END IF;

    EXECUTE $ddl$
        COMMENT ON COLUMN bigname_phase.normalized_events.migration_correlation_ids IS
            'These values identify the per-name ENSv1→ENSv2 migration correlation groups that derive this event.'
    $ddl$;
    EXECUTE $ddl$
        COMMENT ON COLUMN bigname_phase.normalized_events.consumer_visibility IS
            'This value states whether product consumers may read the event.'
    $ddl$;
    EXECUTE $ddl$
        COMMENT ON TABLE bigname_phase.migration_event_associations IS
            'This table records candidate ENSv1→ENSv2 migration meaning attached to independently admitted events and retains old-fork evidence after normalized-event redo cleanup.'
    $ddl$;
    EXECUTE $ddl$
        COMMENT ON TABLE bigname_phase.migration_discovery_associations IS
            'This table attaches ENSv1→ENSv2 migration provenance to ordinary registry-announcement edges.'
    $ddl$;
    EXECUTE $ddl$
        COMMENT ON TABLE bigname_phase.migration_candidate_identity_effects IS
            'This table stores candidate identity changes without mutating ordinary identity rows.'
    $ddl$;
    EXECUTE $ddl$
        COMMENT ON TABLE bigname_phase.migration_candidate_discovery_effects IS
            'This table stores candidate discovery changes without mutating ordinary discovery rows.'
    $ddl$;

    EXECUTE $ddl$
        COMMENT ON COLUMN bigname_phase.migration_event_associations.event_identity IS
            'This plain value identifies the independently admitted normalized event; retained old-fork evidence may outlive that event row.'
    $ddl$;
    EXECUTE $ddl$
        COMMENT ON COLUMN bigname_phase.migration_event_associations.migration_correlation_id IS
            'This value identifies one ENSv1→ENSv2 migration correlation group.'
    $ddl$;
    EXECUTE $ddl$
        COMMENT ON COLUMN bigname_phase.migration_event_associations.correlation_kind IS
            'This value states the ENSv1→ENSv2 migration correlation shape.'
    $ddl$;
    EXECUTE $ddl$
        COMMENT ON COLUMN bigname_phase.migration_event_associations.evidence_refs IS
            'This array stores the complete evidence references.'
    $ddl$;
    EXECUTE $ddl$
        COMMENT ON COLUMN bigname_phase.migration_event_associations.chain_id IS
            'This value identifies the evidence chain.'
    $ddl$;
    EXECUTE $ddl$
        COMMENT ON COLUMN bigname_phase.migration_event_associations.block_number IS
            'This value is the associated event block height.'
    $ddl$;
    EXECUTE $ddl$
        COMMENT ON COLUMN bigname_phase.migration_event_associations.block_hash IS
            'This value identifies the associated event block.'
    $ddl$;
    EXECUTE $ddl$
        COMMENT ON COLUMN bigname_phase.migration_event_associations.transaction_hash IS
            'This value identifies the associated event transaction.'
    $ddl$;
    EXECUTE $ddl$
        COMMENT ON COLUMN bigname_phase.migration_event_associations.transaction_index IS
            'This value orders the associated event transaction.'
    $ddl$;
    EXECUTE $ddl$
        COMMENT ON COLUMN bigname_phase.migration_event_associations.log_index IS
            'This value orders the associated event log.'
    $ddl$;
    EXECUTE $ddl$
        COMMENT ON COLUMN bigname_phase.migration_event_associations.canonicality_state IS
            'This value states how the chain treats the association.'
    $ddl$;
    EXECUTE $ddl$
        COMMENT ON COLUMN bigname_phase.migration_event_associations.consumer_visibility IS
            'This value states whether product consumers may use the association.'
    $ddl$;
    EXECUTE $ddl$
        COMMENT ON COLUMN bigname_phase.migration_event_associations.interpreter_content_hash IS
            'This value identifies the interpreter semantics that derived the association.'
    $ddl$;
    EXECUTE $ddl$
        COMMENT ON COLUMN bigname_phase.migration_event_associations.observed_at IS
            'This time records the stored association.'
    $ddl$;

    EXECUTE $ddl$
        COMMENT ON COLUMN bigname_phase.migration_discovery_associations.logical_edge_identity IS
            'This value is the rebuild-stable identity of the ordinary discovery edge.'
    $ddl$;
    EXECUTE $ddl$
        COMMENT ON COLUMN bigname_phase.migration_discovery_associations.migration_correlation_id IS
            'This value identifies the ENSv1→ENSv2 migration registry-creation group.'
    $ddl$;
    EXECUTE $ddl$
        COMMENT ON COLUMN bigname_phase.migration_discovery_associations.correlation_kind IS
            'This value states the ENSv1→ENSv2 migration registry-creation shape.'
    $ddl$;
    EXECUTE $ddl$
        COMMENT ON COLUMN bigname_phase.migration_discovery_associations.registry_contract_instance_id IS
            'This value identifies the announced registry contract.'
    $ddl$;
    EXECUTE $ddl$
        COMMENT ON COLUMN bigname_phase.migration_discovery_associations.registry_address IS
            'This value stores the announced registry address.'
    $ddl$;
    EXECUTE $ddl$
        COMMENT ON COLUMN bigname_phase.migration_discovery_associations.source_manifest_id IS
            'This value identifies the ordinary registry manifest.'
    $ddl$;
    EXECUTE $ddl$
        COMMENT ON COLUMN bigname_phase.migration_discovery_associations.evidence_refs IS
            'This array stores the complete evidence references.'
    $ddl$;
    EXECUTE $ddl$
        COMMENT ON COLUMN bigname_phase.migration_discovery_associations.chain_id IS
            'This value identifies the evidence chain.'
    $ddl$;
    EXECUTE $ddl$
        COMMENT ON COLUMN bigname_phase.migration_discovery_associations.block_number IS
            'This value is the announcement block height.'
    $ddl$;
    EXECUTE $ddl$
        COMMENT ON COLUMN bigname_phase.migration_discovery_associations.block_hash IS
            'This value identifies the announcement block.'
    $ddl$;
    EXECUTE $ddl$
        COMMENT ON COLUMN bigname_phase.migration_discovery_associations.transaction_hash IS
            'This value identifies the announcement transaction.'
    $ddl$;
    EXECUTE $ddl$
        COMMENT ON COLUMN bigname_phase.migration_discovery_associations.transaction_index IS
            'This value orders the announcement transaction.'
    $ddl$;
    EXECUTE $ddl$
        COMMENT ON COLUMN bigname_phase.migration_discovery_associations.log_index IS
            'This value orders the announcement log.'
    $ddl$;
    EXECUTE $ddl$
        COMMENT ON COLUMN bigname_phase.migration_discovery_associations.canonicality_state IS
            'This value states how the chain treats the association.'
    $ddl$;
    EXECUTE $ddl$
        COMMENT ON COLUMN bigname_phase.migration_discovery_associations.consumer_visibility IS
            'This value states whether product consumers may use the association.'
    $ddl$;
    EXECUTE $ddl$
        COMMENT ON COLUMN bigname_phase.migration_discovery_associations.interpreter_content_hash IS
            'This value identifies the interpreter semantics that derived the association.'
    $ddl$;
    EXECUTE $ddl$
        COMMENT ON COLUMN bigname_phase.migration_discovery_associations.observed_at IS
            'This time records the stored association.'
    $ddl$;

    FOR table_name IN
        SELECT unnest(ARRAY[
            'migration_candidate_identity_effects',
            'migration_candidate_discovery_effects'
        ])
    LOOP
        EXECUTE format(
            'COMMENT ON COLUMN bigname_phase.%I.effect_identity IS %L',
            table_name, 'This value is the stable candidate-effect identity.'
        );
        EXECUTE format(
            'COMMENT ON COLUMN bigname_phase.%I.migration_correlation_ids IS %L',
            table_name, 'These values identify the deriving ENSv1→ENSv2 migration groups.'
        );
        EXECUTE format(
            'COMMENT ON COLUMN bigname_phase.%I.correlation_kind IS %L',
            table_name, 'This value states the ENSv1→ENSv2 migration correlation shape.'
        );
        EXECUTE format(
            'COMMENT ON COLUMN bigname_phase.%I.effect_kind IS %L',
            table_name, 'This value states the proposed effect kind.'
        );
        EXECUTE format(
            'COMMENT ON COLUMN bigname_phase.%I.proposed_effect IS %L',
            table_name, 'This object stores the proposed value or range change.'
        );
        EXECUTE format(
            'COMMENT ON COLUMN bigname_phase.%I.evidence_refs IS %L',
            table_name, 'This array stores the complete evidence references.'
        );
        EXECUTE format(
            'COMMENT ON COLUMN bigname_phase.%I.chain_id IS %L',
            table_name, 'This value identifies the evidence chain.'
        );
        EXECUTE format(
            'COMMENT ON COLUMN bigname_phase.%I.block_number IS %L',
            table_name, 'This value is the effect anchor block height.'
        );
        EXECUTE format(
            'COMMENT ON COLUMN bigname_phase.%I.block_hash IS %L',
            table_name, 'This value identifies the effect anchor block.'
        );
        EXECUTE format(
            'COMMENT ON COLUMN bigname_phase.%I.transaction_hash IS %L',
            table_name, 'This value identifies the effect anchor transaction.'
        );
        EXECUTE format(
            'COMMENT ON COLUMN bigname_phase.%I.transaction_index IS %L',
            table_name, 'This value orders the effect anchor transaction.'
        );
        EXECUTE format(
            'COMMENT ON COLUMN bigname_phase.%I.log_index IS %L',
            table_name, 'This value orders the effect anchor log.'
        );
        EXECUTE format(
            'COMMENT ON COLUMN bigname_phase.%I.canonicality_state IS %L',
            table_name, 'This value states how the chain treats the effect.'
        );
        EXECUTE format(
            'COMMENT ON COLUMN bigname_phase.%I.consumer_visibility IS %L',
            table_name, 'This value states whether product consumers may use the effect.'
        );
        EXECUTE format(
            'COMMENT ON COLUMN bigname_phase.%I.interpreter_content_hash IS %L',
            table_name, 'This value identifies the interpreter semantics that derived the effect.'
        );
        EXECUTE format(
            'COMMENT ON COLUMN bigname_phase.%I.observed_at IS %L',
            table_name, 'This time records the stored effect.'
        );
    END LOOP;
END
$migration$;
