CREATE TABLE IF NOT EXISTS name_current (
    logical_name_id text PRIMARY KEY
        REFERENCES name_surfaces (logical_name_id),
    namespace text NOT NULL,
    raw_name text NOT NULL,
    namehash text NOT NULL,
    surface_binding_id uuid
        REFERENCES surface_bindings (surface_binding_id),
    resource_id uuid
        REFERENCES resources (resource_id),
    serving_resource_id uuid
        REFERENCES resources (resource_id),
    token_lineage_id uuid
        REFERENCES token_lineages (token_lineage_id),
    binding_kind text,
    declared_summary jsonb NOT NULL DEFAULT '{}'::jsonb,
    support_status text NOT NULL,
    unsupported_reason text,
    provenance jsonb NOT NULL DEFAULT '{}'::jsonb,
    chain_positions jsonb NOT NULL DEFAULT '{}'::jsonb,
    canonicality_summary jsonb NOT NULL DEFAULT '{}'::jsonb,
    manifest_version bigint NOT NULL,
    last_recomputed_at timestamptz NOT NULL DEFAULT now(),
    inserted_at timestamptz NOT NULL DEFAULT now(),
    FOREIGN KEY (
        surface_binding_id,
        logical_name_id,
        resource_id,
        binding_kind
    ) REFERENCES surface_bindings (
        surface_binding_id,
        logical_name_id,
        resource_id,
        binding_kind
    ),
    FOREIGN KEY (resource_id, token_lineage_id)
        REFERENCES resources (resource_id, token_lineage_id),
    CHECK (btrim(namespace) <> ''),
    CHECK (btrim(namehash) <> ''),
    CONSTRAINT name_current_logical_identity_check
        CHECK (logical_name_id = namespace || ':' || namehash),
    CHECK (
        (
            surface_binding_id IS NULL
            AND resource_id IS NULL
            AND binding_kind IS NULL
        )
        OR (
            surface_binding_id IS NOT NULL
            AND resource_id IS NOT NULL
            AND binding_kind IS NOT NULL
            AND btrim(binding_kind) <> ''
        )
    ),
    CONSTRAINT name_current_binding_kind_check
        CHECK (
            binding_kind IS NULL
            OR binding_kind IN (
                'declared_registry_path',
                'linked_subregistry_path',
                'resolver_alias_path',
                'observed_wildcard_path',
                'observed_only'
            )
        ),
    CHECK (token_lineage_id IS NULL OR resource_id IS NOT NULL),
    CHECK (jsonb_typeof(declared_summary) = 'object'),
    CHECK (support_status IN ('supported', 'unsupported')),
    CHECK (
        (support_status = 'supported' AND unsupported_reason IS NULL)
        OR (
            support_status = 'unsupported'
            AND unsupported_reason IS NOT NULL
            AND btrim(unsupported_reason) <> ''
        )
    ),
    CHECK (jsonb_typeof(provenance) = 'object'),
    CHECK (jsonb_typeof(chain_positions) = 'object'),
    CHECK (jsonb_typeof(canonicality_summary) = 'object'),
    CHECK (manifest_version > 0)
);

CREATE INDEX IF NOT EXISTS name_current_lookup_idx
    ON name_current (namespace, namehash, logical_name_id);

CREATE INDEX IF NOT EXISTS name_current_resource_idx
    ON name_current (resource_id)
    WHERE resource_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS name_current_serving_resource_idx
    ON name_current (serving_resource_id)
    WHERE serving_resource_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS name_current_resolver_idx
    ON name_current (
        (declared_summary #>> '{resolver,chain_id}'),
        lower(declared_summary #>> '{resolver,address}'),
        logical_name_id
    )
    WHERE declared_summary #>> '{resolver,address}' IS NOT NULL;

-- Namespace-wide expiry window (`GET /v1/names?namespace=&expires_after=&expires_before=`).
-- The projection writes `registration.expiry` as a JSON number of unix seconds; the partial
-- predicate keeps the text-to-float cast off every other shape, so the expression is immutable
-- and the index build cannot fail on a non-numeric string. The reader repeats the same
-- JSONB_TYPEOF guard and cast in its WHERE clause so the planner can match this index.
CREATE INDEX IF NOT EXISTS name_current_registration_expiry_idx
    ON name_current (
        namespace,
        ((declared_summary #>> '{registration,expiry}')::double precision),
        logical_name_id
    )
    WHERE jsonb_typeof(declared_summary #> '{registration,expiry}') = 'number';

CREATE TABLE IF NOT EXISTS children_current (
    parent_logical_name_id text NOT NULL
        REFERENCES name_surfaces (logical_name_id),
    child_logical_name_id text NOT NULL,
    surface_class text NOT NULL DEFAULT 'declared',
    namespace text NOT NULL,
    raw_name bytea,
    decoded_name text,
    raw_label bytea,
    decoded_label text,
    namehash text NOT NULL,
    labelhash text NOT NULL,
    owner text,
    registrant text,
    provenance jsonb NOT NULL DEFAULT '{}'::jsonb,
    chain_positions jsonb NOT NULL DEFAULT '{}'::jsonb,
    canonicality_summary jsonb NOT NULL DEFAULT '{}'::jsonb,
    manifest_version bigint NOT NULL,
    last_recomputed_at timestamptz NOT NULL DEFAULT now(),
    inserted_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (
        parent_logical_name_id,
        child_logical_name_id,
        surface_class
    ),
    CHECK (parent_logical_name_id <> child_logical_name_id),
    CHECK (btrim(child_logical_name_id) <> ''),
    CHECK (surface_class = 'declared'),
    CHECK (btrim(namespace) <> ''),
    CHECK (btrim(namehash) <> ''),
    CONSTRAINT children_current_logical_identity_check
        CHECK (child_logical_name_id = namespace || ':' || namehash),
    CHECK (raw_name IS NULL OR octet_length(raw_name) > 0),
    CONSTRAINT children_current_decoded_name_requires_raw_check
        CHECK (decoded_name IS NULL OR raw_name IS NOT NULL),
    CONSTRAINT children_current_decoded_name_matches_raw_check
        CHECK (
            decoded_name IS NULL
            OR convert_to(decoded_name, 'UTF8') = raw_name
        ),
    CHECK (raw_label IS NULL OR octet_length(raw_label) > 0),
    CONSTRAINT children_current_decoded_label_requires_raw_check
        CHECK (decoded_label IS NULL OR raw_label IS NOT NULL),
    CONSTRAINT children_current_decoded_label_matches_raw_check
        CHECK (
            decoded_label IS NULL
            OR convert_to(decoded_label, 'UTF8') = raw_label
        ),
    CHECK (btrim(labelhash) <> ''),
    CHECK (owner IS NULL OR btrim(owner) <> ''),
    CHECK (registrant IS NULL OR btrim(registrant) <> ''),
    CHECK (jsonb_typeof(provenance) = 'object'),
    CHECK (jsonb_typeof(chain_positions) = 'object'),
    CHECK (jsonb_typeof(canonicality_summary) = 'object'),
    CHECK (manifest_version > 0)
);

CREATE INDEX IF NOT EXISTS children_current_parent_idx
    ON children_current (
        parent_logical_name_id,
        surface_class,
        namehash,
        child_logical_name_id
    );

CREATE INDEX IF NOT EXISTS children_current_namehash_idx
    ON children_current (namespace, namehash);

CREATE INDEX IF NOT EXISTS children_current_labelhash_idx
    ON children_current (
        namespace,
        lower(labelhash),
        parent_logical_name_id,
        child_logical_name_id
    );

CREATE TABLE IF NOT EXISTS permissions_current (
    resource_id uuid NOT NULL
        REFERENCES resources (resource_id),
    subject text NOT NULL,
    scope text NOT NULL,
    scope_kind text NOT NULL,
    scope_detail jsonb NOT NULL DEFAULT '{}'::jsonb,
    effective_powers jsonb NOT NULL DEFAULT '[]'::jsonb,
    grant_source jsonb NOT NULL DEFAULT '{}'::jsonb,
    revocation_source jsonb,
    inheritance_path jsonb NOT NULL DEFAULT '[]'::jsonb,
    transfer_behavior jsonb NOT NULL DEFAULT '{}'::jsonb,
    provenance jsonb NOT NULL DEFAULT '{}'::jsonb,
    chain_positions jsonb NOT NULL DEFAULT '{}'::jsonb,
    canonicality_summary jsonb NOT NULL DEFAULT '{}'::jsonb,
    manifest_version bigint NOT NULL,
    last_recomputed_at timestamptz NOT NULL DEFAULT now(),
    inserted_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (resource_id, subject, scope),
    CHECK (btrim(subject) <> ''),
    CHECK (btrim(scope) <> ''),
    CONSTRAINT permissions_current_scope_kind_check
        CHECK (
            scope_kind IN (
                'root',
                'registry',
                'resource',
                'resolver',
                'record_manager'
            )
        ),
    CHECK (jsonb_typeof(scope_detail) = 'object'),
    CHECK (jsonb_typeof(effective_powers) = 'array'),
    CHECK (jsonb_typeof(grant_source) = 'object'),
    CHECK (
        revocation_source IS NULL
        OR jsonb_typeof(revocation_source) = 'object'
    ),
    CHECK (jsonb_typeof(inheritance_path) = 'array'),
    CHECK (jsonb_typeof(transfer_behavior) = 'object'),
    CHECK (jsonb_typeof(provenance) = 'object'),
    CHECK (jsonb_typeof(chain_positions) = 'object'),
    CHECK (jsonb_typeof(canonicality_summary) = 'object'),
    CHECK (manifest_version > 0)
);

CREATE INDEX IF NOT EXISTS permissions_current_subject_idx
    ON permissions_current (subject, resource_id, scope);

CREATE INDEX IF NOT EXISTS permissions_current_resolver_scope_idx
    ON permissions_current (
        (scope_detail ->> 'chain_id'),
        lower(scope_detail ->> 'resolver_address'),
        resource_id
    )
    WHERE scope_kind = 'resolver'
      AND scope_detail ->> 'resolver_address' IS NOT NULL;

CREATE TABLE IF NOT EXISTS account_permission_state_current (
    chain_id text NOT NULL,
    authority_kind text NOT NULL
        CONSTRAINT account_permission_state_current_authority_kind_check
        CHECK (authority_kind IN ('registry', 'wrapper')),
    authority_contract text NOT NULL CHECK (authority_contract ~ '^0x[0-9a-f]{40}$'),
    authority_contract_instance_id uuid NOT NULL,
    owner text NOT NULL CHECK (owner ~ '^0x[0-9a-f]{40}$'),
    subject text NOT NULL CHECK (subject ~ '^0x[0-9a-f]{40}$'),
    relation_kind text NOT NULL CHECK (relation_kind = 'operator'),
    approved boolean NOT NULL,
    effective_powers jsonb NOT NULL,
    grant_source jsonb NOT NULL,
    revocation_source jsonb,
    inheritance_path jsonb NOT NULL,
    transfer_behavior jsonb NOT NULL,
    provenance jsonb NOT NULL,
    chain_positions jsonb NOT NULL,
    canonicality_summary jsonb NOT NULL,
    manifest_version bigint NOT NULL CHECK (manifest_version > 0),
    last_recomputed_at timestamptz NOT NULL DEFAULT now(),
    inserted_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (chain_id, authority_kind, authority_contract, owner, subject, relation_kind),
    CHECK (btrim(chain_id) <> ''),
    CONSTRAINT account_permission_state_current_effective_powers_check CHECK (
        (approved AND authority_kind = 'registry'
            AND effective_powers = '["registry_control"]'::jsonb)
        OR (approved AND authority_kind = 'wrapper'
            AND effective_powers = '["wrapper_control"]'::jsonb)
        OR (NOT approved AND effective_powers = '[]'::jsonb)),
    CHECK (jsonb_typeof(grant_source) = 'object'),
    CHECK (revocation_source IS NULL OR jsonb_typeof(revocation_source) = 'object'),
    CHECK (jsonb_typeof(inheritance_path) = 'array'),
    CHECK (jsonb_typeof(transfer_behavior) = 'object'),
    CHECK (jsonb_typeof(provenance) = 'object'),
    CHECK (jsonb_typeof(chain_positions) = 'object'),
    CHECK (jsonb_typeof(canonicality_summary) = 'object')
);

CREATE INDEX IF NOT EXISTS account_permission_state_current_active_subject_idx
    ON account_permission_state_current (subject, chain_id, authority_contract, owner)
    WHERE approved;
CREATE INDEX IF NOT EXISTS account_permission_state_current_applicability_idx
    ON account_permission_state_current (chain_id, authority_contract, owner, subject)
    WHERE approved;

COMMENT ON TABLE account_permission_state_current IS 'Latest account-wide permission states.';
COMMENT ON COLUMN account_permission_state_current.chain_id IS 'The chain identifier.';
COMMENT ON COLUMN account_permission_state_current.authority_kind IS 'The authority class: registry (ENSv1/Basenames registry operators) or wrapper (NameWrapper operators).';
COMMENT ON COLUMN account_permission_state_current.authority_contract IS 'The authority contract address.';
COMMENT ON COLUMN account_permission_state_current.authority_contract_instance_id IS 'The admitted contract instance.';
COMMENT ON COLUMN account_permission_state_current.owner IS 'The approving account.';
COMMENT ON COLUMN account_permission_state_current.subject IS 'The approved operator.';
COMMENT ON COLUMN account_permission_state_current.relation_kind IS 'The permission relation.';
COMMENT ON COLUMN account_permission_state_current.approved IS 'The latest approval Boolean.';
COMMENT ON COLUMN account_permission_state_current.effective_powers IS 'The effective powers.';
COMMENT ON COLUMN account_permission_state_current.grant_source IS 'The grant evidence.';
COMMENT ON COLUMN account_permission_state_current.revocation_source IS 'The revocation evidence.';
COMMENT ON COLUMN account_permission_state_current.inheritance_path IS 'The inheritance path.';
COMMENT ON COLUMN account_permission_state_current.transfer_behavior IS 'The owner-change behavior.';
COMMENT ON COLUMN account_permission_state_current.provenance IS 'The source evidence.';
COMMENT ON COLUMN account_permission_state_current.chain_positions IS 'The selected chain positions.';
COMMENT ON COLUMN account_permission_state_current.canonicality_summary IS 'The selected block states.';
COMMENT ON COLUMN account_permission_state_current.manifest_version IS 'The source manifest version.';
COMMENT ON COLUMN account_permission_state_current.last_recomputed_at IS 'The latest rebuild time.';
COMMENT ON COLUMN account_permission_state_current.inserted_at IS 'The row creation time.';

CREATE TABLE IF NOT EXISTS permissions_current_resource_summary (
    resource_id uuid PRIMARY KEY
        REFERENCES resources (resource_id),
    authority_kind text,
    root_resource_id uuid
        REFERENCES resources (resource_id),
    registry_owner text,
    registry_contract text,
    registry_binding_provenance jsonb,
    registry_binding_chain_positions jsonb,
    resource_restrictions jsonb,
    support_status text NOT NULL,
    unsupported_reason text,
    provenance jsonb NOT NULL DEFAULT '{}'::jsonb,
    chain_positions jsonb NOT NULL DEFAULT '{}'::jsonb,
    canonicality_summary jsonb NOT NULL DEFAULT '{}'::jsonb,
    manifest_version bigint NOT NULL,
    last_recomputed_at timestamptz NOT NULL DEFAULT now(),
    CHECK (authority_kind IS NULL OR btrim(authority_kind) <> ''),
    CONSTRAINT permissions_current_resource_summary_registry_binding_check CHECK (
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
    ),
    CONSTRAINT permissions_current_resource_summary_restrictions_check
        CHECK (resource_restrictions IS NULL OR jsonb_typeof(resource_restrictions) = 'object'),
    CHECK (support_status IN ('supported', 'unsupported')),
    CHECK (
        (support_status = 'supported' AND unsupported_reason IS NULL)
        OR (
            support_status = 'unsupported'
            AND unsupported_reason IS NOT NULL
            AND btrim(unsupported_reason) <> ''
        )
    ),
    CHECK (jsonb_typeof(provenance) = 'object'),
    CHECK (jsonb_typeof(chain_positions) = 'object'),
    CHECK (jsonb_typeof(canonicality_summary) = 'object'),
    CHECK (manifest_version > 0)
);

CREATE TABLE IF NOT EXISTS record_inventory_current (
    resource_id uuid NOT NULL
        REFERENCES resources (resource_id),
    record_version_boundary_key text NOT NULL,
    record_version_boundary jsonb NOT NULL DEFAULT '{}'::jsonb,
    selectors jsonb NOT NULL DEFAULT '[]'::jsonb,
    unsupported_families jsonb NOT NULL DEFAULT '[]'::jsonb,
    last_change jsonb,
    entries jsonb NOT NULL DEFAULT '[]'::jsonb,
    support_status text NOT NULL,
    unsupported_reason text,
    provenance jsonb NOT NULL DEFAULT '{}'::jsonb,
    chain_positions jsonb NOT NULL DEFAULT '{}'::jsonb,
    canonicality_summary jsonb NOT NULL DEFAULT '{}'::jsonb,
    manifest_version bigint NOT NULL,
    last_recomputed_at timestamptz NOT NULL DEFAULT now(),
    inserted_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (resource_id, record_version_boundary_key),
    CHECK (btrim(record_version_boundary_key) <> ''),
    CHECK (jsonb_typeof(record_version_boundary) = 'object'),
    CHECK (jsonb_typeof(selectors) = 'array'),
    CHECK (jsonb_typeof(unsupported_families) = 'array'),
    CHECK (last_change IS NULL OR jsonb_typeof(last_change) = 'object'),
    CHECK (jsonb_typeof(entries) = 'array'),
    CHECK (support_status IN ('supported', 'unsupported')),
    CHECK (
        (support_status = 'supported' AND unsupported_reason IS NULL)
        OR (
            support_status = 'unsupported'
            AND unsupported_reason IS NOT NULL
            AND btrim(unsupported_reason) <> ''
        )
    ),
    CHECK (jsonb_typeof(provenance) = 'object'),
    CHECK (jsonb_typeof(chain_positions) = 'object'),
    CHECK (jsonb_typeof(canonicality_summary) = 'object'),
    CHECK (manifest_version > 0)
);

CREATE INDEX IF NOT EXISTS record_inventory_current_resolver_idx
    ON record_inventory_current (
        (provenance ->> 'chain_id'),
        lower(provenance ->> 'resolver_address'),
        resource_id
    )
    WHERE provenance ->> 'resolver_address' IS NOT NULL;

CREATE TABLE IF NOT EXISTS resolver_current (
    chain_id text NOT NULL,
    resolver_address text NOT NULL,
    declared_summary jsonb NOT NULL DEFAULT '{}'::jsonb,
    support_status text NOT NULL,
    unsupported_reason text,
    provenance jsonb NOT NULL DEFAULT '{}'::jsonb,
    chain_positions jsonb NOT NULL DEFAULT '{}'::jsonb,
    canonicality_summary jsonb NOT NULL DEFAULT '{}'::jsonb,
    manifest_version bigint NOT NULL,
    last_recomputed_at timestamptz NOT NULL DEFAULT now(),
    inserted_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (chain_id, resolver_address),
    CHECK (btrim(chain_id) <> ''),
    CHECK (btrim(resolver_address) <> ''),
    CHECK (jsonb_typeof(declared_summary) = 'object'),
    CHECK (support_status IN ('supported', 'unsupported')),
    CHECK (
        (support_status = 'supported' AND unsupported_reason IS NULL)
        OR (
            support_status = 'unsupported'
            AND unsupported_reason IS NOT NULL
            AND btrim(unsupported_reason) <> ''
        )
    ),
    CHECK (jsonb_typeof(provenance) = 'object'),
    CHECK (jsonb_typeof(chain_positions) = 'object'),
    CHECK (jsonb_typeof(canonicality_summary) = 'object'),
    CHECK (manifest_version > 0)
);

CREATE INDEX IF NOT EXISTS resolver_current_address_idx
    ON resolver_current (chain_id, lower(resolver_address));

CREATE TABLE IF NOT EXISTS address_names_current (
    address text NOT NULL,
    logical_name_id text NOT NULL
        REFERENCES name_surfaces (logical_name_id),
    relation text NOT NULL,
    namespace text NOT NULL,
    raw_name text NOT NULL,
    namehash text NOT NULL,
    surface_binding_id uuid NOT NULL
        REFERENCES surface_bindings (surface_binding_id),
    resource_id uuid NOT NULL
        REFERENCES resources (resource_id),
    token_lineage_id uuid
        REFERENCES token_lineages (token_lineage_id),
    binding_kind text NOT NULL,
    support_status text NOT NULL,
    unsupported_reason text,
    provenance jsonb NOT NULL DEFAULT '{}'::jsonb,
    chain_positions jsonb NOT NULL DEFAULT '{}'::jsonb,
    canonicality_summary jsonb NOT NULL DEFAULT '{}'::jsonb,
    manifest_version bigint NOT NULL,
    last_recomputed_at timestamptz NOT NULL DEFAULT now(),
    inserted_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (address, logical_name_id, relation),
    FOREIGN KEY (
        surface_binding_id,
        logical_name_id,
        resource_id,
        binding_kind
    ) REFERENCES surface_bindings (
        surface_binding_id,
        logical_name_id,
        resource_id,
        binding_kind
    ),
    FOREIGN KEY (resource_id, token_lineage_id)
        REFERENCES resources (resource_id, token_lineage_id),
    CHECK (btrim(address) <> ''),
    CONSTRAINT address_names_current_relation_check
        CHECK (
            relation IN (
                'registrant',
                'token_holder',
                'effective_controller'
            )
        ),
    CHECK (btrim(namespace) <> ''),
    CHECK (btrim(namehash) <> ''),
    CONSTRAINT address_names_current_logical_identity_check
        CHECK (logical_name_id = namespace || ':' || namehash),
    CONSTRAINT address_names_current_binding_kind_check
        CHECK (
            binding_kind IN (
                'declared_registry_path',
                'linked_subregistry_path',
                'resolver_alias_path',
                'observed_wildcard_path',
                'observed_only'
            )
        ),
    CHECK (token_lineage_id IS NULL OR resource_id IS NOT NULL),
    CHECK (support_status IN ('supported', 'unsupported')),
    CHECK (
        (support_status = 'supported' AND unsupported_reason IS NULL)
        OR (
            support_status = 'unsupported'
            AND unsupported_reason IS NOT NULL
            AND btrim(unsupported_reason) <> ''
        )
    ),
    CHECK (jsonb_typeof(provenance) = 'object'),
    CHECK (jsonb_typeof(chain_positions) = 'object'),
    CHECK (jsonb_typeof(canonicality_summary) = 'object'),
    CHECK (manifest_version > 0)
);

CREATE INDEX IF NOT EXISTS address_names_current_address_idx
    ON address_names_current (
        lower(address),
        relation,
        namespace,
        namehash,
        logical_name_id
    );

CREATE INDEX IF NOT EXISTS address_names_current_name_idx
    ON address_names_current (logical_name_id, relation, lower(address));

-- Reverse index over current `addr:<coin_type>` resolver records: one row per (address the
-- record resolves to, coin type, current name). Rows are derived from the published
-- record inventory of the name's record-serving resource; they answer "which names resolve to
-- this address" without re-deciding forward record values.
CREATE TABLE IF NOT EXISTS address_records_current (
    address text NOT NULL,
    coin_type text NOT NULL,
    logical_name_id text NOT NULL
        REFERENCES name_surfaces (logical_name_id),
    namespace text NOT NULL,
    raw_name text NOT NULL,
    namehash text NOT NULL,
    surface_binding_id uuid
        REFERENCES surface_bindings (surface_binding_id),
    resource_id uuid
        REFERENCES resources (resource_id),
    record_resource_id uuid NOT NULL
        REFERENCES resources (resource_id),
    binding_kind text,
    record_key text NOT NULL,
    support_status text NOT NULL,
    unsupported_reason text,
    provenance jsonb NOT NULL DEFAULT '{}'::jsonb,
    chain_positions jsonb NOT NULL DEFAULT '{}'::jsonb,
    canonicality_summary jsonb NOT NULL DEFAULT '{}'::jsonb,
    manifest_version bigint NOT NULL,
    last_recomputed_at timestamptz NOT NULL DEFAULT now(),
    inserted_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (address, coin_type, logical_name_id),
    CHECK (address = lower(address) AND address ~ '^0x[0-9a-f]{40}$'),
    CHECK (coin_type ~ '^[0-9]+$'),
    CHECK (btrim(namespace) <> ''),
    CHECK (btrim(namehash) <> ''),
    CONSTRAINT address_records_current_logical_identity_check
        CHECK (logical_name_id = namespace || ':' || namehash),
    CHECK (record_key = 'addr:' || coin_type OR record_key = 'addr:2147483648'),
    CHECK (support_status IN ('supported', 'unsupported')),
    CHECK (
        (support_status = 'supported' AND unsupported_reason IS NULL)
        OR (
            support_status = 'unsupported'
            AND unsupported_reason IS NOT NULL
            AND btrim(unsupported_reason) <> ''
        )
    ),
    CHECK (jsonb_typeof(provenance) = 'object'),
    CHECK (jsonb_typeof(chain_positions) = 'object'),
    CHECK (jsonb_typeof(canonicality_summary) = 'object'),
    CHECK (manifest_version > 0)
);

CREATE INDEX IF NOT EXISTS address_records_current_address_sort_idx
    ON address_records_current (address, coin_type, namespace, raw_name, logical_name_id);

CREATE INDEX IF NOT EXISTS address_records_current_name_idx
    ON address_records_current (logical_name_id);

CREATE INDEX IF NOT EXISTS address_records_current_resource_idx
    ON address_records_current (resource_id);

CREATE INDEX IF NOT EXISTS address_records_current_record_resource_idx
    ON address_records_current (record_resource_id);

COMMENT ON TABLE address_records_current IS
    'Project-owned reverse index over current addr:<coin_type> resolver records: one row per address a record resolves to, coin type, and current name. Rebuilt from record_inventory_current; not serving truth for forward record values.';
COMMENT ON COLUMN address_records_current.address IS
    'Lowercase EVM address stored by the selected address record.';
COMMENT ON COLUMN address_records_current.coin_type IS
    'Decimal coin type selected for reverse address-record membership.';
COMMENT ON COLUMN address_records_current.logical_name_id IS
    'Logical name identity selected by Project for this reverse membership.';
COMMENT ON COLUMN address_records_current.namespace IS
    'Namespace of the selected logical name.';
COMMENT ON COLUMN address_records_current.raw_name IS
    'Selected name text used for reverse address-record ordering.';
COMMENT ON COLUMN address_records_current.namehash IS
    'Namehash of the selected logical name.';
COMMENT ON COLUMN address_records_current.surface_binding_id IS
    'Binding selected by Project, absent when only a serving resource is known.';
COMMENT ON COLUMN address_records_current.resource_id IS
    'Registration resource referenced by the selected name binding, absent without authority.';
COMMENT ON COLUMN address_records_current.record_resource_id IS
    'Resource whose resolver record inventory supplies the address value.';
COMMENT ON COLUMN address_records_current.binding_kind IS
    'Kind of the selected name binding, absent without authority.';
COMMENT ON COLUMN address_records_current.record_key IS
    'Address record inventory key, including the default EVM key when used as a fallback.';
COMMENT ON COLUMN address_records_current.support_status IS
    'Whether the selected address record is supported for serving.';
COMMENT ON COLUMN address_records_current.unsupported_reason IS
    'Reason the selected address record is unsupported, or null for supported rows.';
COMMENT ON COLUMN address_records_current.provenance IS
    'Evidence for the selected name, resolver, and address record.';
COMMENT ON COLUMN address_records_current.chain_positions IS
    'Chain positions used by Project to rebuild this membership.';
COMMENT ON COLUMN address_records_current.canonicality_summary IS
    'Canonicality summary for the projected membership.';
COMMENT ON COLUMN address_records_current.manifest_version IS
    'Manifest version used to derive the membership.';
COMMENT ON COLUMN address_records_current.last_recomputed_at IS
    'Database timestamp when Project last rebuilt the membership.';
COMMENT ON COLUMN address_records_current.inserted_at IS
    'Database timestamp when this projection row was inserted.';

CREATE SEQUENCE IF NOT EXISTS reverse_hydration_attempt_ordinal_seq AS bigint;

CREATE TABLE IF NOT EXISTS primary_names_current (
    address text NOT NULL,
    coin_type text NOT NULL,
    namespace text NOT NULL,
    claim_status text NOT NULL DEFAULT 'unsupported',
    raw_claim_name text,
    claim_name_is_normalized boolean NOT NULL DEFAULT false,
    unsupported_reason text,
    claim_provenance jsonb NOT NULL DEFAULT '{}'::jsonb,
    reverse_hydration_attempted_block_number bigint,
    reverse_hydration_attempted_block_hash text,
    reverse_hydration_attempt_ordinal bigint,
    PRIMARY KEY (address, coin_type, namespace),
    CHECK (btrim(address) <> ''),
    CHECK (btrim(coin_type) <> ''),
    CHECK (btrim(namespace) <> ''),
    CHECK (
        claim_status IN (
            'success',
            'not_found',
            'unsupported',
            'invalid_name'
        )
    ),
    CONSTRAINT primary_names_current_claim_name_check CHECK (
        (
            claim_status IN ('success', 'invalid_name')
            AND raw_claim_name IS NOT NULL
            AND btrim(raw_claim_name) <> ''
        )
        OR (
            claim_status IN ('not_found', 'unsupported')
            AND raw_claim_name IS NULL
        )
    ),
    CONSTRAINT primary_names_current_normalized_claim_check
        CHECK (NOT claim_name_is_normalized OR claim_status = 'success'),
    CONSTRAINT primary_names_current_unsupported_reason_coherence_check CHECK (
        (claim_status = 'unsupported' AND unsupported_reason IS NOT NULL)
        OR (claim_status <> 'unsupported' AND unsupported_reason IS NULL)
    ),
    CHECK (
        unsupported_reason IS NULL
        OR btrim(unsupported_reason) <> ''
    ),
    CHECK (jsonb_typeof(claim_provenance) = 'object'),
    CONSTRAINT primary_names_current_reverse_hydration_attempt_check CHECK (
        (
            reverse_hydration_attempted_block_number IS NULL
            AND reverse_hydration_attempted_block_hash IS NULL
            AND reverse_hydration_attempt_ordinal IS NULL
        )
        OR (
            reverse_hydration_attempted_block_number IS NOT NULL
            AND reverse_hydration_attempted_block_number >= 0
            AND reverse_hydration_attempted_block_hash IS NOT NULL
            AND btrim(reverse_hydration_attempted_block_hash) <> ''
            AND reverse_hydration_attempt_ordinal IS NOT NULL
            AND reverse_hydration_attempt_ordinal > 0
        )
    )
);

CREATE INDEX IF NOT EXISTS primary_names_current_claim_idx
    ON primary_names_current (
        namespace,
        coin_type,
        address
    )
    WHERE claim_status = 'success';

CREATE INDEX IF NOT EXISTS primary_names_current_reverse_node_idx
    ON primary_names_current (
        (claim_provenance ->> 'chain_id'),
        lower(claim_provenance ->> 'reverse_node'),
        address,
        coin_type,
        namespace
    )
    WHERE claim_provenance ->> 'reverse_node' IS NOT NULL;

CREATE INDEX IF NOT EXISTS permissions_current_resource_wrapper_expiry_idx
    ON permissions_current_resource_summary (
        (provenance ->> 'chain_id'),
        ((provenance -> 'wrapper_expiry_boundary' ->> 'expiry_seconds')::numeric),
        resource_id
    )
    WHERE provenance ? 'wrapper_expiry_boundary';

CREATE INDEX IF NOT EXISTS permissions_current_resource_registry_binding_idx
    ON permissions_current_resource_summary (registry_contract, registry_owner, resource_id)
    WHERE registry_owner IS NOT NULL;

COMMENT ON TABLE name_current IS
    'This table stores the current product row for each visible name.';
COMMENT ON COLUMN name_current.logical_name_id IS
    'This value identifies the name.';
COMMENT ON COLUMN name_current.namespace IS
    'This value identifies the name system.';
COMMENT ON COLUMN name_current.raw_name IS
    'This value is the verbatim name.';
COMMENT ON COLUMN name_current.namehash IS
    'This value is the name hash.';
COMMENT ON COLUMN name_current.surface_binding_id IS
    'This value identifies the current name-to-authority link.';
COMMENT ON COLUMN name_current.resource_id IS
    'This value identifies the current authority object.';
COMMENT ON COLUMN name_current.serving_resource_id IS
    'This event-derived resource is used for resolver and record serving. It does not establish a current authority, registration, or surface binding.';
COMMENT ON COLUMN name_current.token_lineage_id IS
    'This value identifies the current token history.';
COMMENT ON COLUMN name_current.binding_kind IS
    'This value states the current link kind.';
COMMENT ON COLUMN name_current.declared_summary IS
    'This object stores the current declared state.';
COMMENT ON COLUMN name_current.support_status IS
    'This value states whether the name setup is supported.';
COMMENT ON COLUMN name_current.unsupported_reason IS
    'This value explains an unsupported name setup.';
COMMENT ON COLUMN name_current.provenance IS
    'This object identifies the source rows.';
COMMENT ON COLUMN name_current.chain_positions IS
    'This object identifies the selected chain positions.';
COMMENT ON COLUMN name_current.canonicality_summary IS
    'This object summarizes the selected block states.';
COMMENT ON COLUMN name_current.manifest_version IS
    'This value is the source manifest version.';
COMMENT ON COLUMN name_current.last_recomputed_at IS
    'This time records the latest rebuild.';
COMMENT ON COLUMN name_current.inserted_at IS
    'This time records row creation.';

COMMENT ON TABLE children_current IS
    'This table stores current direct children. Verbatim child-name and label bytes are present when a preimage was observed on chain and are null when only topology hashes are known.';
COMMENT ON COLUMN children_current.parent_logical_name_id IS
    'This value identifies the parent name.';
COMMENT ON COLUMN children_current.child_logical_name_id IS
    'This value identifies the child name.';
COMMENT ON COLUMN children_current.surface_class IS
    'This value states the child-link class.';
COMMENT ON COLUMN children_current.namespace IS
    'This value identifies the name system.';
COMMENT ON COLUMN children_current.raw_name IS
    'These bytes are the verbatim child name.';
COMMENT ON COLUMN children_current.decoded_name IS
    'This optional text is present only when it exactly decodes the raw name bytes.';
COMMENT ON COLUMN children_current.raw_label IS
    'These bytes are the verbatim child label.';
COMMENT ON COLUMN children_current.decoded_label IS
    'This optional text is present only when it exactly decodes the raw label bytes.';
COMMENT ON COLUMN children_current.namehash IS
    'This value is the child name hash.';
COMMENT ON COLUMN children_current.labelhash IS
    'This value is the child label hash.';
COMMENT ON COLUMN children_current.owner IS
    'This value is the current owner address.';
COMMENT ON COLUMN children_current.registrant IS
    'This value is the current registrant address.';
COMMENT ON COLUMN children_current.provenance IS
    'This object identifies the source rows.';
COMMENT ON COLUMN children_current.chain_positions IS
    'This object identifies the selected chain positions.';
COMMENT ON COLUMN children_current.canonicality_summary IS
    'This object summarizes the selected block states.';
COMMENT ON COLUMN children_current.manifest_version IS
    'This value is the source manifest version.';
COMMENT ON COLUMN children_current.last_recomputed_at IS
    'This time records the latest rebuild.';
COMMENT ON COLUMN children_current.inserted_at IS
    'This time records row creation.';

COMMENT ON TABLE permissions_current IS
    'This table stores current effective permissions by authority object.';
COMMENT ON COLUMN permissions_current.resource_id IS
    'This value identifies the authority object.';
COMMENT ON COLUMN permissions_current.subject IS
    'This value identifies the permission holder.';
COMMENT ON COLUMN permissions_current.scope IS
    'This value identifies the permission scope.';
COMMENT ON COLUMN permissions_current.scope_kind IS
    'This value states the scope kind.';
COMMENT ON COLUMN permissions_current.scope_detail IS
    'This object stores the scope fields.';
COMMENT ON COLUMN permissions_current.effective_powers IS
    'This array stores the effective powers.';
COMMENT ON COLUMN permissions_current.grant_source IS
    'This object identifies the current grant.';
COMMENT ON COLUMN permissions_current.revocation_source IS
    'This object identifies the latest revocation.';
COMMENT ON COLUMN permissions_current.inheritance_path IS
    'This array identifies inherited grants.';
COMMENT ON COLUMN permissions_current.transfer_behavior IS
    'This object states transfer effects.';
COMMENT ON COLUMN permissions_current.provenance IS
    'This object identifies the source rows.';
COMMENT ON COLUMN permissions_current.chain_positions IS
    'This object identifies the selected chain positions.';
COMMENT ON COLUMN permissions_current.canonicality_summary IS
    'This object summarizes the selected block states.';
COMMENT ON COLUMN permissions_current.manifest_version IS
    'This value is the source manifest version.';
COMMENT ON COLUMN permissions_current.last_recomputed_at IS
    'This time records the latest rebuild.';
COMMENT ON COLUMN permissions_current.inserted_at IS
    'This time records row creation.';

COMMENT ON TABLE permissions_current_resource_summary IS
    'This table stores permission support for each authority object.';
COMMENT ON COLUMN permissions_current_resource_summary.resource_id IS
    'This value identifies the authority object.';
COMMENT ON COLUMN permissions_current_resource_summary.authority_kind IS
    'This value states the authority kind.';
COMMENT ON COLUMN permissions_current_resource_summary.root_resource_id IS
    'This value identifies the registry root authority.';
COMMENT ON COLUMN permissions_current_resource_summary.registry_owner IS
    'This value identifies the proven current registry owner.';
COMMENT ON COLUMN permissions_current_resource_summary.registry_contract IS
    'This value identifies the registry that supplied the owner.';
COMMENT ON COLUMN permissions_current_resource_summary.registry_binding_provenance IS
    'This object identifies the registry-owner evidence.';
COMMENT ON COLUMN permissions_current_resource_summary.registry_binding_chain_positions IS
    'This object identifies the registry-owner chain position.';
COMMENT ON COLUMN permissions_current_resource_summary.resource_restrictions IS
    'The registration-level restriction block: NameWrapper state, expiry-effective fuses, and expiry, or ENSv2 locked roles.';
COMMENT ON COLUMN permissions_current_resource_summary.support_status IS
    'This value states whether permission reads are supported.';
COMMENT ON COLUMN permissions_current_resource_summary.unsupported_reason IS
    'This value explains unsupported permission reads.';
COMMENT ON COLUMN permissions_current_resource_summary.provenance IS
    'This object identifies the source rows.';
COMMENT ON COLUMN permissions_current_resource_summary.chain_positions IS
    'This object identifies the selected chain positions.';
COMMENT ON COLUMN permissions_current_resource_summary.canonicality_summary IS
    'This object summarizes the selected block states.';
COMMENT ON COLUMN permissions_current_resource_summary.manifest_version IS
    'This value is the source manifest version.';
COMMENT ON COLUMN permissions_current_resource_summary.last_recomputed_at IS
    'This time records the latest rebuild.';

COMMENT ON TABLE record_inventory_current IS
    'This table stores the current record selectors for each authority object.';
COMMENT ON COLUMN record_inventory_current.resource_id IS
    'This value identifies the authority object.';
COMMENT ON COLUMN record_inventory_current.record_version_boundary_key IS
    'This value identifies the resolver record version.';
COMMENT ON COLUMN record_inventory_current.record_version_boundary IS
    'This object stores the resolver record version.';
COMMENT ON COLUMN record_inventory_current.selectors IS
    'This array stores known record selectors.';
COMMENT ON COLUMN record_inventory_current.unsupported_families IS
    'This array stores unsupported record groups.';
COMMENT ON COLUMN record_inventory_current.last_change IS
    'This object identifies the latest record change.';
COMMENT ON COLUMN record_inventory_current.entries IS
    'This array stores current record entries.';
COMMENT ON COLUMN record_inventory_current.support_status IS
    'This value states whether record reads are supported.';
COMMENT ON COLUMN record_inventory_current.unsupported_reason IS
    'This value explains unsupported record reads.';
COMMENT ON COLUMN record_inventory_current.provenance IS
    'This object identifies the source rows.';
COMMENT ON COLUMN record_inventory_current.chain_positions IS
    'This object identifies the selected chain positions.';
COMMENT ON COLUMN record_inventory_current.canonicality_summary IS
    'This object summarizes the selected block states.';
COMMENT ON COLUMN record_inventory_current.manifest_version IS
    'This value is the source manifest version.';
COMMENT ON COLUMN record_inventory_current.last_recomputed_at IS
    'This time records the latest rebuild.';
COMMENT ON COLUMN record_inventory_current.inserted_at IS
    'This time records row creation.';

COMMENT ON TABLE resolver_current IS
    'This table stores the current product row for each resolver.';
COMMENT ON COLUMN resolver_current.chain_id IS
    'This value identifies the chain.';
COMMENT ON COLUMN resolver_current.resolver_address IS
    'This value is the resolver address.';
COMMENT ON COLUMN resolver_current.declared_summary IS
    'This object stores the current resolver state.';
COMMENT ON COLUMN resolver_current.support_status IS
    'This value states whether resolver reads are supported.';
COMMENT ON COLUMN resolver_current.unsupported_reason IS
    'This value explains unsupported resolver reads.';
COMMENT ON COLUMN resolver_current.provenance IS
    'This object identifies the source rows.';
COMMENT ON COLUMN resolver_current.chain_positions IS
    'This object identifies the selected chain positions.';
COMMENT ON COLUMN resolver_current.canonicality_summary IS
    'This object summarizes the selected block states.';
COMMENT ON COLUMN resolver_current.manifest_version IS
    'This value is the source manifest version.';
COMMENT ON COLUMN resolver_current.last_recomputed_at IS
    'This time records the latest rebuild.';
COMMENT ON COLUMN resolver_current.inserted_at IS
    'This time records row creation.';

COMMENT ON TABLE address_names_current IS
    'This table stores current address-to-name relations.';
COMMENT ON COLUMN address_names_current.address IS
    'This value is the related address.';
COMMENT ON COLUMN address_names_current.logical_name_id IS
    'This value identifies the name.';
COMMENT ON COLUMN address_names_current.relation IS
    'This value states the address relation.';
COMMENT ON COLUMN address_names_current.namespace IS
    'This value identifies the name system.';
COMMENT ON COLUMN address_names_current.raw_name IS
    'This value is the verbatim name.';
COMMENT ON COLUMN address_names_current.namehash IS
    'This value is the name hash.';
COMMENT ON COLUMN address_names_current.surface_binding_id IS
    'This value identifies the name-to-authority link.';
COMMENT ON COLUMN address_names_current.resource_id IS
    'This value identifies the authority object.';
COMMENT ON COLUMN address_names_current.token_lineage_id IS
    'This value identifies the token history.';
COMMENT ON COLUMN address_names_current.binding_kind IS
    'This value states the name-to-authority link kind.';
COMMENT ON COLUMN address_names_current.support_status IS
    'This value states whether the name setup is supported.';
COMMENT ON COLUMN address_names_current.unsupported_reason IS
    'This value explains an unsupported name setup.';
COMMENT ON COLUMN address_names_current.provenance IS
    'This object identifies the source rows.';
COMMENT ON COLUMN address_names_current.chain_positions IS
    'This object identifies the selected chain positions.';
COMMENT ON COLUMN address_names_current.canonicality_summary IS
    'This object summarizes the selected block states.';
COMMENT ON COLUMN address_names_current.manifest_version IS
    'This value is the source manifest version.';
COMMENT ON COLUMN address_names_current.last_recomputed_at IS
    'This time records the latest rebuild.';
COMMENT ON COLUMN address_names_current.inserted_at IS
    'This time records row creation.';

COMMENT ON TABLE primary_names_current IS
    'This table stores the current primary-name claim for each address.';
COMMENT ON COLUMN primary_names_current.address IS
    'This value is the claimed address.';
COMMENT ON COLUMN primary_names_current.coin_type IS
    'This value identifies the address format.';
COMMENT ON COLUMN primary_names_current.namespace IS
    'This value identifies the name system.';
COMMENT ON COLUMN primary_names_current.claim_status IS
    'This value states the claim result.';
COMMENT ON COLUMN primary_names_current.raw_claim_name IS
    'This value is the verbatim claimed name.';
COMMENT ON COLUMN primary_names_current.claim_name_is_normalized IS
    'This flag states whether the raw claim passes normalization.';
COMMENT ON COLUMN primary_names_current.unsupported_reason IS
    'This value explains an unsupported claim.';
COMMENT ON COLUMN primary_names_current.claim_provenance IS
    'This object identifies the claim source.';
COMMENT ON COLUMN primary_names_current.reverse_hydration_attempted_block_number IS
    'This internal reverse-name polling selection value identifies the head height of the latest attempt. Readers never use it as serving data.';
COMMENT ON COLUMN primary_names_current.reverse_hydration_attempted_block_hash IS
    'This internal reverse-name polling selection value identifies the head hash of the latest attempt. Readers never use it as serving data.';
COMMENT ON COLUMN primary_names_current.reverse_hydration_attempt_ordinal IS
    'This internal value orders reverse-name polling attempts for fair rolling selection. It never records or validates a provider result.';

COMMENT ON SEQUENCE reverse_hydration_attempt_ordinal_seq IS
    'This sequence assigns durable order to reverse-name polling batches; its values are not serving data.';

COMMENT ON INDEX name_current_lookup_idx IS
    'This bounded index supports namespace and name identity lookup by name hash. Verbatim names remain unbounded payload and are not btree-indexed.';
COMMENT ON INDEX children_current_parent_idx IS
    'This bounded index supports direct-child enumeration by parent, surface class, and child name hash. Verbatim child names and labels remain unbounded payload.';
COMMENT ON INDEX address_names_current_address_idx IS
    'This bounded index supports address relation reads by namespace and name hash. Verbatim names remain unbounded payload and are not btree-indexed.';
COMMENT ON INDEX primary_names_current_claim_idx IS
    'This bounded partial index supports successful-claim scans by namespace, coin type, and address. The verbatim claim is returned payload, not an index key.';

CREATE TABLE IF NOT EXISTS child_registration_events (
    parent_logical_name_id text NOT NULL,
    event_identity text NOT NULL,
    child_logical_name_id text NOT NULL,
    namespace text NOT NULL,
    chain_id text NOT NULL,
    block_number bigint NOT NULL,
    block_hash text NOT NULL,
    transaction_order_key bigint NOT NULL,
    log_order_key bigint NOT NULL,
    event_kind text NOT NULL,
    manifest_version bigint NOT NULL,
    provenance jsonb NOT NULL DEFAULT '{}'::jsonb,
    target_block_number bigint NOT NULL,
    target_block_hash text NOT NULL,
    last_recomputed_at timestamptz NOT NULL DEFAULT now(),
    inserted_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (parent_logical_name_id, event_identity),
    CHECK (parent_logical_name_id <> child_logical_name_id),
    CHECK (btrim(namespace) <> ''),
    CONSTRAINT child_registration_events_same_namespace_check
        CHECK (
            starts_with(parent_logical_name_id, namespace || ':')
            AND starts_with(child_logical_name_id, namespace || ':')
        ),
    CHECK (btrim(event_identity) <> ''),
    CHECK (btrim(chain_id) <> ''),
    CHECK (block_number >= 0),
    CHECK (btrim(block_hash) <> ''),
    CHECK (transaction_order_key >= -1),
    CHECK (log_order_key >= -1),
    CHECK (event_kind IN ('RegistrationGranted', 'LabelRegistered')),
    CHECK (manifest_version >= 0),
    CHECK (jsonb_typeof(provenance) = 'object'),
    CHECK (target_block_number >= block_number),
    CHECK (btrim(target_block_hash) <> '')
);

CREATE INDEX IF NOT EXISTS child_registration_events_parent_history_idx
    ON child_registration_events (
        parent_logical_name_id,
        chain_id,
        block_number,
        block_hash,
        transaction_order_key,
        log_order_key,
        event_identity
    );

CREATE INDEX IF NOT EXISTS child_registration_events_chain_block_idx
    ON child_registration_events (chain_id, block_number);

COMMENT ON TABLE child_registration_events IS
    'Project-owned historical membership of direct child registration events: one row per parent name and registration event of a name exactly one label below it. Rebuilt from canonical normalized events and name surfaces; event payloads stay in normalized_events.';
COMMENT ON COLUMN child_registration_events.parent_logical_name_id IS
    'This value identifies the parent name: the event namespace and the namehash of the child surface labels without its first label.';
COMMENT ON COLUMN child_registration_events.event_identity IS
    'This value identifies the registration event in normalized_events; name history joins the event by it.';
COMMENT ON COLUMN child_registration_events.child_logical_name_id IS
    'This value identifies the child name the event carried when it happened.';
COMMENT ON COLUMN child_registration_events.namespace IS
    'This value identifies the name system shared by the parent and the child.';
COMMENT ON COLUMN child_registration_events.chain_id IS
    'This value identifies the chain of the event and of the child surface.';
COMMENT ON COLUMN child_registration_events.block_number IS
    'This value is the event block height, the first history order key.';
COMMENT ON COLUMN child_registration_events.block_hash IS
    'This value is the event block hash, a history order key and the readable-lineage check.';
COMMENT ON COLUMN child_registration_events.transaction_order_key IS
    'This value is the event transaction index in its block, or -1 when the event has none, so it orders as history orders a missing index.';
COMMENT ON COLUMN child_registration_events.log_order_key IS
    'This value is the event log index, or -1 when the event has none, so it orders as history orders a missing index.';
COMMENT ON COLUMN child_registration_events.event_kind IS
    'This value is the stored registration kind of the event.';
COMMENT ON COLUMN child_registration_events.manifest_version IS
    'This value records the manifest version that admitted the event.';
COMMENT ON COLUMN child_registration_events.provenance IS
    'This object cites the normalized event row and source family the membership was derived from.';
COMMENT ON COLUMN child_registration_events.target_block_number IS
    'This value identifies the Project target height of the publication that wrote the row.';
COMMENT ON COLUMN child_registration_events.target_block_hash IS
    'This value identifies the Project target hash of the publication that wrote the row.';
COMMENT ON COLUMN child_registration_events.last_recomputed_at IS
    'This Project-owned maintenance time records the latest rebuild of the row.';
COMMENT ON COLUMN child_registration_events.inserted_at IS
    'This Project-owned maintenance time records the first insertion of the row.';
COMMENT ON INDEX child_registration_events_parent_history_idx IS
    'This bounded index serves one parent''s child registrations in history order on one chain, in both directions. Every key is a bounded identifier, hash or number.';
COMMENT ON INDEX child_registration_events_chain_block_idx IS
    'This bounded index lets Project replace one chain''s rows by block range.';

-- Owned key families (TYR-36 step 2): Project-owned shadow tables filled block by block after
-- each batch commits and read by no served path yet. docs/projections.md, "Owned key families".

CREATE TABLE IF NOT EXISTS project_family_marker (
    chain_id text NOT NULL,
    current_block_number bigint,
    current_block_hash text,
    block_timestamp timestamptz,
    input_content_hash text,
    sequence bigint NOT NULL DEFAULT 0,
    interpret_input_content_hash text,
    interpret_redo_attempt bigint,
    state text NOT NULL,
    PRIMARY KEY (chain_id),
    CHECK ((current_block_number IS NULL) = (current_block_hash IS NULL)),
    CHECK (state IN ('live', 'bootstrap_pending')),
    CHECK (sequence >= 0)
);
COMMENT ON TABLE project_family_marker IS
    'Project-owned shadow marker of the owned key families: the last block the family loop applied on each chain, the generation every family block and family undo advances, and the input revision it read. It is not the served marker; chain_phase_state keeps that role. Step 2 of TYR-36 writes it block by block beside the served tables and nothing reads it yet.';
COMMENT ON COLUMN project_family_marker.chain_id IS
    'This value is the chain the marker belongs to.';
COMMENT ON COLUMN project_family_marker.current_block_number IS
    'This value is the last block whose facts the families hold; null before the first block.';
COMMENT ON COLUMN project_family_marker.current_block_hash IS
    'This value is the readable hash that block had when it was applied.';
COMMENT ON COLUMN project_family_marker.block_timestamp IS
    'This value is that block''s timestamp from chain_lineage, the block clock the family reads will use.';
COMMENT ON COLUMN project_family_marker.input_content_hash IS
    'This value is the interpreter content hash of the binary that applied the block.';
COMMENT ON COLUMN project_family_marker.sequence IS
    'This value counts every family block and every family undo applied on the chain; it only grows. It is the explicit publication generation of the design, named so because schema-v2 reserves generation for authorised columns.';
COMMENT ON COLUMN project_family_marker.interpret_input_content_hash IS
    'This value is the Interpret row''s input_content_hash read before the block, the first half of the input revision; null while Interpret was in redo.';
COMMENT ON COLUMN project_family_marker.interpret_redo_attempt IS
    'This value is the Interpret row''s redo_attempt_generation read before the block, the second half of the input revision; null while Interpret was in redo.';
COMMENT ON COLUMN project_family_marker.state IS
    'This value is live when the marker follows the served publication and bootstrap_pending while a rebuild is populating the families.';

CREATE TABLE IF NOT EXISTS project_family_undo (
    chain_id text NOT NULL,
    block_number bigint NOT NULL,
    block_hash text NOT NULL,
    family text NOT NULL,
    key text NOT NULL,
    before_image jsonb,
    PRIMARY KEY (chain_id, block_number, family, key),
    CHECK (btrim(block_hash) <> '')
);
COMMENT ON TABLE project_family_undo IS
    'Project-owned undo record of the owned key families: per applied block, the image each family row had before the block first changed it, plus the prior marker under family marker. Undoing a block restores these images; rows below the retained depth are pruned as the marker advances. Step 2 of TYR-36 writes it block by block beside the served tables and nothing reads it yet.';
COMMENT ON COLUMN project_family_undo.chain_id IS
    'This value is the chain of the block.';
COMMENT ON COLUMN project_family_undo.block_number IS
    'This value is the block whose change the row undoes.';
COMMENT ON COLUMN project_family_undo.block_hash IS
    'This value is the readable hash the block had when it was applied.';
COMMENT ON COLUMN project_family_undo.family IS
    'This value names the family table of the row, or marker for the prior family marker.';
COMMENT ON COLUMN project_family_undo.key IS
    'This value is the row''s primary key as a JSON array in key column order, or the chain id for the marker.';
COMMENT ON COLUMN project_family_undo.before_image IS
    'This value is to_jsonb of the row before the block, or null when the row did not exist.';

CREATE TABLE IF NOT EXISTS project_repair_record (
    chain_id text NOT NULL,
    attempt bigint NOT NULL,
    reason text NOT NULL,
    trusted_base_number bigint,
    trusted_base_hash text,
    replay_target_number bigint NOT NULL,
    replay_target_hash text NOT NULL,
    state text NOT NULL,
    prefix_interpret_input_content_hash text,
    prefix_interpret_redo_attempt bigint,
    invalidation_from bigint,
    pending_undo_target bigint,
    completed_sequence bigint,
    completed_marker_number bigint,
    completed_marker_hash text,
    completed_input_hash text,
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (chain_id),
    CHECK (reason IN ('required_redo_range', 'orphaned_lineage', 'content_hash_rebuild', 'operator_redo')),
    CHECK (state IN ('undoing', 'replaying', 'rebuilding', 'complete')),
    CHECK ((state = 'complete') = (completed_sequence IS NOT NULL AND completed_marker_number IS NOT NULL AND completed_marker_hash IS NOT NULL AND completed_input_hash IS NOT NULL)),
    CHECK (state = 'complete' OR (completed_sequence IS NULL AND completed_marker_number IS NULL AND completed_marker_hash IS NULL AND completed_input_hash IS NULL)),
    CHECK (state <> 'undoing' OR (prefix_interpret_input_content_hash IS NULL AND prefix_interpret_redo_attempt IS NULL)),
    CHECK ((trusted_base_number IS NULL) = (trusted_base_hash IS NULL)),
    CHECK (state <> 'rebuilding' OR trusted_base_number IS NULL)
);
COMMENT ON TABLE project_repair_record IS
    'Project-owned repair record: the durable description of the latest family undo-then-replay or rebuild of a chain, its attempt, reason, trusted base, replay target, state, input revision and completion identity. Undo never rewrites it. Step 2 of TYR-36 writes it block by block beside the served tables and nothing reads it yet.';
COMMENT ON COLUMN project_repair_record.chain_id IS
    'This value is the chain under repair.';
COMMENT ON COLUMN project_repair_record.attempt IS
    'This value is the Project row''s redo_attempt_generation when the repair began.';
COMMENT ON COLUMN project_repair_record.reason IS
    'This value says why the families are repaired: a required redo range, an orphaned lineage, a content-hash rebuild or an operator redo.';
COMMENT ON COLUMN project_repair_record.trusted_base_number IS
    'This value is the block below the repaired range whose facts stay; null for a rebuild.';
COMMENT ON COLUMN project_repair_record.trusted_base_hash IS
    'This value is the trusted base''s readable hash.';
COMMENT ON COLUMN project_repair_record.replay_target_number IS
    'This value is the block the replay must reach, captured before the first undo.';
COMMENT ON COLUMN project_repair_record.replay_target_hash IS
    'This value is the replay target''s hash when captured.';
COMMENT ON COLUMN project_repair_record.state IS
    'This value is undoing, replaying, rebuilding or complete.';
COMMENT ON COLUMN project_repair_record.prefix_interpret_input_content_hash IS
    'This value is the Interpret input_content_hash of the input revision the replay started from; null while undoing.';
COMMENT ON COLUMN project_repair_record.prefix_interpret_redo_attempt IS
    'This value is the Interpret redo_attempt_generation of that input revision; null while undoing.';
COMMENT ON COLUMN project_repair_record.invalidation_from IS
    'This value is the lowest block a stamp invalidated while the repair was active; step 2 never sets it.';
COMMENT ON COLUMN project_repair_record.pending_undo_target IS
    'This value is the block the undo must reach before replay may start; null once replay starts.';
COMMENT ON COLUMN project_repair_record.completed_sequence IS
    'This value is the family marker sequence the completing block produced; null until complete.';
COMMENT ON COLUMN project_repair_record.completed_marker_number IS
    'This value is the family marker block when the repair completed; null until complete.';
COMMENT ON COLUMN project_repair_record.completed_marker_hash IS
    'This value is the family marker hash when the repair completed; null until complete.';
COMMENT ON COLUMN project_repair_record.completed_input_hash IS
    'This value is the interpreter content hash the completing loop ran under; null until complete.';
COMMENT ON COLUMN project_repair_record.updated_at IS
    'This value is when the record last changed.';

CREATE TABLE IF NOT EXISTS project_name_state (
    namespace text NOT NULL,
    logical_name_id text NOT NULL,
    chain_id text NOT NULL,
    block_number bigint NOT NULL,
    transaction_index bigint,
    log_index bigint,
    event_identity text NOT NULL,
    normalized_event_id bigint,
    migration_path text,
    migration_evidence jsonb,
    migration_position jsonb,
    migrated_at timestamptz,
    authority_start_positions jsonb NOT NULL DEFAULT '{}'::jsonb,
    PRIMARY KEY (namespace, logical_name_id),
    CHECK ((transaction_index IS NULL) = (log_index IS NULL))
);
COMMENT ON TABLE project_name_state IS
    'Project-owned name facts of family F1: the latest MigrationApplied of a name and the latest authority epoch start per authority arm (docs/projections.md, Owned key families). Step 2 of TYR-36 writes it block by block beside the served tables and nothing reads it yet.';
COMMENT ON COLUMN project_name_state.namespace IS
    'This value is the name''s namespace.';
COMMENT ON COLUMN project_name_state.logical_name_id IS
    'This value identifies the name.';
COMMENT ON COLUMN project_name_state.chain_id IS
    'This value is the chain whose events wrote the row.';
COMMENT ON COLUMN project_name_state.block_number IS
    'This value is the block number of the event that last wrote the row.';
COMMENT ON COLUMN project_name_state.transaction_index IS
    'This value is the transaction index of the event that last wrote the row; null with log_index for a synthesised event, which sorts before every transaction of its block.';
COMMENT ON COLUMN project_name_state.log_index IS
    'This value is the log index of the event that last wrote the row; null with transaction_index for a synthesised event.';
COMMENT ON COLUMN project_name_state.event_identity IS
    'This value is the event identity of the event that last wrote the row, the final tiebreak of the canonical event order, compared as bytes.';
COMMENT ON COLUMN project_name_state.normalized_event_id IS
    'This value names the event that last wrote the row in normalized_events as attribution only; it never takes part in ordering.';
COMMENT ON COLUMN project_name_state.migration_path IS
    'This value is the migration_path of the name''s latest MigrationApplied, as children.rs reads it; served as history, never a gate.';
COMMENT ON COLUMN project_name_state.migration_evidence IS
    'This value is that event''s evidence array.';
COMMENT ON COLUMN project_name_state.migration_position IS
    'This value is that event''s position as a JSON object of the four position fields.';
COMMENT ON COLUMN project_name_state.migrated_at IS
    'This value is that event''s block timestamp.';
COMMENT ON COLUMN project_name_state.authority_start_positions IS
    'This value maps each authority arm to the position of the name''s latest AuthorityEpochChanged in that arm.';

CREATE TABLE IF NOT EXISTS project_binding_candidate (
    surface_binding_id uuid NOT NULL,
    logical_name_id text NOT NULL,
    namespace text NOT NULL,
    chain_id text NOT NULL,
    authority_arm text NOT NULL,
    resource_id uuid NOT NULL,
    binding_kind text NOT NULL,
    canonicality_state text NOT NULL,
    active_from timestamptz,
    surface_namehash text,
    block_number bigint NOT NULL,
    transaction_index bigint,
    log_index bigint,
    event_identity text NOT NULL,
    normalized_event_id bigint,
    state_derived boolean,
    authority_kind text,
    registry_only boolean NOT NULL DEFAULT false,
    predecessor_resource_id uuid,
    predecessor_position jsonb,
    lease_resource_id uuid,
    lease_position jsonb,
    wrapped_registrar_resource_id uuid,
    node text,
    transaction_hash text,
    emitting_address text,
    surface_bound_position jsonb,
    PRIMARY KEY (surface_binding_id),
    CHECK ((transaction_index IS NULL) = (log_index IS NULL))
);
COMMENT ON TABLE project_binding_candidate IS
    'Project-owned binding candidates of family F1: every surface binding of a name, selected or not, with the registry-only handoff facts and the wrapper facts the authority admission reads at publication. Step 2 of TYR-36 writes it block by block beside the served tables and nothing reads it yet.';
COMMENT ON COLUMN project_binding_candidate.surface_binding_id IS
    'This value identifies the surface binding.';
COMMENT ON COLUMN project_binding_candidate.logical_name_id IS
    'This value is the bound name.';
COMMENT ON COLUMN project_binding_candidate.namespace IS
    'This value is the name''s namespace.';
COMMENT ON COLUMN project_binding_candidate.chain_id IS
    'This value is the binding''s chain.';
COMMENT ON COLUMN project_binding_candidate.authority_arm IS
    'This value is the binding''s authority arm.';
COMMENT ON COLUMN project_binding_candidate.resource_id IS
    'This value is the bound resource.';
COMMENT ON COLUMN project_binding_candidate.binding_kind IS
    'This value is the binding kind.';
COMMENT ON COLUMN project_binding_candidate.canonicality_state IS
    'This value is the binding row''s canonicality when the block applied it.';
COMMENT ON COLUMN project_binding_candidate.active_from IS
    'This value is the binding''s active_from.';
COMMENT ON COLUMN project_binding_candidate.surface_namehash IS
    'This value is the lower-cased namehash of the bound surface, which the direct-binding pass compares with a registrar event''s namehash.';
COMMENT ON COLUMN project_binding_candidate.block_number IS
    'This value is the block number of the binding row: its block and the transaction and log index of its provenance; event_identity is the surface binding id, the tiebreak stage.rs uses.';
COMMENT ON COLUMN project_binding_candidate.transaction_index IS
    'This value is the transaction index of the binding row: its block and the transaction and log index of its provenance; event_identity is the surface binding id, the tiebreak stage.rs uses; null with log_index for a synthesised event, which sorts before every transaction of its block.';
COMMENT ON COLUMN project_binding_candidate.log_index IS
    'This value is the log index of the binding row: its block and the transaction and log index of its provenance; event_identity is the surface binding id, the tiebreak stage.rs uses; null with transaction_index for a synthesised event.';
COMMENT ON COLUMN project_binding_candidate.event_identity IS
    'This value is the event identity of the binding row: its block and the transaction and log index of its provenance; event_identity is the surface binding id, the tiebreak stage.rs uses, the final tiebreak of the canonical event order, compared as bytes.';
COMMENT ON COLUMN project_binding_candidate.normalized_event_id IS
    'This value names the binding row: its block and the transaction and log index of its provenance; event_identity is the surface binding id, the tiebreak stage.rs uses in normalized_events as attribution only; it never takes part in ordering.';
COMMENT ON COLUMN project_binding_candidate.state_derived IS
    'This value is the state_derived flag of the latest SurfaceBound for this name and resource.';
COMMENT ON COLUMN project_binding_candidate.authority_kind IS
    'This value is the authority_kind of that SurfaceBound.';
COMMENT ON COLUMN project_binding_candidate.registry_only IS
    'This value is true when an AuthorityEpochChanged registry_only was seen on this name and resource.';
COMMENT ON COLUMN project_binding_candidate.predecessor_resource_id IS
    'This value is the resource of the latest same-arm candidate positioned before a registry-only binding.';
COMMENT ON COLUMN project_binding_candidate.predecessor_position IS
    'This value is that predecessor candidate''s position.';
COMMENT ON COLUMN project_binding_candidate.lease_resource_id IS
    'This value is the successor registrar lease a registry-only binding recorded.';
COMMENT ON COLUMN project_binding_candidate.lease_position IS
    'This value is the position of the grant that created that lease.';
COMMENT ON COLUMN project_binding_candidate.wrapped_registrar_resource_id IS
    'This value is the registrar lease a wrapper SurfaceBound for this name and resource recorded.';
COMMENT ON COLUMN project_binding_candidate.node IS
    'This value is the lower-cased node of that wrapper SurfaceBound.';
COMMENT ON COLUMN project_binding_candidate.transaction_hash IS
    'This value is that wrapper SurfaceBound''s transaction hash.';
COMMENT ON COLUMN project_binding_candidate.emitting_address IS
    'This value is the lower-cased address that emitted that wrapper SurfaceBound.';
COMMENT ON COLUMN project_binding_candidate.surface_bound_position IS
    'This value is the position of the latest SurfaceBound that set the flags and wrapper facts above.';

CREATE TABLE IF NOT EXISTS project_lifecycle_key_state (
    chain_id text NOT NULL,
    resource_id uuid NOT NULL,
    logical_name_id text,
    block_number bigint NOT NULL,
    transaction_index bigint,
    log_index bigint,
    event_identity text NOT NULL,
    normalized_event_id bigint,
    last_grant jsonb,
    last_reservation jsonb,
    last_active jsonb,
    last_release_any jsonb,
    last_path_expiry jsonb,
    last_explicit_release jsonb,
    last_renewal jsonb,
    last_revival jsonb,
    last_expiry_changed jsonb,
    PRIMARY KEY (chain_id, resource_id),
    CHECK ((transaction_index IS NULL) = (log_index IS NULL))
);
COMMENT ON TABLE project_lifecycle_key_state IS
    'Project-owned lifecycle state of family F2a per resource: membership-only maxima over the resource''s own lifecycle events in the canonical event order. Step 2 of TYR-36 writes it block by block beside the served tables and nothing reads it yet.';
COMMENT ON COLUMN project_lifecycle_key_state.chain_id IS
    'This value is the chain.';
COMMENT ON COLUMN project_lifecycle_key_state.resource_id IS
    'This value is the lifecycle key, a resource.';
COMMENT ON COLUMN project_lifecycle_key_state.logical_name_id IS
    'This value is the name of the resource''s latest named lifecycle event.';
COMMENT ON COLUMN project_lifecycle_key_state.block_number IS
    'This value is the block number of the event that last wrote the row.';
COMMENT ON COLUMN project_lifecycle_key_state.transaction_index IS
    'This value is the transaction index of the event that last wrote the row; null with log_index for a synthesised event, which sorts before every transaction of its block.';
COMMENT ON COLUMN project_lifecycle_key_state.log_index IS
    'This value is the log index of the event that last wrote the row; null with transaction_index for a synthesised event.';
COMMENT ON COLUMN project_lifecycle_key_state.event_identity IS
    'This value is the event identity of the event that last wrote the row, the final tiebreak of the canonical event order, compared as bytes.';
COMMENT ON COLUMN project_lifecycle_key_state.normalized_event_id IS
    'This value names the event that last wrote the row in normalized_events as attribution only; it never takes part in ordering.';
COMMENT ON COLUMN project_lifecycle_key_state.last_grant IS
    'This value holds the latest RegistrationGranted: position, registrant, expiry, authority_kind, status and the registered_at source.';
COMMENT ON COLUMN project_lifecycle_key_state.last_reservation IS
    'This value holds the latest RegistrationReserved: position, registrant, expiry and status.';
COMMENT ON COLUMN project_lifecycle_key_state.last_active IS
    'This value holds the kind and position of the later of last_grant and last_reservation.';
COMMENT ON COLUMN project_lifecycle_key_state.last_release_any IS
    'This value holds the position of the latest RegistrationReleased of any kind.';
COMMENT ON COLUMN project_lifecycle_key_state.last_path_expiry IS
    'This value holds the latest path-expiry release (RegistryPathExpired, interpreter_state, registry_name_binding_expired): position, released_at, expiry, source_event, derived_from, terminal_reason.';
COMMENT ON COLUMN project_lifecycle_key_state.last_explicit_release IS
    'This value holds the latest release that is not a path expiry: position and released_at; witnessing is computed at read.';
COMMENT ON COLUMN project_lifecycle_key_state.last_renewal IS
    'This value holds the latest RegistrationRenewed: position, expiry and revived_from_expiry.';
COMMENT ON COLUMN project_lifecycle_key_state.last_revival IS
    'This value holds the position of the latest RegistrationRenewed with revived_from_expiry applied after this key''s own path-expiry release; raw-resource domain, never merged.';
COMMENT ON COLUMN project_lifecycle_key_state.last_expiry_changed IS
    'This value holds the position of the latest ExpiryChanged; it feeds the five-kind selection only.';

CREATE TABLE IF NOT EXISTS project_lifecycle_triple_summary (
    chain_id text NOT NULL,
    logical_name_id text NOT NULL,
    registry_identifier text NOT NULL,
    token_id text NOT NULL,
    block_number bigint NOT NULL,
    transaction_index bigint,
    log_index bigint,
    event_identity text NOT NULL,
    normalized_event_id bigint,
    last_grant jsonb,
    last_reservation jsonb,
    last_active jsonb,
    last_release_any jsonb,
    last_path_expiry jsonb,
    last_explicit_release jsonb,
    last_renewal jsonb,
    last_expiry_changed jsonb,
    PRIMARY KEY (chain_id, logical_name_id, registry_identifier, token_id),
    CHECK ((transaction_index IS NULL) = (log_index IS NULL))
);
COMMENT ON TABLE project_lifecycle_triple_summary IS
    'Project-owned lifecycle state of family F2a per (name, registry, token) triple: the same maxima over the triple''s null-resource ENSv2 lifecycle events only; a read merges it into the resource its association row targets. Step 2 of TYR-36 writes it block by block beside the served tables and nothing reads it yet.';
COMMENT ON COLUMN project_lifecycle_triple_summary.chain_id IS
    'This value is the chain.';
COMMENT ON COLUMN project_lifecycle_triple_summary.logical_name_id IS
    'This value is the name of the triple.';
COMMENT ON COLUMN project_lifecycle_triple_summary.registry_identifier IS
    'This value is COALESCE(registry_contract_instance_id, emitting address, registry) of the triple''s events.';
COMMENT ON COLUMN project_lifecycle_triple_summary.token_id IS
    'This value is the triple''s token id; empty text when the events carry none.';
COMMENT ON COLUMN project_lifecycle_triple_summary.block_number IS
    'This value is the block number of the event that last wrote the row.';
COMMENT ON COLUMN project_lifecycle_triple_summary.transaction_index IS
    'This value is the transaction index of the event that last wrote the row; null with log_index for a synthesised event, which sorts before every transaction of its block.';
COMMENT ON COLUMN project_lifecycle_triple_summary.log_index IS
    'This value is the log index of the event that last wrote the row; null with transaction_index for a synthesised event.';
COMMENT ON COLUMN project_lifecycle_triple_summary.event_identity IS
    'This value is the event identity of the event that last wrote the row, the final tiebreak of the canonical event order, compared as bytes.';
COMMENT ON COLUMN project_lifecycle_triple_summary.normalized_event_id IS
    'This value names the event that last wrote the row in normalized_events as attribution only; it never takes part in ordering.';
COMMENT ON COLUMN project_lifecycle_triple_summary.last_grant IS
    'This value holds the latest RegistrationGranted: position, registrant, expiry, authority_kind, status and the registered_at source.';
COMMENT ON COLUMN project_lifecycle_triple_summary.last_reservation IS
    'This value holds the latest RegistrationReserved: position, registrant, expiry and status.';
COMMENT ON COLUMN project_lifecycle_triple_summary.last_active IS
    'This value holds the kind and position of the later of last_grant and last_reservation.';
COMMENT ON COLUMN project_lifecycle_triple_summary.last_release_any IS
    'This value holds the position of the latest RegistrationReleased of any kind.';
COMMENT ON COLUMN project_lifecycle_triple_summary.last_path_expiry IS
    'This value holds the latest path-expiry release (RegistryPathExpired, interpreter_state, registry_name_binding_expired): position, released_at, expiry, source_event, derived_from, terminal_reason.';
COMMENT ON COLUMN project_lifecycle_triple_summary.last_explicit_release IS
    'This value holds the latest release that is not a path expiry: position and released_at; witnessing is computed at read.';
COMMENT ON COLUMN project_lifecycle_triple_summary.last_renewal IS
    'This value holds the latest RegistrationRenewed: position, expiry and revived_from_expiry.';
COMMENT ON COLUMN project_lifecycle_triple_summary.last_expiry_changed IS
    'This value holds the position of the latest ExpiryChanged; it feeds the five-kind selection only.';

CREATE TABLE IF NOT EXISTS project_lifecycle_association (
    chain_id text NOT NULL,
    logical_name_id text NOT NULL,
    registry_identifier text NOT NULL,
    token_id text NOT NULL,
    target_resource_id uuid NOT NULL,
    event_kind text NOT NULL,
    block_number bigint NOT NULL,
    transaction_index bigint,
    log_index bigint,
    event_identity text NOT NULL,
    normalized_event_id bigint,
    PRIMARY KEY (chain_id, logical_name_id, registry_identifier, token_id),
    CHECK ((transaction_index IS NULL) = (log_index IS NULL))
);
COMMENT ON TABLE project_lifecycle_association IS
    'Project-owned lifecycle association of family F2a: per triple, the resource of the latest resource-bearing RegistrationGranted or RegistrationReserved in the canonical event order. Step 2 of TYR-36 writes it block by block beside the served tables and nothing reads it yet.';
COMMENT ON COLUMN project_lifecycle_association.chain_id IS
    'This value is the chain.';
COMMENT ON COLUMN project_lifecycle_association.logical_name_id IS
    'This value is the name of the triple.';
COMMENT ON COLUMN project_lifecycle_association.registry_identifier IS
    'This value is COALESCE(registry_contract_instance_id, emitting address, registry) of the triple''s events.';
COMMENT ON COLUMN project_lifecycle_association.token_id IS
    'This value is the triple''s token id; empty text when the events carry none.';
COMMENT ON COLUMN project_lifecycle_association.target_resource_id IS
    'This value is the resource the triple''s null-resource events currently belong to.';
COMMENT ON COLUMN project_lifecycle_association.event_kind IS
    'This value is the kind of the winning grant or reservation.';
COMMENT ON COLUMN project_lifecycle_association.block_number IS
    'This value is the block number of the winning grant or reservation.';
COMMENT ON COLUMN project_lifecycle_association.transaction_index IS
    'This value is the transaction index of the winning grant or reservation; null with log_index for a synthesised event, which sorts before every transaction of its block.';
COMMENT ON COLUMN project_lifecycle_association.log_index IS
    'This value is the log index of the winning grant or reservation; null with transaction_index for a synthesised event.';
COMMENT ON COLUMN project_lifecycle_association.event_identity IS
    'This value is the event identity of the winning grant or reservation, the final tiebreak of the canonical event order, compared as bytes.';
COMMENT ON COLUMN project_lifecycle_association.normalized_event_id IS
    'This value names the winning grant or reservation in normalized_events as attribution only; it never takes part in ordering.';

CREATE TABLE IF NOT EXISTS project_lifecycle_event (
    chain_id text NOT NULL,
    state_kind text NOT NULL,
    state_key text NOT NULL,
    block_number bigint NOT NULL,
    transaction_index bigint,
    log_index bigint,
    event_identity text NOT NULL,
    normalized_event_id bigint,
    event_kind text NOT NULL,
    original_logical_name_id text,
    decoded_logical_name_id text,
    resource_id uuid,
    source_family text NOT NULL,
    authority_kind text,
    transaction_hash text,
    to_address text,
    namehash text,
    registrant text,
    before_registrant text,
    expiry jsonb,
    expiry_seconds bigint,
    status text,
    released_at jsonb,
    source_event text,
    derived_from text,
    terminal_reason text,
    revived_from_expiry boolean,
    state_derived boolean,
    surface_materialization boolean,
    registrar_surface_snapshot boolean,
    original_registered_at bigint,
    owner_getter text,
    owner_word_unmasked boolean,
    registry_owner text,
    PRIMARY KEY (chain_id, state_kind, state_key, event_identity),
    CHECK ((transaction_index IS NULL) = (log_index IS NULL)),
    CHECK (state_kind IN ('resource', 'triple'))
);
COMMENT ON TABLE project_lifecycle_event IS
    'Project-owned retained lifecycle events of family F2a: every RegistrationGranted, RegistrationRenewed, RegistrationReleased, RegistrationReserved, ExpiryChanged and TokenControlTransferred of a resource or triple, keyed by position, with the reader fields and admission evidence the authority-admitted readers consume. Unpruned; a row leaves only when undo removes its block. Step 2 of TYR-36 writes it block by block beside the served tables and nothing reads it yet.';
COMMENT ON COLUMN project_lifecycle_event.chain_id IS
    'This value is the chain.';
COMMENT ON COLUMN project_lifecycle_event.state_kind IS
    'This value is resource for an event of a resource key and triple for a null-resource event of a triple.';
COMMENT ON COLUMN project_lifecycle_event.state_key IS
    'This value is the resource id, or the triple as a JSON array of name, registry identifier and token id.';
COMMENT ON COLUMN project_lifecycle_event.block_number IS
    'This value is the block number of the event that last wrote the row.';
COMMENT ON COLUMN project_lifecycle_event.transaction_index IS
    'This value is the transaction index of the event that last wrote the row; null with log_index for a synthesised event, which sorts before every transaction of its block.';
COMMENT ON COLUMN project_lifecycle_event.log_index IS
    'This value is the log index of the event that last wrote the row; null with transaction_index for a synthesised event.';
COMMENT ON COLUMN project_lifecycle_event.event_identity IS
    'This value is the event identity of the event that last wrote the row, the final tiebreak of the canonical event order, compared as bytes.';
COMMENT ON COLUMN project_lifecycle_event.normalized_event_id IS
    'This value names the event that last wrote the row in normalized_events as attribution only; it never takes part in ordering.';
COMMENT ON COLUMN project_lifecycle_event.event_kind IS
    'This value is the event kind.';
COMMENT ON COLUMN project_lifecycle_event.original_logical_name_id IS
    'This value is the logical name the adapter emitted, null when it emitted the event unnamed; it is never rewritten.';
COMMENT ON COLUMN project_lifecycle_event.decoded_logical_name_id IS
    'This value is the name the two staging passes attached at write time; informational, the read recomputes it.';
COMMENT ON COLUMN project_lifecycle_event.resource_id IS
    'This value is the event''s resource.';
COMMENT ON COLUMN project_lifecycle_event.source_family IS
    'This value is the event''s source family.';
COMMENT ON COLUMN project_lifecycle_event.authority_kind IS
    'This value is COALESCE(NULLIF(after_state authority_kind, ''''), ''registrar'').';
COMMENT ON COLUMN project_lifecycle_event.transaction_hash IS
    'This value is the event''s transaction hash.';
COMMENT ON COLUMN project_lifecycle_event.to_address IS
    'This value is the lower-cased recipient of a TokenControlTransferred.';
COMMENT ON COLUMN project_lifecycle_event.namehash IS
    'This value is the lower-cased namehash the event carries.';
COMMENT ON COLUMN project_lifecycle_event.registrant IS
    'This value is the lower-cased after-state registrant.';
COMMENT ON COLUMN project_lifecycle_event.before_registrant IS
    'This value is the lower-cased before-state registrant of a RegistrationReleased.';
COMMENT ON COLUMN project_lifecycle_event.expiry IS
    'This value is the after-state expiry as the event carries it.';
COMMENT ON COLUMN project_lifecycle_event.expiry_seconds IS
    'This value is that expiry as seconds when it is an integral JSON number within the served range, else null.';
COMMENT ON COLUMN project_lifecycle_event.status IS
    'This value is the after-state status.';
COMMENT ON COLUMN project_lifecycle_event.released_at IS
    'This value is the after-state released_at.';
COMMENT ON COLUMN project_lifecycle_event.source_event IS
    'This value is the after-state source_event.';
COMMENT ON COLUMN project_lifecycle_event.derived_from IS
    'This value is the after-state derived_from.';
COMMENT ON COLUMN project_lifecycle_event.terminal_reason IS
    'This value is the after-state terminal_reason.';
COMMENT ON COLUMN project_lifecycle_event.revived_from_expiry IS
    'This value is the after-state revived_from_expiry.';
COMMENT ON COLUMN project_lifecycle_event.state_derived IS
    'This value is the after-state state_derived.';
COMMENT ON COLUMN project_lifecycle_event.surface_materialization IS
    'This value is the after-state surface_materialization.';
COMMENT ON COLUMN project_lifecycle_event.registrar_surface_snapshot IS
    'This value is the after-state registrar_surface_snapshot.';
COMMENT ON COLUMN project_lifecycle_event.original_registered_at IS
    'This value is the after-state original_registered_at in seconds.';
COMMENT ON COLUMN project_lifecycle_event.owner_getter IS
    'This value is the lower-cased after-state owner_getter.';
COMMENT ON COLUMN project_lifecycle_event.owner_word_unmasked IS
    'This value is the after-state owner_word_unmasked.';
COMMENT ON COLUMN project_lifecycle_event.registry_owner IS
    'This value is the lower-cased after-state registry_owner.';

CREATE TABLE IF NOT EXISTS project_child_registration_state (
    chain_id text NOT NULL,
    logical_name_id text NOT NULL,
    registry_contract_instance_id text NOT NULL,
    event_kind text,
    registrant text,
    block_number bigint NOT NULL,
    transaction_index bigint,
    log_index bigint,
    event_identity text NOT NULL,
    normalized_event_id bigint,
    exists boolean NOT NULL DEFAULT false,
    PRIMARY KEY (chain_id, logical_name_id, registry_contract_instance_id),
    CHECK ((transaction_index IS NULL) = (log_index IS NULL))
);
COMMENT ON TABLE project_child_registration_state IS
    'Project-owned per-registry child registration row of family F2a: the latest RegistrationGranted, RegistrationRenewed or RegistrationReleased of a name for one registry contract instance, and whether any reservation, grant or renewal exists there. Step 2 of TYR-36 writes it block by block beside the served tables and nothing reads it yet.';
COMMENT ON COLUMN project_child_registration_state.chain_id IS
    'This value is the chain.';
COMMENT ON COLUMN project_child_registration_state.logical_name_id IS
    'This value is the child name.';
COMMENT ON COLUMN project_child_registration_state.registry_contract_instance_id IS
    'This value is the registry contract instance the events carry.';
COMMENT ON COLUMN project_child_registration_state.event_kind IS
    'This value is the kind of the latest granted, renewed or released event; null when only a reservation exists.';
COMMENT ON COLUMN project_child_registration_state.registrant IS
    'This value is that event''s lower-cased registrant.';
COMMENT ON COLUMN project_child_registration_state.block_number IS
    'This value is the block number of the latest event of the three kinds, or the first reservation when none exists.';
COMMENT ON COLUMN project_child_registration_state.transaction_index IS
    'This value is the transaction index of the latest event of the three kinds, or the first reservation when none exists; null with log_index for a synthesised event, which sorts before every transaction of its block.';
COMMENT ON COLUMN project_child_registration_state.log_index IS
    'This value is the log index of the latest event of the three kinds, or the first reservation when none exists; null with transaction_index for a synthesised event.';
COMMENT ON COLUMN project_child_registration_state.event_identity IS
    'This value is the event identity of the latest event of the three kinds, or the first reservation when none exists, the final tiebreak of the canonical event order, compared as bytes.';
COMMENT ON COLUMN project_child_registration_state.normalized_event_id IS
    'This value names the latest event of the three kinds, or the first reservation when none exists in normalized_events as attribution only; it never takes part in ordering.';
COMMENT ON COLUMN project_child_registration_state.exists IS
    'This value is true once any reservation, grant or renewal carries this registry.';

CREATE TABLE IF NOT EXISTS project_wrapper_state (
    chain_id text NOT NULL,
    resource_id uuid NOT NULL,
    logical_name_id text,
    block_number bigint NOT NULL,
    transaction_index bigint,
    log_index bigint,
    event_identity text NOT NULL,
    normalized_event_id bigint,
    wrapper_state text,
    fuses bigint,
    wrapper_state_position jsonb,
    expiry_seconds numeric,
    expiry_position jsonb,
    owner_word_unmasked boolean,
    PRIMARY KEY (chain_id, resource_id),
    CHECK ((transaction_index IS NULL) = (log_index IS NULL))
);
COMMENT ON TABLE project_wrapper_state IS
    'Project-owned wrapper state of family F2b per wrapper resource: the latest wrapper_state and fuses and the latest wrapper expiry, unmasked; masks are applied at read against the block clock. Step 2 of TYR-36 writes it block by block beside the served tables and nothing reads it yet.';
COMMENT ON COLUMN project_wrapper_state.chain_id IS
    'This value is the chain.';
COMMENT ON COLUMN project_wrapper_state.resource_id IS
    'This value is the wrapper resource.';
COMMENT ON COLUMN project_wrapper_state.logical_name_id IS
    'This value is the wrapped name.';
COMMENT ON COLUMN project_wrapper_state.block_number IS
    'This value is the block number of the event that last wrote the row.';
COMMENT ON COLUMN project_wrapper_state.transaction_index IS
    'This value is the transaction index of the event that last wrote the row; null with log_index for a synthesised event, which sorts before every transaction of its block.';
COMMENT ON COLUMN project_wrapper_state.log_index IS
    'This value is the log index of the event that last wrote the row; null with transaction_index for a synthesised event.';
COMMENT ON COLUMN project_wrapper_state.event_identity IS
    'This value is the event identity of the event that last wrote the row, the final tiebreak of the canonical event order, compared as bytes.';
COMMENT ON COLUMN project_wrapper_state.normalized_event_id IS
    'This value names the event that last wrote the row in normalized_events as attribution only; it never takes part in ordering.';
COMMENT ON COLUMN project_wrapper_state.wrapper_state IS
    'This value is wrapped, emancipated or locked from the latest PermissionScopeChanged; null for any other value.';
COMMENT ON COLUMN project_wrapper_state.fuses IS
    'This value is that event''s fuses when a JSON number from 0 to 4294967295.';
COMMENT ON COLUMN project_wrapper_state.wrapper_state_position IS
    'This value is that PermissionScopeChanged''s position.';
COMMENT ON COLUMN project_wrapper_state.expiry_seconds IS
    'This value is the latest wrapper expiry: a JSON number from 0 to 18446744073709551615.';
COMMENT ON COLUMN project_wrapper_state.expiry_position IS
    'This value is that ExpiryChanged''s position.';
COMMENT ON COLUMN project_wrapper_state.owner_word_unmasked IS
    'This value is the latest owner_word_unmasked flag the wrapper events carried.';

CREATE TABLE IF NOT EXISTS project_registry_node_state (
    chain_id text NOT NULL,
    namespace text NOT NULL,
    node text NOT NULL,
    block_number bigint NOT NULL,
    transaction_index bigint,
    log_index bigint,
    event_identity text NOT NULL,
    normalized_event_id bigint,
    owner text,
    owner_getter text,
    owner_getter_reason text,
    owner_word_unmasked boolean,
    registry_owner text,
    emitter_role text,
    registry_contract text,
    has_old_record boolean NOT NULL DEFAULT false,
    first_current_record_block bigint,
    PRIMARY KEY (chain_id, namespace, node),
    CHECK ((transaction_index IS NULL) = (log_index IS NULL))
);
COMMENT ON TABLE project_registry_node_state IS
    'Project-owned registry ownership of family F2c per ENSv1 or Basenames registry node: the latest owner with the zero-owner override facts, and the registry generation facts. Step 2 of TYR-36 writes it block by block beside the served tables and nothing reads it yet.';
COMMENT ON COLUMN project_registry_node_state.chain_id IS
    'This value is the chain.';
COMMENT ON COLUMN project_registry_node_state.namespace IS
    'This value is the namespace.';
COMMENT ON COLUMN project_registry_node_state.node IS
    'This value is the lower-cased node the event addresses: child_node, else node.';
COMMENT ON COLUMN project_registry_node_state.block_number IS
    'This value is the block number of the event that last wrote the row.';
COMMENT ON COLUMN project_registry_node_state.transaction_index IS
    'This value is the transaction index of the event that last wrote the row; null with log_index for a synthesised event, which sorts before every transaction of its block.';
COMMENT ON COLUMN project_registry_node_state.log_index IS
    'This value is the log index of the event that last wrote the row; null with transaction_index for a synthesised event.';
COMMENT ON COLUMN project_registry_node_state.event_identity IS
    'This value is the event identity of the event that last wrote the row, the final tiebreak of the canonical event order, compared as bytes.';
COMMENT ON COLUMN project_registry_node_state.normalized_event_id IS
    'This value names the event that last wrote the row in normalized_events as attribution only; it never takes part in ordering.';
COMMENT ON COLUMN project_registry_node_state.owner IS
    'This value is the lower-cased after-state owner.';
COMMENT ON COLUMN project_registry_node_state.owner_getter IS
    'This value is the after-state owner_getter as written.';
COMMENT ON COLUMN project_registry_node_state.owner_getter_reason IS
    'This value is the after-state owner_getter_reason.';
COMMENT ON COLUMN project_registry_node_state.owner_word_unmasked IS
    'This value is the after-state owner_word_unmasked.';
COMMENT ON COLUMN project_registry_node_state.registry_owner IS
    'This value is the lower-cased after-state registry_owner.';
COMMENT ON COLUMN project_registry_node_state.emitter_role IS
    'This value is the after-state emitter_role of the latest event.';
COMMENT ON COLUMN project_registry_node_state.registry_contract IS
    'This value is the lower-cased emitting address, else the after-state registry_contract.';
COMMENT ON COLUMN project_registry_node_state.has_old_record IS
    'This value is true once any event with emitter_role registry_old addressed the node.';
COMMENT ON COLUMN project_registry_node_state.first_current_record_block IS
    'This value is the first block with an emitter_role registry event for the node.';

CREATE TABLE IF NOT EXISTS project_registry_binding_observation (
    chain_id text NOT NULL,
    resource_id uuid NOT NULL,
    attributed_via text NOT NULL,
    block_number bigint NOT NULL,
    transaction_index bigint,
    log_index bigint,
    event_identity text NOT NULL,
    normalized_event_id bigint,
    event_kind text NOT NULL,
    registry_owner text,
    registry_contract text,
    provenance jsonb,
    applicable boolean NOT NULL,
    clear_event_identity text,
    PRIMARY KEY (chain_id, resource_id, attributed_via),
    CHECK ((transaction_index IS NULL) = (log_index IS NULL)),
    CHECK (attributed_via IN ('own', 'name'))
);
COMMENT ON TABLE project_registry_binding_observation IS
    'Project-owned registry binding observations of family F2c: per resource and attribution, the latest AuthorityTransferred, SubregistryChanged, SurfaceBound or SurfaceUnbound observation; a read takes the latest of the two attributions. Step 2 of TYR-36 writes it block by block beside the served tables and nothing reads it yet.';
COMMENT ON COLUMN project_registry_binding_observation.chain_id IS
    'This value is the chain.';
COMMENT ON COLUMN project_registry_binding_observation.resource_id IS
    'This value is the observed resource.';
COMMENT ON COLUMN project_registry_binding_observation.attributed_via IS
    'This value is own when the event''s resource is the resource and name when the event reached it through the name''s current resource.';
COMMENT ON COLUMN project_registry_binding_observation.block_number IS
    'This value is the block number of the event that last wrote the row.';
COMMENT ON COLUMN project_registry_binding_observation.transaction_index IS
    'This value is the transaction index of the event that last wrote the row; null with log_index for a synthesised event, which sorts before every transaction of its block.';
COMMENT ON COLUMN project_registry_binding_observation.log_index IS
    'This value is the log index of the event that last wrote the row; null with transaction_index for a synthesised event.';
COMMENT ON COLUMN project_registry_binding_observation.event_identity IS
    'This value is the event identity of the event that last wrote the row, the final tiebreak of the canonical event order, compared as bytes.';
COMMENT ON COLUMN project_registry_binding_observation.normalized_event_id IS
    'This value names the event that last wrote the row in normalized_events as attribution only; it never takes part in ordering.';
COMMENT ON COLUMN project_registry_binding_observation.event_kind IS
    'This value is the observation''s event kind.';
COMMENT ON COLUMN project_registry_binding_observation.registry_owner IS
    'This value is the lower-cased owner_getter; null after SurfaceUnbound.';
COMMENT ON COLUMN project_registry_binding_observation.registry_contract IS
    'This value is the lower-cased registry contract the observation names.';
COMMENT ON COLUMN project_registry_binding_observation.provenance IS
    'This value is the observation''s source family, event kind and namespace.';
COMMENT ON COLUMN project_registry_binding_observation.applicable IS
    'This value is true when owner and contract are well-formed addresses and the owner is not zero.';
COMMENT ON COLUMN project_registry_binding_observation.clear_event_identity IS
    'This value is the event identity when the observation is not applicable.';

CREATE TABLE IF NOT EXISTS project_resolver_classification (
    chain_id text NOT NULL,
    resolver_address text NOT NULL,
    block_number bigint NOT NULL,
    transaction_index bigint,
    log_index bigint,
    event_identity text NOT NULL,
    normalized_event_id bigint,
    classification jsonb,
    support_status text NOT NULL,
    unsupported_reason text,
    manifest_id bigint,
    manifest_event_id bigint,
    admission_namespace text,
    summary_version text,
    PRIMARY KEY (chain_id, resolver_address),
    CHECK ((transaction_index IS NULL) = (log_index IS NULL)),
    CHECK (support_status IN ('supported', 'unsupported'))
);
COMMENT ON TABLE project_resolver_classification IS
    'Project-owned resolver classification of family F3, pinned to the declaration epoch active at the block that last touched the resolver: resolver_current without its sampled sections. Step 2 of TYR-36 writes it block by block beside the served tables and nothing reads it yet.';
COMMENT ON COLUMN project_resolver_classification.chain_id IS
    'This value is the chain.';
COMMENT ON COLUMN project_resolver_classification.resolver_address IS
    'This value is the lower-cased resolver address.';
COMMENT ON COLUMN project_resolver_classification.block_number IS
    'This value is the block number of the event that last wrote the row.';
COMMENT ON COLUMN project_resolver_classification.transaction_index IS
    'This value is the transaction index of the event that last wrote the row; null with log_index for a synthesised event, which sorts before every transaction of its block.';
COMMENT ON COLUMN project_resolver_classification.log_index IS
    'This value is the log index of the event that last wrote the row; null with transaction_index for a synthesised event.';
COMMENT ON COLUMN project_resolver_classification.event_identity IS
    'This value is the event identity of the event that last wrote the row, the final tiebreak of the canonical event order, compared as bytes.';
COMMENT ON COLUMN project_resolver_classification.normalized_event_id IS
    'This value names the event that last wrote the row in normalized_events as attribution only; it never takes part in ordering.';
COMMENT ON COLUMN project_resolver_classification.classification IS
    'This value is the source family, role and basis of the declaration or upgrade that classifies the resolver.';
COMMENT ON COLUMN project_resolver_classification.support_status IS
    'This value is supported or unsupported.';
COMMENT ON COLUMN project_resolver_classification.unsupported_reason IS
    'This value is the reason when unsupported.';
COMMENT ON COLUMN project_resolver_classification.manifest_id IS
    'This value is the declaring manifest.';
COMMENT ON COLUMN project_resolver_classification.manifest_event_id IS
    'This value is the SourceManifestUpdated event of that manifest.';
COMMENT ON COLUMN project_resolver_classification.admission_namespace IS
    'This value is the namespace of the declaring manifest.';
COMMENT ON COLUMN project_resolver_classification.summary_version IS
    'This value is the classification summary version.';

CREATE TABLE IF NOT EXISTS project_registry_pointer (
    chain_id text NOT NULL,
    namespace text NOT NULL,
    node text NOT NULL,
    block_number bigint NOT NULL,
    transaction_index bigint,
    log_index bigint,
    event_identity text NOT NULL,
    normalized_event_id bigint,
    resolver_address text NOT NULL,
    resource_id uuid,
    source_family text NOT NULL,
    PRIMARY KEY (chain_id, namespace, node),
    CHECK ((transaction_index IS NULL) = (log_index IS NULL))
);
COMMENT ON TABLE project_registry_pointer IS
    'Project-owned ENSv1 registry-node resolver pointer of family F4: the latest ResolverChanged per node, clears included. Step 2 of TYR-36 writes it block by block beside the served tables and nothing reads it yet.';
COMMENT ON COLUMN project_registry_pointer.chain_id IS
    'This value is the chain.';
COMMENT ON COLUMN project_registry_pointer.namespace IS
    'This value is the namespace.';
COMMENT ON COLUMN project_registry_pointer.node IS
    'This value is lower(COALESCE(child_node, namehash, node)) of the event.';
COMMENT ON COLUMN project_registry_pointer.block_number IS
    'This value is the block number of the event that last wrote the row.';
COMMENT ON COLUMN project_registry_pointer.transaction_index IS
    'This value is the transaction index of the event that last wrote the row; null with log_index for a synthesised event, which sorts before every transaction of its block.';
COMMENT ON COLUMN project_registry_pointer.log_index IS
    'This value is the log index of the event that last wrote the row; null with transaction_index for a synthesised event.';
COMMENT ON COLUMN project_registry_pointer.event_identity IS
    'This value is the event identity of the event that last wrote the row, the final tiebreak of the canonical event order, compared as bytes.';
COMMENT ON COLUMN project_registry_pointer.normalized_event_id IS
    'This value names the event that last wrote the row in normalized_events as attribution only; it never takes part in ordering.';
COMMENT ON COLUMN project_registry_pointer.resolver_address IS
    'This value is the lower-cased resolver, the zero address for a clear.';
COMMENT ON COLUMN project_registry_pointer.resource_id IS
    'This value is the event''s resource when it names one.';
COMMENT ON COLUMN project_registry_pointer.source_family IS
    'This value is the event''s source family.';

CREATE TABLE IF NOT EXISTS project_resource_pointer (
    chain_id text NOT NULL,
    resource_id uuid NOT NULL,
    block_number bigint NOT NULL,
    transaction_index bigint,
    log_index bigint,
    event_identity text NOT NULL,
    normalized_event_id bigint,
    resolver_address text,
    pointer_position jsonb,
    namespace text,
    source_family text,
    namehash text,
    nonzero_resolver_address text,
    nonzero_position jsonb,
    boundary_kind text,
    boundary_position jsonb,
    boundary_block_timestamp timestamptz,
    PRIMARY KEY (chain_id, resource_id),
    CHECK ((transaction_index IS NULL) = (log_index IS NULL))
);
CREATE INDEX IF NOT EXISTS project_resource_pointer_resolver_idx ON project_resource_pointer (chain_id, resolver_address, resource_id);
COMMENT ON TABLE project_resource_pointer IS
    'Project-owned resource resolver pointer of family F5: the current pointer with clears, the latest non-zero pointer and the record version boundary of a resource. Step 2 of TYR-36 writes it block by block beside the served tables and nothing reads it yet.';
COMMENT ON COLUMN project_resource_pointer.chain_id IS
    'This value is the chain.';
COMMENT ON COLUMN project_resource_pointer.resource_id IS
    'This value is the resource.';
COMMENT ON COLUMN project_resource_pointer.block_number IS
    'This value is the block number of the event that last wrote the row.';
COMMENT ON COLUMN project_resource_pointer.transaction_index IS
    'This value is the transaction index of the event that last wrote the row; null with log_index for a synthesised event, which sorts before every transaction of its block.';
COMMENT ON COLUMN project_resource_pointer.log_index IS
    'This value is the log index of the event that last wrote the row; null with transaction_index for a synthesised event.';
COMMENT ON COLUMN project_resource_pointer.event_identity IS
    'This value is the event identity of the event that last wrote the row, the final tiebreak of the canonical event order, compared as bytes.';
COMMENT ON COLUMN project_resource_pointer.normalized_event_id IS
    'This value names the event that last wrote the row in normalized_events as attribution only; it never takes part in ordering.';
COMMENT ON COLUMN project_resource_pointer.resolver_address IS
    'This value is the lower-cased resolver of the latest ResolverChanged, the zero address for a clear.';
COMMENT ON COLUMN project_resource_pointer.pointer_position IS
    'This value is that ResolverChanged''s position.';
COMMENT ON COLUMN project_resource_pointer.namespace IS
    'This value is that event''s namespace.';
COMMENT ON COLUMN project_resource_pointer.source_family IS
    'This value is that event''s source family.';
COMMENT ON COLUMN project_resource_pointer.namehash IS
    'This value is the lower-cased namehash that event carries.';
COMMENT ON COLUMN project_resource_pointer.nonzero_resolver_address IS
    'This value is the resolver of the latest non-zero ResolverChanged.';
COMMENT ON COLUMN project_resource_pointer.nonzero_position IS
    'This value is that event''s position.';
COMMENT ON COLUMN project_resource_pointer.boundary_kind IS
    'This value is the kind of the latest RecordVersionChanged or ResolverChanged on the resource.';
COMMENT ON COLUMN project_resource_pointer.boundary_position IS
    'This value is that boundary event''s position.';
COMMENT ON COLUMN project_resource_pointer.boundary_block_timestamp IS
    'This value is that boundary event''s block timestamp.';

CREATE TABLE IF NOT EXISTS project_node_record_partition (
    chain_id text NOT NULL,
    resolver_address text NOT NULL,
    arm text NOT NULL,
    arm_identity text NOT NULL,
    block_number bigint NOT NULL,
    transaction_index bigint,
    log_index bigint,
    event_identity text NOT NULL,
    normalized_event_id bigint,
    node text,
    logical_name_id text,
    source_family text NOT NULL,
    namespace text NOT NULL,
    source_manifest_id bigint,
    version_position jsonb,
    PRIMARY KEY (chain_id, resolver_address, arm, arm_identity),
    CHECK ((transaction_index IS NULL) = (log_index IS NULL)),
    CHECK (arm IN ('named', 'native', 'guarded'))
);
COMMENT ON TABLE project_node_record_partition IS
    'Project-owned node record partitions of family F6: per resolver, attribution arm and arm identity, the latest record version event. Step 2 of TYR-36 writes it block by block beside the served tables and nothing reads it yet.';
COMMENT ON COLUMN project_node_record_partition.chain_id IS
    'This value is the chain.';
COMMENT ON COLUMN project_node_record_partition.resolver_address IS
    'This value is the lower-cased resolver.';
COMMENT ON COLUMN project_node_record_partition.arm IS
    'This value is named, native or guarded.';
COMMENT ON COLUMN project_node_record_partition.arm_identity IS
    'This value is the logical name for named; node and source family for native; node, source family, namespace and manifest for guarded, joined by a vertical bar.';
COMMENT ON COLUMN project_node_record_partition.block_number IS
    'This value is the block number of the event that last wrote the row.';
COMMENT ON COLUMN project_node_record_partition.transaction_index IS
    'This value is the transaction index of the event that last wrote the row; null with log_index for a synthesised event, which sorts before every transaction of its block.';
COMMENT ON COLUMN project_node_record_partition.log_index IS
    'This value is the log index of the event that last wrote the row; null with transaction_index for a synthesised event.';
COMMENT ON COLUMN project_node_record_partition.event_identity IS
    'This value is the event identity of the event that last wrote the row, the final tiebreak of the canonical event order, compared as bytes.';
COMMENT ON COLUMN project_node_record_partition.normalized_event_id IS
    'This value names the event that last wrote the row in normalized_events as attribution only; it never takes part in ordering.';
COMMENT ON COLUMN project_node_record_partition.node IS
    'This value is the lower-cased node.';
COMMENT ON COLUMN project_node_record_partition.logical_name_id IS
    'This value is the name the events carry.';
COMMENT ON COLUMN project_node_record_partition.source_family IS
    'This value is the events'' source family.';
COMMENT ON COLUMN project_node_record_partition.namespace IS
    'This value is the events'' namespace.';
COMMENT ON COLUMN project_node_record_partition.source_manifest_id IS
    'This value is the events'' source manifest.';
COMMENT ON COLUMN project_node_record_partition.version_position IS
    'This value is the position of the partition''s latest RecordVersionChanged.';

CREATE TABLE IF NOT EXISTS project_node_record_value (
    chain_id text NOT NULL,
    resolver_address text NOT NULL,
    arm text NOT NULL,
    arm_identity text NOT NULL,
    record_key text NOT NULL,
    block_number bigint NOT NULL,
    transaction_index bigint,
    log_index bigint,
    event_identity text NOT NULL,
    normalized_event_id bigint,
    status text NOT NULL,
    value jsonb,
    record_family text,
    selector_key text,
    contenthash_hex text,
    address_bytes_hex text,
    source_event text,
    storage_model text,
    sibling_value jsonb,
    sibling_position jsonb,
    node text,
    logical_name_id text,
    resource_id uuid,
    source_family text NOT NULL,
    namespace text NOT NULL,
    source_manifest_id bigint,
    hydrated_value jsonb,
    hydrated_at_block bigint,
    PRIMARY KEY (chain_id, resolver_address, arm, arm_identity, record_key),
    CHECK ((transaction_index IS NULL) = (log_index IS NULL)),
    CHECK (arm IN ('named', 'native', 'guarded'))
);
COMMENT ON TABLE project_node_record_value IS
    'Project-owned node record values of family F6: per partition and record key, the latest record in the canonical event order, with its coin-60 compatibility sibling. Step 2 of TYR-36 writes it block by block beside the served tables and nothing reads it yet.';
COMMENT ON COLUMN project_node_record_value.chain_id IS
    'This value is the chain.';
COMMENT ON COLUMN project_node_record_value.resolver_address IS
    'This value is the lower-cased resolver.';
COMMENT ON COLUMN project_node_record_value.arm IS
    'This value is the partition''s arm.';
COMMENT ON COLUMN project_node_record_value.arm_identity IS
    'This value is the partition''s arm identity.';
COMMENT ON COLUMN project_node_record_value.record_key IS
    'This value is the record key.';
COMMENT ON COLUMN project_node_record_value.block_number IS
    'This value is the block number of the event that last wrote the row.';
COMMENT ON COLUMN project_node_record_value.transaction_index IS
    'This value is the transaction index of the event that last wrote the row; null with log_index for a synthesised event, which sorts before every transaction of its block.';
COMMENT ON COLUMN project_node_record_value.log_index IS
    'This value is the log index of the event that last wrote the row; null with transaction_index for a synthesised event.';
COMMENT ON COLUMN project_node_record_value.event_identity IS
    'This value is the event identity of the event that last wrote the row, the final tiebreak of the canonical event order, compared as bytes.';
COMMENT ON COLUMN project_node_record_value.normalized_event_id IS
    'This value names the event that last wrote the row in normalized_events as attribution only; it never takes part in ordering.';
COMMENT ON COLUMN project_node_record_value.status IS
    'This value is success, not_found or unsupported, as the inventory builder classifies the value.';
COMMENT ON COLUMN project_node_record_value.value IS
    'This value is the record value as the event carries it.';
COMMENT ON COLUMN project_node_record_value.record_family IS
    'This value is the after-state record_family.';
COMMENT ON COLUMN project_node_record_value.selector_key IS
    'This value is the after-state selector_key.';
COMMENT ON COLUMN project_node_record_value.contenthash_hex IS
    'This value is the after-state contenthash_hex.';
COMMENT ON COLUMN project_node_record_value.address_bytes_hex IS
    'This value is the after-state address_bytes_hex.';
COMMENT ON COLUMN project_node_record_value.source_event IS
    'This value is the after-state source_event.';
COMMENT ON COLUMN project_node_record_value.storage_model IS
    'This value is the after-state storage_model.';
COMMENT ON COLUMN project_node_record_value.sibling_value IS
    'This value is the AddressChanged half''s value when this record is the AddrChanged half of a coin-60 pair.';
COMMENT ON COLUMN project_node_record_value.sibling_position IS
    'This value is that AddressChanged half''s own position.';
COMMENT ON COLUMN project_node_record_value.node IS
    'This value is the lower-cased node.';
COMMENT ON COLUMN project_node_record_value.logical_name_id IS
    'This value is the name the record carries.';
COMMENT ON COLUMN project_node_record_value.resource_id IS
    'This value is the resource the record carries.';
COMMENT ON COLUMN project_node_record_value.source_family IS
    'This value is the record''s source family.';
COMMENT ON COLUMN project_node_record_value.namespace IS
    'This value is the record''s namespace.';
COMMENT ON COLUMN project_node_record_value.source_manifest_id IS
    'This value is the record''s source manifest.';
COMMENT ON COLUMN project_node_record_value.hydrated_value IS
    'This value is the hydrated text value; null until hydration moves into the block.';
COMMENT ON COLUMN project_node_record_value.hydrated_at_block IS
    'This value is the block the hydrated value was read at.';

CREATE TABLE IF NOT EXISTS project_record_id_value (
    chain_id text NOT NULL,
    resolver_address text NOT NULL,
    record_id text NOT NULL,
    record_key text NOT NULL,
    block_number bigint NOT NULL,
    transaction_index bigint,
    log_index bigint,
    event_identity text NOT NULL,
    normalized_event_id bigint,
    status text NOT NULL,
    value jsonb,
    record_family text,
    selector_key text,
    contenthash_hex text,
    address_bytes_hex text,
    source_event text,
    storage_model text,
    source_family text NOT NULL,
    namespace text NOT NULL,
    source_manifest_id bigint,
    PRIMARY KEY (chain_id, resolver_address, record_id, record_key),
    CHECK ((transaction_index IS NULL) = (log_index IS NULL))
);
COMMENT ON TABLE project_record_id_value IS
    'Project-owned record-id values of family F7: per resolver, record id and record key, the latest RecordChanged with storage model resolver_record_id. Step 2 of TYR-36 writes it block by block beside the served tables and nothing reads it yet.';
COMMENT ON COLUMN project_record_id_value.chain_id IS
    'This value is the chain.';
COMMENT ON COLUMN project_record_id_value.resolver_address IS
    'This value is the lower-cased resolver.';
COMMENT ON COLUMN project_record_id_value.record_id IS
    'This value is the resolver record id.';
COMMENT ON COLUMN project_record_id_value.record_key IS
    'This value is the record key.';
COMMENT ON COLUMN project_record_id_value.block_number IS
    'This value is the block number of the event that last wrote the row.';
COMMENT ON COLUMN project_record_id_value.transaction_index IS
    'This value is the transaction index of the event that last wrote the row; null with log_index for a synthesised event, which sorts before every transaction of its block.';
COMMENT ON COLUMN project_record_id_value.log_index IS
    'This value is the log index of the event that last wrote the row; null with transaction_index for a synthesised event.';
COMMENT ON COLUMN project_record_id_value.event_identity IS
    'This value is the event identity of the event that last wrote the row, the final tiebreak of the canonical event order, compared as bytes.';
COMMENT ON COLUMN project_record_id_value.normalized_event_id IS
    'This value names the event that last wrote the row in normalized_events as attribution only; it never takes part in ordering.';
COMMENT ON COLUMN project_record_id_value.status IS
    'This value is success, not_found or unsupported, as the inventory builder classifies the value.';
COMMENT ON COLUMN project_record_id_value.value IS
    'This value is the record value as the event carries it.';
COMMENT ON COLUMN project_record_id_value.record_family IS
    'This value is the after-state record_family.';
COMMENT ON COLUMN project_record_id_value.selector_key IS
    'This value is the after-state selector_key.';
COMMENT ON COLUMN project_record_id_value.contenthash_hex IS
    'This value is the after-state contenthash_hex.';
COMMENT ON COLUMN project_record_id_value.address_bytes_hex IS
    'This value is the after-state address_bytes_hex.';
COMMENT ON COLUMN project_record_id_value.source_event IS
    'This value is the after-state source_event.';
COMMENT ON COLUMN project_record_id_value.storage_model IS
    'This value is the after-state storage_model.';
COMMENT ON COLUMN project_record_id_value.source_family IS
    'This value is the record''s source family.';
COMMENT ON COLUMN project_record_id_value.namespace IS
    'This value is the record''s namespace.';
COMMENT ON COLUMN project_record_id_value.source_manifest_id IS
    'This value is the record''s source manifest.';

CREATE TABLE IF NOT EXISTS project_resolver_link (
    chain_id text NOT NULL,
    resolver_address text NOT NULL,
    node text NOT NULL,
    block_number bigint NOT NULL,
    transaction_index bigint,
    log_index bigint,
    event_identity text NOT NULL,
    normalized_event_id bigint,
    record_id text NOT NULL,
    storage_model text,
    PRIMARY KEY (chain_id, resolver_address, node),
    CHECK ((transaction_index IS NULL) = (log_index IS NULL))
);
COMMENT ON TABLE project_resolver_link IS
    'Project-owned resolver links of family F7: per resolver and node, the latest ResolverRecordLinked; record id 0 is an explicit clear. Step 2 of TYR-36 writes it block by block beside the served tables and nothing reads it yet.';
COMMENT ON COLUMN project_resolver_link.chain_id IS
    'This value is the chain.';
COMMENT ON COLUMN project_resolver_link.resolver_address IS
    'This value is the lower-cased resolver.';
COMMENT ON COLUMN project_resolver_link.node IS
    'This value is the lower-cased node; 32 zero bytes is the default link.';
COMMENT ON COLUMN project_resolver_link.block_number IS
    'This value is the block number of the event that last wrote the row.';
COMMENT ON COLUMN project_resolver_link.transaction_index IS
    'This value is the transaction index of the event that last wrote the row; null with log_index for a synthesised event, which sorts before every transaction of its block.';
COMMENT ON COLUMN project_resolver_link.log_index IS
    'This value is the log index of the event that last wrote the row; null with transaction_index for a synthesised event.';
COMMENT ON COLUMN project_resolver_link.event_identity IS
    'This value is the event identity of the event that last wrote the row, the final tiebreak of the canonical event order, compared as bytes.';
COMMENT ON COLUMN project_resolver_link.normalized_event_id IS
    'This value names the event that last wrote the row in normalized_events as attribution only; it never takes part in ordering.';
COMMENT ON COLUMN project_resolver_link.record_id IS
    'This value is the linked record id, 0 for an unlink.';
COMMENT ON COLUMN project_resolver_link.storage_model IS
    'This value is the after-state storage_model.';

CREATE TABLE IF NOT EXISTS project_grant (
    chain_id text NOT NULL,
    resource_id uuid NOT NULL,
    subject text NOT NULL,
    scope text NOT NULL,
    block_number bigint NOT NULL,
    transaction_index bigint,
    log_index bigint,
    event_identity text NOT NULL,
    normalized_event_id bigint,
    event_kind text NOT NULL,
    scope_kind text,
    scope_detail jsonb,
    effective_powers jsonb NOT NULL,
    grant_source jsonb,
    revocation_source jsonb,
    inheritance_path jsonb,
    transfer_behavior jsonb,
    revoked boolean NOT NULL,
    registration_position jsonb,
    PRIMARY KEY (chain_id, resource_id, subject, scope),
    CHECK ((transaction_index IS NULL) = (log_index IS NULL))
);
COMMENT ON TABLE project_grant IS
    'Project-owned raw grants of family F8: per resource, subject and scope, the latest PermissionChanged or RootPermissionChanged, unmasked; wrapper masks, grace and expiry retirement apply at read. Step 2 of TYR-36 writes it block by block beside the served tables and nothing reads it yet.';
COMMENT ON COLUMN project_grant.chain_id IS
    'This value is the chain.';
COMMENT ON COLUMN project_grant.resource_id IS
    'This value is the resource.';
COMMENT ON COLUMN project_grant.subject IS
    'This value is the lower-cased subject.';
COMMENT ON COLUMN project_grant.scope IS
    'This value is the scope key as permissions.rs builds it.';
COMMENT ON COLUMN project_grant.block_number IS
    'This value is the block number of the event that last wrote the row.';
COMMENT ON COLUMN project_grant.transaction_index IS
    'This value is the transaction index of the event that last wrote the row; null with log_index for a synthesised event, which sorts before every transaction of its block.';
COMMENT ON COLUMN project_grant.log_index IS
    'This value is the log index of the event that last wrote the row; null with transaction_index for a synthesised event.';
COMMENT ON COLUMN project_grant.event_identity IS
    'This value is the event identity of the event that last wrote the row, the final tiebreak of the canonical event order, compared as bytes.';
COMMENT ON COLUMN project_grant.normalized_event_id IS
    'This value names the event that last wrote the row in normalized_events as attribution only; it never takes part in ordering.';
COMMENT ON COLUMN project_grant.event_kind IS
    'This value is PermissionChanged or RootPermissionChanged.';
COMMENT ON COLUMN project_grant.scope_kind IS
    'This value is the scope kind with registry_root folded into root.';
COMMENT ON COLUMN project_grant.scope_detail IS
    'This value is the after-state scope object.';
COMMENT ON COLUMN project_grant.effective_powers IS
    'This value is the after-state effective_powers array, unmasked.';
COMMENT ON COLUMN project_grant.grant_source IS
    'This value is the after-state grant_source.';
COMMENT ON COLUMN project_grant.revocation_source IS
    'This value is the after-state revocation_source.';
COMMENT ON COLUMN project_grant.inheritance_path IS
    'This value is the after-state inheritance_path.';
COMMENT ON COLUMN project_grant.transfer_behavior IS
    'This value is the after-state transfer_behavior.';
COMMENT ON COLUMN project_grant.revoked IS
    'This value is true when the effective powers are empty; the row stays as a clear.';
COMMENT ON COLUMN project_grant.registration_position IS
    'This value is the position of the resource''s latest grant or reservation when the grant was written, the registration the grant belongs to.';

CREATE TABLE IF NOT EXISTS project_resource_admin_aggregate (
    chain_id text NOT NULL,
    resource_id uuid NOT NULL,
    block_number bigint NOT NULL,
    transaction_index bigint,
    log_index bigint,
    event_identity text NOT NULL,
    normalized_event_id bigint,
    admin_powers jsonb NOT NULL,
    PRIMARY KEY (chain_id, resource_id),
    CHECK ((transaction_index IS NULL) = (log_index IS NULL))
);
COMMENT ON TABLE project_resource_admin_aggregate IS
    'Project-owned admin aggregate of family F8: per resource, the admin powers any subject holds through a registry or root scope grant. Step 2 of TYR-36 writes it block by block beside the served tables and nothing reads it yet.';
COMMENT ON COLUMN project_resource_admin_aggregate.chain_id IS
    'This value is the chain.';
COMMENT ON COLUMN project_resource_admin_aggregate.resource_id IS
    'This value is the resource.';
COMMENT ON COLUMN project_resource_admin_aggregate.block_number IS
    'This value is the block number of the event that last wrote the row.';
COMMENT ON COLUMN project_resource_admin_aggregate.transaction_index IS
    'This value is the transaction index of the event that last wrote the row; null with log_index for a synthesised event, which sorts before every transaction of its block.';
COMMENT ON COLUMN project_resource_admin_aggregate.log_index IS
    'This value is the log index of the event that last wrote the row; null with transaction_index for a synthesised event.';
COMMENT ON COLUMN project_resource_admin_aggregate.event_identity IS
    'This value is the event identity of the event that last wrote the row, the final tiebreak of the canonical event order, compared as bytes.';
COMMENT ON COLUMN project_resource_admin_aggregate.normalized_event_id IS
    'This value names the event that last wrote the row in normalized_events as attribution only; it never takes part in ordering.';
COMMENT ON COLUMN project_resource_admin_aggregate.admin_powers IS
    'This value is the sorted distinct admin powers.';

CREATE TABLE IF NOT EXISTS project_account_approval (
    chain_id text NOT NULL,
    authority_kind text NOT NULL,
    authority_contract text NOT NULL,
    owner text NOT NULL,
    subject text NOT NULL,
    relation_kind text NOT NULL,
    block_number bigint NOT NULL,
    transaction_index bigint,
    log_index bigint,
    event_identity text NOT NULL,
    normalized_event_id bigint,
    authority_contract_instance_id text,
    approved boolean NOT NULL,
    effective_powers jsonb,
    grant_source jsonb,
    revocation_source jsonb,
    inheritance_path jsonb,
    transfer_behavior jsonb,
    PRIMARY KEY (chain_id, authority_kind, authority_contract, owner, subject, relation_kind),
    CHECK ((transaction_index IS NULL) = (log_index IS NULL))
);
COMMENT ON TABLE project_account_approval IS
    'Project-owned account approvals of family F9: the latest AccountPermissionChanged per authority contract, owner, subject and relation; an explicit false stays as a row. Step 2 of TYR-36 writes it block by block beside the served tables and nothing reads it yet.';
COMMENT ON COLUMN project_account_approval.chain_id IS
    'This value is the chain.';
COMMENT ON COLUMN project_account_approval.authority_kind IS
    'This value is registry or wrapper.';
COMMENT ON COLUMN project_account_approval.authority_contract IS
    'This value is the lower-cased authority contract.';
COMMENT ON COLUMN project_account_approval.owner IS
    'This value is the lower-cased owner.';
COMMENT ON COLUMN project_account_approval.subject IS
    'This value is the lower-cased approved operator.';
COMMENT ON COLUMN project_account_approval.relation_kind IS
    'This value is the relation kind.';
COMMENT ON COLUMN project_account_approval.block_number IS
    'This value is the block number of the event that last wrote the row.';
COMMENT ON COLUMN project_account_approval.transaction_index IS
    'This value is the transaction index of the event that last wrote the row; null with log_index for a synthesised event, which sorts before every transaction of its block.';
COMMENT ON COLUMN project_account_approval.log_index IS
    'This value is the log index of the event that last wrote the row; null with transaction_index for a synthesised event.';
COMMENT ON COLUMN project_account_approval.event_identity IS
    'This value is the event identity of the event that last wrote the row, the final tiebreak of the canonical event order, compared as bytes.';
COMMENT ON COLUMN project_account_approval.normalized_event_id IS
    'This value names the event that last wrote the row in normalized_events as attribution only; it never takes part in ordering.';
COMMENT ON COLUMN project_account_approval.authority_contract_instance_id IS
    'This value is the authority contract instance.';
COMMENT ON COLUMN project_account_approval.approved IS
    'This value is the after-state approved flag.';
COMMENT ON COLUMN project_account_approval.effective_powers IS
    'This value is the after-state effective_powers.';
COMMENT ON COLUMN project_account_approval.grant_source IS
    'This value is the after-state grant_source.';
COMMENT ON COLUMN project_account_approval.revocation_source IS
    'This value is the after-state revocation_source.';
COMMENT ON COLUMN project_account_approval.inheritance_path IS
    'This value is the after-state inheritance_path.';
COMMENT ON COLUMN project_account_approval.transfer_behavior IS
    'This value is the after-state transfer_behavior.';

CREATE TABLE IF NOT EXISTS project_name_alias (
    chain_id text NOT NULL,
    logical_name_id text NOT NULL,
    block_number bigint NOT NULL,
    transaction_index bigint,
    log_index bigint,
    event_identity text NOT NULL,
    normalized_event_id bigint,
    active boolean NOT NULL,
    alias_state text,
    to_logical_name_id text,
    to_name text,
    to_resource_id text,
    to_normalized_name text,
    to_canonical_display_name text,
    to_namehash text,
    resolver_address text,
    PRIMARY KEY (chain_id, logical_name_id),
    CHECK ((transaction_index IS NULL) = (log_index IS NULL))
);
COMMENT ON TABLE project_name_alias IS
    'Project-owned name aliases of family F10: per source name, the latest AliasChanged with its event-carried target. Step 2 of TYR-36 writes it block by block beside the served tables and nothing reads it yet.';
COMMENT ON COLUMN project_name_alias.chain_id IS
    'This value is the chain.';
COMMENT ON COLUMN project_name_alias.logical_name_id IS
    'This value is the source name.';
COMMENT ON COLUMN project_name_alias.block_number IS
    'This value is the block number of the event that last wrote the row.';
COMMENT ON COLUMN project_name_alias.transaction_index IS
    'This value is the transaction index of the event that last wrote the row; null with log_index for a synthesised event, which sorts before every transaction of its block.';
COMMENT ON COLUMN project_name_alias.log_index IS
    'This value is the log index of the event that last wrote the row; null with transaction_index for a synthesised event.';
COMMENT ON COLUMN project_name_alias.event_identity IS
    'This value is the event identity of the event that last wrote the row, the final tiebreak of the canonical event order, compared as bytes.';
COMMENT ON COLUMN project_name_alias.normalized_event_id IS
    'This value names the event that last wrote the row in normalized_events as attribution only; it never takes part in ordering.';
COMMENT ON COLUMN project_name_alias.active IS
    'This value is the after-state active flag, true when absent.';
COMMENT ON COLUMN project_name_alias.alias_state IS
    'This value is the after-state alias_state.';
COMMENT ON COLUMN project_name_alias.to_logical_name_id IS
    'This value is the after-state to_logical_name_id.';
COMMENT ON COLUMN project_name_alias.to_name IS
    'This value is the after-state to_name.';
COMMENT ON COLUMN project_name_alias.to_resource_id IS
    'This value is the after-state to_resource_id.';
COMMENT ON COLUMN project_name_alias.to_normalized_name IS
    'This value is the after-state to_normalized_name.';
COMMENT ON COLUMN project_name_alias.to_canonical_display_name IS
    'This value is the after-state to_canonical_display_name.';
COMMENT ON COLUMN project_name_alias.to_namehash IS
    'This value is the after-state to_namehash.';
COMMENT ON COLUMN project_name_alias.resolver_address IS
    'This value is the lower-cased resolver the alias was written at.';

CREATE TABLE IF NOT EXISTS project_resolver_alias (
    chain_id text NOT NULL,
    resolver_address text NOT NULL,
    alias_identity text NOT NULL,
    block_number bigint NOT NULL,
    transaction_index bigint,
    log_index bigint,
    event_identity text NOT NULL,
    normalized_event_id bigint,
    active boolean NOT NULL,
    alias_state text,
    from_dns_encoded_name text,
    to_dns_encoded_name text,
    from_name text,
    to_logical_name_id text,
    to_name text,
    to_resource_id text,
    logical_name_id text,
    PRIMARY KEY (chain_id, resolver_address, alias_identity),
    CHECK ((transaction_index IS NULL) = (log_index IS NULL))
);
COMMENT ON TABLE project_resolver_alias IS
    'Project-owned per-resolver alias state of family F10: per resolver and alias identity, the latest AliasChanged. Step 2 of TYR-36 writes it block by block beside the served tables and nothing reads it yet.';
COMMENT ON COLUMN project_resolver_alias.chain_id IS
    'This value is the chain.';
COMMENT ON COLUMN project_resolver_alias.resolver_address IS
    'This value is lower(COALESCE(after resolver, before resolver, emitting address)).';
COMMENT ON COLUMN project_resolver_alias.alias_identity IS
    'This value is COALESCE(logical_name_id, from_logical_name_id, from_namehash, from_dns_encoded_name, from_name, event_identity).';
COMMENT ON COLUMN project_resolver_alias.block_number IS
    'This value is the block number of the event that last wrote the row.';
COMMENT ON COLUMN project_resolver_alias.transaction_index IS
    'This value is the transaction index of the event that last wrote the row; null with log_index for a synthesised event, which sorts before every transaction of its block.';
COMMENT ON COLUMN project_resolver_alias.log_index IS
    'This value is the log index of the event that last wrote the row; null with transaction_index for a synthesised event.';
COMMENT ON COLUMN project_resolver_alias.event_identity IS
    'This value is the event identity of the event that last wrote the row, the final tiebreak of the canonical event order, compared as bytes.';
COMMENT ON COLUMN project_resolver_alias.normalized_event_id IS
    'This value names the event that last wrote the row in normalized_events as attribution only; it never takes part in ordering.';
COMMENT ON COLUMN project_resolver_alias.active IS
    'This value is the after-state active flag, true when absent.';
COMMENT ON COLUMN project_resolver_alias.alias_state IS
    'This value is the after-state alias_state, active when absent.';
COMMENT ON COLUMN project_resolver_alias.from_dns_encoded_name IS
    'This value is the from_dns_encoded_name.';
COMMENT ON COLUMN project_resolver_alias.to_dns_encoded_name IS
    'This value is the to_dns_encoded_name.';
COMMENT ON COLUMN project_resolver_alias.from_name IS
    'This value is the from_name.';
COMMENT ON COLUMN project_resolver_alias.to_logical_name_id IS
    'This value is the after-state to_logical_name_id.';
COMMENT ON COLUMN project_resolver_alias.to_name IS
    'This value is the after-state to_name.';
COMMENT ON COLUMN project_resolver_alias.to_resource_id IS
    'This value is the after-state to_resource_id.';
COMMENT ON COLUMN project_resolver_alias.logical_name_id IS
    'This value is the event''s name.';

CREATE TABLE IF NOT EXISTS project_child_edge_candidate (
    chain_id text NOT NULL,
    namespace text NOT NULL,
    parent_node text NOT NULL,
    child_node text NOT NULL,
    authority_arm text NOT NULL,
    block_number bigint NOT NULL,
    transaction_index bigint,
    log_index bigint,
    event_identity text NOT NULL,
    normalized_event_id bigint,
    owner text,
    owner_getter text,
    labelhash text,
    source_family text NOT NULL,
    PRIMARY KEY (chain_id, namespace, parent_node, child_node, authority_arm),
    CHECK ((transaction_index IS NULL) = (log_index IS NULL))
);
COMMENT ON TABLE project_child_edge_candidate IS
    'Project-owned ENSv1 and Basenames child edge candidates of family F11: the latest SubregistryChanged per child and arm, kept while ineligible; a later edge for the child under another parent replaces it. Step 2 of TYR-36 writes it block by block beside the served tables and nothing reads it yet.';
COMMENT ON COLUMN project_child_edge_candidate.chain_id IS
    'This value is the chain.';
COMMENT ON COLUMN project_child_edge_candidate.namespace IS
    'This value is the namespace.';
COMMENT ON COLUMN project_child_edge_candidate.parent_node IS
    'This value is the lower-cased parent node.';
COMMENT ON COLUMN project_child_edge_candidate.child_node IS
    'This value is the lower-cased child node.';
COMMENT ON COLUMN project_child_edge_candidate.authority_arm IS
    'This value is ens_v1 or basenames.';
COMMENT ON COLUMN project_child_edge_candidate.block_number IS
    'This value is the block number of the event that last wrote the row.';
COMMENT ON COLUMN project_child_edge_candidate.transaction_index IS
    'This value is the transaction index of the event that last wrote the row; null with log_index for a synthesised event, which sorts before every transaction of its block.';
COMMENT ON COLUMN project_child_edge_candidate.log_index IS
    'This value is the log index of the event that last wrote the row; null with transaction_index for a synthesised event.';
COMMENT ON COLUMN project_child_edge_candidate.event_identity IS
    'This value is the event identity of the event that last wrote the row, the final tiebreak of the canonical event order, compared as bytes.';
COMMENT ON COLUMN project_child_edge_candidate.normalized_event_id IS
    'This value names the event that last wrote the row in normalized_events as attribution only; it never takes part in ordering.';
COMMENT ON COLUMN project_child_edge_candidate.owner IS
    'This value is the lower-cased edge owner.';
COMMENT ON COLUMN project_child_edge_candidate.owner_getter IS
    'This value is the lower-cased edge owner_getter.';
COMMENT ON COLUMN project_child_edge_candidate.labelhash IS
    'This value is the lower-cased labelhash.';
COMMENT ON COLUMN project_child_edge_candidate.source_family IS
    'This value is the event''s source family.';

CREATE TABLE IF NOT EXISTS project_parent_subregistry (
    chain_id text NOT NULL,
    logical_name_id text NOT NULL,
    block_number bigint NOT NULL,
    transaction_index bigint,
    log_index bigint,
    event_identity text NOT NULL,
    normalized_event_id bigint,
    subregistry_address text NOT NULL,
    PRIMARY KEY (chain_id, logical_name_id),
    CHECK ((transaction_index IS NULL) = (log_index IS NULL))
);
COMMENT ON TABLE project_parent_subregistry IS
    'Project-owned ENSv2 parent subregistry of family F11: per parent name, the latest SubregistryChanged address, clears included. Step 2 of TYR-36 writes it block by block beside the served tables and nothing reads it yet.';
COMMENT ON COLUMN project_parent_subregistry.chain_id IS
    'This value is the chain.';
COMMENT ON COLUMN project_parent_subregistry.logical_name_id IS
    'This value is the parent name.';
COMMENT ON COLUMN project_parent_subregistry.block_number IS
    'This value is the block number of the event that last wrote the row.';
COMMENT ON COLUMN project_parent_subregistry.transaction_index IS
    'This value is the transaction index of the event that last wrote the row; null with log_index for a synthesised event, which sorts before every transaction of its block.';
COMMENT ON COLUMN project_parent_subregistry.log_index IS
    'This value is the log index of the event that last wrote the row; null with transaction_index for a synthesised event.';
COMMENT ON COLUMN project_parent_subregistry.event_identity IS
    'This value is the event identity of the event that last wrote the row, the final tiebreak of the canonical event order, compared as bytes.';
COMMENT ON COLUMN project_parent_subregistry.normalized_event_id IS
    'This value names the event that last wrote the row in normalized_events as attribution only; it never takes part in ordering.';
COMMENT ON COLUMN project_parent_subregistry.subregistry_address IS
    'This value is the lower-cased subregistry, empty or zero for a clear.';

CREATE TABLE IF NOT EXISTS project_reverse_tuple (
    address text NOT NULL,
    coin_type text NOT NULL,
    namespace text NOT NULL,
    chain_id text NOT NULL,
    block_number bigint NOT NULL,
    transaction_index bigint,
    log_index bigint,
    event_identity text NOT NULL,
    normalized_event_id bigint,
    reverse_node text,
    source_event text,
    claim_provenance jsonb,
    reverse_position jsonb,
    raw_name jsonb,
    raw_name_bytes jsonb,
    claim_event_identity text,
    claim_position jsonb,
    hydrated_name text,
    attempt_block bigint,
    attempt_hash text,
    attempt_ordinal bigint,
    baseline jsonb,
    PRIMARY KEY (address, coin_type, namespace),
    CHECK ((transaction_index IS NULL) = (log_index IS NULL))
);
COMMENT ON TABLE project_reverse_tuple IS
    'Project-owned reverse tuples of family F12: per address, coin type and namespace, the latest ReverseChanged and the latest direct claim, with the hydration result once hydration moves into the block. Step 2 of TYR-36 writes it block by block beside the served tables and nothing reads it yet.';
COMMENT ON COLUMN project_reverse_tuple.address IS
    'This value is the lower-cased address.';
COMMENT ON COLUMN project_reverse_tuple.coin_type IS
    'This value is the coin type.';
COMMENT ON COLUMN project_reverse_tuple.namespace IS
    'This value is the namespace.';
COMMENT ON COLUMN project_reverse_tuple.chain_id IS
    'This value is the chain.';
COMMENT ON COLUMN project_reverse_tuple.block_number IS
    'This value is the block number of the event that last wrote the row.';
COMMENT ON COLUMN project_reverse_tuple.transaction_index IS
    'This value is the transaction index of the event that last wrote the row; null with log_index for a synthesised event, which sorts before every transaction of its block.';
COMMENT ON COLUMN project_reverse_tuple.log_index IS
    'This value is the log index of the event that last wrote the row; null with transaction_index for a synthesised event.';
COMMENT ON COLUMN project_reverse_tuple.event_identity IS
    'This value is the event identity of the event that last wrote the row, the final tiebreak of the canonical event order, compared as bytes.';
COMMENT ON COLUMN project_reverse_tuple.normalized_event_id IS
    'This value names the event that last wrote the row in normalized_events as attribution only; it never takes part in ordering.';
COMMENT ON COLUMN project_reverse_tuple.reverse_node IS
    'This value is the lower-cased reverse node of the latest ReverseChanged.';
COMMENT ON COLUMN project_reverse_tuple.source_event IS
    'This value is that event''s source_event.';
COMMENT ON COLUMN project_reverse_tuple.claim_provenance IS
    'This value is that event''s claim_provenance.';
COMMENT ON COLUMN project_reverse_tuple.reverse_position IS
    'This value is that ReverseChanged''s position.';
COMMENT ON COLUMN project_reverse_tuple.raw_name IS
    'This value is the raw_name of the latest direct claim.';
COMMENT ON COLUMN project_reverse_tuple.raw_name_bytes IS
    'This value is the raw_name_bytes of that claim.';
COMMENT ON COLUMN project_reverse_tuple.claim_event_identity IS
    'This value is that claim''s event identity.';
COMMENT ON COLUMN project_reverse_tuple.claim_position IS
    'This value is that claim''s position.';
COMMENT ON COLUMN project_reverse_tuple.hydrated_name IS
    'This value is the hydrated reverse name; null until hydration moves into the block.';
COMMENT ON COLUMN project_reverse_tuple.attempt_block IS
    'This value is the hydration attempt block.';
COMMENT ON COLUMN project_reverse_tuple.attempt_hash IS
    'This value is the hydration attempt block hash.';
COMMENT ON COLUMN project_reverse_tuple.attempt_ordinal IS
    'This value is the hydration attempt ordinal.';
COMMENT ON COLUMN project_reverse_tuple.baseline IS
    'This value is the pre-hydration baseline.';

CREATE TABLE IF NOT EXISTS project_reverse_node_claim (
    namespace text NOT NULL,
    reverse_node text NOT NULL,
    chain_id text NOT NULL,
    block_number bigint NOT NULL,
    transaction_index bigint,
    log_index bigint,
    event_identity text NOT NULL,
    normalized_event_id bigint,
    resolver_address text,
    raw_name jsonb,
    raw_name_bytes jsonb,
    PRIMARY KEY (namespace, reverse_node),
    CHECK ((transaction_index IS NULL) = (log_index IS NULL))
);
COMMENT ON TABLE project_reverse_node_claim IS
    'Project-owned node-selected claim facts of family F12: per node, the latest name record, the claim a ReverseClaimed tuple selects through the node''s resolver. Step 2 of TYR-36 writes it block by block beside the served tables and nothing reads it yet.';
COMMENT ON COLUMN project_reverse_node_claim.namespace IS
    'This value is the namespace.';
COMMENT ON COLUMN project_reverse_node_claim.reverse_node IS
    'This value is the lower-cased node.';
COMMENT ON COLUMN project_reverse_node_claim.chain_id IS
    'This value is the chain.';
COMMENT ON COLUMN project_reverse_node_claim.block_number IS
    'This value is the block number of the event that last wrote the row.';
COMMENT ON COLUMN project_reverse_node_claim.transaction_index IS
    'This value is the transaction index of the event that last wrote the row; null with log_index for a synthesised event, which sorts before every transaction of its block.';
COMMENT ON COLUMN project_reverse_node_claim.log_index IS
    'This value is the log index of the event that last wrote the row; null with transaction_index for a synthesised event.';
COMMENT ON COLUMN project_reverse_node_claim.event_identity IS
    'This value is the event identity of the event that last wrote the row, the final tiebreak of the canonical event order, compared as bytes.';
COMMENT ON COLUMN project_reverse_node_claim.normalized_event_id IS
    'This value names the event that last wrote the row in normalized_events as attribution only; it never takes part in ordering.';
COMMENT ON COLUMN project_reverse_node_claim.resolver_address IS
    'This value is the lower-cased resolver the name record was written at.';
COMMENT ON COLUMN project_reverse_node_claim.raw_name IS
    'This value is the record''s raw_name.';
COMMENT ON COLUMN project_reverse_node_claim.raw_name_bytes IS
    'This value is the record''s raw_name_bytes.';

CREATE TABLE IF NOT EXISTS project_claim_normalization (
    chain_id text NOT NULL,
    claim_event_identity text NOT NULL,
    block_number bigint NOT NULL,
    transaction_index bigint,
    log_index bigint,
    event_identity text NOT NULL,
    normalized_event_id bigint,
    status text NOT NULL,
    normalized_name text,
    reason text,
    PRIMARY KEY (chain_id, claim_event_identity),
    CHECK ((transaction_index IS NULL) = (log_index IS NULL))
);
COMMENT ON TABLE project_claim_normalization IS
    'Project-owned claim normalization of family F12: the normalization result of each claim event, stored once. Step 2 of TYR-36 writes it block by block beside the served tables and nothing reads it yet.';
COMMENT ON COLUMN project_claim_normalization.chain_id IS
    'This value is the chain.';
COMMENT ON COLUMN project_claim_normalization.claim_event_identity IS
    'This value is the claim event.';
COMMENT ON COLUMN project_claim_normalization.block_number IS
    'This value is the block number of the event that last wrote the row.';
COMMENT ON COLUMN project_claim_normalization.transaction_index IS
    'This value is the transaction index of the event that last wrote the row; null with log_index for a synthesised event, which sorts before every transaction of its block.';
COMMENT ON COLUMN project_claim_normalization.log_index IS
    'This value is the log index of the event that last wrote the row; null with transaction_index for a synthesised event.';
COMMENT ON COLUMN project_claim_normalization.event_identity IS
    'This value is the event identity of the event that last wrote the row, the final tiebreak of the canonical event order, compared as bytes.';
COMMENT ON COLUMN project_claim_normalization.normalized_event_id IS
    'This value names the event that last wrote the row in normalized_events as attribution only; it never takes part in ordering.';
COMMENT ON COLUMN project_claim_normalization.status IS
    'This value is success, not_found, invalid_name or unsupported.';
COMMENT ON COLUMN project_claim_normalization.normalized_name IS
    'This value is the normalized name on success.';
COMMENT ON COLUMN project_claim_normalization.reason IS
    'This value is the reason when not successful.';

CREATE TABLE IF NOT EXISTS project_address_name_fold (
    chain_id text NOT NULL,
    logical_name_id text NOT NULL,
    block_number bigint NOT NULL,
    transaction_index bigint,
    log_index bigint,
    event_identity text NOT NULL,
    normalized_event_id bigint,
    controller text,
    controller_action text,
    controller_subject text,
    controller_position jsonb,
    token_holder text,
    token_holder_position jsonb,
    registrant text,
    registrant_position jsonb,
    PRIMARY KEY (chain_id, logical_name_id),
    CHECK ((transaction_index IS NULL) = (log_index IS NULL))
);
COMMENT ON TABLE project_address_name_fold IS
    'Project-owned per-name address fold of family F13: the ordered controller fold, the token holder and the registrant, unmasked. Step 2 of TYR-36 writes it block by block beside the served tables and nothing reads it yet.';
COMMENT ON COLUMN project_address_name_fold.chain_id IS
    'This value is the chain.';
COMMENT ON COLUMN project_address_name_fold.logical_name_id IS
    'This value is the name.';
COMMENT ON COLUMN project_address_name_fold.block_number IS
    'This value is the block number of the event that last wrote the row.';
COMMENT ON COLUMN project_address_name_fold.transaction_index IS
    'This value is the transaction index of the event that last wrote the row; null with log_index for a synthesised event, which sorts before every transaction of its block.';
COMMENT ON COLUMN project_address_name_fold.log_index IS
    'This value is the log index of the event that last wrote the row; null with transaction_index for a synthesised event.';
COMMENT ON COLUMN project_address_name_fold.event_identity IS
    'This value is the event identity of the event that last wrote the row, the final tiebreak of the canonical event order, compared as bytes.';
COMMENT ON COLUMN project_address_name_fold.normalized_event_id IS
    'This value names the event that last wrote the row in normalized_events as attribution only; it never takes part in ordering.';
COMMENT ON COLUMN project_address_name_fold.controller IS
    'This value is the controller the fold holds after the latest event.';
COMMENT ON COLUMN project_address_name_fold.controller_action IS
    'This value is set or revoke, the latest controller action.';
COMMENT ON COLUMN project_address_name_fold.controller_subject IS
    'This value is that action''s lower-cased subject.';
COMMENT ON COLUMN project_address_name_fold.controller_position IS
    'This value is that action''s position.';
COMMENT ON COLUMN project_address_name_fold.token_holder IS
    'This value is the lower-cased recipient of the latest TokenControlTransferred.';
COMMENT ON COLUMN project_address_name_fold.token_holder_position IS
    'This value is that transfer''s position.';
COMMENT ON COLUMN project_address_name_fold.registrant IS
    'This value is the lower-cased registrant of the latest grant naming one.';
COMMENT ON COLUMN project_address_name_fold.registrant_position IS
    'This value is that grant''s position.';

CREATE TABLE IF NOT EXISTS project_address_name_index (
    address text NOT NULL,
    logical_name_id text NOT NULL,
    relation text NOT NULL,
    chain_id text NOT NULL,
    PRIMARY KEY (address, logical_name_id, relation)
);
COMMENT ON TABLE project_address_name_index IS
    'Project-owned address-to-name index of family F13, re-derived from project_address_name_fold and never journalled. Step 2 of TYR-36 writes it block by block beside the served tables and nothing reads it yet.';
COMMENT ON COLUMN project_address_name_index.address IS
    'This value is the lower-cased address.';
COMMENT ON COLUMN project_address_name_index.logical_name_id IS
    'This value is the name.';
COMMENT ON COLUMN project_address_name_index.relation IS
    'This value is controller, token_holder or registrant.';
COMMENT ON COLUMN project_address_name_index.chain_id IS
    'This value is the chain.';

CREATE TABLE IF NOT EXISTS project_address_record_node_index (
    address text NOT NULL,
    coin_type text NOT NULL,
    chain_id text NOT NULL,
    resolver_address text NOT NULL,
    node text NOT NULL,
    PRIMARY KEY (address, coin_type, chain_id, resolver_address, node)
);
COMMENT ON TABLE project_address_record_node_index IS
    'Project-owned inverse address record index of family F14 for node-keyed values, re-derived from project_node_record_value and never journalled. Step 2 of TYR-36 writes it block by block beside the served tables and nothing reads it yet.';
COMMENT ON COLUMN project_address_record_node_index.address IS
    'This value is the lower-cased address.';
COMMENT ON COLUMN project_address_record_node_index.coin_type IS
    'This value is the coin type.';
COMMENT ON COLUMN project_address_record_node_index.chain_id IS
    'This value is the chain.';
COMMENT ON COLUMN project_address_record_node_index.resolver_address IS
    'This value is the resolver.';
COMMENT ON COLUMN project_address_record_node_index.node IS
    'This value is the node.';

CREATE TABLE IF NOT EXISTS project_address_record_id_index (
    address text NOT NULL,
    coin_type text NOT NULL,
    chain_id text NOT NULL,
    resolver_address text NOT NULL,
    record_id text NOT NULL,
    PRIMARY KEY (address, coin_type, chain_id, resolver_address, record_id)
);
COMMENT ON TABLE project_address_record_id_index IS
    'Project-owned inverse address record index of family F14 for record-id values, re-derived from project_record_id_value and never journalled. Step 2 of TYR-36 writes it block by block beside the served tables and nothing reads it yet.';
COMMENT ON COLUMN project_address_record_id_index.address IS
    'This value is the lower-cased address.';
COMMENT ON COLUMN project_address_record_id_index.coin_type IS
    'This value is the coin type.';
COMMENT ON COLUMN project_address_record_id_index.chain_id IS
    'This value is the chain.';
COMMENT ON COLUMN project_address_record_id_index.resolver_address IS
    'This value is the resolver.';
COMMENT ON COLUMN project_address_record_id_index.record_id IS
    'This value is the record id.';
