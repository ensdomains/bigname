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
    transaction_order_key text NOT NULL,
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
    'This value is the event transaction hash, or an empty string when the event has none, so it orders as history orders a missing hash.';
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
    'This bounded index serves one parent''s child registrations in history order on one chain, in both directions. Every key is a bounded identifier or hash.';
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
