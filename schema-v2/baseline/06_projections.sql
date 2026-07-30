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
    ON name_current (namespace, raw_name, logical_name_id);

CREATE INDEX IF NOT EXISTS name_current_resource_idx
    ON name_current (resource_id)
    WHERE resource_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS children_current (
    parent_logical_name_id text NOT NULL
        REFERENCES name_surfaces (logical_name_id),
    child_logical_name_id text NOT NULL,
    surface_class text NOT NULL DEFAULT 'declared',
    namespace text NOT NULL,
    raw_name text NOT NULL,
    raw_label text,
    namehash text NOT NULL,
    labelhash text,
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
    CHECK (raw_label IS NULL OR raw_label <> ''),
    CHECK (labelhash IS NULL OR btrim(labelhash) <> ''),
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
        raw_name,
        child_logical_name_id
    );

CREATE INDEX IF NOT EXISTS children_current_namehash_idx
    ON children_current (namespace, namehash);

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
    CHECK (btrim(scope_kind) <> ''),
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

CREATE TABLE IF NOT EXISTS permissions_current_resource_summary (
    resource_id uuid PRIMARY KEY
        REFERENCES resources (resource_id),
    authority_kind text,
    root_resource_id uuid
        REFERENCES resources (resource_id),
    support_status text NOT NULL,
    unsupported_reason text,
    provenance jsonb NOT NULL DEFAULT '{}'::jsonb,
    chain_positions jsonb NOT NULL DEFAULT '{}'::jsonb,
    canonicality_summary jsonb NOT NULL DEFAULT '{}'::jsonb,
    manifest_version bigint NOT NULL,
    last_recomputed_at timestamptz NOT NULL DEFAULT now(),
    CHECK (authority_kind IS NULL OR btrim(authority_kind) <> ''),
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

CREATE INDEX IF NOT EXISTS record_inventory_current_resource_idx
    ON record_inventory_current (resource_id, record_version_boundary_key);

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
    CHECK (btrim(relation) <> ''),
    CHECK (btrim(namespace) <> ''),
    CHECK (btrim(namehash) <> ''),
    CONSTRAINT address_names_current_logical_identity_check
        CHECK (logical_name_id = namespace || ':' || namehash),
    CHECK (btrim(binding_kind) <> ''),
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
        raw_name,
        logical_name_id
    );

CREATE INDEX IF NOT EXISTS address_names_current_name_idx
    ON address_names_current (logical_name_id, relation, lower(address));

CREATE TABLE IF NOT EXISTS primary_names_current (
    address text NOT NULL,
    coin_type text NOT NULL,
    namespace text NOT NULL,
    claim_status text NOT NULL DEFAULT 'unsupported',
    raw_claim_name text,
    claim_name_is_normalized boolean NOT NULL DEFAULT false,
    unsupported_reason text,
    claim_provenance jsonb NOT NULL DEFAULT '{}'::jsonb,
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
    CHECK (
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
    CHECK (NOT claim_name_is_normalized OR claim_status = 'success'),
    CHECK (
        (claim_status = 'unsupported' AND unsupported_reason IS NOT NULL)
        OR (claim_status <> 'unsupported' AND unsupported_reason IS NULL)
    ),
    CHECK (
        unsupported_reason IS NULL
        OR btrim(unsupported_reason) <> ''
    ),
    CHECK (jsonb_typeof(claim_provenance) = 'object')
);

CREATE INDEX IF NOT EXISTS primary_names_current_claim_idx
    ON primary_names_current (
        namespace,
        raw_claim_name,
        coin_type,
        address
    )
    WHERE claim_status = 'success';

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
    'This table stores the current direct children of each name.';
COMMENT ON COLUMN children_current.parent_logical_name_id IS
    'This value identifies the parent name.';
COMMENT ON COLUMN children_current.child_logical_name_id IS
    'This value identifies the child name.';
COMMENT ON COLUMN children_current.surface_class IS
    'This value states the child-link class.';
COMMENT ON COLUMN children_current.namespace IS
    'This value identifies the name system.';
COMMENT ON COLUMN children_current.raw_name IS
    'This value is the verbatim child name.';
COMMENT ON COLUMN children_current.raw_label IS
    'This value is the verbatim child label.';
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
