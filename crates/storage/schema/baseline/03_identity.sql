CREATE EXTENSION IF NOT EXISTS btree_gist WITH SCHEMA public;
CREATE EXTENSION IF NOT EXISTS pgcrypto WITH SCHEMA public;

CREATE TABLE IF NOT EXISTS contract_instances (
    contract_instance_id uuid PRIMARY KEY,
    chain_id text NOT NULL,
    contract_kind text NOT NULL,
    provenance jsonb NOT NULL DEFAULT '{}'::jsonb,
    inserted_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (chain_id, contract_instance_id),
    CHECK (btrim(chain_id) <> ''),
    CONSTRAINT contract_instances_contract_kind_check
        CHECK (contract_kind IN ('root', 'contract')),
    CHECK (jsonb_typeof(provenance) = 'object')
);

CREATE TABLE IF NOT EXISTS contract_instance_addresses (
    contract_instance_address_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    contract_instance_id uuid NOT NULL,
    chain_id text NOT NULL,
    address text NOT NULL,
    active_from_block_number bigint,
    active_from_block_hash text,
    active_to_block_number bigint,
    active_to_block_hash text,
    source_manifest_id bigint,
    admitted_at timestamptz NOT NULL DEFAULT now(),
    deactivated_at timestamptz,
    provenance jsonb NOT NULL DEFAULT '{}'::jsonb,
    CONSTRAINT contract_instance_addresses_instance_chain_fkey
        FOREIGN KEY (chain_id, contract_instance_id)
        REFERENCES contract_instances (chain_id, contract_instance_id),
    CHECK (btrim(chain_id) <> ''),
    CHECK (btrim(address) <> ''),
    CHECK (
        active_from_block_hash IS NULL
        OR active_from_block_number IS NOT NULL
    ),
    CHECK (
        active_to_block_hash IS NULL
        OR active_to_block_number IS NOT NULL
    ),
    CHECK (active_from_block_number IS NULL OR active_from_block_number >= 0),
    CHECK (active_to_block_number IS NULL OR active_to_block_number >= 0),
    CHECK (
        active_from_block_number IS NULL
        OR active_to_block_number IS NULL
        OR active_to_block_number >= active_from_block_number
    ),
    CHECK (deactivated_at IS NULL OR deactivated_at >= admitted_at),
    CHECK (jsonb_typeof(provenance) = 'object'),
    CONSTRAINT contract_instance_addresses_no_overlap
        EXCLUDE USING gist (
            contract_instance_id WITH =,
            int8range(
                active_from_block_number,
                active_to_block_number,
                '[]'
            ) WITH &&
        )
);

CREATE UNIQUE INDEX IF NOT EXISTS contract_instance_addresses_active_idx
    ON contract_instance_addresses (chain_id, lower(address))
    WHERE deactivated_at IS NULL;

CREATE UNIQUE INDEX IF NOT EXISTS contract_instance_addresses_active_instance_idx
    ON contract_instance_addresses (contract_instance_id)
    WHERE deactivated_at IS NULL;

CREATE INDEX IF NOT EXISTS contract_instance_addresses_instance_idx
    ON contract_instance_addresses (
        contract_instance_id,
        active_from_block_number,
        active_to_block_number
    );

-- The owned key family loop reads the addresses that start or stop at each block.
CREATE INDEX IF NOT EXISTS project_families_contract_instance_addresses_from_block_idx
    ON contract_instance_addresses (chain_id, active_from_block_number);

CREATE INDEX IF NOT EXISTS project_families_contract_instance_addresses_to_block_idx
    ON contract_instance_addresses (chain_id, active_to_block_number)
    WHERE active_to_block_number IS NOT NULL;

CREATE TABLE IF NOT EXISTS discovery_edges (
    discovery_edge_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    chain_id text NOT NULL,
    edge_kind text NOT NULL,
    from_contract_instance_id uuid NOT NULL,
    to_contract_instance_id uuid NOT NULL,
    discovery_source text NOT NULL,
    admission_basis text NOT NULL,
    source_manifest_id bigint,
    active_from_block_number bigint,
    active_from_block_hash text,
    active_to_block_number bigint,
    active_to_block_hash text,
    canonicality_state canonicality_state NOT NULL DEFAULT 'observed',
    admitted_at timestamptz NOT NULL DEFAULT now(),
    deactivated_at timestamptz,
    provenance jsonb NOT NULL DEFAULT '{}'::jsonb,
    CONSTRAINT discovery_edges_from_instance_chain_fkey
        FOREIGN KEY (chain_id, from_contract_instance_id)
        REFERENCES contract_instances (chain_id, contract_instance_id),
    CONSTRAINT discovery_edges_to_instance_chain_fkey
        FOREIGN KEY (chain_id, to_contract_instance_id)
        REFERENCES contract_instances (chain_id, contract_instance_id),
    CHECK (btrim(chain_id) <> ''),
    CONSTRAINT discovery_edges_edge_kind_check
        CHECK (
            edge_kind IN (
                'resolver',
                'subregistry',
                'proxy_implementation',
                'migration',
                'registry_announcement'
            )
        ),
    -- The multi-column checks below carry explicit names. PostgreSQL numbers
    -- unnamed ones in declaration order, so naming only the self-edge check
    -- would rename its siblings on fresh installs but not on upgraded ones.
    CONSTRAINT discovery_edges_self_edge_check CHECK (
        edge_kind = 'registry_announcement'
        OR (edge_kind = 'resolver' AND discovery_source = 'ResolverCreated')
        OR from_contract_instance_id <> to_contract_instance_id
    ),
    CHECK (btrim(discovery_source) <> ''),
    CHECK (btrim(admission_basis) <> ''),
    CONSTRAINT discovery_edges_check1 CHECK (
        (active_from_block_number IS NULL)
        = (active_from_block_hash IS NULL)
    ),
    CONSTRAINT discovery_edges_check2 CHECK (
        (active_to_block_number IS NULL)
        = (active_to_block_hash IS NULL)
    ),
    CHECK (active_from_block_number IS NULL OR active_from_block_number >= 0),
    CHECK (active_to_block_number IS NULL OR active_to_block_number >= 0),
    CONSTRAINT discovery_edges_check3 CHECK (
        active_from_block_number IS NULL
        OR active_to_block_number IS NULL
        OR active_to_block_number >= active_from_block_number
    ),
    CONSTRAINT discovery_edges_check4
        CHECK (deactivated_at IS NULL OR deactivated_at >= admitted_at),
    CHECK (jsonb_typeof(provenance) = 'object')
);

CREATE INDEX IF NOT EXISTS discovery_edges_active_from_idx
    ON discovery_edges (chain_id, from_contract_instance_id, edge_kind)
    WHERE deactivated_at IS NULL;

CREATE INDEX IF NOT EXISTS discovery_edges_active_to_idx
    ON discovery_edges (chain_id, to_contract_instance_id, edge_kind)
    WHERE deactivated_at IS NULL;

-- The owned key family loop reads the resolver edges that start or stop at each block.
CREATE INDEX IF NOT EXISTS project_families_discovery_edges_resolver_from_block_idx
    ON discovery_edges (chain_id, active_from_block_number)
    WHERE edge_kind = 'resolver';

CREATE INDEX IF NOT EXISTS project_families_discovery_edges_resolver_to_block_idx
    ON discovery_edges (chain_id, active_to_block_number)
    WHERE edge_kind = 'resolver' AND active_to_block_number IS NOT NULL;

-- It also asks whether an address that starts or stops at the block is any resolver edge's
-- destination, deactivated edges included.
CREATE INDEX IF NOT EXISTS project_families_discovery_edges_resolver_destination_idx
    ON discovery_edges (chain_id, to_contract_instance_id)
    WHERE edge_kind = 'resolver';

-- It classifies a block's touched resolvers from each one's contract instance, asking for one
-- active resolver edge per active manifest.
CREATE INDEX IF NOT EXISTS project_families_discovery_edges_resolver_admission_idx
    ON discovery_edges (
        chain_id,
        to_contract_instance_id,
        source_manifest_id,
        active_from_block_number
    )
    WHERE edge_kind = 'resolver' AND deactivated_at IS NULL;

-- Interpret closes and orders historical observations as well as active ones.
CREATE INDEX IF NOT EXISTS discovery_edges_observation_history_idx
    ON discovery_edges (
        chain_id,
        from_contract_instance_id,
        edge_kind,
        (provenance ->> 'observation_key'),
        active_from_block_number
    )
    WHERE canonicality_state <> 'orphaned';

-- Reopening an exact observation must also find orphaned and closed rows.
CREATE INDEX IF NOT EXISTS discovery_edges_reopen_idx
    ON discovery_edges (
        chain_id,
        from_contract_instance_id,
        edge_kind,
        active_from_block_number,
        (provenance ->> 'observation_key')
    );

CREATE TABLE IF NOT EXISTS token_lineages (
    token_lineage_id uuid PRIMARY KEY,
    chain_id text NOT NULL,
    block_hash text NOT NULL,
    block_number bigint NOT NULL,
    provenance jsonb NOT NULL DEFAULT '{}'::jsonb,
    canonicality_state canonicality_state NOT NULL DEFAULT 'observed',
    observed_at timestamptz NOT NULL DEFAULT now(),
    inserted_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (chain_id, token_lineage_id),
    FOREIGN KEY (chain_id, block_hash, block_number)
        REFERENCES chain_lineage (chain_id, block_hash, block_number),
    CHECK (block_number >= 0),
    CHECK (jsonb_typeof(provenance) = 'object')
);

CREATE INDEX IF NOT EXISTS token_lineages_block_idx
    ON token_lineages (chain_id, block_hash);

CREATE TABLE IF NOT EXISTS resources (
    resource_id uuid PRIMARY KEY,
    token_lineage_id uuid UNIQUE,
    chain_id text NOT NULL,
    block_hash text NOT NULL,
    block_number bigint NOT NULL,
    provenance jsonb NOT NULL DEFAULT '{}'::jsonb,
    canonicality_state canonicality_state NOT NULL DEFAULT 'observed',
    observed_at timestamptz NOT NULL DEFAULT now(),
    inserted_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (chain_id, resource_id),
    UNIQUE (resource_id, token_lineage_id),
    FOREIGN KEY (chain_id, token_lineage_id)
        REFERENCES token_lineages (chain_id, token_lineage_id),
    FOREIGN KEY (chain_id, block_hash, block_number)
        REFERENCES chain_lineage (chain_id, block_hash, block_number),
    CHECK (block_number >= 0),
    CHECK (jsonb_typeof(provenance) = 'object')
);

CREATE INDEX IF NOT EXISTS resources_block_idx
    ON resources (chain_id, block_hash);

CREATE INDEX IF NOT EXISTS resources_chain_block_number_idx
    ON resources (chain_id, block_number);

CREATE TABLE IF NOT EXISTS name_surfaces (
    logical_name_id text PRIMARY KEY,
    namespace text NOT NULL,
    raw_name text,
    raw_labels text[],
    dns_encoded_name bytea,
    namehash text NOT NULL,
    labelhashes text[] NOT NULL,
    normalizer_version text NOT NULL,
    visibility_state text NOT NULL,
    normalization_errors jsonb NOT NULL DEFAULT '[]'::jsonb,
    deactivation_reason text,
    deactivated_at timestamptz,
    chain_id text NOT NULL,
    block_hash text NOT NULL,
    block_number bigint NOT NULL,
    provenance jsonb NOT NULL DEFAULT '{}'::jsonb,
    canonicality_state canonicality_state NOT NULL DEFAULT 'observed',
    observed_at timestamptz NOT NULL DEFAULT now(),
    inserted_at timestamptz NOT NULL DEFAULT now(),
    preimage_event_identity text,
    UNIQUE (chain_id, logical_name_id),
    FOREIGN KEY (chain_id, block_hash, block_number)
        REFERENCES chain_lineage (chain_id, block_hash, block_number),
    CHECK (btrim(namespace) <> ''),
    CHECK (btrim(namehash) <> ''),
    CONSTRAINT name_surfaces_logical_identity_check
        CHECK (logical_name_id = namespace || ':' || namehash),
    -- The raw bytes of every label are known together or not at all. A row without them
    -- still names its node through the complete label-hash path, and unknown bytes are
    -- never a normalization failure.
    CONSTRAINT name_surfaces_raw_evidence_check CHECK (
        (
            raw_name IS NULL
            AND raw_labels IS NULL
            AND dns_encoded_name IS NULL
            AND preimage_event_identity IS NULL
            AND cardinality(labelhashes) > 0
            AND visibility_state = 'active'
        )
        OR (
            raw_name IS NOT NULL
            AND raw_labels IS NOT NULL
            AND dns_encoded_name IS NOT NULL
            AND (
                cardinality(raw_labels) = cardinality(labelhashes)
                OR (
                    visibility_state = 'shadow'
                    AND raw_name = ''
                    AND cardinality(raw_labels) = 0
                    AND cardinality(labelhashes) > 0
                    AND preimage_event_identity IS NOT NULL
                    AND btrim(preimage_event_identity) <> ''
                )
            )
            AND (preimage_event_identity IS NULL OR btrim(preimage_event_identity) <> '')
        )
    ),
    CHECK (btrim(normalizer_version) <> ''),
    CHECK (visibility_state IN ('active', 'shadow')),
    CHECK (jsonb_typeof(normalization_errors) = 'array'),
    CONSTRAINT name_surfaces_visibility_coherence_check CHECK (
        (
            visibility_state = 'active'
            AND deactivation_reason IS NULL
            AND deactivated_at IS NULL
            AND normalization_errors = '[]'::jsonb
        )
        OR (
            visibility_state = 'shadow'
            AND deactivation_reason IS NOT NULL
            AND btrim(deactivation_reason) <> ''
            AND deactivated_at IS NOT NULL
        )
    ),
    CHECK (block_number >= 0),
    CHECK (jsonb_typeof(provenance) = 'object')
);

CREATE UNIQUE INDEX IF NOT EXISTS name_surfaces_hash_idx
    ON name_surfaces (namespace, namehash);

CREATE INDEX IF NOT EXISTS name_surfaces_visibility_idx
    ON name_surfaces (namespace, visibility_state, namehash);

CREATE INDEX IF NOT EXISTS name_surfaces_block_idx
    ON name_surfaces (chain_id, block_hash);

CREATE INDEX IF NOT EXISTS name_surfaces_chain_block_number_idx
    ON name_surfaces (chain_id, block_number);

-- The name-ordered readers (storage:families.name.search_candidates and
-- storage:families.name.bound_candidates) walk readable surfaces in this order. raw_name has
-- no length bound, and an index entry larger than about 2.7 KB fails the insert, so longer
-- names stay out of the index and those readers carry the same bound.
CREATE INDEX IF NOT EXISTS name_surfaces_name_order_idx
    ON name_surfaces (raw_name, namespace, namehash, logical_name_id)
    WHERE visibility_state = 'active'
      AND canonicality_state IN ('canonical', 'safe', 'finalized')
      AND octet_length(raw_name) <= 2000;

CREATE TABLE IF NOT EXISTS surface_bindings (
    surface_binding_id uuid PRIMARY KEY,
    logical_name_id text NOT NULL,
    resource_id uuid NOT NULL,
    binding_kind text NOT NULL,
    authority_arm text NOT NULL,
    active_from timestamptz NOT NULL,
    active_to timestamptz,
    chain_id text NOT NULL,
    block_hash text NOT NULL,
    block_number bigint NOT NULL,
    provenance jsonb NOT NULL DEFAULT '{}'::jsonb,
    canonicality_state canonicality_state NOT NULL DEFAULT 'observed',
    observed_at timestamptz NOT NULL DEFAULT now(),
    inserted_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (
        surface_binding_id,
        logical_name_id,
        resource_id,
        binding_kind
    ),
    FOREIGN KEY (chain_id, logical_name_id)
        REFERENCES name_surfaces (chain_id, logical_name_id),
    FOREIGN KEY (chain_id, resource_id)
        REFERENCES resources (chain_id, resource_id),
    FOREIGN KEY (chain_id, block_hash, block_number)
        REFERENCES chain_lineage (chain_id, block_hash, block_number),
    CONSTRAINT surface_bindings_binding_kind_check
        CHECK (
            binding_kind IN (
                'declared_registry_path',
                'linked_subregistry_path',
                'resolver_alias_path',
                'observed_wildcard_path',
                'observed_only'
            )
        ),
    CONSTRAINT surface_bindings_authority_arm_check
        CHECK (authority_arm IN ('ens_v1', 'ens_v2', 'basenames')),
    CHECK (active_to IS NULL OR active_to > active_from),
    CHECK (block_number >= 0),
    CHECK (jsonb_typeof(provenance) = 'object'),
    CONSTRAINT surface_bindings_no_overlap
        EXCLUDE USING gist (
            chain_id WITH =,
            logical_name_id WITH =,
            authority_arm WITH =,
            tstzrange(
                active_from,
                COALESCE(active_to, 'infinity'::timestamptz),
                '[)'
            ) WITH &&
        )
        WHERE (
            canonicality_state IN ('canonical', 'safe', 'finalized')
        )
);

CREATE INDEX IF NOT EXISTS surface_bindings_name_idx
    ON surface_bindings (
        logical_name_id,
        active_from,
        active_to,
        surface_binding_id
    )
    WHERE canonicality_state IN ('canonical', 'safe', 'finalized');

-- Interpret redo also reads orphaned bindings when restoring historical closes.
-- The canonical-only name index cannot serve that lookup.
CREATE INDEX IF NOT EXISTS surface_bindings_chain_name_history_idx
    ON surface_bindings (chain_id, logical_name_id);

CREATE INDEX IF NOT EXISTS surface_bindings_resource_idx
    ON surface_bindings (
        resource_id,
        active_from,
        active_to,
        logical_name_id,
        surface_binding_id
    )
    WHERE canonicality_state IN ('canonical', 'safe', 'finalized');

CREATE INDEX IF NOT EXISTS surface_bindings_block_idx
    ON surface_bindings (chain_id, block_hash);

CREATE INDEX IF NOT EXISTS surface_bindings_chain_block_number_idx
    ON surface_bindings (chain_id, block_number);

COMMENT ON TABLE contract_instances IS
    'This table stores stable identities for admitted contracts.';
COMMENT ON COLUMN contract_instances.contract_instance_id IS
    'This value is the stable contract ID.';
COMMENT ON COLUMN contract_instances.chain_id IS
    'This value identifies the chain.';
COMMENT ON COLUMN contract_instances.contract_kind IS
    'This value states the contract kind.';
COMMENT ON COLUMN contract_instances.provenance IS
    'This object identifies the admission source.';
COMMENT ON COLUMN contract_instances.inserted_at IS
    'This time records row creation.';

COMMENT ON TABLE contract_instance_addresses IS
    'This table stores the address ranges for admitted contracts.';
COMMENT ON COLUMN contract_instance_addresses.contract_instance_address_id IS
    'This value identifies the address range.';
COMMENT ON COLUMN contract_instance_addresses.contract_instance_id IS
    'This value identifies the admitted contract.';
COMMENT ON COLUMN contract_instance_addresses.chain_id IS
    'This value identifies the chain.';
COMMENT ON COLUMN contract_instance_addresses.address IS
    'This value is the contract address.';
COMMENT ON COLUMN contract_instance_addresses.active_from_block_number IS
    'This value is the first active block height.';
COMMENT ON COLUMN contract_instance_addresses.active_from_block_hash IS
    'This value identifies the first active block.';
COMMENT ON COLUMN contract_instance_addresses.active_to_block_number IS
    'This value is the last active block height.';
COMMENT ON COLUMN contract_instance_addresses.active_to_block_hash IS
    'This value identifies the last active block.';
COMMENT ON COLUMN contract_instance_addresses.source_manifest_id IS
    'This value identifies the declaring manifest.';
COMMENT ON COLUMN contract_instance_addresses.admitted_at IS
    'This time records admission.';
COMMENT ON COLUMN contract_instance_addresses.deactivated_at IS
    'This time records deactivation.';
COMMENT ON COLUMN contract_instance_addresses.provenance IS
    'This object identifies the address source.';

COMMENT ON TABLE discovery_edges IS
    'This table stores declared and announced contract links.';
COMMENT ON COLUMN discovery_edges.discovery_edge_id IS
    'This value identifies the contract link.';
COMMENT ON COLUMN discovery_edges.chain_id IS
    'This value identifies the chain.';
COMMENT ON COLUMN discovery_edges.edge_kind IS
    'This value states the link kind.';
COMMENT ON COLUMN discovery_edges.from_contract_instance_id IS
    'This value identifies the source contract.';
COMMENT ON COLUMN discovery_edges.to_contract_instance_id IS
    'This value identifies the target contract.';
COMMENT ON COLUMN discovery_edges.discovery_source IS
    'This value identifies the declaration or event.';
COMMENT ON COLUMN discovery_edges.admission_basis IS
    'This value states why the link is admitted.';
COMMENT ON COLUMN discovery_edges.source_manifest_id IS
    'This value identifies the governing manifest.';
COMMENT ON COLUMN discovery_edges.active_from_block_number IS
    'This value is the first active block height.';
COMMENT ON COLUMN discovery_edges.active_from_block_hash IS
    'This value identifies the first active block.';
COMMENT ON COLUMN discovery_edges.active_to_block_number IS
    'This value is the last active block height.';
COMMENT ON COLUMN discovery_edges.active_to_block_hash IS
    'This value identifies the last active block.';
COMMENT ON COLUMN discovery_edges.canonicality_state IS
    'This value states how the chain treats the link.';
COMMENT ON COLUMN discovery_edges.admitted_at IS
    'This time records admission.';
COMMENT ON COLUMN discovery_edges.deactivated_at IS
    'This time records deactivation.';
COMMENT ON COLUMN discovery_edges.provenance IS
    'This object identifies the link source.';

COMMENT ON TABLE token_lineages IS
    'This table stores stable token histories.';
COMMENT ON COLUMN token_lineages.token_lineage_id IS
    'This value is the stable token-history ID.';
COMMENT ON COLUMN token_lineages.chain_id IS
    'This value identifies the chain.';
COMMENT ON COLUMN token_lineages.block_hash IS
    'This value identifies the first observed block.';
COMMENT ON COLUMN token_lineages.block_number IS
    'This value is the first observed block height.';
COMMENT ON COLUMN token_lineages.provenance IS
    'This object identifies the token source.';
COMMENT ON COLUMN token_lineages.canonicality_state IS
    'This value states how the chain treats the token history.';
COMMENT ON COLUMN token_lineages.observed_at IS
    'This time records the stored observation.';
COMMENT ON COLUMN token_lineages.inserted_at IS
    'This time records row creation.';

COMMENT ON TABLE resources IS
    'This table stores stable authority objects.';
COMMENT ON COLUMN resources.resource_id IS
    'This value is the stable authority-object ID.';
COMMENT ON COLUMN resources.token_lineage_id IS
    'This value identifies the linked token history.';
COMMENT ON COLUMN resources.chain_id IS
    'This value identifies the chain.';
COMMENT ON COLUMN resources.block_hash IS
    'This value identifies the first observed block.';
COMMENT ON COLUMN resources.block_number IS
    'This value is the first observed block height.';
COMMENT ON COLUMN resources.provenance IS
    'This object identifies the authority source.';
COMMENT ON COLUMN resources.canonicality_state IS
    'This value states how the chain treats the authority object.';
COMMENT ON COLUMN resources.observed_at IS
    'This time records the stored observation.';
COMMENT ON COLUMN resources.inserted_at IS
    'This time records row creation.';

COMMENT ON TABLE name_surfaces IS
    'This table stores name identities, their raw names when known, and their visibility state.';
COMMENT ON COLUMN name_surfaces.logical_name_id IS
    'This value is the namespace and name hash joined by a colon.';
COMMENT ON COLUMN name_surfaces.namespace IS
    'This value identifies the name system.';
COMMENT ON COLUMN name_surfaces.raw_name IS
    'This value is the verbatim name, or NULL while the bytes of a label are unknown.';
COMMENT ON COLUMN name_surfaces.raw_labels IS
    'This array stores the verbatim labels, or NULL while the bytes of a label are unknown.';
COMMENT ON COLUMN name_surfaces.dns_encoded_name IS
    'This value is the DNS wire name, or NULL while the bytes of a label are unknown.';
COMMENT ON COLUMN name_surfaces.namehash IS
    'This value is the name hash.';
COMMENT ON COLUMN name_surfaces.labelhashes IS
    'This array stores the ordered label hashes.';
COMMENT ON COLUMN name_surfaces.normalizer_version IS
    'This value identifies the last normalization rules.';
COMMENT ON COLUMN name_surfaces.visibility_state IS
    'This value states whether readers can use the name.';
COMMENT ON COLUMN name_surfaces.normalization_errors IS
    'This array stores normalization errors.';
COMMENT ON COLUMN name_surfaces.deactivation_reason IS
    'This value explains why readers cannot use the row.';
COMMENT ON COLUMN name_surfaces.deactivated_at IS
    'This time records the latest deactivation.';
COMMENT ON COLUMN name_surfaces.chain_id IS
    'This value identifies the chain.';
COMMENT ON COLUMN name_surfaces.block_hash IS
    'This value identifies the first observed block.';
COMMENT ON COLUMN name_surfaces.block_number IS
    'This value is the first observed block height.';
COMMENT ON COLUMN name_surfaces.provenance IS
    'This object identifies the name source.';
COMMENT ON COLUMN name_surfaces.canonicality_state IS
    'This value states how the chain treats the name.';
COMMENT ON COLUMN name_surfaces.observed_at IS
    'This time records the stored observation.';
COMMENT ON COLUMN name_surfaces.inserted_at IS
    'This time records row creation.';
COMMENT ON COLUMN name_surfaces.preimage_event_identity IS
    'This value is the event identity of the earliest canonical preimage observation of the name.';

COMMENT ON INDEX name_surfaces_visibility_idx IS
    'This bounded index filters surfaces by namespace and visibility using the name hash. Verbatim names are unbounded chain input and are sorted after filtering when needed.';

COMMENT ON TABLE surface_bindings IS
    'This table stores time ranges between names and authority objects.';
COMMENT ON COLUMN surface_bindings.surface_binding_id IS
    'This value identifies the name-to-authority link.';
COMMENT ON COLUMN surface_bindings.logical_name_id IS
    'This value identifies the name.';
COMMENT ON COLUMN surface_bindings.resource_id IS
    'This value identifies the authority object.';
COMMENT ON COLUMN surface_bindings.binding_kind IS
    'This value states the link kind.';
COMMENT ON COLUMN surface_bindings.authority_arm IS
    'This value states which protocol authority arm owns this binding interval.';
COMMENT ON COLUMN surface_bindings.active_from IS
    'This time starts the active range.';
COMMENT ON COLUMN surface_bindings.active_to IS
    'This time ends the active range.';
COMMENT ON COLUMN surface_bindings.chain_id IS
    'This value identifies the chain.';
COMMENT ON COLUMN surface_bindings.block_hash IS
    'This value identifies the source block.';
COMMENT ON COLUMN surface_bindings.block_number IS
    'This value is the source block height.';
COMMENT ON COLUMN surface_bindings.provenance IS
    'This object identifies the link source.';
COMMENT ON COLUMN surface_bindings.canonicality_state IS
    'This value states how the chain treats the link.';
COMMENT ON COLUMN surface_bindings.observed_at IS
    'This time records the stored observation.';
COMMENT ON COLUMN surface_bindings.inserted_at IS
    'This time records row creation.';

-- Labels are chain data of any length, and an index entry larger than about 2.7 KB fails
-- the insert. The label indexes therefore hold fixed-size 64-bit hashes (one per label,
-- in label order, from this function, or one for the whole array), and every query that
-- uses them also compares the labels themselves; see ops/project-progressive/README.md.
CREATE OR REPLACE FUNCTION label_hashes(labels text[])
RETURNS bigint[]
LANGUAGE sql
IMMUTABLE
STRICT
PARALLEL SAFE
AS $$
    SELECT ARRAY(SELECT pg_catalog.hashtextextended(label, 0) FROM pg_catalog.unnest(labels) AS label)
$$;
CREATE INDEX IF NOT EXISTS name_surfaces_project_label_hashes_idx ON name_surfaces USING gin(label_hashes(raw_labels));
CREATE INDEX IF NOT EXISTS name_surfaces_project_suffix_hash_idx ON name_surfaces(namespace, hash_array_extended(raw_labels, 0));
CREATE INDEX IF NOT EXISTS name_surfaces_project_node_idx ON name_surfaces(namespace, lower(namehash));

-- Current spelling and lexical membership are derived atomically by Interpret and verified imports.
CREATE TABLE IF NOT EXISTS name_search_documents (
    search_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    logical_name_id text NOT NULL UNIQUE REFERENCES name_surfaces(logical_name_id) ON DELETE CASCADE,
    chain_id text NOT NULL,
    namespace text NOT NULL,
    namehash text NOT NULL,
    name text NOT NULL CHECK (name <> ''),
    display_name_override text,
    spelling_class smallint NOT NULL,
    CONSTRAINT name_search_documents_class_check CHECK (
        (spelling_class IN (0, 1) AND octet_length(name) <= 2000)
        OR (spelling_class = 2 AND octet_length(name) > 2000)),
    CONSTRAINT name_search_documents_display_check CHECK (
        display_name_override IS NULL OR spelling_class = 0)
);
COMMENT ON TABLE name_search_documents IS
    'Identity-derived current search spelling. Class 0 is raw-backed, 1 structural through 2000 bytes, 2 longer structural. Only raw-backed display may differ. Interpret and verified imports update this atomically with postings; Project owns no text.';
COMMENT ON COLUMN name_search_documents.search_id IS
    'Database-generated key for this current search document, referenced by its token postings.';
COMMENT ON COLUMN name_search_documents.logical_name_id IS
    'Source name_surfaces identity for this spelling; deleting the identity deletes this document.';
COMMENT ON COLUMN name_search_documents.chain_id IS
    'Chain of the source identity, used to match its published search eligibility.';
COMMENT ON COLUMN name_search_documents.namespace IS
    'Source namespace used to partition lexical search candidates.';
COMMENT ON COLUMN name_search_documents.namehash IS
    'Source namehash used to verify identity and order search results.';
COMMENT ON COLUMN name_search_documents.name IS
    'Current search spelling: normalized for a raw-backed name, or rendered from the structural label-hash path and verified preimages.';
COMMENT ON COLUMN name_search_documents.display_name_override IS
    'Canonical display spelling when it differs from the normalized raw-backed name; absent for structural spellings.';
COMMENT ON COLUMN name_search_documents.spelling_class IS
    'Spelling source and byte-size class: 0 raw-backed, 1 structural through 2000 UTF-8 bytes, 2 structural above 2000 bytes.';
CREATE INDEX IF NOT EXISTS name_search_documents_structural_order_idx
    ON name_search_documents (name, namespace, namehash, logical_name_id)
    WHERE spelling_class = 1;
CREATE INDEX IF NOT EXISTS name_search_documents_class_idx
    ON name_search_documents (namespace, spelling_class, search_id);
CREATE TABLE IF NOT EXISTS name_search_postings (
    namespace text NOT NULL,
    spelling_class smallint NOT NULL CHECK (spelling_class BETWEEN 0 AND 2),
    token_kind smallint NOT NULL CHECK (token_kind IN (1, 2)),
    token_length smallint NOT NULL CHECK (token_length BETWEEN 1 AND 3),
    token_bytes bytea NOT NULL CHECK (octet_length(token_bytes) BETWEEN 1 AND 12),
    search_id bigint NOT NULL REFERENCES name_search_documents(search_id) ON DELETE CASCADE,
    PRIMARY KEY (namespace, spelling_class, token_kind, token_length, token_bytes, search_id)
);
COMMENT ON TABLE name_search_postings IS
    'Distinct Unicode-scalar contains (kind 1) and anchored-prefix (kind 2) tokens of lengths 1 through 3. The leading key supports a bounded ordered probe; search_id supports atomic exact membership replacement.';
COMMENT ON COLUMN name_search_postings.namespace IS
    'Namespace copied from the owning search document for lexical probes.';
COMMENT ON COLUMN name_search_postings.spelling_class IS
    'Spelling class copied from the owning search document: 0 raw-backed, 1 short structural, 2 long structural.';
COMMENT ON COLUMN name_search_postings.token_kind IS
    'Lexical token kind: 1 contains, 2 anchored prefix.';
COMMENT ON COLUMN name_search_postings.token_length IS
    'Number of Unicode scalar values in the token, from 1 through 3.';
COMMENT ON COLUMN name_search_postings.token_bytes IS
    'Exact UTF-8 bytes of the token, from 1 through 12 bytes.';
COMMENT ON COLUMN name_search_postings.search_id IS
    'Owning search document key; deleting the document deletes its postings.';
CREATE INDEX IF NOT EXISTS name_search_postings_document_idx ON name_search_postings (search_id);
CREATE INDEX IF NOT EXISTS name_surfaces_search_labelhashes_idx
    ON name_surfaces USING gin (labelhashes) WHERE raw_name IS NULL;
