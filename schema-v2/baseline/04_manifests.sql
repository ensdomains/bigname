CREATE TABLE IF NOT EXISTS manifest_versions (
    manifest_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    manifest_version bigint NOT NULL,
    namespace text NOT NULL,
    source_family text NOT NULL,
    chain_id text NOT NULL,
    deployment_label text NOT NULL,
    rollout_status text NOT NULL,
    normalizer_version text NOT NULL,
    file_path text NOT NULL UNIQUE,
    manifest_payload jsonb NOT NULL,
    loaded_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (manifest_id, chain_id),
    UNIQUE (
        manifest_id,
        namespace,
        source_family,
        manifest_version,
        chain_id
    ),
    UNIQUE (
        namespace,
        source_family,
        chain_id,
        deployment_label,
        manifest_version
    ),
    CHECK (manifest_version > 0),
    CHECK (btrim(namespace) <> ''),
    CHECK (btrim(source_family) <> ''),
    CHECK (btrim(chain_id) <> ''),
    CHECK (btrim(deployment_label) <> ''),
    CHECK (rollout_status IN ('draft', 'shadow', 'active', 'deprecated')),
    CHECK (btrim(normalizer_version) <> ''),
    CHECK (btrim(file_path) <> ''),
    CHECK (jsonb_typeof(manifest_payload) = 'object')
);

CREATE UNIQUE INDEX IF NOT EXISTS manifest_versions_one_active_idx
    ON manifest_versions (namespace, source_family, chain_id)
    WHERE rollout_status = 'active';

CREATE TABLE IF NOT EXISTS manifest_contract_instances (
    manifest_contract_instance_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    manifest_id bigint NOT NULL,
    chain_id text NOT NULL,
    declaration_kind text NOT NULL,
    declaration_name text NOT NULL,
    contract_instance_id uuid NOT NULL,
    declared_address text NOT NULL,
    abi_ref text,
    role text,
    proxy_kind text NOT NULL,
    implementation_contract_instance_id uuid,
    declared_implementation_address text,
    start_block_number bigint,
    CONSTRAINT manifest_contract_instances_manifest_fkey
        FOREIGN KEY (manifest_id, chain_id)
        REFERENCES manifest_versions (manifest_id, chain_id)
        ON DELETE CASCADE,
    FOREIGN KEY (chain_id, contract_instance_id)
        REFERENCES contract_instances (chain_id, contract_instance_id),
    FOREIGN KEY (chain_id, implementation_contract_instance_id)
        REFERENCES contract_instances (chain_id, contract_instance_id),
    UNIQUE (manifest_id, declaration_kind, declaration_name),
    CHECK (btrim(chain_id) <> ''),
    CHECK (declaration_kind IN ('root', 'contract')),
    CHECK (btrim(declaration_name) <> ''),
    CHECK (btrim(declared_address) <> ''),
    CHECK (abi_ref IS NULL OR btrim(abi_ref) <> ''),
    CHECK (
        (declaration_kind = 'root' AND role IS NULL)
        OR (
            declaration_kind = 'contract'
            AND role IS NOT NULL
            AND btrim(role) <> ''
        )
    ),
    CHECK (btrim(proxy_kind) <> ''),
    CHECK (
        (
            proxy_kind = 'none'
            AND implementation_contract_instance_id IS NULL
            AND declared_implementation_address IS NULL
        )
        OR (
            proxy_kind <> 'none'
            AND implementation_contract_instance_id IS NOT NULL
            AND declared_implementation_address IS NOT NULL
            AND btrim(declared_implementation_address) <> ''
        )
    ),
    CHECK (start_block_number IS NULL OR start_block_number >= 0)
);

CREATE INDEX IF NOT EXISTS manifest_contract_instances_role_idx
    ON manifest_contract_instances (manifest_id, role)
    WHERE role IS NOT NULL;

CREATE TABLE IF NOT EXISTS manifest_discovery_rules (
    manifest_discovery_rule_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    manifest_id bigint NOT NULL,
    edge_kind text NOT NULL,
    from_role text,
    admission text NOT NULL,
    rule_payload jsonb NOT NULL DEFAULT '{}'::jsonb,
    CONSTRAINT manifest_discovery_rules_manifest_fkey
        FOREIGN KEY (manifest_id)
        REFERENCES manifest_versions (manifest_id)
        ON DELETE CASCADE,
    CONSTRAINT manifest_discovery_rules_edge_kind_check
        CHECK (
            edge_kind IN (
                'resolver',
                'subregistry',
                'proxy_implementation',
                'migration'
            )
        ),
    CHECK (from_role IS NULL OR btrim(from_role) <> ''),
    CHECK (btrim(admission) <> ''),
    CHECK (jsonb_typeof(rule_payload) = 'object')
);

CREATE INDEX IF NOT EXISTS manifest_discovery_rules_manifest_idx
    ON manifest_discovery_rules (manifest_id, edge_kind);

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conrelid = 'contract_instance_addresses'::regclass
          AND conname = 'contract_instance_addresses_manifest_fkey'
    ) THEN
        ALTER TABLE contract_instance_addresses
            ADD CONSTRAINT contract_instance_addresses_manifest_fkey
            FOREIGN KEY (source_manifest_id, chain_id)
            REFERENCES manifest_versions (manifest_id, chain_id)
            ON DELETE SET NULL (source_manifest_id);
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conrelid = 'discovery_edges'::regclass
          AND conname = 'discovery_edges_manifest_fkey'
    ) THEN
        ALTER TABLE discovery_edges
            ADD CONSTRAINT discovery_edges_manifest_fkey
            FOREIGN KEY (source_manifest_id, chain_id)
            REFERENCES manifest_versions (manifest_id, chain_id)
            ON DELETE SET NULL (source_manifest_id);
    END IF;
END
$$;

COMMENT ON TABLE manifest_versions IS
    'This table stores each loaded manifest declaration.';
COMMENT ON COLUMN manifest_versions.manifest_id IS
    'This value identifies the loaded manifest.';
COMMENT ON COLUMN manifest_versions.manifest_version IS
    'This value is the declared manifest version.';
COMMENT ON COLUMN manifest_versions.namespace IS
    'This value identifies the name system.';
COMMENT ON COLUMN manifest_versions.source_family IS
    'This value identifies the declared source group.';
COMMENT ON COLUMN manifest_versions.chain_id IS
    'This value identifies the chain.';
COMMENT ON COLUMN manifest_versions.deployment_label IS
    'This value stores the authored deployment epoch label.';
COMMENT ON COLUMN manifest_versions.rollout_status IS
    'This value states whether the manifest is active.';
COMMENT ON COLUMN manifest_versions.normalizer_version IS
    'This value identifies the declared normalization rules.';
COMMENT ON COLUMN manifest_versions.file_path IS
    'This value is the repository path.';
COMMENT ON COLUMN manifest_versions.manifest_payload IS
    'This object stores the complete declaration.';
COMMENT ON COLUMN manifest_versions.loaded_at IS
    'This time records the last load.';

COMMENT ON TABLE manifest_contract_instances IS
    'This table links manifest entries to admitted contracts.';
COMMENT ON COLUMN manifest_contract_instances.manifest_contract_instance_id IS
    'This value identifies the manifest entry.';
COMMENT ON COLUMN manifest_contract_instances.manifest_id IS
    'This value identifies the manifest.';
COMMENT ON COLUMN manifest_contract_instances.chain_id IS
    'This value identifies the manifest and contract chain.';
COMMENT ON COLUMN manifest_contract_instances.declaration_kind IS
    'This value states whether the entry is a root or contract.';
COMMENT ON COLUMN manifest_contract_instances.declaration_name IS
    'This value identifies the entry in the manifest.';
COMMENT ON COLUMN manifest_contract_instances.contract_instance_id IS
    'This value identifies the admitted contract.';
COMMENT ON COLUMN manifest_contract_instances.declared_address IS
    'This value is the declared contract address.';
COMMENT ON COLUMN manifest_contract_instances.abi_ref IS
    'This value identifies the declared ABI.';
COMMENT ON COLUMN manifest_contract_instances.role IS
    'This value states the declared contract role.';
COMMENT ON COLUMN manifest_contract_instances.proxy_kind IS
    'This value states the proxy kind.';
COMMENT ON COLUMN manifest_contract_instances.implementation_contract_instance_id IS
    'This value identifies the declared implementation.';
COMMENT ON COLUMN manifest_contract_instances.declared_implementation_address IS
    'This value is the declared implementation address.';
COMMENT ON COLUMN manifest_contract_instances.start_block_number IS
    'This value is the first declared block height.';

COMMENT ON TABLE manifest_discovery_rules IS
    'This table stores contract-admission rules from manifests.';
COMMENT ON COLUMN manifest_discovery_rules.manifest_discovery_rule_id IS
    'This value identifies the admission rule.';
COMMENT ON COLUMN manifest_discovery_rules.manifest_id IS
    'This value identifies the manifest.';
COMMENT ON COLUMN manifest_discovery_rules.edge_kind IS
    'This value states the admitted link kind.';
COMMENT ON COLUMN manifest_discovery_rules.from_role IS
    'This value identifies the declaring contract role.';
COMMENT ON COLUMN manifest_discovery_rules.admission IS
    'This value states the admission method.';
COMMENT ON COLUMN manifest_discovery_rules.rule_payload IS
    'This object stores the rule fields.';
