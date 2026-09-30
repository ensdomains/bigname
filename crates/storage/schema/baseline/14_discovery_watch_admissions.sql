CREATE TABLE IF NOT EXISTS discovery_watch_admissions (
    chain_id text NOT NULL,
    manifest_authority_fingerprint text NOT NULL,
    lineage_orphaning_epoch bigint NOT NULL,
    namespace text NOT NULL,
    target_source_family text NOT NULL,
    target_deployment_label text NOT NULL,
    address text NOT NULL,
    topic0 text NOT NULL,
    active_from_block_number bigint NOT NULL,
    active_to_block_number bigint NOT NULL,
    acknowledged_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (
        chain_id,
        manifest_authority_fingerprint,
        lineage_orphaning_epoch,
        namespace,
        target_source_family,
        target_deployment_label,
        address,
        topic0,
        active_from_block_number,
        active_to_block_number
    ),
    CHECK (btrim(chain_id) <> ''),
    CHECK (btrim(manifest_authority_fingerprint) <> ''),
    CHECK (lineage_orphaning_epoch >= 0),
    CHECK (btrim(namespace) <> ''),
    CHECK (btrim(target_source_family) <> ''),
    CHECK (btrim(target_deployment_label) <> ''),
    CHECK (btrim(address) <> ''),
    CHECK (btrim(topic0) <> ''),
    CHECK (active_from_block_number >= 0),
    CHECK (active_to_block_number >= active_from_block_number)
);

COMMENT ON TABLE discovery_watch_admissions IS
    'Interpret-owned replay coordination snapshot; not raw-fact evidence, redo authority, projection, or serving data';
COMMENT ON COLUMN discovery_watch_admissions.chain_id IS
    'Chain whose discovery-derived concrete watch intervals were acknowledged';
COMMENT ON COLUMN discovery_watch_admissions.manifest_authority_fingerprint IS
    'Active manifest/watch-plan fingerprint that scopes the acknowledged interval union';
COMMENT ON COLUMN discovery_watch_admissions.lineage_orphaning_epoch IS
    'Chain lineage epoch that scopes the acknowledged interval union';
COMMENT ON COLUMN discovery_watch_admissions.namespace IS
    'Manifest namespace that admitted the target discovery family';
COMMENT ON COLUMN discovery_watch_admissions.target_source_family IS
    'Resolver source family whose ABI supplies the watched topic';
COMMENT ON COLUMN discovery_watch_admissions.target_deployment_label IS
    'Deployment authority label shared by the discovery source and resolver target';
COMMENT ON COLUMN discovery_watch_admissions.address IS
    'Normalized concrete resolver address admitted by discovery';
COMMENT ON COLUMN discovery_watch_admissions.topic0 IS
    'Normalized event topic admitted for the concrete resolver address';
COMMENT ON COLUMN discovery_watch_admissions.active_from_block_number IS
    'Inclusive start of the acknowledged effective address and discovery-edge interval';
COMMENT ON COLUMN discovery_watch_admissions.active_to_block_number IS
    'Inclusive end of the acknowledged effective address and discovery-edge interval';
COMMENT ON COLUMN discovery_watch_admissions.acknowledged_at IS
    'Time Interpret atomically acknowledged the completed admission union';
