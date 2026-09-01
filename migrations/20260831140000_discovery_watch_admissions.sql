-- Existing schema-v2 databases gain the Interpret-owned discovery admission
-- snapshot. An empty schema-migration database has no phase baseline yet, so
-- this migration is a no-op there; phase-runner init-schema installs the same
-- table afterward.
DO $migration$
BEGIN
IF to_regclass('bigname_phase.chain_phase_state') IS NOT NULL THEN
EXECUTE $ddl$
CREATE TABLE IF NOT EXISTS bigname_phase.discovery_watch_admissions (
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
)
$ddl$;

EXECUTE $ddl$
COMMENT ON TABLE bigname_phase.discovery_watch_admissions IS
    'Interpret-owned replay coordination snapshot; not raw-fact evidence, redo authority, projection, or serving data'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.discovery_watch_admissions.chain_id IS
    'Chain whose discovery-derived concrete watch intervals were acknowledged'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.discovery_watch_admissions.manifest_authority_fingerprint IS
    'Active manifest/watch-plan fingerprint that scopes the acknowledged interval union'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.discovery_watch_admissions.lineage_orphaning_epoch IS
    'Chain lineage epoch that scopes the acknowledged interval union'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.discovery_watch_admissions.namespace IS
    'Manifest namespace that admitted the target discovery family'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.discovery_watch_admissions.target_source_family IS
    'Resolver source family whose ABI supplies the watched topic'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.discovery_watch_admissions.target_deployment_label IS
    'Deployment authority label shared by the discovery source and resolver target'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.discovery_watch_admissions.address IS
    'Normalized concrete resolver address admitted by discovery'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.discovery_watch_admissions.topic0 IS
    'Normalized event topic admitted for the concrete resolver address'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.discovery_watch_admissions.active_from_block_number IS
    'Inclusive start of the acknowledged effective address and discovery-edge interval'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.discovery_watch_admissions.active_to_block_number IS
    'Inclusive end of the acknowledged effective address and discovery-edge interval'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.discovery_watch_admissions.acknowledged_at IS
    'Time Interpret atomically acknowledged the completed admission union'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.chain_phase_state.redo_attempt_generation IS
    'This nonnegative, row-local counter increments when an explicit redo begins, when the phase runner installs or extends required downstream redo, and when the shared required-Ingest installer records genuinely new manifest or discovery demand. Repeated observation of unchanged semantic demand is suppressed before installation and does not advance it.'
$ddl$;
END IF;
END
$migration$;
