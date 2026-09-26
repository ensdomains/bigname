-- Existing schema-v2 databases gain the Project-owned owned key family tables
-- project_resolver_classification, project_registry_pointer,
-- project_resource_pointer (TYR-36 step 2). The tables are additive and
-- unread by every served path; the family loop fills them block by block
-- after each Project batch commits. An empty schema-migration database has no
-- phase baseline yet, so this migration is a no-op there and phase-runner
-- init-schema installs the same tables.
DO $migration$
BEGIN
IF to_regclass('bigname_phase.name_current') IS NULL THEN
    RETURN;
END IF;

EXECUTE $ddl$
CREATE TABLE IF NOT EXISTS bigname_phase.project_resolver_classification (
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
)
$ddl$;
EXECUTE $ddl$
COMMENT ON TABLE bigname_phase.project_resolver_classification IS
    'Project-owned resolver classification of family F3, pinned to the declaration epoch active at the block that last touched the resolver: resolver_current without its sampled sections. Step 2 of TYR-36 writes it block by block beside the served tables and nothing reads it yet.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_resolver_classification.chain_id IS
    'This value is the chain.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_resolver_classification.resolver_address IS
    'This value is the lower-cased resolver address.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_resolver_classification.block_number IS
    'This value is the block number of the event that last wrote the row.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_resolver_classification.transaction_index IS
    'This value is the transaction index of the event that last wrote the row; null with log_index for a synthesised event, which sorts before every transaction of its block.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_resolver_classification.log_index IS
    'This value is the log index of the event that last wrote the row; null with transaction_index for a synthesised event.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_resolver_classification.event_identity IS
    'This value is the event identity of the event that last wrote the row, the final tiebreak of the canonical event order, compared as bytes.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_resolver_classification.normalized_event_id IS
    'This value names the event that last wrote the row in normalized_events as attribution only; it never takes part in ordering.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_resolver_classification.classification IS
    'This value is the source family, role and basis of the declaration or upgrade that classifies the resolver.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_resolver_classification.support_status IS
    'This value is supported or unsupported.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_resolver_classification.unsupported_reason IS
    'This value is the reason when unsupported.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_resolver_classification.manifest_id IS
    'This value is the declaring manifest.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_resolver_classification.manifest_event_id IS
    'This value is the SourceManifestUpdated event of that manifest.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_resolver_classification.admission_namespace IS
    'This value is the namespace of the declaring manifest.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_resolver_classification.summary_version IS
    'This value is the classification summary version.'
$ddl$;
EXECUTE $ddl$
CREATE TABLE IF NOT EXISTS bigname_phase.project_registry_pointer (
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
)
$ddl$;
EXECUTE $ddl$
COMMENT ON TABLE bigname_phase.project_registry_pointer IS
    'Project-owned ENSv1 registry-node resolver pointer of family F4: the latest ResolverChanged per node, clears included. Step 2 of TYR-36 writes it block by block beside the served tables and nothing reads it yet.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_registry_pointer.chain_id IS
    'This value is the chain.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_registry_pointer.namespace IS
    'This value is the namespace.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_registry_pointer.node IS
    'This value is lower(COALESCE(child_node, namehash, node)) of the event.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_registry_pointer.block_number IS
    'This value is the block number of the event that last wrote the row.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_registry_pointer.transaction_index IS
    'This value is the transaction index of the event that last wrote the row; null with log_index for a synthesised event, which sorts before every transaction of its block.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_registry_pointer.log_index IS
    'This value is the log index of the event that last wrote the row; null with transaction_index for a synthesised event.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_registry_pointer.event_identity IS
    'This value is the event identity of the event that last wrote the row, the final tiebreak of the canonical event order, compared as bytes.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_registry_pointer.normalized_event_id IS
    'This value names the event that last wrote the row in normalized_events as attribution only; it never takes part in ordering.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_registry_pointer.resolver_address IS
    'This value is the lower-cased resolver, the zero address for a clear.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_registry_pointer.resource_id IS
    'This value is the event''s resource when it names one.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_registry_pointer.source_family IS
    'This value is the event''s source family.'
$ddl$;
EXECUTE $ddl$
CREATE TABLE IF NOT EXISTS bigname_phase.project_resource_pointer (
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
)
$ddl$;
EXECUTE $ddl$
CREATE INDEX IF NOT EXISTS project_resource_pointer_resolver_idx ON bigname_phase.project_resource_pointer (chain_id, resolver_address, resource_id)
$ddl$;
EXECUTE $ddl$
COMMENT ON TABLE bigname_phase.project_resource_pointer IS
    'Project-owned resource resolver pointer of family F5: the current pointer with clears, the latest non-zero pointer and the record version boundary of a resource. Step 2 of TYR-36 writes it block by block beside the served tables and nothing reads it yet.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_resource_pointer.chain_id IS
    'This value is the chain.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_resource_pointer.resource_id IS
    'This value is the resource.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_resource_pointer.block_number IS
    'This value is the block number of the event that last wrote the row.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_resource_pointer.transaction_index IS
    'This value is the transaction index of the event that last wrote the row; null with log_index for a synthesised event, which sorts before every transaction of its block.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_resource_pointer.log_index IS
    'This value is the log index of the event that last wrote the row; null with transaction_index for a synthesised event.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_resource_pointer.event_identity IS
    'This value is the event identity of the event that last wrote the row, the final tiebreak of the canonical event order, compared as bytes.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_resource_pointer.normalized_event_id IS
    'This value names the event that last wrote the row in normalized_events as attribution only; it never takes part in ordering.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_resource_pointer.resolver_address IS
    'This value is the lower-cased resolver of the latest ResolverChanged, the zero address for a clear.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_resource_pointer.pointer_position IS
    'This value is that ResolverChanged''s position.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_resource_pointer.namespace IS
    'This value is that event''s namespace.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_resource_pointer.source_family IS
    'This value is that event''s source family.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_resource_pointer.namehash IS
    'This value is the lower-cased namehash that event carries.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_resource_pointer.nonzero_resolver_address IS
    'This value is the resolver of the latest non-zero ResolverChanged.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_resource_pointer.nonzero_position IS
    'This value is that event''s position.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_resource_pointer.boundary_kind IS
    'This value is the kind of the latest RecordVersionChanged or ResolverChanged on the resource.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_resource_pointer.boundary_position IS
    'This value is that boundary event''s position.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_resource_pointer.boundary_block_timestamp IS
    'This value is that boundary event''s block timestamp.'
$ddl$;
END
$migration$;
