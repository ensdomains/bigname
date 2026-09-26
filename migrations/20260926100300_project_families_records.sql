-- Existing schema-v2 databases gain the Project-owned owned key family tables
-- project_node_record_partition, project_node_record_value,
-- project_record_id_value, project_resolver_link (TYR-36 step 2). The tables
-- are additive and unread by every served path; the family loop fills them
-- block by block after each Project batch commits. An empty schema-migration
-- database has no phase baseline yet, so this migration is a no-op there and
-- phase-runner init-schema installs the same tables.
DO $migration$
BEGIN
IF to_regclass('bigname_phase.name_current') IS NULL THEN
    RETURN;
END IF;

EXECUTE $ddl$
CREATE TABLE IF NOT EXISTS bigname_phase.project_node_record_partition (
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
)
$ddl$;
EXECUTE $ddl$
COMMENT ON TABLE bigname_phase.project_node_record_partition IS
    'Project-owned node record partitions of family F6: per resolver, attribution arm and arm identity, the latest record version event. Step 2 of TYR-36 writes it block by block beside the served tables and nothing reads it yet.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_node_record_partition.chain_id IS
    'This value is the chain.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_node_record_partition.resolver_address IS
    'This value is the lower-cased resolver.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_node_record_partition.arm IS
    'This value is named, native or guarded.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_node_record_partition.arm_identity IS
    'This value is the logical name for named; node and source family for native; node, source family, namespace and manifest for guarded, joined by a vertical bar.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_node_record_partition.block_number IS
    'This value is the block number of the event that last wrote the row.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_node_record_partition.transaction_index IS
    'This value is the transaction index of the event that last wrote the row; null with log_index for a synthesised event, which sorts before every transaction of its block.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_node_record_partition.log_index IS
    'This value is the log index of the event that last wrote the row; null with transaction_index for a synthesised event.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_node_record_partition.event_identity IS
    'This value is the event identity of the event that last wrote the row, the final tiebreak of the canonical event order, compared as bytes.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_node_record_partition.normalized_event_id IS
    'This value names the event that last wrote the row in normalized_events as attribution only; it never takes part in ordering.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_node_record_partition.node IS
    'This value is the lower-cased node.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_node_record_partition.logical_name_id IS
    'This value is the name the events carry.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_node_record_partition.source_family IS
    'This value is the events'' source family.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_node_record_partition.namespace IS
    'This value is the events'' namespace.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_node_record_partition.source_manifest_id IS
    'This value is the events'' source manifest.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_node_record_partition.version_position IS
    'This value is the position of the partition''s latest RecordVersionChanged.'
$ddl$;
EXECUTE $ddl$
CREATE TABLE IF NOT EXISTS bigname_phase.project_node_record_value (
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
)
$ddl$;
EXECUTE $ddl$
COMMENT ON TABLE bigname_phase.project_node_record_value IS
    'Project-owned node record values of family F6: per partition and record key, the latest record in the canonical event order, with its coin-60 compatibility sibling. Step 2 of TYR-36 writes it block by block beside the served tables and nothing reads it yet.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_node_record_value.chain_id IS
    'This value is the chain.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_node_record_value.resolver_address IS
    'This value is the lower-cased resolver.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_node_record_value.arm IS
    'This value is the partition''s arm.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_node_record_value.arm_identity IS
    'This value is the partition''s arm identity.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_node_record_value.record_key IS
    'This value is the record key.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_node_record_value.block_number IS
    'This value is the block number of the event that last wrote the row.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_node_record_value.transaction_index IS
    'This value is the transaction index of the event that last wrote the row; null with log_index for a synthesised event, which sorts before every transaction of its block.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_node_record_value.log_index IS
    'This value is the log index of the event that last wrote the row; null with transaction_index for a synthesised event.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_node_record_value.event_identity IS
    'This value is the event identity of the event that last wrote the row, the final tiebreak of the canonical event order, compared as bytes.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_node_record_value.normalized_event_id IS
    'This value names the event that last wrote the row in normalized_events as attribution only; it never takes part in ordering.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_node_record_value.status IS
    'This value is success, not_found or unsupported, as the inventory builder classifies the value.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_node_record_value.value IS
    'This value is the record value as the event carries it.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_node_record_value.record_family IS
    'This value is the after-state record_family.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_node_record_value.selector_key IS
    'This value is the after-state selector_key.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_node_record_value.contenthash_hex IS
    'This value is the after-state contenthash_hex.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_node_record_value.address_bytes_hex IS
    'This value is the after-state address_bytes_hex.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_node_record_value.source_event IS
    'This value is the after-state source_event.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_node_record_value.storage_model IS
    'This value is the after-state storage_model.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_node_record_value.sibling_value IS
    'This value is the AddressChanged half''s value when this record is the AddrChanged half of a coin-60 pair.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_node_record_value.sibling_position IS
    'This value is that AddressChanged half''s own position.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_node_record_value.node IS
    'This value is the lower-cased node.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_node_record_value.logical_name_id IS
    'This value is the name the record carries.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_node_record_value.resource_id IS
    'This value is the resource the record carries.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_node_record_value.source_family IS
    'This value is the record''s source family.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_node_record_value.namespace IS
    'This value is the record''s namespace.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_node_record_value.source_manifest_id IS
    'This value is the record''s source manifest.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_node_record_value.hydrated_value IS
    'This value is the hydrated text value; null until hydration moves into the block.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_node_record_value.hydrated_at_block IS
    'This value is the block the hydrated value was read at.'
$ddl$;
EXECUTE $ddl$
CREATE TABLE IF NOT EXISTS bigname_phase.project_record_id_value (
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
)
$ddl$;
EXECUTE $ddl$
COMMENT ON TABLE bigname_phase.project_record_id_value IS
    'Project-owned record-id values of family F7: per resolver, record id and record key, the latest RecordChanged with storage model resolver_record_id. Step 2 of TYR-36 writes it block by block beside the served tables and nothing reads it yet.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_record_id_value.chain_id IS
    'This value is the chain.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_record_id_value.resolver_address IS
    'This value is the lower-cased resolver.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_record_id_value.record_id IS
    'This value is the resolver record id.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_record_id_value.record_key IS
    'This value is the record key.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_record_id_value.block_number IS
    'This value is the block number of the event that last wrote the row.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_record_id_value.transaction_index IS
    'This value is the transaction index of the event that last wrote the row; null with log_index for a synthesised event, which sorts before every transaction of its block.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_record_id_value.log_index IS
    'This value is the log index of the event that last wrote the row; null with transaction_index for a synthesised event.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_record_id_value.event_identity IS
    'This value is the event identity of the event that last wrote the row, the final tiebreak of the canonical event order, compared as bytes.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_record_id_value.normalized_event_id IS
    'This value names the event that last wrote the row in normalized_events as attribution only; it never takes part in ordering.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_record_id_value.status IS
    'This value is success, not_found or unsupported, as the inventory builder classifies the value.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_record_id_value.value IS
    'This value is the record value as the event carries it.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_record_id_value.record_family IS
    'This value is the after-state record_family.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_record_id_value.selector_key IS
    'This value is the after-state selector_key.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_record_id_value.contenthash_hex IS
    'This value is the after-state contenthash_hex.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_record_id_value.address_bytes_hex IS
    'This value is the after-state address_bytes_hex.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_record_id_value.source_event IS
    'This value is the after-state source_event.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_record_id_value.storage_model IS
    'This value is the after-state storage_model.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_record_id_value.source_family IS
    'This value is the record''s source family.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_record_id_value.namespace IS
    'This value is the record''s namespace.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_record_id_value.source_manifest_id IS
    'This value is the record''s source manifest.'
$ddl$;
EXECUTE $ddl$
CREATE TABLE IF NOT EXISTS bigname_phase.project_resolver_link (
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
)
$ddl$;
EXECUTE $ddl$
COMMENT ON TABLE bigname_phase.project_resolver_link IS
    'Project-owned resolver links of family F7: per resolver and node, the latest ResolverRecordLinked; record id 0 is an explicit clear. Step 2 of TYR-36 writes it block by block beside the served tables and nothing reads it yet.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_resolver_link.chain_id IS
    'This value is the chain.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_resolver_link.resolver_address IS
    'This value is the lower-cased resolver.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_resolver_link.node IS
    'This value is the lower-cased node; 32 zero bytes is the default link.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_resolver_link.block_number IS
    'This value is the block number of the event that last wrote the row.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_resolver_link.transaction_index IS
    'This value is the transaction index of the event that last wrote the row; null with log_index for a synthesised event, which sorts before every transaction of its block.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_resolver_link.log_index IS
    'This value is the log index of the event that last wrote the row; null with transaction_index for a synthesised event.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_resolver_link.event_identity IS
    'This value is the event identity of the event that last wrote the row, the final tiebreak of the canonical event order, compared as bytes.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_resolver_link.normalized_event_id IS
    'This value names the event that last wrote the row in normalized_events as attribution only; it never takes part in ordering.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_resolver_link.record_id IS
    'This value is the linked record id, 0 for an unlink.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_resolver_link.storage_model IS
    'This value is the after-state storage_model.'
$ddl$;
END
$migration$;
