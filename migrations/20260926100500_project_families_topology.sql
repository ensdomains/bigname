-- Existing schema-v2 databases gain the Project-owned owned key family tables
-- project_name_alias, project_resolver_alias, project_child_edge_candidate,
-- project_parent_subregistry (TYR-36 step 2). The tables are additive and
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
CREATE TABLE IF NOT EXISTS bigname_phase.project_name_alias (
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
)
$ddl$;
EXECUTE $ddl$
COMMENT ON TABLE bigname_phase.project_name_alias IS
    'Project-owned name aliases of family F10: per source name, the latest AliasChanged with its event-carried target. Step 2 of TYR-36 writes it block by block beside the served tables and nothing reads it yet.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_name_alias.chain_id IS
    'This value is the chain.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_name_alias.logical_name_id IS
    'This value is the source name.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_name_alias.block_number IS
    'This value is the block number of the event that last wrote the row.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_name_alias.transaction_index IS
    'This value is the transaction index of the event that last wrote the row; null with log_index for a synthesised event, which sorts before every transaction of its block.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_name_alias.log_index IS
    'This value is the log index of the event that last wrote the row; null with transaction_index for a synthesised event.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_name_alias.event_identity IS
    'This value is the event identity of the event that last wrote the row, the final tiebreak of the canonical event order, compared as bytes.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_name_alias.normalized_event_id IS
    'This value names the event that last wrote the row in normalized_events as attribution only; it never takes part in ordering.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_name_alias.active IS
    'This value is the after-state active flag, true when absent.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_name_alias.alias_state IS
    'This value is the after-state alias_state.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_name_alias.to_logical_name_id IS
    'This value is the after-state to_logical_name_id.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_name_alias.to_name IS
    'This value is the after-state to_name.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_name_alias.to_resource_id IS
    'This value is the after-state to_resource_id.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_name_alias.to_normalized_name IS
    'This value is the after-state to_normalized_name.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_name_alias.to_canonical_display_name IS
    'This value is the after-state to_canonical_display_name.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_name_alias.to_namehash IS
    'This value is the after-state to_namehash.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_name_alias.resolver_address IS
    'This value is the lower-cased resolver the alias was written at.'
$ddl$;
EXECUTE $ddl$
CREATE TABLE IF NOT EXISTS bigname_phase.project_resolver_alias (
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
)
$ddl$;
EXECUTE $ddl$
COMMENT ON TABLE bigname_phase.project_resolver_alias IS
    'Project-owned per-resolver alias state of family F10: per resolver and alias identity, the latest AliasChanged. Step 2 of TYR-36 writes it block by block beside the served tables and nothing reads it yet.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_resolver_alias.chain_id IS
    'This value is the chain.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_resolver_alias.resolver_address IS
    'This value is lower(COALESCE(after resolver, before resolver, emitting address)).'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_resolver_alias.alias_identity IS
    'This value is COALESCE(logical_name_id, from_logical_name_id, from_namehash, from_dns_encoded_name, from_name, event_identity).'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_resolver_alias.block_number IS
    'This value is the block number of the event that last wrote the row.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_resolver_alias.transaction_index IS
    'This value is the transaction index of the event that last wrote the row; null with log_index for a synthesised event, which sorts before every transaction of its block.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_resolver_alias.log_index IS
    'This value is the log index of the event that last wrote the row; null with transaction_index for a synthesised event.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_resolver_alias.event_identity IS
    'This value is the event identity of the event that last wrote the row, the final tiebreak of the canonical event order, compared as bytes.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_resolver_alias.normalized_event_id IS
    'This value names the event that last wrote the row in normalized_events as attribution only; it never takes part in ordering.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_resolver_alias.active IS
    'This value is the after-state active flag, true when absent.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_resolver_alias.alias_state IS
    'This value is the after-state alias_state, active when absent.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_resolver_alias.from_dns_encoded_name IS
    'This value is the from_dns_encoded_name.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_resolver_alias.to_dns_encoded_name IS
    'This value is the to_dns_encoded_name.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_resolver_alias.from_name IS
    'This value is the from_name.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_resolver_alias.to_logical_name_id IS
    'This value is the after-state to_logical_name_id.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_resolver_alias.to_name IS
    'This value is the after-state to_name.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_resolver_alias.to_resource_id IS
    'This value is the after-state to_resource_id.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_resolver_alias.logical_name_id IS
    'This value is the event''s name.'
$ddl$;
EXECUTE $ddl$
CREATE TABLE IF NOT EXISTS bigname_phase.project_child_edge_candidate (
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
)
$ddl$;
EXECUTE $ddl$
COMMENT ON TABLE bigname_phase.project_child_edge_candidate IS
    'Project-owned ENSv1 and Basenames child edge candidates of family F11: the latest SubregistryChanged per parent, child and arm, kept while ineligible. Candidates are retained per parent: a later edge for the child under another parent adds a row and leaves the earlier parent''s row in place, so the reader selects the latest per child and arm. Step 2 of TYR-36 writes it block by block beside the served tables and nothing reads it yet.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_child_edge_candidate.chain_id IS
    'This value is the chain.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_child_edge_candidate.namespace IS
    'This value is the namespace.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_child_edge_candidate.parent_node IS
    'This value is the lower-cased parent node.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_child_edge_candidate.child_node IS
    'This value is the lower-cased child node.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_child_edge_candidate.authority_arm IS
    'This value is ens_v1 or basenames.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_child_edge_candidate.block_number IS
    'This value is the block number of the event that last wrote the row.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_child_edge_candidate.transaction_index IS
    'This value is the transaction index of the event that last wrote the row; null with log_index for a synthesised event, which sorts before every transaction of its block.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_child_edge_candidate.log_index IS
    'This value is the log index of the event that last wrote the row; null with transaction_index for a synthesised event.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_child_edge_candidate.event_identity IS
    'This value is the event identity of the event that last wrote the row, the final tiebreak of the canonical event order, compared as bytes.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_child_edge_candidate.normalized_event_id IS
    'This value names the event that last wrote the row in normalized_events as attribution only; it never takes part in ordering.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_child_edge_candidate.owner IS
    'This value is the lower-cased edge owner.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_child_edge_candidate.owner_getter IS
    'This value is the lower-cased edge owner_getter.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_child_edge_candidate.labelhash IS
    'This value is the lower-cased labelhash.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_child_edge_candidate.source_family IS
    'This value is the event''s source family.'
$ddl$;
EXECUTE $ddl$
CREATE TABLE IF NOT EXISTS bigname_phase.project_parent_subregistry (
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
)
$ddl$;
EXECUTE $ddl$
COMMENT ON TABLE bigname_phase.project_parent_subregistry IS
    'Project-owned ENSv2 parent subregistry of family F11: per parent name, the latest SubregistryChanged address, clears included. Step 2 of TYR-36 writes it block by block beside the served tables and nothing reads it yet.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_parent_subregistry.chain_id IS
    'This value is the chain.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_parent_subregistry.logical_name_id IS
    'This value is the parent name.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_parent_subregistry.block_number IS
    'This value is the block number of the event that last wrote the row.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_parent_subregistry.transaction_index IS
    'This value is the transaction index of the event that last wrote the row; null with log_index for a synthesised event, which sorts before every transaction of its block.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_parent_subregistry.log_index IS
    'This value is the log index of the event that last wrote the row; null with transaction_index for a synthesised event.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_parent_subregistry.event_identity IS
    'This value is the event identity of the event that last wrote the row, the final tiebreak of the canonical event order, compared as bytes.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_parent_subregistry.normalized_event_id IS
    'This value names the event that last wrote the row in normalized_events as attribution only; it never takes part in ordering.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_parent_subregistry.subregistry_address IS
    'This value is the lower-cased subregistry, empty or zero for a clear.'
$ddl$;
END
$migration$;
