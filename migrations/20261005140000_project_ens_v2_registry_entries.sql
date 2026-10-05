-- Add the Project-owned ENSv2 registry entry family (F16) to existing schema-v2 databases:
-- project_ens_v2_entry_owner, the current token owner of each registry entry, and
-- project_ens_v2_registry_parent, the parent registry and label each registry announced. They
-- are the facts a reader joins ENSv2 registry operator approvals (ApprovalForAll rows of
-- project_account_approval with authority_kind ens_v2_registry) to. The change that adds the
-- family also rotates the interpreter content hash, so the families are rebuilt under the new
-- build before they serve; until then the empty tables hold no entry, and no reader reads them.
-- Tables and their indexes only, plus the comment of project_account_approval.authority_kind,
-- which now also names ens_v2_registry: no existing row changes. An empty schema-migration
-- database has no phase baseline yet, so this schema-migration is a no-op there and
-- phase-runner init-schema installs the same tables. The guard names the family marker, the
-- table every family publication writes.
DO $migration$
BEGIN
IF to_regclass('bigname_phase.project_family_marker') IS NULL THEN
    RETURN;
END IF;

EXECUTE $ddl$
CREATE TABLE IF NOT EXISTS bigname_phase.project_ens_v2_entry_owner (
    chain_id text NOT NULL,
    registry text NOT NULL,
    entry_key text NOT NULL,
    registry_contract_instance_id text,
    token_id text NOT NULL,
    upstream_resource text,
    resource_id uuid,
    status text NOT NULL,
    owner text,
    expiry numeric,
    owner_position jsonb,
    resource_position jsonb,
    block_number bigint NOT NULL,
    transaction_index bigint,
    log_index bigint,
    event_identity text NOT NULL,
    normalized_event_id bigint,
    PRIMARY KEY (chain_id, registry, entry_key),
    CHECK ((transaction_index IS NULL) = (log_index IS NULL)),
    CHECK (status IN ('registered', 'reserved', 'unregistered', 'unknown')),
    CHECK (status = 'registered' OR owner IS NULL)
)
$ddl$;
EXECUTE $ddl$
CREATE INDEX IF NOT EXISTS project_ens_v2_entry_owner_owner_idx
    ON bigname_phase.project_ens_v2_entry_owner (chain_id, owner, registry, entry_key)
    WHERE owner IS NOT NULL
$ddl$;
EXECUTE $ddl$
CREATE INDEX IF NOT EXISTS project_ens_v2_entry_owner_resource_idx
    ON bigname_phase.project_ens_v2_entry_owner (chain_id, resource_id)
    WHERE resource_id IS NOT NULL
$ddl$;
EXECUTE $ddl$
COMMENT ON TABLE bigname_phase.project_ens_v2_entry_owner IS
    'Project-owned ENSv2 registry entries of family F16: per registry and entry (the labelhash with its 32 version bits cleared), what the registry''s own logs last said about the entry''s token. It follows the contract, not the name: an entry whose own expiry has passed keeps its owner, because the registry burns nothing at expiry, and a name whose path was released keeps its entry. Readers compare expiry with the block they serve.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_ens_v2_entry_owner.chain_id IS
    'This value is the chain.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_ens_v2_entry_owner.registry IS
    'This value is the lower-cased address of the registry that emitted the logs.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_ens_v2_entry_owner.entry_key IS
    'This value is the entry: a 32-byte token id, resource or labelhash of the label with its low 32 bits cleared, as 0x and 64 lower-case hex digits.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_ens_v2_entry_owner.registry_contract_instance_id IS
    'This value is the registry contract instance the latest event carrying one named.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_ens_v2_entry_owner.token_id IS
    'This value is the token id the latest registry log of the entry named; its low 32 bits are the token version. With status unregistered it is the burned token.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_ens_v2_entry_owner.upstream_resource IS
    'This value is the resource the registry announced for the current token (TokenResource); its low 32 bits are the role version. Null from a registration or reservation until that log.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_ens_v2_entry_owner.resource_id IS
    'This value is the bigname resource of upstream_resource; null with it.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_ens_v2_entry_owner.status IS
    'This value is registered (a token was minted or transferred), reserved (the label is held without a token), unregistered (the token was burned by unregister) or unknown (the first log seen for the entry says nothing about its owner).'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_ens_v2_entry_owner.owner IS
    'This value is the lower-cased token owner while status is registered; null otherwise. Under status unknown null means not known, not the zero address.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_ens_v2_entry_owner.expiry IS
    'This value is the entry''s own expiry in Unix seconds: from the latest registration, reservation or renewal, or the block time of an unregister. Null when no log has stated it.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_ens_v2_entry_owner.owner_position IS
    'This value is the position of the registration, reservation, transfer or unregister that last set status and owner.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_ens_v2_entry_owner.resource_position IS
    'This value is the position of the TokenResource log that set the resource; null with it.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_ens_v2_entry_owner.block_number IS
    'This value is the block number of the event that last wrote the row.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_ens_v2_entry_owner.transaction_index IS
    'This value is the transaction index of the event that last wrote the row; null with log_index for a synthesised event.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_ens_v2_entry_owner.log_index IS
    'This value is the log index of the event that last wrote the row; null with transaction_index for a synthesised event.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_ens_v2_entry_owner.event_identity IS
    'This value is the event identity of the event that last wrote the row, the final tiebreak of the canonical event order, compared as bytes.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_ens_v2_entry_owner.normalized_event_id IS
    'This value names the event that last wrote the row in normalized_events as attribution only; it never takes part in ordering.'
$ddl$;
EXECUTE $ddl$
COMMENT ON INDEX bigname_phase.project_ens_v2_entry_owner_owner_idx IS
    'This index finds the entries an account owns, by registry, for joining an owner''s operator approvals to the tokens they reach.'
$ddl$;
EXECUTE $ddl$
COMMENT ON INDEX bigname_phase.project_ens_v2_entry_owner_resource_idx IS
    'This index finds the entry of a resource, for reading the current owner of a permission resource.'
$ddl$;
EXECUTE $ddl$
CREATE TABLE IF NOT EXISTS bigname_phase.project_ens_v2_registry_parent (
    chain_id text NOT NULL,
    registry text NOT NULL,
    parent text,
    raw_label_hex text,
    parent_entry_key text,
    block_number bigint NOT NULL,
    transaction_index bigint,
    log_index bigint,
    event_identity text NOT NULL,
    normalized_event_id bigint,
    PRIMARY KEY (chain_id, registry),
    CHECK ((transaction_index IS NULL) = (log_index IS NULL))
)
$ddl$;
EXECUTE $ddl$
CREATE INDEX IF NOT EXISTS project_ens_v2_registry_parent_entry_idx
    ON bigname_phase.project_ens_v2_registry_parent (chain_id, parent, parent_entry_key)
    WHERE parent IS NOT NULL
$ddl$;
EXECUTE $ddl$
COMMENT ON TABLE bigname_phase.project_ens_v2_registry_parent IS
    'Project-owned ENSv2 registry parents of family F16: per registry, the parent registry and label its latest ParentUpdated named. An ENSv1→ENSv2 migration-created WrapperRegistry gives its root roles to the owner of that label''s entry in the parent, and to that owner''s operators there.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_ens_v2_registry_parent.chain_id IS
    'This value is the chain.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_ens_v2_registry_parent.registry IS
    'This value is the lower-cased address of the registry that emitted ParentUpdated.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_ens_v2_registry_parent.parent IS
    'This value is the lower-cased parent registry; null when the registry named the zero address.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_ens_v2_registry_parent.raw_label_hex IS
    'This value is the label bytes the registry named, as lower-case hex without a prefix.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_ens_v2_registry_parent.parent_entry_key IS
    'This value is the entry of that label in the parent: the keccak-256 of the label bytes with its low 32 bits cleared, the key project_ens_v2_entry_owner uses.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_ens_v2_registry_parent.block_number IS
    'This value is the block number of the event that last wrote the row.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_ens_v2_registry_parent.transaction_index IS
    'This value is the transaction index of the event that last wrote the row; null with log_index for a synthesised event.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_ens_v2_registry_parent.log_index IS
    'This value is the log index of the event that last wrote the row; null with transaction_index for a synthesised event.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_ens_v2_registry_parent.event_identity IS
    'This value is the event identity of the event that last wrote the row, the final tiebreak of the canonical event order, compared as bytes.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_ens_v2_registry_parent.normalized_event_id IS
    'This value names the event that last wrote the row in normalized_events as attribution only; it never takes part in ordering.'
$ddl$;
EXECUTE $ddl$
COMMENT ON INDEX bigname_phase.project_ens_v2_registry_parent_entry_idx IS
    'This index finds the registries that name a parent entry, for reading which registries an entry''s owner holds root roles on.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_account_approval.authority_kind IS
    'This value is registry (an ENSv1 or Basenames registry), wrapper (the NameWrapper) or ens_v2_registry (an ENSv2 registry).'
$ddl$;
END
$migration$;
