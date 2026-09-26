-- Existing schema-v2 databases gain the Project-owned owned key family tables
-- project_grant, project_resource_admin_aggregate, project_account_approval
-- (TYR-36 step 2). The tables are additive and unread by every served path;
-- the family loop fills them block by block after each Project batch commits.
-- An empty schema-migration database has no phase baseline yet, so this
-- migration is a no-op there and phase-runner init-schema installs the same
-- tables.
DO $migration$
BEGIN
IF to_regclass('bigname_phase.name_current') IS NULL THEN
    RETURN;
END IF;

EXECUTE $ddl$
CREATE TABLE IF NOT EXISTS bigname_phase.project_grant (
    chain_id text NOT NULL,
    resource_id uuid NOT NULL,
    subject text NOT NULL,
    scope text NOT NULL,
    block_number bigint NOT NULL,
    transaction_index bigint,
    log_index bigint,
    event_identity text NOT NULL,
    normalized_event_id bigint,
    event_kind text NOT NULL,
    scope_kind text,
    scope_detail jsonb,
    effective_powers jsonb NOT NULL,
    grant_source jsonb,
    revocation_source jsonb,
    inheritance_path jsonb,
    transfer_behavior jsonb,
    revoked boolean NOT NULL,
    registration_position jsonb,
    PRIMARY KEY (chain_id, resource_id, subject, scope),
    CHECK ((transaction_index IS NULL) = (log_index IS NULL))
)
$ddl$;
EXECUTE $ddl$
COMMENT ON TABLE bigname_phase.project_grant IS
    'Project-owned raw grants of family F8: per resource, subject and scope, the latest PermissionChanged or RootPermissionChanged, unmasked; wrapper masks, grace and expiry retirement apply at read. Step 2 of TYR-36 writes it block by block beside the served tables and nothing reads it yet.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_grant.chain_id IS
    'This value is the chain.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_grant.resource_id IS
    'This value is the resource.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_grant.subject IS
    'This value is the lower-cased subject.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_grant.scope IS
    'This value is the scope key as permissions.rs builds it.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_grant.block_number IS
    'This value is the block number of the event that last wrote the row.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_grant.transaction_index IS
    'This value is the transaction index of the event that last wrote the row; null with log_index for a synthesised event, which sorts before every transaction of its block.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_grant.log_index IS
    'This value is the log index of the event that last wrote the row; null with transaction_index for a synthesised event.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_grant.event_identity IS
    'This value is the event identity of the event that last wrote the row, the final tiebreak of the canonical event order, compared as bytes.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_grant.normalized_event_id IS
    'This value names the event that last wrote the row in normalized_events as attribution only; it never takes part in ordering.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_grant.event_kind IS
    'This value is PermissionChanged or RootPermissionChanged.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_grant.scope_kind IS
    'This value is the scope kind with registry_root folded into root.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_grant.scope_detail IS
    'This value is the after-state scope object.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_grant.effective_powers IS
    'This value is the after-state effective_powers array, unmasked.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_grant.grant_source IS
    'This value is the after-state grant_source.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_grant.revocation_source IS
    'This value is the after-state revocation_source.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_grant.inheritance_path IS
    'This value is the after-state inheritance_path.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_grant.transfer_behavior IS
    'This value is the after-state transfer_behavior.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_grant.revoked IS
    'This value is true when the effective powers are empty; the row stays as a clear.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_grant.registration_position IS
    'This value is the position of the resource''s latest grant or reservation when the grant was written, the registration the grant belongs to.'
$ddl$;
EXECUTE $ddl$
CREATE TABLE IF NOT EXISTS bigname_phase.project_resource_admin_aggregate (
    chain_id text NOT NULL,
    resource_id uuid NOT NULL,
    block_number bigint NOT NULL,
    transaction_index bigint,
    log_index bigint,
    event_identity text NOT NULL,
    normalized_event_id bigint,
    admin_powers jsonb NOT NULL,
    PRIMARY KEY (chain_id, resource_id),
    CHECK ((transaction_index IS NULL) = (log_index IS NULL))
)
$ddl$;
EXECUTE $ddl$
COMMENT ON TABLE bigname_phase.project_resource_admin_aggregate IS
    'Project-owned admin aggregate of family F8: per resource, the admin powers any subject holds through a registry or root scope grant. Step 2 of TYR-36 writes it block by block beside the served tables and nothing reads it yet.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_resource_admin_aggregate.chain_id IS
    'This value is the chain.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_resource_admin_aggregate.resource_id IS
    'This value is the resource.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_resource_admin_aggregate.block_number IS
    'This value is the block number of the event that last wrote the row.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_resource_admin_aggregate.transaction_index IS
    'This value is the transaction index of the event that last wrote the row; null with log_index for a synthesised event, which sorts before every transaction of its block.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_resource_admin_aggregate.log_index IS
    'This value is the log index of the event that last wrote the row; null with transaction_index for a synthesised event.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_resource_admin_aggregate.event_identity IS
    'This value is the event identity of the event that last wrote the row, the final tiebreak of the canonical event order, compared as bytes.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_resource_admin_aggregate.normalized_event_id IS
    'This value names the event that last wrote the row in normalized_events as attribution only; it never takes part in ordering.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_resource_admin_aggregate.admin_powers IS
    'This value is the sorted distinct admin powers.'
$ddl$;
EXECUTE $ddl$
CREATE TABLE IF NOT EXISTS bigname_phase.project_account_approval (
    chain_id text NOT NULL,
    authority_kind text NOT NULL,
    authority_contract text NOT NULL,
    owner text NOT NULL,
    subject text NOT NULL,
    relation_kind text NOT NULL,
    block_number bigint NOT NULL,
    transaction_index bigint,
    log_index bigint,
    event_identity text NOT NULL,
    normalized_event_id bigint,
    authority_contract_instance_id text,
    approved boolean NOT NULL,
    effective_powers jsonb,
    grant_source jsonb,
    revocation_source jsonb,
    inheritance_path jsonb,
    transfer_behavior jsonb,
    PRIMARY KEY (chain_id, authority_kind, authority_contract, owner, subject, relation_kind),
    CHECK ((transaction_index IS NULL) = (log_index IS NULL))
)
$ddl$;
EXECUTE $ddl$
COMMENT ON TABLE bigname_phase.project_account_approval IS
    'Project-owned account approvals of family F9: the latest AccountPermissionChanged per authority contract, owner, subject and relation; an explicit false stays as a row. Step 2 of TYR-36 writes it block by block beside the served tables and nothing reads it yet.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_account_approval.chain_id IS
    'This value is the chain.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_account_approval.authority_kind IS
    'This value is registry or wrapper.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_account_approval.authority_contract IS
    'This value is the lower-cased authority contract.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_account_approval.owner IS
    'This value is the lower-cased owner.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_account_approval.subject IS
    'This value is the lower-cased approved operator.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_account_approval.relation_kind IS
    'This value is the relation kind.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_account_approval.block_number IS
    'This value is the block number of the event that last wrote the row.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_account_approval.transaction_index IS
    'This value is the transaction index of the event that last wrote the row; null with log_index for a synthesised event, which sorts before every transaction of its block.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_account_approval.log_index IS
    'This value is the log index of the event that last wrote the row; null with transaction_index for a synthesised event.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_account_approval.event_identity IS
    'This value is the event identity of the event that last wrote the row, the final tiebreak of the canonical event order, compared as bytes.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_account_approval.normalized_event_id IS
    'This value names the event that last wrote the row in normalized_events as attribution only; it never takes part in ordering.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_account_approval.authority_contract_instance_id IS
    'This value is the authority contract instance.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_account_approval.approved IS
    'This value is the after-state approved flag.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_account_approval.effective_powers IS
    'This value is the after-state effective_powers.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_account_approval.grant_source IS
    'This value is the after-state grant_source.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_account_approval.revocation_source IS
    'This value is the after-state revocation_source.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_account_approval.inheritance_path IS
    'This value is the after-state inheritance_path.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_account_approval.transfer_behavior IS
    'This value is the after-state transfer_behavior.'
$ddl$;
END
$migration$;
