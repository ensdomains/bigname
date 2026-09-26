-- Existing schema-v2 databases gain project_registry_owner_event (TYR-36
-- step 2): every owner-setting registry event of an ENSv1 or Basenames node,
-- kept by position, since the node row keeps only the latest owner group and
-- step 3 reads the owner history. A database without the table has families
-- built without that history, so it resets every owned key family and the next
-- family run rebuilds them; the families are unread by every served path. An
-- empty schema-migration database has no phase baseline yet, so this migration
-- is a no-op there and phase-runner init-schema installs the same table.
DO $migration$
BEGIN
IF to_regclass('bigname_phase.name_current') IS NULL THEN
    RETURN;
END IF;

IF to_regclass('bigname_phase.project_registry_owner_event') IS NULL THEN
    EXECUTE $ddl$DELETE FROM bigname_phase.project_family_marker$ddl$;
    EXECUTE $ddl$DELETE FROM bigname_phase.project_family_undo$ddl$;
    EXECUTE $ddl$DELETE FROM bigname_phase.project_repair_record$ddl$;
    EXECUTE $ddl$DELETE FROM bigname_phase.project_name_state$ddl$;
    EXECUTE $ddl$DELETE FROM bigname_phase.project_binding_candidate$ddl$;
    EXECUTE $ddl$DELETE FROM bigname_phase.project_lifecycle_key_state$ddl$;
    EXECUTE $ddl$DELETE FROM bigname_phase.project_lifecycle_triple_summary$ddl$;
    EXECUTE $ddl$DELETE FROM bigname_phase.project_lifecycle_association$ddl$;
    EXECUTE $ddl$DELETE FROM bigname_phase.project_lifecycle_event$ddl$;
    EXECUTE $ddl$DELETE FROM bigname_phase.project_child_registration_state$ddl$;
    EXECUTE $ddl$DELETE FROM bigname_phase.project_wrapper_state$ddl$;
    EXECUTE $ddl$DELETE FROM bigname_phase.project_registry_node_state$ddl$;
    EXECUTE $ddl$DELETE FROM bigname_phase.project_registry_binding_observation$ddl$;
    EXECUTE $ddl$DELETE FROM bigname_phase.project_resolver_classification$ddl$;
    EXECUTE $ddl$DELETE FROM bigname_phase.project_registry_pointer$ddl$;
    EXECUTE $ddl$DELETE FROM bigname_phase.project_resource_pointer$ddl$;
    EXECUTE $ddl$DELETE FROM bigname_phase.project_node_record_partition$ddl$;
    EXECUTE $ddl$DELETE FROM bigname_phase.project_node_record_value$ddl$;
    EXECUTE $ddl$DELETE FROM bigname_phase.project_record_id_value$ddl$;
    EXECUTE $ddl$DELETE FROM bigname_phase.project_resolver_link$ddl$;
    EXECUTE $ddl$DELETE FROM bigname_phase.project_grant$ddl$;
    EXECUTE $ddl$DELETE FROM bigname_phase.project_resource_admin_aggregate$ddl$;
    EXECUTE $ddl$DELETE FROM bigname_phase.project_account_approval$ddl$;
    EXECUTE $ddl$DELETE FROM bigname_phase.project_name_alias$ddl$;
    EXECUTE $ddl$DELETE FROM bigname_phase.project_resolver_alias$ddl$;
    EXECUTE $ddl$DELETE FROM bigname_phase.project_child_edge_candidate$ddl$;
    EXECUTE $ddl$DELETE FROM bigname_phase.project_parent_subregistry$ddl$;
    EXECUTE $ddl$DELETE FROM bigname_phase.project_reverse_tuple$ddl$;
    EXECUTE $ddl$DELETE FROM bigname_phase.project_reverse_node_claim$ddl$;
    EXECUTE $ddl$DELETE FROM bigname_phase.project_claim_normalization$ddl$;
    EXECUTE $ddl$DELETE FROM bigname_phase.project_address_name_fold$ddl$;
    EXECUTE $ddl$DELETE FROM bigname_phase.project_address_controller_candidate$ddl$;
    EXECUTE $ddl$DELETE FROM bigname_phase.project_address_name_index$ddl$;
    EXECUTE $ddl$DELETE FROM bigname_phase.project_address_record_node_index$ddl$;
    EXECUTE $ddl$DELETE FROM bigname_phase.project_address_record_id_index$ddl$;
END IF;

EXECUTE $ddl$
CREATE TABLE IF NOT EXISTS bigname_phase.project_registry_owner_event (
    chain_id text NOT NULL,
    namespace text NOT NULL,
    node text NOT NULL,
    block_number bigint NOT NULL,
    transaction_index bigint,
    log_index bigint,
    event_identity text NOT NULL,
    normalized_event_id bigint,
    transaction_hash text,
    logical_name_id text,
    resource_id uuid,
    event_kind text NOT NULL,
    source_family text NOT NULL,
    authority_kind text,
    owner text,
    owner_getter text,
    owner_getter_reason text,
    PRIMARY KEY (chain_id, namespace, node, event_identity),
    CHECK ((transaction_index IS NULL) = (log_index IS NULL))
)
$ddl$;
EXECUTE $ddl$
COMMENT ON TABLE bigname_phase.project_registry_owner_event IS
    'Project-owned owner-setting registry events of family F2c: every AuthorityTransferred and SubregistryChanged an ENSv1 or Basenames registry reported for a node, keyed by position, with the name, resource, authority kind and owner facts each carried. The node row keeps only the latest owner group, which a SubregistryChanged after a zero-getter transfer replaces; the served ownerless verdict and owner history are recovered from these rows. Unpruned; a row leaves only when undo removes its block. Step 2 of TYR-36 writes it block by block beside the served tables and nothing reads it yet.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_registry_owner_event.chain_id IS
    'This value is the chain.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_registry_owner_event.namespace IS
    'This value is the namespace.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_registry_owner_event.node IS
    'This value is the lower-cased node the event addresses: child_node, else node.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_registry_owner_event.block_number IS
    'This value is the event''s block number.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_registry_owner_event.transaction_index IS
    'This value is the event''s transaction index; null with log_index for a synthesised event, which sorts before every transaction of its block.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_registry_owner_event.log_index IS
    'This value is the event''s log index; null with transaction_index for a synthesised event.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_registry_owner_event.event_identity IS
    'This value is the event identity, the final tiebreak of the canonical event order, compared as bytes.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_registry_owner_event.normalized_event_id IS
    'This value names the event in normalized_events as attribution only; it never takes part in ordering.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_registry_owner_event.transaction_hash IS
    'This value is the event''s transaction hash, null for a synthesised event.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_registry_owner_event.logical_name_id IS
    'This value is the event''s name, null when it carried none.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_registry_owner_event.resource_id IS
    'This value is the event''s resource.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_registry_owner_event.event_kind IS
    'This value is AuthorityTransferred or SubregistryChanged.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_registry_owner_event.source_family IS
    'This value is the registry source family.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_registry_owner_event.authority_kind IS
    'This value is the after-state authority_kind of the event.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_registry_owner_event.owner IS
    'This value is the lower-cased owner the event reported.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_registry_owner_event.owner_getter IS
    'This value is the lower-cased owner_getter of the event.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_registry_owner_event.owner_getter_reason IS
    'This value is the owner_getter_reason of the event.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_registry_node_state.owner_event_kind IS
    'This value is the kind of the registry event that last set the owner group: AuthorityTransferred or SubregistryChanged, both of which report the owner (name_authority/stage.rs:200-261). Either overwrites the group, so a SubregistryChanged after an AuthorityTransferred whose getter was zero replaces the owner; the served ownerless verdict, which reads AuthorityTransferred only, cannot be recovered from this row, and project_registry_owner_event keeps every owner-setting event for it.'
$ddl$;
END
$migration$;
