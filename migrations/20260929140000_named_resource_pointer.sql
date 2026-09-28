-- Add F5 named-resource pointer keys to existing schema-v2 databases. Older families
-- contain no such keys, so atomically reset every family and its publication marker;
-- the next family run rebuilds from canonical interpreted input under the new build hash.
-- Apply before enabling family serving. With serving enabled, fenced routes return
-- stale publication until replay completes. As with the name-history migration, an
-- in-flight family run fails on its missing marker and the next run rebuilds.
-- SQLx migrations precede fresh phase initialization: no phase schema is a no-op.
DO $migration$
BEGIN
IF to_regclass('bigname_phase.name_current') IS NULL THEN
    RETURN;
END IF;

IF to_regclass('bigname_phase.project_named_resource_pointer') IS NULL THEN
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
    EXECUTE $ddl$DELETE FROM bigname_phase.project_registry_owner_event$ddl$;
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
    EXECUTE $ddl$DELETE FROM bigname_phase.project_name_history$ddl$;
    -- The name-summary migration is on the following TYR-36 slice but sorts before this
    -- upgrade. It owns family state too when that slice has already been installed.
    IF to_regclass('bigname_phase.project_name_summary') IS NOT NULL THEN
        EXECUTE $ddl$DELETE FROM bigname_phase.project_name_summary$ddl$;
    END IF;
END IF;

EXECUTE $ddl$
CREATE TABLE IF NOT EXISTS bigname_phase.project_named_resource_pointer (
    chain_id text NOT NULL,
    resource_id uuid NOT NULL,
    logical_name_id text NOT NULL,
    block_number bigint NOT NULL,
    transaction_index bigint,
    log_index bigint,
    event_identity text NOT NULL,
    normalized_event_id bigint,
    resolver_address text,
    source_family text NOT NULL,
    PRIMARY KEY (chain_id, resource_id, logical_name_id),
    CHECK ((transaction_index IS NULL) = (log_index IS NULL))
)
$ddl$;
EXECUTE $ddl$
CREATE INDEX IF NOT EXISTS project_named_resource_pointer_resolver_idx
    ON bigname_phase.project_named_resource_pointer (chain_id, resolver_address, logical_name_id, resource_id)
$ddl$;
EXECUTE $ddl$
COMMENT ON TABLE bigname_phase.project_named_resource_pointer IS
    'Project-owned named resource resolver pointer of family F5: the latest named ResolverChanged per resource and logical name in canonical event order, including clears. An unnamed pointer or an event naming another name leaves this row unchanged. The composed name reader loads exact resource/name pairs; bound-name discovery walks retained pointer keys by resolver instead of normalized event history. Released names keep their pointer facts and are filtered by the composed binding admission.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_named_resource_pointer.chain_id IS
    'This value is the chain whose events wrote the row.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_named_resource_pointer.resource_id IS
    'This value is the resource named by the event.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_named_resource_pointer.logical_name_id IS
    'This value is the logical name named by the event.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_named_resource_pointer.block_number IS
    'This value is the block number of the latest named ResolverChanged for the key.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_named_resource_pointer.transaction_index IS
    'This value is the transaction index of that event; null with log_index for a synthesised event, which sorts before every transaction of its block.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_named_resource_pointer.log_index IS
    'This value is the log index of that event; null with transaction_index for a synthesised event.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_named_resource_pointer.event_identity IS
    'This value is that event identity, the final tiebreak of the canonical event order, compared as bytes.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_named_resource_pointer.normalized_event_id IS
    'This value names that event in normalized_events as attribution only; it never takes part in ordering.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_named_resource_pointer.resolver_address IS
    'This value is the lower-cased resolver from that event, including null, empty and zero-address clears.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_named_resource_pointer.source_family IS
    'This value is that event''s source family.'
$ddl$;
END
$migration$;
