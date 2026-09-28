-- Existing schema-v2 databases gain project_name_history (TYR-36 step 7b): the
-- whole-history facts of a name no other owned key family keeps, which the
-- composed name reader serves as registration.created_at and reads for the
-- coverage and authority arm of a name with no open binding. A database without
-- the table has families built without that history, so it resets every owned
-- key family and the next family run rebuilds them. An empty schema-migration
-- database has no phase baseline yet, so this migration is a no-op there and
-- phase-runner init-schema installs the same table.
--
-- Apply it before BIGNAME_SERVE_FROM_FAMILIES is ever turned on: the reset
-- deletes the family marker, so with the switch on every fenced route answers
-- 409 stale until the family rebuild finishes. It takes no marker lock, so a
-- family run in flight when it applies fails once on the missing marker and
-- the next run rebuilds the families.
DO $migration$
BEGIN
IF to_regclass('bigname_phase.name_current') IS NULL THEN
    RETURN;
END IF;

IF to_regclass('bigname_phase.project_name_history') IS NULL THEN
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
END IF;

EXECUTE $ddl$
CREATE TABLE IF NOT EXISTS bigname_phase.project_name_history (
    chain_id text NOT NULL,
    logical_name_id text NOT NULL,
    namespace text NOT NULL,
    block_number bigint NOT NULL,
    transaction_index bigint,
    log_index bigint,
    event_identity text NOT NULL,
    normalized_event_id bigint,
    first_block_number bigint NOT NULL,
    created_at timestamptz NOT NULL,
    has_ens_v2_events boolean NOT NULL,
    event_arms jsonb NOT NULL,
    PRIMARY KEY (chain_id, logical_name_id),
    CHECK ((transaction_index IS NULL) = (log_index IS NULL))
)
$ddl$;
EXECUTE $ddl$
COMMENT ON TABLE bigname_phase.project_name_history IS
    'Project-owned whole-history facts of family F1: per name, the block and time of the first readable event naming it, whether ENSv2 events name it, and the authority arms its authority events vote. Written once when the first event naming the name is applied and changed only when a later event adds a fact; a row leaves only when undo removes the block that created it. The composed name reader reads it for created_at, the coverage of a name with no selected arm, and the arm of a name with no open binding.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_name_history.chain_id IS
    'This value is the chain whose events wrote the row; each chain keeps its own row for a name.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_name_history.logical_name_id IS
    'This value is the name.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_name_history.namespace IS
    'This value is the namespace of the first event naming the name.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_name_history.block_number IS
    'This value is the block number of the event that last changed the row.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_name_history.transaction_index IS
    'This value is the transaction index of the event that last changed the row; null with log_index for a synthesised event, which sorts before every transaction of its block.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_name_history.log_index IS
    'This value is the log index of the event that last changed the row; null with transaction_index for a synthesised event.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_name_history.event_identity IS
    'This value is the event identity of the event that last changed the row, the final tiebreak of the canonical event order, compared as bytes.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_name_history.normalized_event_id IS
    'This value names the event that last changed the row in normalized_events as attribution only; it never takes part in ordering.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_name_history.first_block_number IS
    'This value is the block of the first readable event naming the name.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_name_history.created_at IS
    'This value is the block time of the first readable event naming the name, which the name row reports as registration.created_at (name_current/build.sql, the created lateral).'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_name_history.has_ens_v2_events IS
    'This value is whether any event naming the name came from the ENSv2 root, registry or registrar families (name_current/build.sql, the corpus lateral).'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_name_history.event_arms IS
    'This value is the sorted array of authority arms (ens_v1, ens_v2, basenames) voted by the name''s registration, renewal, release, expiry change, authority transfer, token transfer and authority epoch events (name_authority/build.sql, event_arms), without the ENSv2 root and registry expiry changes, which never vote, and releases, which the reader decides against the binding candidates.'
$ddl$;
END
$migration$;
