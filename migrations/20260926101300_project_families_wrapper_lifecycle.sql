-- Existing schema-v2 databases gain the wrapper lifecycle on
-- project_wrapper_state (TYR-36 step 2): the newest NameWrapped mint,
-- NameUnwrapped or holder grant or revoke of a wrapper resource, whether it
-- leaves the resource unwrapped, and the latest unwrap, which the served
-- permissions summary reads. The pinned NameWrapper emits NameWrapped only
-- from _wrap, right after minting the node's token, and NameUnwrapped when
-- _unwrap burns the token or when a mint over a still-held token burns it
-- first, so a re-wrap closes the old epoch before the new NameWrapped
-- (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L878-L903 @ ens_v1@91c966f)
-- (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L1022-L1031 @ ens_v1@91c966f).
-- The upgrade path, which no manifest admits, burns the token without
-- NameUnwrapped; its holder revoke leaves the resource unwrapped
-- (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L483-L509 @ ens_v1@91c966f).
-- Rows written before lack them, so a database
-- with the older shape resets every owned key family and the next family run
-- rebuilds them; the families are unread by every served path. An empty
-- schema-migration database has no phase baseline yet, so this migration is a
-- no-op there and phase-runner init-schema installs the same columns.
DO $migration$
BEGIN
IF to_regclass('bigname_phase.name_current') IS NULL THEN
    RETURN;
END IF;

IF NOT EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema = 'bigname_phase' AND table_name = 'project_wrapper_state'
      AND column_name = 'lifecycle_source'
) THEN
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
    EXECUTE $ddl$
    ALTER TABLE bigname_phase.project_wrapper_state
        ADD COLUMN lifecycle_source text,
        ADD COLUMN lifecycle_unwrapped boolean,
        ADD COLUMN lifecycle_position jsonb,
        ADD COLUMN unwrapped_position jsonb
    $ddl$;
END IF;

EXECUTE $ddl$
COMMENT ON TABLE bigname_phase.project_wrapper_state IS
    'Project-owned wrapper state of family F2b per wrapper resource: the latest wrapper_state and fuses, the latest wrapper expiry, and the newest wrapper lifecycle event with the latest unwrap, unmasked; masks are applied at read against the block clock. Step 2 of TYR-36 writes it block by block beside the served tables and nothing reads it yet.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_wrapper_state.lifecycle_source IS
    'This value is the source of the newest wrapper lifecycle event of the resource: NameWrapped, NameUnwrapped, holder_grant or holder_revoke (resource_summary.rs wrapper_lifecycles). NameWrapped is the mint: the pinned NameWrapper emits it only from _wrap, right after minting the token of the node (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L878-L903 @ ens_v1@91c966f).'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_wrapper_state.lifecycle_unwrapped IS
    'This value is true when the newest wrapper lifecycle event leaves the resource unwrapped: a NameUnwrapped or a holder revoke with no powers. The pinned NameWrapper emits NameUnwrapped when _unwrap burns the token (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L1022-L1031 @ ens_v1@91c966f) and when a mint burns a still-held token first (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L878-L903 @ ens_v1@91c966f); its upgrade, which no manifest admits, burns without NameUnwrapped, so there only the holder revoke leaves the resource unwrapped (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L483-L509 @ ens_v1@91c966f). The served wrapper restrictions are served only while it is false.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_wrapper_state.lifecycle_position IS
    'This value is the canonical position of the newest wrapper lifecycle event.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_wrapper_state.unwrapped_position IS
    'This value is the canonical position of the latest NameUnwrapped of the resource, kept when a later mint or holder grant becomes the newest lifecycle event; a re-wrap over a still-held token emits NameUnwrapped to the zero address before its NameWrapped (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L878-L903 @ ens_v1@91c966f).'
$ddl$;
END
$migration$;
