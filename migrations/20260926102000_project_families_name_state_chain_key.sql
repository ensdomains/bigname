-- Existing schema-v2 databases key project_name_state by chain (TYR-36 step 2),
-- as every other chain-scoped family table is keyed, so one chain's family run
-- never loads or overwrites another chain's row for the same logical name. The
-- undo journal records each row under its table key, so a database with the
-- older key resets every owned key family and the next family run rebuilds
-- them; the families are unread by every served path. An empty
-- schema-migration database has no phase baseline yet, so this migration is a
-- no-op there and phase-runner init-schema installs the same key.
DO $migration$
BEGIN
IF to_regclass('bigname_phase.name_current') IS NULL THEN
    RETURN;
END IF;

IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint constraint_row
    JOIN pg_attribute attribute
      ON attribute.attrelid = constraint_row.conrelid
     AND attribute.attnum = ANY (constraint_row.conkey)
    WHERE constraint_row.conrelid = 'bigname_phase.project_name_state'::regclass
      AND constraint_row.contype = 'p'
      AND attribute.attname = 'chain_id'
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
    EXECUTE $ddl$
    ALTER TABLE bigname_phase.project_name_state
        DROP CONSTRAINT project_name_state_pkey,
        ADD PRIMARY KEY (chain_id, namespace, logical_name_id)
    $ddl$;
END IF;

EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_name_state.chain_id IS
    'This value is the chain whose events wrote the row; each chain keeps its own row for a name.'
$ddl$;
END
$migration$;
