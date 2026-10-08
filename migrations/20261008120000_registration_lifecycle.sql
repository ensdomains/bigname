-- Install the canonical lifecycle payload and exact grace selector together.
-- Lock publication before clearing rebuildable Project state. Existing interpreted/raw facts
-- are untouched. There is no SQL conversion of the old lifecycle semantics: ordinary replay
-- rebuilds all affected families, and missing markers refuse reads until publication completes.
-- A fresh database receives the same shape from the phase baseline.
DO $migration$
DECLARE
    family text;
BEGIN
IF to_regclass('bigname_phase.project_name_summary') IS NULL THEN
    RETURN;
END IF;
LOCK TABLE bigname_phase.project_family_marker IN EXCLUSIVE MODE;
IF NOT EXISTS (
    SELECT 1 FROM pg_attribute
    WHERE attrelid = 'bigname_phase.project_name_summary'::regclass
      AND attname = 'grace_ends_at' AND attnum > 0 AND NOT attisdropped
) OR EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conrelid = 'bigname_phase.project_name_summary'::regclass
      AND conname = 'project_name_summary_search_check'
      AND pg_get_constraintdef(oid) LIKE '%registration_status%'
) THEN
    FOREACH family IN ARRAY ARRAY[
        'project_family_marker', 'project_family_undo', 'project_repair_record',
        'child_registration_events', 'project_name_state', 'project_binding_candidate',
        'project_lifecycle_key_state', 'project_lifecycle_triple_summary', 'project_lifecycle_association',
        'project_lifecycle_event', 'project_child_registration_state', 'project_wrapper_state',
        'project_registry_node_state', 'project_registry_owner_event', 'project_registry_binding_observation',
        'project_resolver_classification', 'project_registry_pointer', 'project_resource_pointer',
        'project_named_resource_pointer', 'project_universal_resolver_proxy', 'project_node_record_partition',
        'project_node_record_value', 'project_record_id_value', 'project_resolver_link',
        'project_grant', 'project_resource_admin_aggregate', 'project_account_approval',
        'project_ens_v2_entry_owner', 'project_ens_v2_registry_parent',
        'project_child_edge_candidate', 'project_parent_subregistry', 'project_reverse_tuple',
        'project_reverse_node_claim', 'project_claim_normalization', 'project_address_name_fold',
        'project_address_controller_candidate', 'project_name_history', 'project_lookup_name',
        'project_lookup_relation', 'project_lookup_inventory', 'project_lookup_record',
        'project_lookup_dependency', 'project_name_summary', 'project_address_history_anchor',
        'project_history_source', 'project_history_source_edge', 'project_history_catalogue_marker',
        'project_text_hydration_work', 'project_reverse_hydration_work', 'project_address_name_index',
        'project_address_record_node_index', 'project_address_record_id_index'
    ] LOOP
        IF to_regclass('bigname_phase.' || family) IS NOT NULL THEN
            EXECUTE format('DELETE FROM bigname_phase.%I', family);
        END IF;
    END LOOP;
END IF;
ALTER TABLE bigname_phase.project_name_summary
    ADD COLUMN IF NOT EXISTS grace_ends_at numeric;
ALTER TABLE bigname_phase.project_name_summary
    DROP CONSTRAINT IF EXISTS project_name_summary_search_check;
ALTER TABLE bigname_phase.project_name_summary
    ADD CONSTRAINT project_name_summary_search_check CHECK (
        (search_supported AND search_fields IS NOT NULL AND jsonb_typeof(search_fields) = 'object'
         AND search_fields ? 'status')
        OR (NOT search_supported AND search_fields IS NULL AND search_creation_transport_resource_id IS NULL));
CREATE INDEX IF NOT EXISTS project_name_summary_grace_idx
    ON bigname_phase.project_name_summary (namespace, grace_ends_at, logical_name_id, chain_id)
    WHERE expiry_listable AND grace_ends_at IS NOT NULL;
CREATE INDEX IF NOT EXISTS project_name_summary_authority_grace_idx
    ON bigname_phase.project_name_summary (namespace, public_authority, grace_ends_at, logical_name_id, chain_id)
    WHERE expiry_listable AND grace_ends_at IS NOT NULL;
COMMENT ON COLUMN bigname_phase.project_name_summary.grace_ends_at IS
    'The exact finite canonical grace deadline in Unix seconds, retained for ended registrations; null for no-expiry or absent registrations.';
COMMENT ON COLUMN bigname_phase.project_name_summary.expiry_listable IS
    'Whether GET /v1/names lists the finite canonical registration by expiry or grace deadline. The supported composed row and stored expires_at/grace_ends_at agree, including ended registrations.';
END
$migration$;
