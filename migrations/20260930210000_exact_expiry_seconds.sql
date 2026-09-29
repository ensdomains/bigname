-- TYR66/67: finite expiry keys keep exact seconds beyond the calendar and bigint ranges.
-- Lock in the writer's order, clear every family and its publication, then change both indexed
-- expiry keys together. Project rebuilds from retained canonical input; routes remain stale
-- until the normal family publication completes. Fresh baselines already have these types.
DO $migration$
DECLARE family text;
BEGIN
IF to_regclass('bigname_phase.project_lifecycle_event') IS NULL THEN RETURN; END IF;
LOCK TABLE bigname_phase.project_family_marker IN EXCLUSIVE MODE;
IF EXISTS (
    SELECT 1 FROM pg_catalog.pg_attribute
    WHERE attrelid = 'bigname_phase.project_lifecycle_event'::regclass
      AND attname = 'expiry_seconds' AND atttypid <> 'numeric'::regtype
) OR EXISTS (
    SELECT 1 FROM pg_catalog.pg_attribute
    WHERE attrelid = 'bigname_phase.project_name_summary'::regclass
      AND attname = 'expires_at' AND atttypid <> 'numeric'::regtype
) THEN
    FOREACH family IN ARRAY ARRAY[
        'project_family_marker', 'project_family_undo', 'project_repair_record',
        'child_registration_events', 'project_name_state', 'project_binding_candidate',
        'project_lifecycle_key_state',
        'project_lifecycle_triple_summary', 'project_lifecycle_association',
        'project_lifecycle_event', 'project_child_registration_state', 'project_wrapper_state',
        'project_registry_node_state', 'project_registry_owner_event',
        'project_registry_binding_observation', 'project_resolver_classification',
        'project_universal_resolver_proxy', 'project_registry_pointer', 'project_resource_pointer', 'project_named_resource_pointer',
        'project_node_record_partition', 'project_node_record_value', 'project_record_id_value',
        'project_resolver_link', 'project_grant', 'project_resource_admin_aggregate',
        'project_account_approval', 'project_name_alias', 'project_resolver_alias',
        'project_child_edge_candidate', 'project_parent_subregistry', 'project_reverse_tuple',
        'project_reverse_node_claim', 'project_claim_normalization', 'project_address_name_fold',
        'project_address_controller_candidate', 'project_address_name_index',
        'project_address_record_node_index', 'project_address_record_id_index',
        'project_name_history', 'project_name_summary'
    ] LOOP
        IF to_regclass('bigname_phase.' || family) IS NOT NULL THEN
            EXECUTE format('DELETE FROM bigname_phase.%I', family);
        END IF;
    END LOOP;
    ALTER TABLE bigname_phase.project_lifecycle_event
        ALTER COLUMN expiry_seconds TYPE numeric USING expiry_seconds::numeric;
    IF EXISTS (SELECT 1 FROM pg_catalog.pg_attribute
        WHERE attrelid = 'bigname_phase.project_name_summary'::regclass
          AND attname = 'expires_at' AND atttypid <> 'numeric'::regtype) THEN
        ALTER TABLE bigname_phase.project_name_summary
            ALTER COLUMN expires_at TYPE numeric USING EXTRACT(EPOCH FROM expires_at);
    END IF;
END IF;
COMMENT ON COLUMN bigname_phase.project_lifecycle_event.expiry_seconds IS
    'This value is the exact integral expiry in Unix seconds, including the full uint64 range; null when no integral expiry is present.';
COMMENT ON COLUMN bigname_phase.project_name_summary.expires_at IS
    'This value is the exact finite expiry in Unix seconds the subnames expiry sort and fence read; contextual no-expiry values are null.';
END
$migration$;
