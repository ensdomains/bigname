-- Existing schema-v2 databases gain project_name_summary.owner (TYR-73): the owner the
-- composed name row serves, which the registry labels' owner and exclude_owner filters read
-- inside the page statement. A database without the column has summaries written without an
-- owner, which those filters would read as ownerless, so it resets every owned key family and
-- its publication marker and the next family run rebuilds them, writing every name's owner.
-- An empty schema-migration database has no phase baseline yet, so this migration is a no-op
-- there and phase-runner init-schema installs the same column.
--
-- Apply it before starting the release that writes the column. The family writer inserts the
-- summary rows by name, so without the column that release's summaries silently lose their
-- owner and the filtered label reads fail. The release rotates the families' input content hash,
-- so its first family run rebuilds anyway; applying this migration first costs no extra
-- rebuild. With the switch on, fenced routes answer 409 stale until the rebuild finishes. It
-- takes no marker lock, so a family run in flight when it applies fails once on the missing
-- marker and the next run rebuilds.
DO $migration$
DECLARE
    family text;
BEGIN
IF to_regclass('bigname_phase.project_name_summary') IS NULL THEN
    RETURN;
END IF;

IF NOT EXISTS (
    SELECT 1 FROM pg_catalog.pg_attribute
    WHERE attrelid = to_regclass('bigname_phase.project_name_summary')
      AND attname = 'owner' AND attnum > 0 AND NOT attisdropped
) THEN
    FOREACH family IN ARRAY ARRAY[
        'project_family_marker', 'project_family_undo', 'project_repair_record',
        'project_name_state', 'project_binding_candidate', 'project_lifecycle_key_state',
        'project_lifecycle_triple_summary', 'project_lifecycle_association',
        'project_lifecycle_event', 'project_child_registration_state', 'project_wrapper_state',
        'project_registry_node_state', 'project_registry_owner_event',
        'project_registry_binding_observation', 'project_resolver_classification',
        'project_registry_pointer', 'project_resource_pointer', 'project_named_resource_pointer',
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
    EXECUTE $ddl$
    ALTER TABLE bigname_phase.project_name_summary ADD COLUMN owner text
    $ddl$;
END IF;

EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_name_summary.owner IS
    'This value is the owner the composed name row serves: declared_summary.control.owner, else control.registry_owner, lower-cased; null when the first present one is blank or the name composes no row. The registry labels'' owner and exclude_owner filters read it.'
$ddl$;
END
$migration$;
