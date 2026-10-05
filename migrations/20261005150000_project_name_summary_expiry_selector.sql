-- Existing schema-v2 databases gain the expiry listing's selector on project_name_summary:
-- expiry_listable (whether GET /v1/names lists the name by expiry) and public_authority (the
-- public authority its row serves), with the two partial indexes that read a namespace's
-- listable names in expiry order, alone and within one authority. The family writer derives
-- both columns from the same composition as the rest of the summary. An empty
-- schema-migration database has no phase baseline yet, so this schema-migration is a no-op
-- there and phase-runner init-schema installs the same columns and indexes.
--
-- Stop the family writer and apply this before starting the release that writes the columns.
-- The writer inserts summary rows by column name, so an older writer fails on the NOT NULL
-- column, and summaries written before the columns existed carry no selector. The reverse
-- mistake is silent: the new writer run against a schema without the columns drops the two
-- values it has no column for and publishes summaries with no selector. A database
-- without the columns therefore resets every owned key family and its publication marker,
-- and the next family run rebuilds them. The release rotates the families' input content
-- hash, so its first family run rebuilds anyway; applying this first costs no extra rebuild.
-- Fenced routes answer 409 stale until the rebuild finishes.
--
-- It first takes the family marker table in EXCLUSIVE mode, the lock a family writer takes
-- first, and holds it to commit, as 20260929170000_project_name_summary_owner.sql does: no
-- family run can create a marker or write a family row between the reset and the new
-- columns. The reset empties project_name_summary, so the indexes are built on an empty
-- table. The reset list is every journalled and derived family table of
-- crates/project/src/families/tables.rs plus the marker, undo and repair tables; the test
-- crates/project/tests/families_summary_owner_migration.rs checks that it stays so.
DO $migration$
DECLARE
    family text;
BEGIN
IF to_regclass('bigname_phase.project_name_summary') IS NULL THEN
    RETURN;
END IF;
LOCK TABLE bigname_phase.project_family_marker IN EXCLUSIVE MODE;

IF (
    SELECT count(*) FROM pg_catalog.pg_attribute
    WHERE attrelid = to_regclass('bigname_phase.project_name_summary')
      AND attname IN ('expiry_listable', 'public_authority')
      AND attnum > 0 AND NOT attisdropped
) < 2 THEN
    FOREACH family IN ARRAY ARRAY[
        'project_family_marker', 'project_family_undo', 'project_repair_record',
        'child_registration_events', 'project_name_state', 'project_binding_candidate',
        'project_lifecycle_key_state',
        'project_lifecycle_triple_summary', 'project_lifecycle_association',
        'project_lifecycle_event', 'project_child_registration_state', 'project_wrapper_state',
        'project_registry_node_state', 'project_registry_owner_event',
        'project_registry_binding_observation', 'project_resolver_classification',
        'project_universal_resolver_proxy', 'project_registry_pointer',
        'project_resource_pointer', 'project_named_resource_pointer',
        'project_node_record_partition', 'project_node_record_value', 'project_record_id_value',
        'project_resolver_link', 'project_grant', 'project_resource_admin_aggregate',
        'project_account_approval',
        'project_child_edge_candidate', 'project_parent_subregistry', 'project_reverse_tuple',
        'project_reverse_node_claim', 'project_claim_normalization', 'project_address_name_fold',
        'project_address_controller_candidate', 'project_text_hydration_work',
        'project_reverse_hydration_work', 'project_address_name_index',
        'project_address_record_node_index', 'project_address_record_id_index',
        'project_name_history', 'project_name_summary'
    ] LOOP
        IF to_regclass('bigname_phase.' || family) IS NOT NULL THEN
            EXECUTE format('DELETE FROM bigname_phase.%I', family);
        END IF;
    END LOOP;
    ALTER TABLE bigname_phase.project_name_summary
        ADD COLUMN IF NOT EXISTS expiry_listable boolean NOT NULL,
        ADD COLUMN IF NOT EXISTS public_authority text;
END IF;

CREATE INDEX IF NOT EXISTS project_name_summary_expiry_idx
    ON bigname_phase.project_name_summary (namespace, expires_at, logical_name_id, chain_id)
    WHERE expiry_listable AND expires_at IS NOT NULL;
CREATE INDEX IF NOT EXISTS project_name_summary_authority_expiry_idx
    ON bigname_phase.project_name_summary
        (namespace, public_authority, expires_at, logical_name_id, chain_id)
    WHERE expiry_listable AND expires_at IS NOT NULL;

COMMENT ON COLUMN bigname_phase.project_name_summary.expiry_listable IS
    'This value is whether the expiry listing of GET /v1/names lists the name: it composes a row whose coverage is not unsupported and whose registration carries a finite expiry. For such a row expires_at is the expiry the listing serves and orders by.';
COMMENT ON COLUMN bigname_phase.project_name_summary.public_authority IS
    'This value is the public authority the composed name row serves (ens_v0, ens_v1 or ens_v2); null when the row serves none (Basenames, an unresolved selection, an ownerless registry row) or the name composes no row. The stored selector the expiry listing''s authority filter will read; no reader uses it yet.';
END
$migration$;
