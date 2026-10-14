-- Existing schema-v2 databases gain the resolver overview's bound-name selector on
-- project_name_summary: the served resolver (resolver_chain_id, resolver_address), whether the
-- name passes every bound-name rule except the resolver match and the capability gate
-- (resolver_listing_eligible), whether it is exempt from that gate (resolver_support_exempt),
-- the surface's raw name (raw_name), and the page order key (order_name, namespace, namehash).
-- The family writer derives them from the same composition as the rest of the summary. Two
-- partial indexes hold the page order: every listed name, for a resolver whose binding
-- enumeration is supported, and only the names exempt from the capability gate, for one whose
-- enumeration is not.
-- An empty schema-migration database has no phase baseline yet, so this schema-migration is a
-- no-op there and phase-runner init-schema installs the same columns and indexes.
--
-- Summaries written before the columns existed carry no selector, so a database without them
-- resets every owned key family and its publication marker, and the next family run rebuilds
-- them. Until it completes, the bound-name page and every other fenced route answer 409 stale
-- rather than an undercount. The release rotates the projection content hash, so its first
-- family run rebuilds anyway. Applying this first costs no extra rebuild.
--
-- Both indexes are built in this transaction rather than concurrently. Their columns are new
-- in this schema-migration, so no index could be built on them before it, and on a database
-- without the columns they are built right after the reset, over an empty summary table. The
-- reset deletes rows rather than truncating, so each build still scans the dead rows the delete
-- left. At mainnet's size that is an estimated few seconds, not measured, inside the
-- maintenance window.
--
-- It takes the family marker table in EXCLUSIVE mode first and holds it to commit, as
-- 20261005150000_project_name_summary_expiry_selector.sql does: no family run can create a
-- marker or write a family row between the reset and the new columns. The reset list is every
-- journalled and derived family table of crates/project/src/families/tables.rs plus the marker,
-- undo and repair tables. crates/project/tests/families_summary_owner_migration.rs checks that
-- it stays so.
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
      AND attname IN ('resolver_chain_id', 'resolver_address', 'resolver_listing_eligible',
                      'resolver_support_exempt', 'raw_name', 'order_name', 'namehash')
      AND attnum > 0 AND NOT attisdropped
) < 7 THEN
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
    ALTER TABLE bigname_phase.project_name_summary
        ADD COLUMN IF NOT EXISTS resolver_chain_id text,
        ADD COLUMN IF NOT EXISTS resolver_address text,
        ADD COLUMN IF NOT EXISTS resolver_listing_eligible boolean NOT NULL,
        ADD COLUMN IF NOT EXISTS resolver_support_exempt boolean NOT NULL,
        ADD COLUMN IF NOT EXISTS raw_name text,
        ADD COLUMN IF NOT EXISTS order_name text,
        ADD COLUMN IF NOT EXISTS namehash text NOT NULL;
END IF;

CREATE INDEX IF NOT EXISTS project_name_summary_bound_order_idx
    ON bigname_phase.project_name_summary
        (resolver_chain_id, resolver_address, order_name, namespace, namehash)
    WHERE resolver_listing_eligible AND octet_length(order_name) <= 2000;
CREATE INDEX IF NOT EXISTS project_name_summary_bound_exempt_order_idx
    ON bigname_phase.project_name_summary
        (resolver_chain_id, resolver_address, order_name, namespace, namehash)
    WHERE resolver_listing_eligible AND resolver_support_exempt
      AND octet_length(order_name) <= 2000;

COMMENT ON COLUMN bigname_phase.project_name_summary.resolver_chain_id IS
    'This value is the chain of the resolver the composed name row serves (declared_summary.resolver.chain_id). It is null when it serves none or the name composes no row.';
COMMENT ON COLUMN bigname_phase.project_name_summary.resolver_address IS
    'This value is the lower-cased address of the resolver the composed name row serves (declared_summary.resolver.address). It is null when it serves none or the name composes no row. It is not the ENSv1 registry pointer.';
COMMENT ON COLUMN bigname_phase.project_name_summary.resolver_listing_eligible IS
    'This value is whether the composed name row passes every rule of the resolver overview''s bound-name list except the resolver match and the capability gate (families/name/bound_rules.rs). It is false when the name composes no row.';
COMMENT ON COLUMN bigname_phase.project_name_summary.resolver_support_exempt IS
    'This value is whether the composed name row is listed whatever its resolver family''s binding-enumeration support: it has a selected resource, or no serving resource. The support itself is read from the resolver''s classification at request time.';
COMMENT ON COLUMN bigname_phase.project_name_summary.raw_name IS
    'This value is the raw name of the name''s surface when the summary was composed, null when the surface stores no raw bytes. Project''s raw-name sweep checks it against the surface.';
COMMENT ON COLUMN bigname_phase.project_name_summary.order_name IS
    'This value is the name as the composed name row renders it from chain data alone: its raw name, else each label as [<labelhash>], with no imported preimage, cut to its first 2000 bytes. The bound-name page orders and continues by it, then namespace and namehash. It is null when the name composes no row.';
COMMENT ON COLUMN bigname_phase.project_name_summary.namehash IS
    'This value is the namehash of the name''s surface, the last key of the bound-name page order.';
END
$migration$;
