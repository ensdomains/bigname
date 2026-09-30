-- TYR-104: install derived hydration work indexes. No chain-wide backfill.
-- Lock the family writer's first relation and atomically remove publication with all family
-- rows on first installation; the next Project run rebuilds and fills the work indexes.
-- Fresh baselines already contain both tables, so reruns preserve completed publication.
DO $migration$
DECLARE family text;
BEGIN
IF to_regclass('bigname_phase.project_family_marker') IS NULL THEN RETURN; END IF;
LOCK TABLE bigname_phase.project_family_marker IN EXCLUSIVE MODE;
IF to_regclass('bigname_phase.project_text_hydration_work') IS NULL
   OR to_regclass('bigname_phase.project_reverse_hydration_work') IS NULL THEN
    FOREACH family IN ARRAY ARRAY[
        'project_family_marker',
        'project_family_undo',
        'project_repair_record',
        'child_registration_events',
        'project_name_state',
        'project_binding_candidate',
        'project_lifecycle_key_state',
        'project_lifecycle_triple_summary',
        'project_lifecycle_association',
        'project_lifecycle_event',
        'project_child_registration_state',
        'project_wrapper_state',
        'project_registry_node_state',
        'project_registry_owner_event',
        'project_registry_binding_observation',
        'project_resolver_classification',
        'project_registry_pointer',
        'project_resource_pointer',
        'project_named_resource_pointer',
        'project_universal_resolver_proxy',
        'project_node_record_partition',
        'project_node_record_value',
        'project_record_id_value',
        'project_resolver_link',
        'project_grant',
        'project_resource_admin_aggregate',
        'project_account_approval',
        'project_name_alias',
        'project_resolver_alias',
        'project_child_edge_candidate',
        'project_parent_subregistry',
        'project_reverse_tuple',
        'project_reverse_node_claim',
        'project_claim_normalization',
        'project_address_name_fold',
        'project_address_controller_candidate',
        'project_name_history',
        'project_name_summary',
        'project_text_hydration_work',
        'project_reverse_hydration_work',
        'project_address_name_index',
        'project_address_record_node_index',
        'project_address_record_id_index'
    ] LOOP
        IF to_regclass('bigname_phase.' || family) IS NOT NULL THEN
            EXECUTE format('DELETE FROM bigname_phase.%I', family);
        END IF;
    END LOOP;
END IF;
-- Derived Project work indexes, refreshed with source-row publication and undo.
CREATE TABLE IF NOT EXISTS bigname_phase.project_text_hydration_work (
    chain_id text NOT NULL,
    resolver_address text NOT NULL,
    arm text NOT NULL,
    arm_identity text NOT NULL,
    record_key text NOT NULL,
    hydrated_at_block bigint,
    PRIMARY KEY (chain_id, resolver_address, arm, arm_identity, record_key)
);
COMMENT ON TABLE bigname_phase.project_text_hydration_work IS
    'Project-owned derived index of text selectors needing hydration or overlay clearing. Rebuilt from affected source keys after publication and undo; contains no provider responses.';
CREATE INDEX IF NOT EXISTS project_text_hydration_work_order_idx
    ON bigname_phase.project_text_hydration_work (chain_id, hydrated_at_block NULLS FIRST,
        resolver_address, arm, arm_identity, record_key);

CREATE TABLE IF NOT EXISTS bigname_phase.project_reverse_hydration_work (
    address text NOT NULL,
    coin_type text NOT NULL,
    namespace text NOT NULL,
    chain_id text NOT NULL,
    eligible boolean NOT NULL,
    attempt_ordinal bigint,
    attempt_block bigint,
    successful_at_block bigint,
    PRIMARY KEY (address, coin_type, namespace)
);
COMMENT ON TABLE bigname_phase.project_reverse_hydration_work IS
    'Project-owned derived index of continuously refreshed reverse tuples and obsolete overlays to clear. Rebuilt from affected source keys after publication and undo; contains no provider responses.';
CREATE INDEX IF NOT EXISTS project_reverse_hydration_work_active_idx
    ON bigname_phase.project_reverse_hydration_work (chain_id, attempt_ordinal NULLS FIRST,
        successful_at_block NULLS FIRST, address) WHERE eligible;
CREATE INDEX IF NOT EXISTS project_reverse_hydration_work_stale_idx
    ON bigname_phase.project_reverse_hydration_work (chain_id, attempt_block, address) WHERE NOT eligible;
CREATE INDEX IF NOT EXISTS project_reverse_tuple_node_idx
    ON bigname_phase.project_reverse_tuple (chain_id, namespace, reverse_node);
CREATE INDEX IF NOT EXISTS project_reverse_tuple_claim_idx
    ON bigname_phase.project_reverse_tuple (chain_id, claim_event_identity);
CREATE INDEX IF NOT EXISTS project_reverse_node_claim_event_idx
    ON bigname_phase.project_reverse_node_claim (chain_id, event_identity);
CREATE INDEX IF NOT EXISTS project_resource_pointer_hydration_node_idx
    ON bigname_phase.project_resource_pointer (chain_id, namespace, namehash);

END
$migration$;
