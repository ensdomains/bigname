-- Run with psql -X -v ON_ERROR_STOP=1, outside any transaction, before a from-zero walk
-- or a full-history Interpret redo. README.md lists what it keeps and why.
-- DROP INDEX CONCURRENTLY waits for every transaction that can use the index, such as a
-- running Interpret batch, so the wait is bounded rather than refused.
SET lock_timeout = '0';
SET statement_timeout = '1h';

-- The indexes belong to the table, which every chain on this database shares. Refuse while
-- any chain may be served: its Project has advanced and its Interpret is not redoing.
DO $$
DECLARE
    served text;
BEGIN
    SELECT string_agg(project.chain_id, ', ' ORDER BY project.chain_id)
    INTO served
    FROM bigname_phase.chain_phase_state project
    WHERE project.phase_name = 'project'
      AND project.current_block_number IS NOT NULL
      AND NOT EXISTS (
          SELECT 1
          FROM bigname_phase.chain_phase_state interpret
          WHERE interpret.chain_id = project.chain_id
            AND interpret.phase_name = 'interpret'
            AND interpret.redo_in_progress
      );
    IF served IS NOT NULL THEN
        RAISE EXCEPTION
            'chains % have projected data and no Interpret redo in progress, so they may be served; follow ops/walk-index-set/README.md before retrying',
            served;
    END IF;
END
$$;

DROP INDEX CONCURRENTLY IF EXISTS bigname_phase.normalized_events_v1_subregistry_after_node_scope_idx;
DROP INDEX CONCURRENTLY IF EXISTS bigname_phase.normalized_events_v1_subregistry_after_child_scope_idx;
DROP INDEX CONCURRENTLY IF EXISTS bigname_phase.normalized_events_v1_subregistry_before_node_scope_idx;
DROP INDEX CONCURRENTLY IF EXISTS bigname_phase.normalized_events_v2_subregistry_pointer_scope_idx;
DROP INDEX CONCURRENTLY IF EXISTS bigname_phase.normalized_events_v1_subregistry_before_child_scope_idx;
DROP INDEX CONCURRENTLY IF EXISTS bigname_phase.normalized_events_block_idx;
DROP INDEX CONCURRENTLY IF EXISTS bigname_phase.normalized_events_emitter_history_idx;
DROP INDEX CONCURRENTLY IF EXISTS bigname_phase.normalized_events_v2_expiry_scope_idx;
DROP INDEX CONCURRENTLY IF EXISTS bigname_phase.normalized_events_ens_v1_record_node_resolver_idx;
DROP INDEX CONCURRENTLY IF EXISTS bigname_phase.normalized_events_basenames_record_node_resolver_idx;
DROP INDEX CONCURRENTLY IF EXISTS bigname_phase.normalized_events_record_id_write_idx;
DROP INDEX CONCURRENTLY IF EXISTS bigname_phase.normalized_events_record_id_link_idx;
DROP INDEX CONCURRENTLY IF EXISTS bigname_phase.normalized_events_resolver_alias_history_idx;
DROP INDEX CONCURRENTLY IF EXISTS bigname_phase.normalized_events_resolver_upgrade_history_idx;
DROP INDEX CONCURRENTLY IF EXISTS bigname_phase.normalized_events_pointer_after_resolver_history_idx;
DROP INDEX CONCURRENTLY IF EXISTS bigname_phase.normalized_events_pointer_before_resolver_history_idx;
DROP INDEX CONCURRENTLY IF EXISTS bigname_phase.normalized_events_permission_after_resolver_history_idx;
DROP INDEX CONCURRENTLY IF EXISTS bigname_phase.normalized_events_permission_before_resolver_history_idx;
DROP INDEX CONCURRENTLY IF EXISTS bigname_phase.normalized_events_subregistry_registration_history_idx;
DROP INDEX CONCURRENTLY IF EXISTS bigname_phase.normalized_events_project_name_node_idx;
DROP INDEX CONCURRENTLY IF EXISTS bigname_phase.normalized_events_project_name_child_idx;
DROP INDEX CONCURRENTLY IF EXISTS bigname_phase.normalized_events_project_name_after_target_idx;
DROP INDEX CONCURRENTLY IF EXISTS bigname_phase.normalized_events_project_name_before_target_idx;
DROP INDEX CONCURRENTLY IF EXISTS bigname_phase.normalized_events_project_primary_after_idx;
DROP INDEX CONCURRENTLY IF EXISTS bigname_phase.normalized_events_project_primary_before_idx;
DROP INDEX CONCURRENTLY IF EXISTS bigname_phase.normalized_events_project_primary_after_source_idx;
DROP INDEX CONCURRENTLY IF EXISTS bigname_phase.normalized_events_project_primary_before_source_idx;
DROP INDEX CONCURRENTLY IF EXISTS bigname_phase.normalized_events_address_registrant_match_idx;
DROP INDEX CONCURRENTLY IF EXISTS bigname_phase.normalized_events_address_token_holder_match_idx;
DROP INDEX CONCURRENTLY IF EXISTS bigname_phase.normalized_events_address_registry_owner_match_idx;
DROP INDEX CONCURRENTLY IF EXISTS bigname_phase.normalized_events_project_node_history_idx;
DROP INDEX CONCURRENTLY IF EXISTS bigname_phase.normalized_events_project_v1_pointer_node_idx;
DROP INDEX CONCURRENTLY IF EXISTS bigname_phase.normalized_events_project_v1_pointer_addressed_node_idx;

-- The indexes left on the table, for the receipt.
SELECT indexrelid::regclass AS index_name, indisvalid, indisready,
       pg_size_pretty(pg_relation_size(indexrelid)) AS index_size
FROM pg_index
WHERE indrelid = 'bigname_phase.normalized_events'::regclass
ORDER BY index_name;
