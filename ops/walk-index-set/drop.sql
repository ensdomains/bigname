-- Run with psql -X -v ON_ERROR_STOP=1, outside any transaction, before a from-zero walk
-- or a full-history Interpret redo. README.md lists what it keeps and why.
-- DROP INDEX CONCURRENTLY waits for every transaction that can use the index, such as a
-- running Interpret batch, so the wait is bounded rather than refused. A drop that fails
-- with an SQL error leaves its index in place and the script continues to the checks after
-- the drops: the served-chain check fails first if a chain may now be served, otherwise the
-- last check fails naming the index. A lost connection or an interrupt ends the script
-- before them; README.md describes the recovery.
SET lock_timeout = '0';
SET statement_timeout = '1h';

-- The indexes belong to the table, which every chain on this database shares. A chain may be
-- served, or Project may be reading these indexes, when its Project is running, has recorded
-- progress or holds a live publication, which it commits before it records progress, and
-- its Interpret is not redoing. The check runs before and after the drops and takes no phase
-- lock, so run the script as the walk or redo starts. The one-hour timeout bounds each drop,
-- not the script, so allow for all 40 before Interpret can complete.
CREATE OR REPLACE FUNCTION pg_temp.walk_index_served_chains() RETURNS text
LANGUAGE sql STABLE AS $$
    SELECT string_agg(projected.chain_id, ', ' ORDER BY projected.chain_id)
    FROM (
        SELECT chain_id
        FROM bigname_phase.chain_phase_state
        WHERE phase_name = 'project'
          AND (current_block_number IS NOT NULL OR phase_status = 'running')
        UNION
        SELECT chain_id
        FROM bigname_phase.project_family_marker
        WHERE state = 'live'
          AND current_block_number IS NOT NULL
    ) projected
    WHERE NOT EXISTS (
        SELECT 1
        FROM bigname_phase.chain_phase_state interpret
        WHERE interpret.chain_id = projected.chain_id
          AND interpret.phase_name = 'interpret'
          AND interpret.redo_in_progress
    )
$$;

CREATE OR REPLACE FUNCTION pg_temp.walk_index_dropped() RETURNS text[]
LANGUAGE sql IMMUTABLE AS $$
    SELECT ARRAY[
        'normalized_events_registry_token_idx',
        'normalized_events_v1_subregistry_after_node_scope_idx',
        'normalized_events_v1_subregistry_after_child_scope_idx',
        'normalized_events_v1_subregistry_before_node_scope_idx',
        'normalized_events_v2_subregistry_pointer_scope_idx',
        'normalized_events_v1_subregistry_before_child_scope_idx',
        'normalized_events_block_idx',
        'normalized_events_emitter_history_idx',
        'normalized_events_v2_expiry_scope_idx',
        'normalized_events_ens_v1_record_node_resolver_idx',
        'normalized_events_basenames_record_node_resolver_idx',
        'normalized_events_history_discovery_name_idx',
        'normalized_events_history_discovery_resource_idx',
        'normalized_events_record_id_write_idx',
        'normalized_events_record_id_link_idx',
        'normalized_events_resolver_alias_history_idx',
        'normalized_events_resolver_upgrade_history_idx',
        'normalized_events_registry_origin_idx',
        'normalized_events_wrapper_departure_idx',
        'normalized_events_user_registry_departure_idx',
        'normalized_events_pointer_after_resolver_history_idx',
        'normalized_events_pointer_before_resolver_history_idx',
        'normalized_events_permission_after_resolver_history_idx',
        'normalized_events_permission_before_resolver_history_idx',
        'normalized_events_subregistry_registration_history_idx',
        'normalized_events_project_name_node_idx',
        'normalized_events_project_name_child_idx',
        'normalized_events_project_name_after_target_idx',
        'normalized_events_project_name_before_target_idx',
        'normalized_events_project_primary_after_idx',
        'normalized_events_project_primary_before_idx',
        'normalized_events_project_primary_after_source_idx',
        'normalized_events_project_primary_before_source_idx',
        'normalized_events_address_registrant_match_idx',
        'normalized_events_address_token_holder_match_idx',
        'normalized_events_address_registry_owner_match_idx',
        'normalized_events_address_root_permission_idx',
        'normalized_events_project_node_history_idx',
        'normalized_events_project_v1_pointer_node_idx',
        'normalized_events_project_v1_pointer_addressed_node_idx'
    ]
$$;

DO $$
DECLARE
    served text := pg_temp.walk_index_served_chains();
BEGIN
    IF served IS NOT NULL THEN
        RAISE EXCEPTION
            'chains % have projected data and no Interpret redo in progress, so they may be served; follow ops/walk-index-set/README.md before retrying',
            served;
    END IF;
END
$$;

-- DROP INDEX matches the name alone, so refuse a name another relation holds.
DO $$
DECLARE
    misplaced text;
BEGIN
    SELECT string_agg(name, ', ' ORDER BY name)
    INTO misplaced
    FROM unnest(pg_temp.walk_index_dropped()) AS name
    WHERE to_regclass('bigname_phase.' || name) IS NOT NULL
      AND NOT EXISTS (
          SELECT 1
          FROM pg_index
          WHERE indexrelid = to_regclass('bigname_phase.' || name)
            AND indrelid = 'bigname_phase.normalized_events'::regclass
      );
    IF misplaced IS NOT NULL THEN
        RAISE EXCEPTION
            'bigname_phase.% is not an index on normalized_events; follow ops/walk-index-set/README.md before retrying',
            misplaced;
    END IF;
END
$$;

\set ON_ERROR_STOP off
DROP INDEX CONCURRENTLY IF EXISTS bigname_phase.normalized_events_registry_token_idx;
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
DROP INDEX CONCURRENTLY IF EXISTS bigname_phase.normalized_events_history_discovery_name_idx;
DROP INDEX CONCURRENTLY IF EXISTS bigname_phase.normalized_events_history_discovery_resource_idx;
DROP INDEX CONCURRENTLY IF EXISTS bigname_phase.normalized_events_record_id_write_idx;
DROP INDEX CONCURRENTLY IF EXISTS bigname_phase.normalized_events_record_id_link_idx;
DROP INDEX CONCURRENTLY IF EXISTS bigname_phase.normalized_events_resolver_alias_history_idx;
DROP INDEX CONCURRENTLY IF EXISTS bigname_phase.normalized_events_resolver_upgrade_history_idx;
DROP INDEX CONCURRENTLY IF EXISTS bigname_phase.normalized_events_registry_origin_idx;
DROP INDEX CONCURRENTLY IF EXISTS bigname_phase.normalized_events_wrapper_departure_idx;
DROP INDEX CONCURRENTLY IF EXISTS bigname_phase.normalized_events_user_registry_departure_idx;
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
DROP INDEX CONCURRENTLY IF EXISTS bigname_phase.normalized_events_address_root_permission_idx;
DROP INDEX CONCURRENTLY IF EXISTS bigname_phase.normalized_events_project_node_history_idx;
DROP INDEX CONCURRENTLY IF EXISTS bigname_phase.normalized_events_project_v1_pointer_node_idx;
DROP INDEX CONCURRENTLY IF EXISTS bigname_phase.normalized_events_project_v1_pointer_addressed_node_idx;
\set ON_ERROR_STOP on

-- The indexes left on the table, for the receipt.
SELECT indexrelid::regclass AS index_name, indisvalid, indisready,
       pg_size_pretty(pg_relation_size(indexrelid)) AS index_size
FROM pg_index
WHERE indrelid = 'bigname_phase.normalized_events'::regclass
ORDER BY index_name;

DO $$
DECLARE
    served text := pg_temp.walk_index_served_chains();
BEGIN
    IF served IS NOT NULL THEN
        RAISE EXCEPTION
            'chains % may now be served while the walk index set is dropped; run ops/walk-index-set/install.sql now',
            served;
    END IF;
END
$$;

DO $$
DECLARE
    remaining text;
BEGIN
    SELECT string_agg(name, ', ' ORDER BY name)
    INTO remaining
    FROM unnest(pg_temp.walk_index_dropped()) AS name
    WHERE to_regclass('bigname_phase.' || name) IS NOT NULL;
    IF remaining IS NOT NULL THEN
        RAISE EXCEPTION
            'drop.sql left % in place; see the errors above and rerun it',
            remaining;
    END IF;
END
$$;
