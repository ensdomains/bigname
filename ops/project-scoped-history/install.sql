-- Run with psql -X -v ON_ERROR_STOP=1 outside any transaction.
-- Concurrent builds can wait for existing writer transactions; inspect progress.
SET lock_timeout = '0';
SET statement_timeout = '30min';

CREATE INDEX CONCURRENTLY IF NOT EXISTS normalized_events_project_name_node_idx
    ON bigname_phase.normalized_events (chain_id, (namespace || ':' || lower(after_state ->> 'node')), block_number)
    INCLUDE (normalized_event_id)
    WHERE (event_kind IN ('SubregistryChanged', 'AliasChanged')
           OR (event_kind = 'AuthorityTransferred'
               AND source_family IN ('ens_v1_registry_l1', 'basenames_base_registry')))
      AND canonicality_state IN ('canonical', 'safe', 'finalized')
      AND (namespace || ':' || lower(after_state ->> 'node')) IS NOT NULL;

CREATE INDEX CONCURRENTLY IF NOT EXISTS normalized_events_project_name_child_idx
    ON bigname_phase.normalized_events (chain_id, (namespace || ':' || lower(after_state ->> 'child_node')), block_number)
    INCLUDE (normalized_event_id)
    WHERE (event_kind IN ('SubregistryChanged', 'AliasChanged')
           OR (event_kind = 'AuthorityTransferred'
               AND source_family IN ('ens_v1_registry_l1', 'basenames_base_registry')))
      AND canonicality_state IN ('canonical', 'safe', 'finalized')
      AND (namespace || ':' || lower(after_state ->> 'child_node')) IS NOT NULL;

CREATE INDEX CONCURRENTLY IF NOT EXISTS normalized_events_project_name_after_target_idx
    ON bigname_phase.normalized_events (chain_id, (after_state ->> 'to_logical_name_id'), block_number)
    INCLUDE (normalized_event_id)
    WHERE (event_kind IN ('SubregistryChanged', 'AliasChanged')
           OR (event_kind = 'AuthorityTransferred'
               AND source_family IN ('ens_v1_registry_l1', 'basenames_base_registry')))
      AND canonicality_state IN ('canonical', 'safe', 'finalized')
      AND (after_state ->> 'to_logical_name_id') IS NOT NULL;

CREATE INDEX CONCURRENTLY IF NOT EXISTS normalized_events_project_name_before_target_idx
    ON bigname_phase.normalized_events (chain_id, (before_state ->> 'to_logical_name_id'), block_number)
    INCLUDE (normalized_event_id)
    WHERE (event_kind IN ('SubregistryChanged', 'AliasChanged')
           OR (event_kind = 'AuthorityTransferred'
               AND source_family IN ('ens_v1_registry_l1', 'basenames_base_registry')))
      AND canonicality_state IN ('canonical', 'safe', 'finalized')
      AND (before_state ->> 'to_logical_name_id') IS NOT NULL;

CREATE INDEX CONCURRENTLY IF NOT EXISTS normalized_events_project_primary_after_idx
    ON bigname_phase.normalized_events (
        chain_id, (lower(after_state ->> 'address')), (after_state ->> 'coin_type'), (after_state ->> 'namespace'), block_number
    ) INCLUDE (normalized_event_id)
    WHERE event_kind IN ('ReverseChanged', 'RecordChanged')
      AND canonicality_state IN ('canonical', 'safe', 'finalized')
      AND (lower(after_state ->> 'address')) IS NOT NULL
      AND (after_state ->> 'coin_type') IS NOT NULL
      AND (after_state ->> 'namespace') IS NOT NULL;

CREATE INDEX CONCURRENTLY IF NOT EXISTS normalized_events_project_primary_before_idx
    ON bigname_phase.normalized_events (
        chain_id, (lower(before_state ->> 'address')), (before_state ->> 'coin_type'), (before_state ->> 'namespace'), block_number
    ) INCLUDE (normalized_event_id)
    WHERE event_kind IN ('ReverseChanged', 'RecordChanged')
      AND canonicality_state IN ('canonical', 'safe', 'finalized')
      AND (lower(before_state ->> 'address')) IS NOT NULL
      AND (before_state ->> 'coin_type') IS NOT NULL
      AND (before_state ->> 'namespace') IS NOT NULL;

CREATE INDEX CONCURRENTLY IF NOT EXISTS normalized_events_project_primary_after_source_idx
    ON bigname_phase.normalized_events (
        chain_id, (lower(after_state -> 'primary_claim_source' ->> 'address')), (after_state -> 'primary_claim_source' ->> 'coin_type'), (after_state -> 'primary_claim_source' ->> 'namespace'), block_number
    ) INCLUDE (normalized_event_id)
    WHERE event_kind IN ('ReverseChanged', 'RecordChanged')
      AND canonicality_state IN ('canonical', 'safe', 'finalized')
      AND (lower(after_state -> 'primary_claim_source' ->> 'address')) IS NOT NULL
      AND (after_state -> 'primary_claim_source' ->> 'coin_type') IS NOT NULL
      AND (after_state -> 'primary_claim_source' ->> 'namespace') IS NOT NULL;

CREATE INDEX CONCURRENTLY IF NOT EXISTS normalized_events_project_primary_before_source_idx
    ON bigname_phase.normalized_events (
        chain_id, (lower(before_state -> 'primary_claim_source' ->> 'address')), (before_state -> 'primary_claim_source' ->> 'coin_type'), (before_state -> 'primary_claim_source' ->> 'namespace'), block_number
    ) INCLUDE (normalized_event_id)
    WHERE event_kind IN ('ReverseChanged', 'RecordChanged')
      AND canonicality_state IN ('canonical', 'safe', 'finalized')
      AND (lower(before_state -> 'primary_claim_source' ->> 'address')) IS NOT NULL
      AND (before_state -> 'primary_claim_source' ->> 'coin_type') IS NOT NULL
      AND (before_state -> 'primary_claim_source' ->> 'namespace') IS NOT NULL;

SELECT indexrelid::regclass AS index_name, indisvalid, indisready,
       pg_size_pretty(pg_relation_size(indexrelid)) AS index_size,
       pg_get_indexdef(indexrelid) AS definition
FROM pg_index
WHERE indexrelid IN (
    to_regclass('bigname_phase.normalized_events_project_name_node_idx'),
    to_regclass('bigname_phase.normalized_events_project_name_child_idx'),
    to_regclass('bigname_phase.normalized_events_project_name_after_target_idx'),
    to_regclass('bigname_phase.normalized_events_project_name_before_target_idx'),
    to_regclass('bigname_phase.normalized_events_project_primary_after_idx'),
    to_regclass('bigname_phase.normalized_events_project_primary_before_idx'),
    to_regclass('bigname_phase.normalized_events_project_primary_after_source_idx'),
    to_regclass('bigname_phase.normalized_events_project_primary_before_source_idx')
) ORDER BY index_name;
