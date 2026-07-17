-- no-transaction

-- A failed concurrent build can leave this name attached to an invalid index.
-- The checked-in migration command validates and repairs that remnant before
-- reporting success, while indexer catch-up applies the same runtime guard.

CREATE INDEX CONCURRENTLY IF NOT EXISTS normalized_events_record_inventory_resource_replay_idx
ON public.normalized_events (
    resource_id,
    block_number,
    log_index NULLS FIRST,
    normalized_event_id
)
WHERE resource_id IS NOT NULL
  AND logical_name_id IS NOT NULL
  AND chain_id IS NOT NULL
  AND block_number IS NOT NULL
  AND block_hash IS NOT NULL
  AND derivation_kind IN (
      'ens_v1_unwrapped_authority',
      'ens_v2_registry_resource_surface',
      'ens_v2_resolver'
  )
  AND event_kind IN ('RecordChanged', 'RecordVersionChanged', 'ResolverChanged')
  AND canonicality_state IN (
      'canonical'::public.canonicality_state,
      'safe'::public.canonicality_state,
      'finalized'::public.canonicality_state
  );
