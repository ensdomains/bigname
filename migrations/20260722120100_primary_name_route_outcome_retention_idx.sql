-- no-transaction

CREATE INDEX CONCURRENTLY IF NOT EXISTS execution_cache_outcomes_route_primary_checkpoint_idx
    ON public.execution_cache_outcomes (
        ((topology_version_boundary #>> '{chain_position,block_number}')::numeric),
        execution_cache_key
    )
    WHERE request_type = 'verified_primary_name'
      AND namespace = 'ens'
      AND topology_version_boundary ->> 'boundary_kind' = 'selected_checkpoint'
      AND record_version_boundary = topology_version_boundary
      AND topology_version_boundary #>> '{chain_position,chain_id}' = 'ethereum-mainnet'
      AND jsonb_typeof(
            topology_version_boundary #> '{chain_position,block_number}'
          ) = 'number';
