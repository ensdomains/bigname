-- no-transaction

CREATE INDEX CONCURRENTLY IF NOT EXISTS execution_traces_route_primary_checkpoint_idx
    ON public.execution_traces (
        ((request_metadata #>> '{cache_identity,topology_version_boundary,chain_position,block_number}')::numeric),
        execution_trace_id
    )
    WHERE request_type = 'verified_primary_name'
      AND namespace = 'ens'
      AND request_metadata ? 'route_local_claim'
      AND request_metadata ->> 'coin_type' = '60'
      AND request_metadata #>> '{cache_identity,topology_version_boundary,boundary_kind}' = 'selected_checkpoint'
      AND request_metadata #> '{cache_identity,record_version_boundary}'
          = request_metadata #> '{cache_identity,topology_version_boundary}'
      AND request_metadata #>> '{cache_identity,topology_version_boundary,chain_position,chain_id}' = 'ethereum-mainnet'
      AND jsonb_typeof(
            request_metadata #> '{cache_identity,topology_version_boundary,chain_position,block_number}'
          ) = 'number';
