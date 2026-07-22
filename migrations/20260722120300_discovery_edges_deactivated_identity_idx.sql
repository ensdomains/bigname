-- no-transaction

CREATE INDEX CONCURRENTLY IF NOT EXISTS discovery_edges_deactivated_identity_idx
    ON public.discovery_edges (chain_id, edge_kind, from_contract_instance_id, to_contract_instance_id, discovery_source)
    WHERE deactivated_at IS NOT NULL;
