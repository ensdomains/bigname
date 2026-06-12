-- no-transaction

CREATE INDEX CONCURRENTLY IF NOT EXISTS surface_bindings_reorg_orphaning_block_hash_idx
    ON public.surface_bindings (chain_id, block_hash)
    WHERE canonicality_state <> 'orphaned'::public.canonicality_state;
