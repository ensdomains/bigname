-- no-transaction

CREATE INDEX CONCURRENTLY IF NOT EXISTS name_surfaces_reorg_orphaning_block_hash_idx
    ON public.name_surfaces (chain_id, block_hash)
    WHERE canonicality_state <> 'orphaned'::public.canonicality_state;
