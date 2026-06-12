-- no-transaction

CREATE INDEX CONCURRENTLY IF NOT EXISTS resources_reorg_orphaning_block_hash_idx
    ON public.resources (chain_id, block_hash)
    WHERE canonicality_state <> 'orphaned'::public.canonicality_state;
