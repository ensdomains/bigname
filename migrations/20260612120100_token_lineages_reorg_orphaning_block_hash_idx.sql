-- no-transaction

CREATE INDEX CONCURRENTLY IF NOT EXISTS token_lineages_reorg_orphaning_block_hash_idx
    ON public.token_lineages (chain_id, block_hash)
    WHERE canonicality_state <> 'orphaned'::public.canonicality_state;
