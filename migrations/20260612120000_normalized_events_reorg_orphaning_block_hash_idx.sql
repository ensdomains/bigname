-- no-transaction

CREATE INDEX CONCURRENTLY IF NOT EXISTS normalized_events_reorg_orphaning_block_hash_idx
    ON public.normalized_events (chain_id, block_hash)
    WHERE canonicality_state <> 'orphaned'::public.canonicality_state;
