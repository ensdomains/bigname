-- no-transaction

-- Exact-window recovery keys and short post-scan mutation fences probe the
-- block revision ledger by chain and inclusive block range. Keep those probes
-- independent from unrelated live-tail revisions.
CREATE INDEX CONCURRENTLY IF NOT EXISTS raw_log_staging_block_revisions_block_range_idx
    ON public.raw_log_staging_block_revisions (chain_id, block_number, revision DESC);
