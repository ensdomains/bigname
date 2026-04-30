-- no-transaction

-- Keep normalized replay's all-source late-arriving older-log rewind guard
-- bounded by recent observation time instead of scanning the historical prefix.
CREATE INDEX CONCURRENTLY IF NOT EXISTS raw_logs_canonical_rewind_observed_idx
  ON raw_logs (chain_id, observed_at, block_number, block_hash)
  WHERE canonicality_state IN (
    'canonical'::canonicality_state,
    'safe'::canonicality_state,
    'finalized'::canonicality_state
  );
