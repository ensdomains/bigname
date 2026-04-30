-- no-transaction

-- Support source-scoped normalized replay range selection by emitter and block
-- without relying on the topic-bearing resolver lookup index.
CREATE INDEX CONCURRENTLY IF NOT EXISTS raw_logs_canonical_emitter_block_idx
  ON raw_logs (chain_id, (lower(emitting_address)), block_number, transaction_index, log_index)
  WHERE canonicality_state IN (
    'canonical'::canonicality_state,
    'safe'::canonicality_state,
    'finalized'::canonicality_state
  );
