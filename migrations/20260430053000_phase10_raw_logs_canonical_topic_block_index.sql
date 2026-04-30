-- no-transaction

-- Support generic ENSv1 resolver-event replay scans by topic across all
-- emitters without relying on emitter-prefixed raw-topic indexes.
CREATE INDEX CONCURRENTLY IF NOT EXISTS raw_logs_canonical_topic_block_idx
  ON raw_logs (chain_id, (lower(topics[1])), block_number, transaction_index, log_index)
  WHERE canonicality_state IN (
    'canonical'::canonicality_state,
    'safe'::canonicality_state,
    'finalized'::canonicality_state
  );
