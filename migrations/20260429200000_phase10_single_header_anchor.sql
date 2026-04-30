-- Phase 10 storage compaction: chain_lineage is the single durable block
-- header-anchor table. Optional auditable header roots/bloom move to a sparse
-- extension table keyed by the same block identity. raw_blocks is removed.

CREATE TABLE chain_header_audit (
  chain_id TEXT NOT NULL,
  block_hash TEXT NOT NULL,
  logs_bloom BYTEA,
  transactions_root TEXT,
  receipts_root TEXT,
  state_root TEXT,
  observed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (chain_id, block_hash),
  FOREIGN KEY (chain_id, block_hash)
    REFERENCES chain_lineage (chain_id, block_hash)
    ON DELETE CASCADE,
  CHECK (
    logs_bloom IS NOT NULL
    OR transactions_root IS NOT NULL
    OR receipts_root IS NOT NULL
    OR state_root IS NOT NULL
  )
);

INSERT INTO chain_lineage (
  chain_id,
  block_hash,
  parent_hash,
  block_number,
  block_timestamp,
  canonicality_state,
  observed_at
)
SELECT
  chain_id,
  block_hash,
  parent_hash,
  block_number,
  block_timestamp,
  canonicality_state,
  observed_at
FROM raw_blocks
ON CONFLICT (chain_id, block_hash) DO NOTHING;

INSERT INTO chain_header_audit (
  chain_id,
  block_hash,
  logs_bloom,
  transactions_root,
  receipts_root,
  state_root,
  observed_at
)
SELECT
  chain_id,
  block_hash,
  logs_bloom,
  transactions_root,
  receipts_root,
  state_root,
  observed_at
FROM chain_lineage
WHERE logs_bloom IS NOT NULL
   OR transactions_root IS NOT NULL
   OR receipts_root IS NOT NULL
   OR state_root IS NOT NULL
ON CONFLICT (chain_id, block_hash) DO UPDATE
SET
  logs_bloom = COALESCE(chain_header_audit.logs_bloom, EXCLUDED.logs_bloom),
  transactions_root = COALESCE(chain_header_audit.transactions_root, EXCLUDED.transactions_root),
  receipts_root = COALESCE(chain_header_audit.receipts_root, EXCLUDED.receipts_root),
  state_root = COALESCE(chain_header_audit.state_root, EXCLUDED.state_root),
  observed_at = now()
WHERE (chain_header_audit.logs_bloom IS NULL OR EXCLUDED.logs_bloom IS NULL OR chain_header_audit.logs_bloom = EXCLUDED.logs_bloom)
  AND (chain_header_audit.transactions_root IS NULL OR EXCLUDED.transactions_root IS NULL OR chain_header_audit.transactions_root = EXCLUDED.transactions_root)
  AND (chain_header_audit.receipts_root IS NULL OR EXCLUDED.receipts_root IS NULL OR chain_header_audit.receipts_root = EXCLUDED.receipts_root)
  AND (chain_header_audit.state_root IS NULL OR EXCLUDED.state_root IS NULL OR chain_header_audit.state_root = EXCLUDED.state_root);

INSERT INTO chain_header_audit (
  chain_id,
  block_hash,
  logs_bloom,
  transactions_root,
  receipts_root,
  state_root,
  observed_at
)
SELECT
  chain_id,
  block_hash,
  logs_bloom,
  transactions_root,
  receipts_root,
  state_root,
  observed_at
FROM raw_blocks
WHERE logs_bloom IS NOT NULL
   OR transactions_root IS NOT NULL
   OR receipts_root IS NOT NULL
   OR state_root IS NOT NULL
ON CONFLICT (chain_id, block_hash) DO UPDATE
SET
  logs_bloom = COALESCE(chain_header_audit.logs_bloom, EXCLUDED.logs_bloom),
  transactions_root = COALESCE(chain_header_audit.transactions_root, EXCLUDED.transactions_root),
  receipts_root = COALESCE(chain_header_audit.receipts_root, EXCLUDED.receipts_root),
  state_root = COALESCE(chain_header_audit.state_root, EXCLUDED.state_root),
  observed_at = now()
WHERE (chain_header_audit.logs_bloom IS NULL OR EXCLUDED.logs_bloom IS NULL OR chain_header_audit.logs_bloom = EXCLUDED.logs_bloom)
  AND (chain_header_audit.transactions_root IS NULL OR EXCLUDED.transactions_root IS NULL OR chain_header_audit.transactions_root = EXCLUDED.transactions_root)
  AND (chain_header_audit.receipts_root IS NULL OR EXCLUDED.receipts_root IS NULL OR chain_header_audit.receipts_root = EXCLUDED.receipts_root)
  AND (chain_header_audit.state_root IS NULL OR EXCLUDED.state_root IS NULL OR chain_header_audit.state_root = EXCLUDED.state_root);

ALTER TABLE chain_lineage
  DROP COLUMN logs_bloom,
  DROP COLUMN transactions_root,
  DROP COLUMN receipts_root,
  DROP COLUMN state_root;

DROP TABLE raw_blocks;

CREATE INDEX chain_lineage_chain_timestamp_canonical_idx
  ON chain_lineage (chain_id, block_timestamp, block_number)
  INCLUDE (block_hash, canonicality_state)
  WHERE canonicality_state IN (
    'canonical'::canonicality_state,
    'safe'::canonicality_state,
    'finalized'::canonicality_state
  );
