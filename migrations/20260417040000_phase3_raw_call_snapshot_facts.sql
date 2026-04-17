-- Phase 3 intake: exact block-anchored eth_call snapshots as immutable raw facts.

CREATE TABLE raw_call_snapshots (
  raw_call_snapshot_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  chain_id TEXT NOT NULL,
  block_hash TEXT NOT NULL,
  block_number BIGINT NOT NULL CHECK (block_number >= 0),
  request_hash TEXT NOT NULL,
  request_payload JSONB NOT NULL,
  response_hash TEXT NOT NULL,
  response_payload JSONB NOT NULL,
  canonicality_state canonicality_state NOT NULL DEFAULT 'observed',
  observed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (chain_id, block_hash, request_hash),
  CHECK (jsonb_typeof(request_payload) = 'object')
);

CREATE INDEX raw_call_snapshots_by_request_hash_idx
  ON raw_call_snapshots (chain_id, request_hash, block_number DESC);

CREATE INDEX raw_call_snapshots_by_response_hash_idx
  ON raw_call_snapshots (chain_id, response_hash, block_number DESC);

CREATE INDEX raw_call_snapshots_by_state_idx
  ON raw_call_snapshots (chain_id, canonicality_state, block_number DESC, request_hash);
