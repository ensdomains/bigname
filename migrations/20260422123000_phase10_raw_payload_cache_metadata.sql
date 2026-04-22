-- Phase 10 raw retention: evictable block-scoped payload-cache metadata.
--
-- This table records what block-scoped payload was fetched without making the
-- payload bytes a durable raw fact. Block hash is identity; block number is
-- retained only as position metadata.

CREATE TABLE raw_payload_cache_metadata (
  raw_payload_cache_metadata_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  chain_id TEXT NOT NULL,
  block_hash TEXT NOT NULL,
  payload_kind TEXT NOT NULL,
  digest_algorithm TEXT,
  retained_digest TEXT,
  block_number BIGINT CHECK (block_number IS NULL OR block_number >= 0),
  payload_size_bytes BIGINT NOT NULL CHECK (payload_size_bytes >= 0),
  content_type TEXT,
  content_encoding TEXT,
  cache_metadata JSONB NOT NULL DEFAULT '{}'::JSONB,
  canonicality_state canonicality_state NOT NULL DEFAULT 'observed',
  first_observed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  last_observed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  CHECK (btrim(chain_id) <> ''),
  CHECK (btrim(block_hash) <> ''),
  CHECK (btrim(payload_kind) <> ''),
  CHECK ((digest_algorithm IS NULL) = (retained_digest IS NULL)),
  CHECK (digest_algorithm IS NULL OR btrim(digest_algorithm) <> ''),
  CHECK (retained_digest IS NULL OR btrim(retained_digest) <> ''),
  CHECK (content_type IS NULL OR btrim(content_type) <> ''),
  CHECK (content_encoding IS NULL OR btrim(content_encoding) <> ''),
  CHECK (jsonb_typeof(cache_metadata) = 'object')
);

CREATE UNIQUE INDEX raw_payload_cache_metadata_identity_idx
  ON raw_payload_cache_metadata (
    chain_id,
    block_hash,
    payload_kind,
    COALESCE(digest_algorithm, ''),
    COALESCE(retained_digest, '')
  );

CREATE INDEX raw_payload_cache_metadata_by_block_idx
  ON raw_payload_cache_metadata (chain_id, block_hash, payload_kind);

CREATE INDEX raw_payload_cache_metadata_by_retained_digest_idx
  ON raw_payload_cache_metadata (digest_algorithm, retained_digest)
  WHERE retained_digest IS NOT NULL;

CREATE INDEX raw_payload_cache_metadata_by_state_idx
  ON raw_payload_cache_metadata (chain_id, canonicality_state, block_number DESC, block_hash);
