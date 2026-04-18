-- Phase 6 projection foundation: resolver overview current storage.

CREATE TABLE resolver_current (
  chain_id TEXT NOT NULL,
  resolver_address TEXT NOT NULL,
  declared_summary JSONB NOT NULL DEFAULT '{}'::JSONB,
  provenance JSONB NOT NULL DEFAULT '{}'::JSONB,
  coverage JSONB NOT NULL DEFAULT '{}'::JSONB,
  chain_positions JSONB NOT NULL DEFAULT '{}'::JSONB,
  canonicality_summary JSONB NOT NULL DEFAULT '{}'::JSONB,
  manifest_version BIGINT NOT NULL CHECK (manifest_version > 0),
  last_recomputed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  inserted_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (chain_id, resolver_address),
  CHECK (chain_id <> ''),
  CHECK (resolver_address <> '')
);
