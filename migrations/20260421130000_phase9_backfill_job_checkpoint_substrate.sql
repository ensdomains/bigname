-- Phase 9 storage substrate: bounded backfill jobs and range checkpoints.
--
-- These tables persist operational backfill progress only. They do not define
-- canonicality and are intentionally separate from chain_lineage and
-- chain_checkpoints.

CREATE TYPE backfill_lifecycle_status AS ENUM (
  'pending',
  'reserved',
  'running',
  'completed',
  'failed'
);

CREATE TABLE backfill_jobs (
  backfill_job_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  deployment_profile TEXT NOT NULL,
  chain_id TEXT NOT NULL,
  source_identity JSONB NOT NULL,
  scan_mode TEXT NOT NULL,
  range_start_block_number BIGINT NOT NULL CHECK (range_start_block_number >= 0),
  range_end_block_number BIGINT NOT NULL CHECK (range_end_block_number >= range_start_block_number),
  idempotency_key TEXT NOT NULL,
  status backfill_lifecycle_status NOT NULL DEFAULT 'pending',
  failure_reason TEXT,
  failure_metadata JSONB NOT NULL DEFAULT '{}'::JSONB,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  completed_at TIMESTAMPTZ,
  UNIQUE (idempotency_key),
  CHECK (jsonb_typeof(source_identity) IN ('object', 'array')),
  CHECK (jsonb_typeof(failure_metadata) = 'object'),
  CHECK ((status = 'failed'::backfill_lifecycle_status) = (failure_reason IS NOT NULL) OR status <> 'failed'::backfill_lifecycle_status),
  CHECK ((status = 'completed'::backfill_lifecycle_status) = (completed_at IS NOT NULL) OR status <> 'completed'::backfill_lifecycle_status)
);

CREATE INDEX backfill_jobs_lookup_idx
  ON backfill_jobs (deployment_profile, chain_id, scan_mode, status);

CREATE INDEX backfill_jobs_range_idx
  ON backfill_jobs (chain_id, range_start_block_number, range_end_block_number);

CREATE TABLE backfill_ranges (
  backfill_range_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  backfill_job_id BIGINT NOT NULL REFERENCES backfill_jobs (backfill_job_id) ON DELETE CASCADE,
  range_start_block_number BIGINT NOT NULL CHECK (range_start_block_number >= 0),
  range_end_block_number BIGINT NOT NULL CHECK (range_end_block_number >= range_start_block_number),
  checkpoint_block_number BIGINT NOT NULL CHECK (checkpoint_block_number >= range_start_block_number AND checkpoint_block_number <= range_end_block_number),
  status backfill_lifecycle_status NOT NULL DEFAULT 'pending',
  lease_token TEXT,
  lease_owner TEXT,
  lease_expires_at TIMESTAMPTZ,
  attempt_count BIGINT NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
  failure_reason TEXT,
  failure_metadata JSONB NOT NULL DEFAULT '{}'::JSONB,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  completed_at TIMESTAMPTZ,
  UNIQUE (backfill_job_id, range_start_block_number, range_end_block_number),
  CHECK (jsonb_typeof(failure_metadata) = 'object'),
  CHECK ((lease_token IS NULL) = (lease_owner IS NULL)),
  CHECK ((lease_token IS NULL) = (lease_expires_at IS NULL)),
  CHECK ((status IN ('reserved'::backfill_lifecycle_status, 'running'::backfill_lifecycle_status)) = (lease_token IS NOT NULL)),
  CHECK ((status = 'failed'::backfill_lifecycle_status) = (failure_reason IS NOT NULL) OR status <> 'failed'::backfill_lifecycle_status),
  CHECK ((status = 'completed'::backfill_lifecycle_status) = (completed_at IS NOT NULL) OR status <> 'completed'::backfill_lifecycle_status)
);

CREATE INDEX backfill_ranges_reservation_idx
  ON backfill_ranges (backfill_job_id, status, range_start_block_number, range_end_block_number);

CREATE INDEX backfill_ranges_lease_expiry_idx
  ON backfill_ranges (lease_expires_at)
  WHERE lease_expires_at IS NOT NULL;

CREATE UNIQUE INDEX backfill_ranges_active_lease_token_idx
  ON backfill_ranges (lease_token)
  WHERE lease_token IS NOT NULL
    AND status IN ('reserved'::backfill_lifecycle_status, 'running'::backfill_lifecycle_status);
