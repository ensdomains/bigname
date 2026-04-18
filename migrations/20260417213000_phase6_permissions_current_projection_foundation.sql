-- Phase 6 projection foundation: resource-centric current permissions storage.

CREATE TABLE permissions_current (
  resource_id UUID NOT NULL REFERENCES resources (resource_id),
  subject TEXT NOT NULL,
  scope TEXT NOT NULL,
  scope_kind TEXT NOT NULL,
  scope_detail JSONB NOT NULL DEFAULT '{}'::JSONB,
  effective_powers JSONB NOT NULL DEFAULT '[]'::JSONB,
  grant_source JSONB NOT NULL DEFAULT '{}'::JSONB,
  revocation_source JSONB,
  inheritance_path JSONB NOT NULL DEFAULT '[]'::JSONB,
  transfer_behavior JSONB NOT NULL DEFAULT '{}'::JSONB,
  provenance JSONB NOT NULL DEFAULT '{}'::JSONB,
  coverage JSONB NOT NULL DEFAULT '{}'::JSONB,
  chain_positions JSONB NOT NULL DEFAULT '{}'::JSONB,
  canonicality_summary JSONB NOT NULL DEFAULT '{}'::JSONB,
  manifest_version BIGINT NOT NULL CHECK (manifest_version > 0),
  last_recomputed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  inserted_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (resource_id, subject, scope),
  CHECK (subject <> ''),
  CHECK (scope <> ''),
  CHECK (
    scope_kind IN (
      'root',
      'registry',
      'resource',
      'resolver',
      'record_manager',
      'migration_derived',
      'transport_derived'
    )
  )
);
