-- Phase 2 identity foundation: public name surfaces, backing resources, and time-ranged bindings.

CREATE EXTENSION IF NOT EXISTS btree_gist;

CREATE TABLE token_lineages (
  token_lineage_id UUID PRIMARY KEY,
  chain_id TEXT NOT NULL,
  block_hash TEXT NOT NULL,
  block_number BIGINT NOT NULL CHECK (block_number >= 0),
  provenance JSONB NOT NULL DEFAULT '{}'::JSONB,
  canonicality_state canonicality_state NOT NULL DEFAULT 'observed',
  observed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  inserted_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE resources (
  resource_id UUID PRIMARY KEY,
  token_lineage_id UUID REFERENCES token_lineages (token_lineage_id),
  chain_id TEXT NOT NULL,
  block_hash TEXT NOT NULL,
  block_number BIGINT NOT NULL CHECK (block_number >= 0),
  provenance JSONB NOT NULL DEFAULT '{}'::JSONB,
  canonicality_state canonicality_state NOT NULL DEFAULT 'observed',
  observed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  inserted_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (token_lineage_id)
);

CREATE TABLE name_surfaces (
  logical_name_id TEXT PRIMARY KEY,
  namespace TEXT NOT NULL,
  input_name TEXT NOT NULL,
  canonical_display_name TEXT NOT NULL,
  normalized_name TEXT NOT NULL,
  dns_encoded_name BYTEA NOT NULL,
  namehash TEXT NOT NULL,
  labelhashes TEXT[] NOT NULL,
  normalizer_version TEXT NOT NULL,
  normalization_warnings JSONB NOT NULL DEFAULT '[]'::JSONB,
  normalization_errors JSONB NOT NULL DEFAULT '[]'::JSONB,
  chain_id TEXT NOT NULL,
  block_hash TEXT NOT NULL,
  block_number BIGINT NOT NULL CHECK (block_number >= 0),
  provenance JSONB NOT NULL DEFAULT '{}'::JSONB,
  canonicality_state canonicality_state NOT NULL DEFAULT 'observed',
  observed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  inserted_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  CHECK (logical_name_id = namespace || ':' || normalized_name)
);

CREATE TABLE surface_bindings (
  surface_binding_id UUID PRIMARY KEY,
  logical_name_id TEXT NOT NULL REFERENCES name_surfaces (logical_name_id),
  resource_id UUID NOT NULL REFERENCES resources (resource_id),
  binding_kind TEXT NOT NULL,
  active_from TIMESTAMPTZ NOT NULL,
  active_to TIMESTAMPTZ,
  chain_id TEXT NOT NULL,
  block_hash TEXT NOT NULL,
  block_number BIGINT NOT NULL CHECK (block_number >= 0),
  provenance JSONB NOT NULL DEFAULT '{}'::JSONB,
  canonicality_state canonicality_state NOT NULL DEFAULT 'observed',
  observed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  inserted_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  CHECK (
    binding_kind IN (
      'declared_registry_path',
      'linked_subregistry_path',
      'resolver_alias_path',
      'observed_wildcard_path',
      'migration_rebind',
      'observed_only'
    )
  ),
  CHECK (active_to IS NULL OR active_to > active_from),
  CONSTRAINT surface_bindings_no_overlap
    EXCLUDE USING gist (
      logical_name_id WITH =,
      tstzrange(active_from, COALESCE(active_to, 'infinity'::TIMESTAMPTZ), '[)') WITH &&
    )
    WHERE (canonicality_state IN ('canonical', 'safe', 'finalized'))
);
