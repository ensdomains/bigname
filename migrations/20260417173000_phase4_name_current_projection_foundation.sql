-- Phase 4 projection foundation: exact-name current projection storage.

CREATE TABLE name_current (
  logical_name_id TEXT PRIMARY KEY REFERENCES name_surfaces (logical_name_id),
  namespace TEXT NOT NULL,
  canonical_display_name TEXT NOT NULL,
  normalized_name TEXT NOT NULL,
  namehash TEXT NOT NULL,
  surface_binding_id UUID REFERENCES surface_bindings (surface_binding_id),
  resource_id UUID REFERENCES resources (resource_id),
  token_lineage_id UUID REFERENCES token_lineages (token_lineage_id),
  binding_kind TEXT,
  declared_summary JSONB NOT NULL DEFAULT '{}'::JSONB,
  provenance JSONB NOT NULL DEFAULT '{}'::JSONB,
  coverage JSONB NOT NULL DEFAULT '{}'::JSONB,
  chain_positions JSONB NOT NULL DEFAULT '{}'::JSONB,
  canonicality_summary JSONB NOT NULL DEFAULT '{}'::JSONB,
  manifest_version BIGINT NOT NULL CHECK (manifest_version > 0),
  last_recomputed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  inserted_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  CHECK (logical_name_id = namespace || ':' || normalized_name),
  CHECK (
    (surface_binding_id IS NULL AND resource_id IS NULL AND binding_kind IS NULL)
    OR
    (surface_binding_id IS NOT NULL AND resource_id IS NOT NULL AND binding_kind IS NOT NULL)
  ),
  CHECK (
    token_lineage_id IS NULL
    OR resource_id IS NOT NULL
  ),
  CHECK (
    binding_kind IS NULL
    OR binding_kind IN (
      'declared_registry_path',
      'linked_subregistry_path',
      'resolver_alias_path',
      'observed_wildcard_path',
      'migration_rebind',
      'observed_only'
    )
  )
);
