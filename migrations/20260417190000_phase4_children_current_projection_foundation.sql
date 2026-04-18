-- Phase 4 projection foundation: declared direct child collection storage.

CREATE TABLE children_current (
  parent_logical_name_id TEXT NOT NULL REFERENCES name_surfaces (logical_name_id),
  child_logical_name_id TEXT NOT NULL REFERENCES name_surfaces (logical_name_id),
  surface_class TEXT NOT NULL DEFAULT 'declared',
  namespace TEXT NOT NULL,
  canonical_display_name TEXT NOT NULL,
  normalized_name TEXT NOT NULL,
  namehash TEXT NOT NULL,
  provenance JSONB NOT NULL DEFAULT '{}'::JSONB,
  chain_positions JSONB NOT NULL DEFAULT '{}'::JSONB,
  canonicality_summary JSONB NOT NULL DEFAULT '{}'::JSONB,
  manifest_version BIGINT NOT NULL CHECK (manifest_version > 0),
  last_recomputed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  inserted_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (parent_logical_name_id, child_logical_name_id, surface_class),
  CHECK (surface_class = 'declared'),
  CHECK (parent_logical_name_id <> child_logical_name_id),
  CHECK (child_logical_name_id = namespace || ':' || normalized_name)
);

CREATE INDEX children_current_parent_sort_idx
  ON children_current (
    parent_logical_name_id,
    surface_class,
    canonical_display_name,
    child_logical_name_id
  );

CREATE INDEX children_current_child_parent_idx
  ON children_current (
    child_logical_name_id,
    surface_class,
    parent_logical_name_id
  );
