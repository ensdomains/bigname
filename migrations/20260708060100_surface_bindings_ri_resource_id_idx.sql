-- no-transaction

-- Full (non-partial) btree on surface_bindings.resource_id so parent-side
-- deletes on resources run their referential-integrity probes as index
-- lookups. The only prior index on this column is partial on
-- canonicality_state, which RI check plans cannot use; during the 2026-07-06
-- Base rederive delete each per-row FK probe seq-scanned the surface_bindings
-- heap until this index was created operationally (renamed to this permanent
-- name by the preceding migration, making this a no-op there).

CREATE INDEX CONCURRENTLY IF NOT EXISTS surface_bindings_ri_resource_id_idx
    ON public.surface_bindings (resource_id);
