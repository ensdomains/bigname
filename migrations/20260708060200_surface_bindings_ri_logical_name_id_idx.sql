-- no-transaction

-- Companion to 20260708060100: full btree on surface_bindings.logical_name_id
-- for referential-integrity probes fired by parent-side deletes on
-- name_surfaces.

CREATE INDEX CONCURRENTLY IF NOT EXISTS surface_bindings_ri_logical_name_id_idx
    ON public.surface_bindings (logical_name_id);
