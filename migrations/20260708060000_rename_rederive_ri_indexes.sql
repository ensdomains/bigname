-- Rename the operationally created 2026-07-06 rederive RI-support indexes to
-- their permanent names (catalog-only, O(1)); no-ops on fresh databases where
-- the following migrations create them under the permanent names directly.

ALTER INDEX IF EXISTS public.surface_bindings_base_rederive_resource_id_idx
    RENAME TO surface_bindings_ri_resource_id_idx;

ALTER INDEX IF EXISTS public.surface_bindings_base_rederive_logical_name_id_idx
    RENAME TO surface_bindings_ri_logical_name_id_idx;
