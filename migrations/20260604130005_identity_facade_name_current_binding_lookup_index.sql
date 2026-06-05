-- no-transaction

CREATE INDEX CONCURRENTLY IF NOT EXISTS name_current_identity_binding_lookup_idx
    ON public.name_current (surface_binding_id, logical_name_id)
    WHERE surface_binding_id IS NOT NULL;
