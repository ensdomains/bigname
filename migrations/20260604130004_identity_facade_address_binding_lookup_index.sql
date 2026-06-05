-- no-transaction

CREATE INDEX CONCURRENTLY IF NOT EXISTS address_names_current_identity_binding_lookup_idx
    ON public.address_names_current (surface_binding_id, address)
    WHERE surface_binding_id IS NOT NULL;
