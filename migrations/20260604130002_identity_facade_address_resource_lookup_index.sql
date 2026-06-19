-- no-transaction

CREATE INDEX CONCURRENTLY IF NOT EXISTS address_names_current_identity_resource_lookup_idx
    ON public.address_names_current (resource_id, address)
    WHERE resource_id IS NOT NULL;
