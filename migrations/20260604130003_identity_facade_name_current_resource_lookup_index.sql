-- no-transaction

CREATE INDEX CONCURRENTLY IF NOT EXISTS name_current_identity_resource_lookup_idx
    ON public.name_current (resource_id, logical_name_id)
    WHERE resource_id IS NOT NULL;
