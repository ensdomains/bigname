-- no-transaction

CREATE INDEX CONCURRENTLY IF NOT EXISTS name_current_identity_token_lineage_lookup_idx
    ON public.name_current (token_lineage_id, logical_name_id)
    WHERE token_lineage_id IS NOT NULL;
