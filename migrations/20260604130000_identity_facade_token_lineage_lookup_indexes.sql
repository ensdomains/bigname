-- no-transaction

CREATE INDEX CONCURRENTLY IF NOT EXISTS address_names_current_identity_token_lineage_lookup_idx
    ON public.address_names_current (token_lineage_id, address)
    WHERE token_lineage_id IS NOT NULL;
