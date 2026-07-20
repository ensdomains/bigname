-- no-transaction

-- The capped live baseline sweep probes each watched address for any retained
-- non-orphaned code observation. Normalize the stored address in the index so
-- that probe stays indexed even though the schema permits mixed-case text.
CREATE INDEX CONCURRENTLY IF NOT EXISTS raw_code_hashes_non_orphaned_lower_address_idx
    ON public.raw_code_hashes (chain_id, LOWER(contract_address))
    WHERE canonicality_state <> 'orphaned'::public.canonicality_state;
