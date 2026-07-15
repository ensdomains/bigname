-- no-transaction

-- Stored-lineage promotion's companion gate resolves candidate log emitters
-- against the watched address surface by normalized address; without an
-- expression index that probe is a full scan of a multi-million-row table on
-- every promotion slice.
CREATE INDEX CONCURRENTLY IF NOT EXISTS contract_instance_addresses_active_lower_address_idx
    ON public.contract_instance_addresses (chain_id, LOWER(address))
    WHERE deactivated_at IS NULL;
