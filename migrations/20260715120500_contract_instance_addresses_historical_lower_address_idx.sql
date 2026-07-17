-- no-transaction

-- Stored-lineage promotion also checks closed watched-address windows. Keep
-- that normalized emitter probe bounded to the historical rows it can select.
CREATE INDEX CONCURRENTLY IF NOT EXISTS contract_instance_addresses_historical_lower_address_idx
    ON public.contract_instance_addresses (chain_id, LOWER(address))
    WHERE deactivated_at IS NOT NULL
      AND active_to_block_number IS NOT NULL;
