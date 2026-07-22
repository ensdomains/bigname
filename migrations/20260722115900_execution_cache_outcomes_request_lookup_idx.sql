-- no-transaction

CREATE INDEX CONCURRENTLY IF NOT EXISTS execution_cache_outcomes_request_lookup_idx
    ON public.execution_cache_outcomes (request_type, namespace, request_key);
