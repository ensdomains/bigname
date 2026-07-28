-- no-transaction

CREATE INDEX CONCURRENTLY IF NOT EXISTS projection_invalidations_pending_state_idx
    ON public.projection_invalidations (state)
    WHERE state = 'pending'::public.projection_invalidation_state;
