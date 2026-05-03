-- no-transaction

-- Compact history routes need resource-anchor lookups, but they do not need the
-- wider projection replay ordering index to find the small per-resource row set.
CREATE INDEX CONCURRENTLY IF NOT EXISTS normalized_events_history_resource_lookup_idx
    ON public.normalized_events (
        resource_id,
        block_number DESC NULLS LAST,
        normalized_event_id DESC
    )
    WHERE resource_id IS NOT NULL
      AND canonicality_state IN (
          'canonical'::canonicality_state,
          'safe'::canonicality_state,
          'finalized'::canonicality_state
      );
