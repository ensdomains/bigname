-- no-transaction

-- Operational repair support for legacy ENSv1 resolver text records. This is
-- intentionally narrow: it indexes only the old lossy generic `text` rows that
-- can be repaired from exact historical TextChanged logs, and it is not a broad
-- normalized-event audit index.
CREATE INDEX CONCURRENTLY IF NOT EXISTS normalized_events_ens_v1_text_record_repair_idx
    ON public.normalized_events (
        chain_id,
        block_number,
        transaction_hash,
        log_index,
        normalized_event_id
    )
    INCLUDE (
        block_hash
    )
    WHERE block_number IS NOT NULL
      AND block_hash IS NOT NULL
      AND transaction_hash IS NOT NULL
      AND log_index IS NOT NULL
      AND derivation_kind = 'ens_v1_unwrapped_authority'
      AND event_kind = 'RecordChanged'
      AND source_family = 'ens_v1_resolver_l1'
      AND after_state->>'record_family' = 'text'
      AND after_state->>'record_key' = 'text'
      AND after_state->'selector_key' = 'null'::jsonb
      AND canonicality_state = ANY (
          ARRAY[
              'canonical'::public.canonicality_state,
              'safe'::public.canonicality_state,
              'finalized'::public.canonicality_state
          ]
      );
