-- no-transaction

-- Operational repair support for ENSv1 resolver text records that still need
-- selector/value reconstruction from exact historical TextChanged logs. This
-- stays intentionally narrow: it covers legacy generic text rows and
-- selector-specific text rows that are missing a retained value.
CREATE INDEX CONCURRENTLY IF NOT EXISTS normalized_events_ens_v1_text_record_value_repair_idx
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
      AND (
          (
              after_state->>'record_key' = 'text'
              AND after_state->'selector_key' = 'null'::jsonb
          )
          OR (
              after_state->>'record_key' LIKE 'text:%'
              AND NOT (after_state ? 'value')
          )
      )
      AND canonicality_state = ANY (
          ARRAY[
              'canonical'::public.canonicality_state,
              'safe'::public.canonicality_state,
              'finalized'::public.canonicality_state
          ]
      );
