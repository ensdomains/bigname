-- A schema-migration database can exist before phase-runner installs the phase
-- baseline. Existing initialized schemas receive the additive emitter lookup
-- used by the contract-scoped event filter and registry event counts.
DO $migration$
BEGIN
    IF to_regclass('bigname_phase.normalized_events') IS NULL THEN
        RETURN;
    END IF;

    CREATE INDEX IF NOT EXISTS normalized_events_emitter_history_idx
    ON bigname_phase.normalized_events (
        lower(raw_fact_ref ->> 'emitting_address'),
        block_number DESC NULLS LAST,
        log_index DESC NULLS LAST,
        normalized_event_id DESC
    )
    WHERE raw_fact_ref ->> 'emitting_address' IS NOT NULL
      AND canonicality_state IN ('canonical', 'safe', 'finalized');
END
$migration$;
