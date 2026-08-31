-- A schema-migration database can exist before phase-runner installs the phase
-- baseline. Existing initialized schemas receive the additive redo-scope index.
DO $migration$
BEGIN
    IF to_regclass('bigname_phase.normalized_events') IS NULL THEN
        RETURN;
    END IF;

    CREATE INDEX IF NOT EXISTS normalized_events_v2_expiry_scope_idx
    ON bigname_phase.normalized_events (
        chain_id,
        ((after_state ->> 'expiry')::numeric),
        block_number,
        logical_name_id
    )
    WHERE logical_name_id IS NOT NULL
      AND source_family IN ('ens_v2_root_l1', 'ens_v2_registry_l1')
      AND event_kind IN (
          'RegistrationGranted', 'RegistrationReserved',
          'RegistrationRenewed', 'RegistrationReleased', 'ExpiryChanged'
      )
      AND canonicality_state IN ('canonical', 'safe', 'finalized')
      AND jsonb_typeof(after_state -> 'expiry') = 'number';
END
$migration$;
