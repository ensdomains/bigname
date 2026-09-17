-- Prebuild these indexes concurrently on large initialized databases using
-- ops/v1-lookahead-indexes/install.sql before applying schema-migrations.
DO $migration$
BEGIN
    IF to_regclass('bigname_phase.normalized_events') IS NULL THEN
        RETURN;
    END IF;

    CREATE INDEX IF NOT EXISTS normalized_events_v1_due_probe_idx
        ON bigname_phase.normalized_events (
            chain_id,
            (CASE WHEN jsonb_typeof(after_state -> 'expiry') IN ('number','string')
                AND after_state ->> 'expiry' ~ '^[+-]?[0-9]+$'
                AND length(ltrim(after_state ->> 'expiry', '+-0')) <= 19
              THEN ((CASE WHEN left(after_state ->> 'expiry', 1) = '-' THEN '-' ELSE '' END)
                || COALESCE(NULLIF(ltrim(after_state ->> 'expiry', '+-0'), ''), '0'))::numeric
            END),
            block_number
        )
        WHERE canonicality_state IN ('canonical','safe','finalized')
          AND source_family = 'ens_v1_registrar_l1'
          AND event_kind IN ('RegistrationGranted','RegistrationRenewed','TokenControlTransferred');

    CREATE INDEX IF NOT EXISTS normalized_events_v1_direct_node_probe_idx
        ON bigname_phase.normalized_events (
            chain_id,
            (COALESCE(namespace || ':' || lower(COALESCE(after_state ->> 'child_node', after_state ->> 'namehash', after_state ->> 'node', after_state #>> '{grant_source,node}', after_state #>> '{revocation_source,node}')), logical_name_id)),
            block_number
        )
        WHERE canonicality_state IN ('canonical','safe','finalized')
          AND source_family LIKE 'ens\_v1\_%';
END
$migration$;
