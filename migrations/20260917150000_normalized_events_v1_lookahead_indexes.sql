-- Prebuild these indexes concurrently on large initialized databases using
-- ops/v1-lookahead-indexes/install.sql before applying schema-migrations.
--
-- CREATE INDEX IF NOT EXISTS matches on the name alone. An interrupted concurrent
-- prebuild leaves an invalid index under the right name, and the statements below
-- then succeed without building anything. The check at the end stops the run
-- instead of recording success over an index no query can use. To recover, follow
-- ops/v1-lookahead-indexes/README.md: confirm no build is running, drop only the
-- invalid index with DROP INDEX CONCURRENTLY, rerun install.sql, then run the
-- schema-migrations again.
DO $migration$
DECLARE
    checked_index text;
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

    FOREACH checked_index IN ARRAY ARRAY[
        'normalized_events_v1_due_probe_idx',
        'normalized_events_v1_direct_node_probe_idx'
    ]
    LOOP
        IF NOT EXISTS (
            SELECT 1
            FROM pg_index
            WHERE indexrelid = to_regclass('bigname_phase.' || checked_index)
              AND indrelid = to_regclass('bigname_phase.normalized_events')
              AND indisvalid
              AND indisready
        ) THEN
            RAISE EXCEPTION
                '% exists but is not a valid and ready index on bigname_phase.normalized_events; follow the recovery steps in ops/v1-lookahead-indexes/README.md, then run the schema-migrations again',
                checked_index;
        END IF;
    END LOOP;
END
$migration$;
