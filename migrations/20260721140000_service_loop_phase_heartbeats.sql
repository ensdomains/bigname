DO $$
DECLARE
    existing_scope_constraint TEXT;
BEGIN
    SELECT constraint_row.conname
    INTO existing_scope_constraint
    FROM pg_constraint AS constraint_row
    WHERE constraint_row.conrelid = 'service_loop_heartbeats'::regclass
      AND constraint_row.contype = 'c'
      AND pg_get_constraintdef(constraint_row.oid) LIKE '%scope_kind%'
    ORDER BY constraint_row.conname
    LIMIT 1;

    IF existing_scope_constraint IS NULL THEN
        RAISE EXCEPTION 'service_loop_heartbeats scope constraint was not found';
    END IF;

    EXECUTE format(
        'ALTER TABLE service_loop_heartbeats DROP CONSTRAINT %I',
        existing_scope_constraint
    );
END
$$;

ALTER TABLE service_loop_heartbeats
    ADD CONSTRAINT service_loop_heartbeats_scope_check CHECK (
        (scope_kind = 'process' AND scope_id = 'process')
        OR (
            service_name = 'indexer'
            AND scope_kind = 'chain'
            AND btrim(scope_id) <> ''
            AND scope_id <> 'process'
        )
        OR (
            service_name = 'worker'
            AND scope_kind = 'phase'
            AND btrim(scope_id) <> ''
            AND scope_id <> 'process'
        )
    );

CREATE INDEX service_loop_heartbeats_active_phase_idx
    ON service_loop_heartbeats (service_name, instance_id, heartbeat_at DESC)
    WHERE scope_kind = 'phase';
