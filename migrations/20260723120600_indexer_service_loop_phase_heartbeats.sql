ALTER TABLE service_loop_heartbeats
    DROP CONSTRAINT service_loop_heartbeats_scope_check;

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
            service_name IN ('indexer', 'worker')
            AND scope_kind = 'phase'
            AND btrim(scope_id) <> ''
            AND scope_id <> 'process'
        )
    );
