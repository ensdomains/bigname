CREATE TABLE service_loop_heartbeats (
    service_name TEXT NOT NULL,
    instance_id TEXT NOT NULL,
    scope_kind TEXT NOT NULL,
    scope_id TEXT NOT NULL,
    started_at TIMESTAMPTZ NOT NULL,
    heartbeat_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (service_name, instance_id, scope_kind, scope_id),
    CHECK (service_name IN ('indexer', 'worker')),
    CHECK (btrim(instance_id) <> ''),
    CHECK (
        (scope_kind = 'process' AND scope_id = 'process')
        OR (
            service_name = 'indexer'
            AND scope_kind = 'chain'
            AND btrim(scope_id) <> ''
            AND scope_id <> 'process'
        )
    ),
    CHECK (heartbeat_at >= started_at)
);

CREATE INDEX service_loop_heartbeats_latest_process_idx
    ON service_loop_heartbeats (service_name, heartbeat_at DESC, instance_id)
    WHERE scope_kind = 'process' AND scope_id = 'process';
