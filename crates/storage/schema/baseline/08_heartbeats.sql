CREATE TABLE IF NOT EXISTS service_heartbeats (
    service_name text NOT NULL,
    instance_id text NOT NULL,
    chain_id text NOT NULL,
    phase_name text NOT NULL,
    started_at timestamptz NOT NULL,
    heartbeat_at timestamptz NOT NULL,
    PRIMARY KEY (service_name, instance_id, chain_id, phase_name),
    CHECK (btrim(service_name) <> ''),
    CHECK (btrim(instance_id) <> ''),
    CHECK (btrim(chain_id) <> ''),
    CHECK (
        phase_name IN (
            'ingest',
            'interpret',
            'project',
            'verify',
            'live'
        )
    ),
    CONSTRAINT service_heartbeats_time_order_check
        CHECK (heartbeat_at >= started_at)
);

CREATE INDEX IF NOT EXISTS service_heartbeats_readiness_idx
    ON service_heartbeats (
        service_name,
        chain_id,
        phase_name,
        heartbeat_at DESC
    );

COMMENT ON TABLE service_heartbeats IS
    'This table stores liveness for each chain phase.';
COMMENT ON COLUMN service_heartbeats.service_name IS
    'This value identifies the service kind.';
COMMENT ON COLUMN service_heartbeats.instance_id IS
    'This value identifies the service process.';
COMMENT ON COLUMN service_heartbeats.chain_id IS
    'This value identifies the chain.';
COMMENT ON COLUMN service_heartbeats.phase_name IS
    'This value identifies the running phase.';
COMMENT ON COLUMN service_heartbeats.started_at IS
    'This time records the phase start.';
COMMENT ON COLUMN service_heartbeats.heartbeat_at IS
    'This time records runner liveness, including refreshes during storage-capacity waits.';
