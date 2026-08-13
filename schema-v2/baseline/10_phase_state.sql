CREATE TABLE IF NOT EXISTS ingest_cursors (
    chain_id text NOT NULL,
    source_key text NOT NULL,
    source_kind text NOT NULL,
    seed_basis text NOT NULL,
    start_block_number bigint NOT NULL,
    next_block_number bigint NOT NULL,
    target_block_number bigint,
    last_processed_block_number bigint,
    last_processed_block_hash text,
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (chain_id, source_key),
    CHECK (btrim(chain_id) <> ''),
    CHECK (btrim(source_key) <> ''),
    CHECK (btrim(source_kind) <> ''),
    CHECK (
        seed_basis IN (
            'base_seam',
            'new_signature_range',
            'ethereum_head'
        )
    ),
    CHECK (start_block_number >= 0),
    CONSTRAINT ingest_cursors_next_block_order_check
        CHECK (next_block_number >= start_block_number),
    CONSTRAINT ingest_cursors_target_block_order_check CHECK (
        target_block_number IS NULL
        OR target_block_number >= start_block_number
    ),
    CONSTRAINT ingest_cursors_last_processed_pair_check CHECK (
        (last_processed_block_number IS NULL)
        = (last_processed_block_hash IS NULL)
    ),
    CONSTRAINT ingest_cursors_last_processed_order_check CHECK (
        last_processed_block_number IS NULL
        OR (
            last_processed_block_number >= start_block_number
            AND last_processed_block_number < next_block_number
        )
    )
);

CREATE INDEX IF NOT EXISTS ingest_cursors_progress_idx
    ON ingest_cursors (
        chain_id,
        next_block_number,
        target_block_number,
        source_key
    );

CREATE TABLE IF NOT EXISTS chain_phase_state (
    chain_id text NOT NULL,
    phase_name text NOT NULL,
    phase_status text NOT NULL DEFAULT 'idle',
    verification_level text,
    settled_while_unconfigured boolean,
    current_block_number bigint,
    current_block_hash text,
    target_block_number bigint,
    target_block_hash text,
    input_content_hash text,
    redo_in_progress boolean NOT NULL DEFAULT false,
    redo_mode text,
    redo_previous_phase_status text,
    redo_previous_last_error text,
    redo_previous_started_at timestamptz,
    redo_previous_finished_at timestamptz,
    redo_from_block_number bigint,
    redo_to_block_number bigint,
    redo_current_block_number bigint,
    redo_current_block_hash text,
    redo_target_block_number bigint,
    redo_target_block_hash text,
    live_handoff_block_number bigint,
    live_handoff_block_hash text,
    last_error text,
    started_at timestamptz,
    finished_at timestamptz,
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (chain_id, phase_name),
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
    CHECK (
        phase_status IN ('idle', 'running', 'paused', 'completed', 'failed')
    ),
    CHECK (
        verification_level IS NULL
        OR verification_level IN (
            'quick_synced',
            'cross_checked',
            'node_checked'
        )
    ),
    CONSTRAINT chain_phase_state_verification_phase_check
        CHECK (verification_level IS NULL OR phase_name = 'verify'),
    CONSTRAINT chain_phase_state_unconfigured_settlement_check CHECK (
        settled_while_unconfigured IS NULL
        OR settled_while_unconfigured
    ),
    CHECK (
        (current_block_number IS NULL)
        = (current_block_hash IS NULL)
    ),
    CHECK (current_block_number IS NULL OR current_block_number >= 0),
    CHECK (
        (target_block_number IS NULL)
        = (target_block_hash IS NULL)
    ),
    CHECK (target_block_number IS NULL OR target_block_number >= 0),
    CHECK (
        input_content_hash IS NULL
        OR btrim(input_content_hash) <> ''
    ),
    CHECK (
        redo_mode IS NULL
        OR redo_mode IN ('redo', 'recompute_flags')
    ),
    CHECK (
        (redo_current_block_number IS NULL)
        = (redo_current_block_hash IS NULL)
    ),
    CHECK (
        (redo_target_block_number IS NULL)
        = (redo_target_block_hash IS NULL)
    ),
    CHECK (
        (
            NOT redo_in_progress
            AND redo_mode IS NULL
            AND redo_previous_phase_status IS NULL
            AND redo_previous_last_error IS NULL
            AND redo_previous_started_at IS NULL
            AND redo_previous_finished_at IS NULL
            AND redo_from_block_number IS NULL
            AND redo_to_block_number IS NULL
            AND redo_current_block_number IS NULL
            AND redo_current_block_hash IS NULL
            AND redo_target_block_number IS NULL
            AND redo_target_block_hash IS NULL
        )
        OR (
            redo_in_progress
            AND phase_status IN ('running', 'paused')
            AND redo_mode IS NOT NULL
            AND redo_previous_phase_status IN (
                'idle',
                'running',
                'paused',
                'completed',
                'failed'
            )
            AND (
                (
                    redo_previous_phase_status = 'idle'
                    AND redo_previous_started_at IS NULL
                    AND redo_previous_finished_at IS NULL
                )
                OR (
                    redo_previous_phase_status IN ('running', 'paused')
                    AND redo_previous_started_at IS NOT NULL
                    AND redo_previous_finished_at IS NULL
                )
                OR (
                    redo_previous_phase_status IN ('completed', 'failed')
                    AND redo_previous_started_at IS NOT NULL
                    AND redo_previous_finished_at IS NOT NULL
                    AND redo_previous_finished_at >= redo_previous_started_at
                )
            )
            AND (
                (
                    redo_previous_phase_status = 'failed'
                    AND redo_previous_last_error IS NOT NULL
                    AND btrim(redo_previous_last_error) <> ''
                )
                OR (
                    redo_previous_phase_status <> 'failed'
                    AND redo_previous_last_error IS NULL
                )
            )
            AND redo_from_block_number IS NOT NULL
            AND redo_from_block_number >= 0
            AND redo_to_block_number IS NOT NULL
            AND redo_to_block_number >= redo_from_block_number
            AND (
                redo_current_block_number IS NULL
                OR redo_current_block_number BETWEEN
                    redo_from_block_number AND redo_to_block_number
            )
            AND (
                redo_target_block_number IS NULL
                OR redo_target_block_number BETWEEN
                    redo_from_block_number AND redo_to_block_number
            )
            AND (
                redo_current_block_number IS NULL
                OR redo_target_block_number IS NULL
                OR redo_current_block_number <= redo_target_block_number
            )
            AND (
                redo_mode <> 'recompute_flags'
                OR phase_name = 'interpret'
            )
        )
    ),
    CHECK (
        (live_handoff_block_number IS NULL)
        = (live_handoff_block_hash IS NULL)
    ),
    CHECK (
        live_handoff_block_number IS NULL
        OR (
            phase_name = 'ingest'
            AND live_handoff_block_number >= 0
        )
    ),
    CHECK (
        (phase_status = 'idle' AND started_at IS NULL AND finished_at IS NULL)
        OR (
            phase_status IN ('running', 'paused')
            AND started_at IS NOT NULL
            AND finished_at IS NULL
        )
        OR (
            phase_status IN ('completed', 'failed')
            AND started_at IS NOT NULL
            AND finished_at IS NOT NULL
            AND finished_at >= started_at
        )
    ),
    CHECK (
        (phase_status = 'failed' AND last_error IS NOT NULL)
        OR (
            redo_in_progress
            AND phase_status IN ('running', 'paused')
        )
        OR (
            phase_status <> 'failed'
            AND NOT redo_in_progress
            AND last_error IS NULL
        )
    ),
    CHECK (last_error IS NULL OR btrim(last_error) <> '')
);

CREATE INDEX IF NOT EXISTS chain_phase_state_status_idx
    ON chain_phase_state (phase_name, phase_status, updated_at);

COMMENT ON TABLE ingest_cursors IS
    'This table stores ingest progress for each chain source.';
COMMENT ON COLUMN ingest_cursors.chain_id IS
    'This value identifies the chain.';
COMMENT ON COLUMN ingest_cursors.source_key IS
    'This value identifies the ingest source.';
COMMENT ON COLUMN ingest_cursors.source_kind IS
    'This value states the ingest source kind.';
COMMENT ON COLUMN ingest_cursors.seed_basis IS
    'This value states how the start block was selected.';
COMMENT ON COLUMN ingest_cursors.start_block_number IS
    'This value is the first source block height.';
COMMENT ON COLUMN ingest_cursors.next_block_number IS
    'This value is the next source block height.';
COMMENT ON COLUMN ingest_cursors.target_block_number IS
    'This value is the current ingest target.';
COMMENT ON COLUMN ingest_cursors.last_processed_block_number IS
    'This value is the latest stored source block height.';
COMMENT ON COLUMN ingest_cursors.last_processed_block_hash IS
    'This value identifies the latest stored source block.';
COMMENT ON COLUMN ingest_cursors.updated_at IS
    'This time records the latest progress change.';

COMMENT ON TABLE chain_phase_state IS
    'This table stores the current state of each chain phase.';
COMMENT ON COLUMN chain_phase_state.chain_id IS
    'This value identifies the chain.';
COMMENT ON COLUMN chain_phase_state.phase_name IS
    'This value identifies the phase.';
COMMENT ON COLUMN chain_phase_state.phase_status IS
    'This value states the current phase state, including capacity pauses.';
COMMENT ON COLUMN chain_phase_state.verification_level IS
    'This value states how source data was checked.';
COMMENT ON COLUMN chain_phase_state.settled_while_unconfigured IS
    'True only when startup settled an active phase row for a chain absent from runtime configuration; NULL identifies ordinary phase state.';
COMMENT ON COLUMN chain_phase_state.current_block_number IS
    'This value is the latest phase block height.';
COMMENT ON COLUMN chain_phase_state.current_block_hash IS
    'This value identifies the latest phase block.';
COMMENT ON COLUMN chain_phase_state.target_block_number IS
    'This value is the phase target block height.';
COMMENT ON COLUMN chain_phase_state.target_block_hash IS
    'This value identifies the phase target block.';
COMMENT ON COLUMN chain_phase_state.input_content_hash IS
    'This value identifies the interpretation inputs.';
COMMENT ON COLUMN chain_phase_state.redo_in_progress IS
    'This value records an unfinished explicit redo.';
COMMENT ON COLUMN chain_phase_state.redo_mode IS
    'This value identifies the explicit redo operation.';
COMMENT ON COLUMN chain_phase_state.redo_previous_phase_status IS
    'This value preserves the normal phase state while an explicit redo is active.';
COMMENT ON COLUMN chain_phase_state.redo_previous_last_error IS
    'This value preserves the normal phase error while an explicit redo is active.';
COMMENT ON COLUMN chain_phase_state.redo_previous_started_at IS
    'This value preserves the normal phase start time while an explicit redo is active.';
COMMENT ON COLUMN chain_phase_state.redo_previous_finished_at IS
    'This value preserves the normal phase finish time while an explicit redo is active.';
COMMENT ON COLUMN chain_phase_state.redo_from_block_number IS
    'This value is the first block in the explicit redo range.';
COMMENT ON COLUMN chain_phase_state.redo_to_block_number IS
    'This value is the last block in the explicit redo range.';
COMMENT ON COLUMN chain_phase_state.redo_current_block_number IS
    'This value is the latest block completed only by the active redo.';
COMMENT ON COLUMN chain_phase_state.redo_current_block_hash IS
    'This value identifies the latest block completed only by the active redo.';
COMMENT ON COLUMN chain_phase_state.redo_target_block_number IS
    'This value is the current target block only for the active redo.';
COMMENT ON COLUMN chain_phase_state.redo_target_block_hash IS
    'This value identifies the current target block only for the active redo.';
COMMENT ON COLUMN chain_phase_state.live_handoff_block_number IS
    'This value is the first live block height.';
COMMENT ON COLUMN chain_phase_state.live_handoff_block_hash IS
    'This value identifies the first live block.';
COMMENT ON COLUMN chain_phase_state.last_error IS
    'This value describes the latest phase failure.';
COMMENT ON COLUMN chain_phase_state.started_at IS
    'This time records the phase start.';
COMMENT ON COLUMN chain_phase_state.finished_at IS
    'This time records the phase end.';
COMMENT ON COLUMN chain_phase_state.updated_at IS
    'This time records the latest state change.';
