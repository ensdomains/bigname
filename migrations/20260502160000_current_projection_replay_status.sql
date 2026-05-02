CREATE TABLE public.current_projection_replay_status (
    projection TEXT PRIMARY KEY,
    replay_version INTEGER NOT NULL,
    completed_normalized_target_block BIGINT,
    requested_key_count BIGINT NOT NULL,
    upserted_row_count BIGINT NOT NULL,
    deleted_row_count BIGINT NOT NULL,
    completed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT current_projection_replay_status_requested_check CHECK (requested_key_count >= 0),
    CONSTRAINT current_projection_replay_status_upserted_check CHECK (upserted_row_count >= 0),
    CONSTRAINT current_projection_replay_status_deleted_check CHECK (deleted_row_count >= 0)
);

CREATE INDEX current_projection_replay_status_version_idx
    ON public.current_projection_replay_status (replay_version, projection);
