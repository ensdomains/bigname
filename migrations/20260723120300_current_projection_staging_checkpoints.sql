-- Durable, resumable staging state for automatic all-current projection replay.
CREATE TABLE public.current_projection_full_replay_input_revision (
    singleton BOOLEAN PRIMARY KEY DEFAULT true,
    revision BIGINT DEFAULT 0 NOT NULL,
    updated_at TIMESTAMPTZ DEFAULT now() NOT NULL,
    CONSTRAINT current_projection_full_replay_input_revision_singleton_check CHECK (singleton),
    CONSTRAINT current_projection_full_replay_input_revision_revision_check CHECK (revision >= 0)
);

INSERT INTO public.current_projection_full_replay_input_revision (singleton)
VALUES (true);

ALTER TABLE public.current_projection_replay_status
    ADD COLUMN full_replay_input_revision BIGINT DEFAULT 0 NOT NULL,
    ADD CONSTRAINT current_projection_replay_status_input_revision_check CHECK (
        full_replay_input_revision >= 0
    );

CREATE TABLE public.current_projection_replay_attempt (
    singleton BOOLEAN PRIMARY KEY DEFAULT true,
    replay_version INTEGER NOT NULL,
    normalized_target_block BIGINT,
    full_replay_input_revision BIGINT NOT NULL,
    apply_baseline_change_id BIGINT NOT NULL,
    started_at TIMESTAMPTZ DEFAULT now() NOT NULL,
    updated_at TIMESTAMPTZ DEFAULT now() NOT NULL,
    CONSTRAINT current_projection_replay_attempt_singleton_check CHECK (singleton),
    CONSTRAINT current_projection_replay_attempt_replay_version_check CHECK (
        replay_version > 0
    ),
    CONSTRAINT current_projection_replay_attempt_input_revision_check CHECK (
        full_replay_input_revision >= 0
    ),
    CONSTRAINT current_projection_replay_attempt_apply_baseline_check CHECK (
        apply_baseline_change_id >= 0
    )
);

CREATE TABLE public.current_projection_staging_checkpoints (
    projection TEXT PRIMARY KEY,
    replay_version INTEGER NOT NULL,
    staging_schema_version INTEGER NOT NULL,
    completed_normalized_target_block BIGINT,
    full_replay_input_revision BIGINT NOT NULL,
    validated_normalized_change_id BIGINT NOT NULL,
    stage_tables TEXT[] NOT NULL,
    last_source_key JSONB,
    completed_source_count BIGINT DEFAULT 0 NOT NULL,
    staged_row_count BIGINT DEFAULT 0 NOT NULL,
    staged_aux_row_count BIGINT DEFAULT 0 NOT NULL,
    status TEXT DEFAULT 'running' NOT NULL,
    started_at TIMESTAMPTZ DEFAULT now() NOT NULL,
    updated_at TIMESTAMPTZ DEFAULT now() NOT NULL,
    staging_completed_at TIMESTAMPTZ,
    CONSTRAINT current_projection_staging_checkpoints_version_check CHECK (
        replay_version > 0 AND staging_schema_version > 0
    ),
    CONSTRAINT current_projection_staging_checkpoints_input_check CHECK (
        full_replay_input_revision >= 0
        AND validated_normalized_change_id >= 0
    ),
    CONSTRAINT current_projection_staging_checkpoints_tables_check CHECK (
        cardinality(stage_tables) > 0
        AND array_position(stage_tables, '') IS NULL
    ),
    CONSTRAINT current_projection_staging_checkpoints_counts_check CHECK (
        completed_source_count >= 0
        AND staged_row_count >= 0
        AND staged_aux_row_count >= 0
    ),
    CONSTRAINT current_projection_staging_checkpoints_status_check CHECK (
        status IN ('running', 'staging_complete')
    ),
    CONSTRAINT current_projection_staging_checkpoints_completed_check CHECK (
        (status = 'staging_complete') = (staging_completed_at IS NOT NULL)
    )
);

CREATE INDEX current_projection_staging_checkpoints_version_idx
    ON public.current_projection_staging_checkpoints (
        replay_version,
        staging_schema_version,
        projection
    );
