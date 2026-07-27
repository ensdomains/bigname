-- Completed startup adapter rows survive successful boots and are reusable
-- only by the adapter derivation and applied schema state that produced them.
-- Existing rows intentionally remain NULL so the first upgraded boot fails
-- closed and rebuilds each family before publishing a versioned completion.
ALTER TABLE public.normalized_replay_adapter_checkpoints
    ADD COLUMN adapter_semantic_version bigint,
    ADD COLUMN schema_migration_count bigint,
    ADD COLUMN schema_migration_max_version bigint,
    ADD CONSTRAINT normalized_replay_adapter_checkpoints_adapter_version_check CHECK (
        adapter_semantic_version IS NULL
        OR adapter_semantic_version > 0
    ),
    ADD CONSTRAINT normalized_replay_adapter_checkpoints_schema_version_check CHECK (
        (
            schema_migration_count IS NULL
            AND schema_migration_max_version IS NULL
        )
        OR (
            schema_migration_count > 0
            AND schema_migration_max_version > 0
        )
    );
