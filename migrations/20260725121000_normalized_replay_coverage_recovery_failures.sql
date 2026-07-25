-- Persist retry scheduling and terminal operator evidence independently from
-- individual provider jobs. A new in-range raw-log revision may require a new
-- immutable job, but it must not reset the bounded attempt history for the
-- same generation-bound violation window.
CREATE TABLE public.normalized_replay_coverage_recovery_failures (
    deployment_profile text NOT NULL,
    chain_id text NOT NULL,
    raw_log_retention_generation bigint NOT NULL,
    source_family text NOT NULL,
    emitting_address text NOT NULL,
    required_from_block bigint NOT NULL,
    required_to_block bigint NOT NULL,
    state text NOT NULL,
    attempt_count bigint NOT NULL DEFAULT 0,
    retry_not_before timestamp with time zone,
    last_backfill_job_id bigint REFERENCES public.backfill_jobs(backfill_job_id)
        ON DELETE SET NULL,
    last_job_attempt_count bigint NOT NULL DEFAULT 0,
    failure_reason text NOT NULL,
    failure_metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
    first_failed_at timestamp with time zone NOT NULL DEFAULT now(),
    last_failed_at timestamp with time zone NOT NULL DEFAULT now(),
    updated_at timestamp with time zone NOT NULL DEFAULT now(),
    CONSTRAINT normalized_replay_coverage_recovery_failures_pkey PRIMARY KEY (
        deployment_profile,
        chain_id,
        raw_log_retention_generation,
        source_family,
        emitting_address,
        required_from_block,
        required_to_block
    ),
    CONSTRAINT normalized_replay_coverage_recovery_failures_generation_check CHECK (
        raw_log_retention_generation >= 0
    ),
    CONSTRAINT normalized_replay_coverage_recovery_failures_range_check CHECK (
        required_from_block >= 0
        AND required_to_block >= required_from_block
    ),
    CONSTRAINT normalized_replay_coverage_recovery_failures_state_check CHECK (
        state IN ('retry_backoff', 'terminal')
    ),
    CONSTRAINT normalized_replay_coverage_recovery_failures_attempt_check CHECK (
        attempt_count >= 0
        AND last_job_attempt_count >= 0
    ),
    CONSTRAINT normalized_replay_coverage_recovery_failures_retry_shape_check CHECK (
        (state = 'retry_backoff' AND retry_not_before IS NOT NULL)
        OR (state = 'terminal' AND retry_not_before IS NULL)
    ),
    CONSTRAINT normalized_replay_coverage_recovery_failures_metadata_check CHECK (
        jsonb_typeof(failure_metadata) = 'object'
    )
);

COMMENT ON TABLE public.normalized_replay_coverage_recovery_failures IS
    'Per violation-window and raw-log retention-generation retry budget, backoff deadline, and terminal operator evidence for automatic normalized replay coverage recovery.';

-- One cumulative failure record can move between immutable provider job
-- revisions as topic/provider identity changes. Keep each job's observed
-- attempt watermark so returning to an older revision does not count its
-- already-journaled attempts again. Deleting the window record on success or
-- re-arm removes these watermarks with it.
CREATE TABLE public.normalized_replay_coverage_recovery_job_attempts (
    deployment_profile text NOT NULL,
    chain_id text NOT NULL,
    raw_log_retention_generation bigint NOT NULL,
    source_family text NOT NULL,
    emitting_address text NOT NULL,
    required_from_block bigint NOT NULL,
    required_to_block bigint NOT NULL,
    backfill_job_id bigint NOT NULL REFERENCES public.backfill_jobs(backfill_job_id)
        ON DELETE CASCADE,
    observed_attempt_count bigint NOT NULL,
    updated_at timestamp with time zone NOT NULL DEFAULT now(),
    CONSTRAINT coverage_recovery_job_attempts_pkey PRIMARY KEY (
        deployment_profile,
        chain_id,
        raw_log_retention_generation,
        source_family,
        emitting_address,
        required_from_block,
        required_to_block,
        backfill_job_id
    ),
    CONSTRAINT coverage_recovery_job_attempts_failure_fkey FOREIGN KEY (
        deployment_profile,
        chain_id,
        raw_log_retention_generation,
        source_family,
        emitting_address,
        required_from_block,
        required_to_block
    ) REFERENCES public.normalized_replay_coverage_recovery_failures (
        deployment_profile,
        chain_id,
        raw_log_retention_generation,
        source_family,
        emitting_address,
        required_from_block,
        required_to_block
    ) ON DELETE CASCADE,
    CONSTRAINT coverage_recovery_job_attempts_count_check CHECK (
        observed_attempt_count >= 0
    )
);

COMMENT ON TABLE public.normalized_replay_coverage_recovery_job_attempts IS
    'Per immutable recovery job attempt watermark used to add each provider attempt to its exact-window cumulative budget once.';

-- Keep a tombstone even when the current failure row is cleared. An operator
-- re-arm or successful recovery increments this epoch, so a poll that planned
-- against older state cannot recreate a terminal failure afterward.
CREATE TABLE public.normalized_replay_coverage_recovery_epochs (
    deployment_profile text NOT NULL,
    chain_id text NOT NULL,
    raw_log_retention_generation bigint NOT NULL,
    source_family text NOT NULL,
    emitting_address text NOT NULL,
    required_from_block bigint NOT NULL,
    required_to_block bigint NOT NULL,
    write_epoch bigint NOT NULL DEFAULT 0,
    updated_at timestamp with time zone NOT NULL DEFAULT now(),
    CONSTRAINT normalized_replay_coverage_recovery_epochs_pkey PRIMARY KEY (
        deployment_profile,
        chain_id,
        raw_log_retention_generation,
        source_family,
        emitting_address,
        required_from_block,
        required_to_block
    ),
    CONSTRAINT normalized_replay_coverage_recovery_epochs_generation_check CHECK (
        raw_log_retention_generation >= 0
    ),
    CONSTRAINT normalized_replay_coverage_recovery_epochs_range_check CHECK (
        required_from_block >= 0
        AND required_to_block >= required_from_block
    ),
    CONSTRAINT normalized_replay_coverage_recovery_epochs_value_check CHECK (
        write_epoch >= 0
    )
);

COMMENT ON TABLE public.normalized_replay_coverage_recovery_epochs IS
    'Monotonic compare-and-set fence retained across automatic coverage-recovery success and operator re-arm so stale polls cannot republish failure state.';

-- Bind each reusable recovery job to the same epoch without changing the
-- immutable idempotency key that created the job. Re-arm advances this value
-- together with resetting the exact window's unfinished attempts.
ALTER TABLE public.backfill_jobs
    ADD COLUMN coverage_recovery_write_epoch bigint,
    ADD COLUMN coverage_recovery_bound_attempt_count bigint;

ALTER TABLE public.backfill_jobs
    ADD CONSTRAINT backfill_jobs_coverage_recovery_binding_check CHECK (
        (
            coverage_recovery_write_epoch IS NULL
            AND coverage_recovery_bound_attempt_count IS NULL
        )
        OR (
            coverage_recovery_write_epoch >= 0
            AND coverage_recovery_bound_attempt_count >= 0
        )
    );

COMMENT ON COLUMN public.backfill_jobs.coverage_recovery_write_epoch IS
    'Current per-window automatic coverage-recovery epoch allowed to reserve this job; NULL for ordinary backfill jobs.';

COMMENT ON COLUMN public.backfill_jobs.coverage_recovery_bound_attempt_count IS
    'Maximum child-range attempt count already journaled when this job became the one reservation-eligible revision for its exact recovery window.';
