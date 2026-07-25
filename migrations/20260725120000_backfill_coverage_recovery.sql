-- Operational evidence for coverage recovery that can reuse proven retained
-- raw logs before fetching true gaps.
--
-- Provider query counts stay on the immutable job row so operators can see
-- the minimum aggregate-plus-initial-window estimate before paid work and the
-- durable actual after each returned query, including retries, pagination,
-- and filter-pack splits.
-- The fenced snapshot binds locally derived coverage to one raw-log revision.
-- Coverage readers reject that evidence after retention rotation or
-- a later mutation in the covered interval.
ALTER TABLE public.backfill_jobs
    ADD COLUMN projected_minimum_provider_query_count bigint NOT NULL DEFAULT 0,
    ADD COLUMN actual_provider_query_count bigint NOT NULL DEFAULT 0,
    ADD COLUMN stored_verification_raw_log_input_revision bigint,
    ADD COLUMN stored_verification_from_block bigint,
    ADD COLUMN stored_verification_to_block bigint,
    ADD COLUMN stored_verification_log_count bigint,
    ADD COLUMN stored_verification_digest text,
    ADD CONSTRAINT backfill_jobs_provider_query_counts_check CHECK (
        projected_minimum_provider_query_count >= 0
        AND actual_provider_query_count >= 0
    ),
    ADD CONSTRAINT backfill_jobs_stored_verification_shape_check CHECK (
        (
            stored_verification_raw_log_input_revision IS NULL
            AND stored_verification_from_block IS NULL
            AND stored_verification_to_block IS NULL
            AND stored_verification_log_count IS NULL
            AND stored_verification_digest IS NULL
        )
        OR (
            stored_verification_raw_log_input_revision IS NOT NULL
            AND stored_verification_from_block IS NOT NULL
            AND stored_verification_to_block IS NOT NULL
            AND stored_verification_log_count IS NOT NULL
            AND stored_verification_digest IS NOT NULL
        )
    ),
    ADD CONSTRAINT backfill_jobs_stored_verification_values_check CHECK (
        COALESCE(stored_verification_raw_log_input_revision, 0) >= 0
        AND COALESCE(stored_verification_log_count, 0) >= 0
        AND (
            stored_verification_from_block IS NULL
            OR (
                stored_verification_from_block >= range_start_block_number
                AND stored_verification_to_block <= range_end_block_number
                AND stored_verification_from_block <= stored_verification_to_block
            )
        )
        AND (
            stored_verification_digest IS NULL
            OR stored_verification_digest ~ '^[0-9a-f]{32}$'
        )
    );

COMMENT ON COLUMN public.backfill_jobs.projected_minimum_provider_query_count IS
    'Lower-bound pre-fetch projection: required aggregate verification queries plus one row query per configured initial block window containing a true gap; retries, pagination, and filter-pack splits appear only in actual_provider_query_count.';

-- A durable frontier may reuse its requirement snapshot only while the raw
-- input generation is unchanged and no later revision touched its verified
-- interval. Existing rows become revision zero and are revalidated before
-- their next reuse when the chain has observed raw input.
ALTER TABLE public.stored_lineage_coverage_frontiers
    ADD COLUMN raw_log_input_revision bigint NOT NULL DEFAULT 0,
    ADD COLUMN raw_log_retention_generation bigint NOT NULL DEFAULT 0,
    ADD CONSTRAINT stored_lineage_coverage_frontiers_raw_input_check CHECK (
        raw_log_input_revision >= 0
        AND raw_log_retention_generation >= 0
    );
