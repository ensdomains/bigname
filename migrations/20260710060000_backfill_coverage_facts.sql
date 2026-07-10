-- Durable per-tuple backfill coverage facts, recorded at job completion from
-- the job's own in-memory selector plan. Facts are append-only: re-derivation
-- must be idempotent via ON CONFLICT DO NOTHING against the tuple key, and no
-- code path may UPDATE rows. A `family` scope row means every address of the
-- source family is covered by a topics-complete fetch over the block range.
CREATE TABLE public.backfill_coverage_facts (
    backfill_coverage_fact_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    backfill_job_id bigint NOT NULL REFERENCES public.backfill_jobs(backfill_job_id) ON DELETE CASCADE,
    chain_id text NOT NULL,
    source_family text NOT NULL,
    scope text NOT NULL,
    address text,
    covered_from_block bigint NOT NULL,
    covered_to_block bigint NOT NULL,
    derivation text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT backfill_coverage_facts_scope_check CHECK (
        scope = ANY (ARRAY['address'::text, 'family'::text])
    ),
    CONSTRAINT backfill_coverage_facts_address_scope_check CHECK (
        (scope = 'address'::text) = (address IS NOT NULL)
    ),
    CONSTRAINT backfill_coverage_facts_block_range_check CHECK (
        covered_from_block <= covered_to_block
    ),
    CONSTRAINT backfill_coverage_facts_derivation_check CHECK (
        derivation = ANY (ARRAY['job_completion'::text, 'legacy_full_payload_identity'::text])
    ),
    -- covered_to_block participates so two targets sharing a clamped start
    -- block keep their distinct intervals; readers check containment against
    -- any row, so multiple interval rows per tuple are expected.
    CONSTRAINT backfill_coverage_facts_tuple_key UNIQUE NULLS NOT DISTINCT (
        backfill_job_id,
        source_family,
        scope,
        address,
        covered_from_block,
        covered_to_block
    )
);

CREATE INDEX backfill_coverage_facts_tuple_read_idx
    ON public.backfill_coverage_facts (chain_id, source_family, address, covered_from_block, covered_to_block);

CREATE INDEX backfill_coverage_facts_family_scope_idx
    ON public.backfill_coverage_facts (chain_id, source_family, covered_from_block)
    WHERE scope = 'family'::text;
