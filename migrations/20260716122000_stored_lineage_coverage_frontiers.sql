-- Durable operational proof that watched source intervals have complete
-- generation-independent fetch coverage for stored-lineage promotion.
--
-- This migration intentionally seeds no rows. A missing row is a cold state:
-- the indexer must verify the current authoritative candidate before it may
-- publish a first snapshot or promote a checkpoint from stored lineage.
CREATE TABLE stored_lineage_coverage_frontiers (
    chain_id TEXT PRIMARY KEY,
    snapshot_revision BIGINT NOT NULL,
    proof_format_version TEXT NOT NULL,
    discovery_admission_epoch BIGINT NOT NULL,
    verified_from_block BIGINT NOT NULL,
    verified_through_block BIGINT NOT NULL,
    topic0s_by_family JSONB NOT NULL,
    requirement_row_count BIGINT NOT NULL,
    requirement_digest TEXT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CHECK (snapshot_revision > 0),
    CHECK (discovery_admission_epoch >= 0),
    CHECK (verified_from_block >= 0),
    CHECK (verified_through_block >= verified_from_block),
    CHECK (verified_through_block < 9223372036854775807),
    CHECK (jsonb_typeof(topic0s_by_family) = 'object'),
    CHECK (requirement_row_count >= 0),
    CHECK (requirement_digest ~ '^[0-9a-f]{32}$')
);

CREATE TABLE stored_lineage_coverage_frontier_requirements (
    chain_id TEXT NOT NULL REFERENCES stored_lineage_coverage_frontiers(chain_id)
        ON DELETE CASCADE,
    source_family TEXT NOT NULL,
    address TEXT NOT NULL,
    required_intervals INT8MULTIRANGE NOT NULL,
    PRIMARY KEY (chain_id, source_family, address),
    CHECK (source_family <> ''),
    CHECK (address = lower(address)),
    CHECK (required_intervals <> '{}'::INT8MULTIRANGE),
    CONSTRAINT stored_lineage_coverage_requirements_finite
        CHECK (
            lower(required_intervals) IS NOT NULL
            AND upper(required_intervals) IS NOT NULL
        )
);
