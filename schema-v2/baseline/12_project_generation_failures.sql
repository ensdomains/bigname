CREATE TABLE IF NOT EXISTS project_generation_failures (
    chain_id text NOT NULL,
    target_block_number bigint NOT NULL,
    target_block_hash text NOT NULL,
    interpreter_content_hash text NOT NULL,
    failure_kind text NOT NULL,
    failure_fingerprint text NOT NULL,
    logical_name_id text NOT NULL,
    evidence jsonb NOT NULL,
    detected_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (
        chain_id, target_block_number, target_block_hash,
        interpreter_content_hash, failure_kind, failure_fingerprint
    ),
    CHECK (btrim(chain_id) <> ''),
    CHECK (target_block_number >= 0),
    CHECK (btrim(target_block_hash) <> ''),
    CHECK (btrim(interpreter_content_hash) <> ''),
    CHECK (failure_kind IN (
        'dual_current_exact_name_authority',
        'dual_current_child_authority'
    )),
    CHECK (failure_fingerprint ~ '^[0-9a-f]{64}$'),
    CHECK (btrim(logical_name_id) <> ''),
    CHECK (jsonb_typeof(evidence) = 'object')
);

CREATE INDEX IF NOT EXISTS project_generation_failures_name_idx
    ON project_generation_failures (chain_id, logical_name_id, target_block_number);

COMMENT ON TABLE project_generation_failures IS
    'This append-only table records each projection-blocking invariant failure that aborted a projection generation; its rows are operator diagnostics and never product projections.';
COMMENT ON COLUMN project_generation_failures.chain_id IS
    'This value identifies the chain whose projection generation failed.';
COMMENT ON COLUMN project_generation_failures.target_block_number IS
    'This value is the block number of the target the aborted projection generation was publishing.';
COMMENT ON COLUMN project_generation_failures.target_block_hash IS
    'This value is the block hash of that target, resolvable through lineage as canonical or orphaned after a later reorganization.';
COMMENT ON COLUMN project_generation_failures.interpreter_content_hash IS
    'This value identifies the interpreter build whose derived input produced the failure.';
COMMENT ON COLUMN project_generation_failures.failure_kind IS
    'This value names the projection-blocking invariant that aborted the projection generation.';
COMMENT ON COLUMN project_generation_failures.failure_fingerprint IS
    'This value deterministically fingerprints the semantic conflict so a retried projection generation records no duplicate.';
COMMENT ON COLUMN project_generation_failures.logical_name_id IS
    'This value identifies the logical name whose conflicting authority blocked publication.';
COMMENT ON COLUMN project_generation_failures.evidence IS
    'This payload carries the conflicting binding and resource identities, the activated boundary event identity, each block, transaction, and log position, and the canonicality observed at failure.';
COMMENT ON COLUMN project_generation_failures.detected_at IS
    'This time records when the phase runner appended the row, after the projection generation transaction rolled back.';
