CREATE TABLE IF NOT EXISTS manifest_authority_attestations (
    chain_id text NOT NULL,
    phase_name text NOT NULL,
    generation_token text NOT NULL,
    authority_fingerprint text NOT NULL,
    redo_from_block_number bigint NOT NULL,
    redo_to_block_number bigint NOT NULL,
    attested_by text NOT NULL,
    attested_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (chain_id, phase_name, generation_token),
    CHECK (btrim(chain_id) <> ''),
    CHECK (phase_name = 'interpret'),
    CHECK (btrim(generation_token) <> ''),
    CHECK (authority_fingerprint ~ '^[0-9a-f]{64}$'),
    CHECK (redo_from_block_number >= 0),
    CHECK (redo_to_block_number >= redo_from_block_number),
    CHECK (btrim(attested_by) <> '')
);

COMMENT ON TABLE manifest_authority_attestations IS
    'This append-only table records each operator-attested manifest-authority discharge.';
COMMENT ON COLUMN manifest_authority_attestations.chain_id IS
    'This value identifies the chain.';
COMMENT ON COLUMN manifest_authority_attestations.phase_name IS
    'This value identifies the attested derived phase.';
COMMENT ON COLUMN manifest_authority_attestations.generation_token IS
    'This value uniquely identifies the manifest-authority invalidation being discharged.';
COMMENT ON COLUMN manifest_authority_attestations.authority_fingerprint IS
    'This value fingerprints the desired manifest authority for the invalidation.';
COMMENT ON COLUMN manifest_authority_attestations.redo_from_block_number IS
    'This value is the first block in the attested redo range.';
COMMENT ON COLUMN manifest_authority_attestations.redo_to_block_number IS
    'This value is the last block in the attested redo range.';
COMMENT ON COLUMN manifest_authority_attestations.attested_by IS
    'This value identifies the phase-runner command context that supplied the attestation.';
COMMENT ON COLUMN manifest_authority_attestations.attested_at IS
    'This time records the transaction that began the attested redo.';
