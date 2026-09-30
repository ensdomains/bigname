CREATE TABLE IF NOT EXISTS interpret_decode_skips (
    chain_id text NOT NULL,
    block_hash text NOT NULL,
    block_number bigint NOT NULL,
    transaction_hash text NOT NULL,
    log_index bigint NOT NULL,
    emitting_address text NOT NULL,
    source_family text NOT NULL,
    selection_topic0 text NOT NULL,
    match_all boolean NOT NULL,
    decode_context text NOT NULL,
    interpreter_content_hash text NOT NULL,
    detected_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (
        chain_id, block_hash, transaction_hash, log_index, interpreter_content_hash
    ),
    CHECK (btrim(chain_id) <> ''),
    CHECK (btrim(block_hash) <> ''),
    CHECK (block_number >= 0),
    CHECK (btrim(transaction_hash) <> ''),
    CHECK (log_index >= 0),
    CHECK (btrim(emitting_address) <> ''),
    CHECK (btrim(source_family) <> ''),
    CHECK (btrim(selection_topic0) <> ''),
    CHECK (btrim(decode_context) <> ''),
    CHECK (btrim(interpreter_content_hash) <> '')
);

COMMENT ON TABLE interpret_decode_skips IS
    'This append-only table records malformed event logs that interpretation skipped; its rows are operator diagnostics and never product data.';
COMMENT ON COLUMN interpret_decode_skips.chain_id IS
    'This value identifies the chain containing the skipped log.';
COMMENT ON COLUMN interpret_decode_skips.block_hash IS
    'This value identifies the block containing the skipped log, including after a later reorganization.';
COMMENT ON COLUMN interpret_decode_skips.block_number IS
    'This value is the block number of the block containing the skipped log.';
COMMENT ON COLUMN interpret_decode_skips.transaction_hash IS
    'This value identifies the transaction containing the skipped log.';
COMMENT ON COLUMN interpret_decode_skips.log_index IS
    'This value is the log position within the block.';
COMMENT ON COLUMN interpret_decode_skips.emitting_address IS
    'This value is the address that emitted the malformed log.';
COMMENT ON COLUMN interpret_decode_skips.source_family IS
    'This value identifies the selected source family whose event decoder rejected the log.';
COMMENT ON COLUMN interpret_decode_skips.selection_topic0 IS
    'This value is the event signature topic selected from the active manifest catalog.';
COMMENT ON COLUMN interpret_decode_skips.match_all IS
    'This value is true when event selection accepted the signature from every emitting address.';
COMMENT ON COLUMN interpret_decode_skips.decode_context IS
    'This value describes the event shape that the selected ABI decoder could not decode.';
COMMENT ON COLUMN interpret_decode_skips.interpreter_content_hash IS
    'This value identifies the interpreter build that made the skip decision.';
COMMENT ON COLUMN interpret_decode_skips.detected_at IS
    'This time records when Interpret first appended the diagnostic row.';
