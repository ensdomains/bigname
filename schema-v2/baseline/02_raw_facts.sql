CREATE TABLE IF NOT EXISTS raw_transactions (
    chain_id text NOT NULL,
    block_hash text NOT NULL,
    block_number bigint NOT NULL,
    transaction_hash text NOT NULL,
    transaction_index bigint NOT NULL,
    from_address text NOT NULL,
    to_address text,
    input bytea NOT NULL DEFAULT '\x',
    value numeric(78, 0),
    observed_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (chain_id, block_hash, transaction_hash),
    UNIQUE (chain_id, block_hash, transaction_index),
    CONSTRAINT raw_transactions_transaction_position_key
        UNIQUE (
            chain_id,
            block_hash,
            transaction_hash,
            transaction_index
        ),
    FOREIGN KEY (chain_id, block_hash, block_number)
        REFERENCES chain_lineage (chain_id, block_hash, block_number),
    CHECK (block_number >= 0),
    CHECK (transaction_index >= 0),
    CHECK (btrim(transaction_hash) <> ''),
    CHECK (btrim(from_address) <> ''),
    CHECK (to_address IS NULL OR btrim(to_address) <> ''),
    CHECK (value IS NULL OR value >= 0)
);

CREATE INDEX IF NOT EXISTS raw_transactions_hash_idx
    ON raw_transactions (chain_id, transaction_hash);

CREATE INDEX IF NOT EXISTS raw_transactions_to_address_idx
    ON raw_transactions (chain_id, lower(to_address), block_number DESC)
    WHERE to_address IS NOT NULL;

CREATE TABLE IF NOT EXISTS raw_receipts (
    chain_id text NOT NULL,
    block_hash text NOT NULL,
    block_number bigint NOT NULL,
    transaction_hash text NOT NULL,
    transaction_index bigint NOT NULL,
    contract_address text,
    status boolean,
    gas_used numeric(78, 0),
    cumulative_gas_used numeric(78, 0),
    logs_bloom bytea,
    observed_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (chain_id, block_hash, transaction_hash),
    UNIQUE (chain_id, block_hash, transaction_index),
    FOREIGN KEY (chain_id, block_hash, block_number)
        REFERENCES chain_lineage (chain_id, block_hash, block_number),
    CONSTRAINT raw_receipts_transaction_position_fkey
        FOREIGN KEY (
            chain_id,
            block_hash,
            transaction_hash,
            transaction_index
        )
        REFERENCES raw_transactions (
            chain_id,
            block_hash,
            transaction_hash,
            transaction_index
        ),
    CHECK (block_number >= 0),
    CHECK (transaction_index >= 0),
    CHECK (contract_address IS NULL OR btrim(contract_address) <> ''),
    CHECK (gas_used IS NULL OR gas_used >= 0),
    CHECK (cumulative_gas_used IS NULL OR cumulative_gas_used >= 0)
);

CREATE INDEX IF NOT EXISTS raw_receipts_hash_idx
    ON raw_receipts (chain_id, transaction_hash);

CREATE TABLE IF NOT EXISTS raw_logs (
    chain_id text NOT NULL,
    block_hash text NOT NULL,
    block_number bigint NOT NULL,
    transaction_hash text NOT NULL,
    transaction_index bigint NOT NULL,
    log_index bigint NOT NULL,
    emitting_address text NOT NULL,
    topics text[] NOT NULL DEFAULT ARRAY[]::text[],
    data bytea NOT NULL DEFAULT '\x',
    observed_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (chain_id, block_hash, log_index),
    FOREIGN KEY (chain_id, block_hash, block_number)
        REFERENCES chain_lineage (chain_id, block_hash, block_number),
    CONSTRAINT raw_logs_transaction_position_fkey
        FOREIGN KEY (
            chain_id,
            block_hash,
            transaction_hash,
            transaction_index
        )
        REFERENCES raw_transactions (
            chain_id,
            block_hash,
            transaction_hash,
            transaction_index
        ),
    CHECK (block_number >= 0),
    CHECK (transaction_index >= 0),
    CHECK (log_index >= 0),
    CHECK (btrim(emitting_address) <> '')
);

CREATE INDEX IF NOT EXISTS raw_logs_replay_idx
    ON raw_logs (
        chain_id,
        block_number,
        transaction_index,
        log_index,
        block_hash
    );

CREATE INDEX IF NOT EXISTS raw_logs_emitter_idx
    ON raw_logs (
        chain_id,
        lower(emitting_address),
        block_number,
        transaction_index,
        log_index
    );

CREATE INDEX IF NOT EXISTS raw_logs_topic_idx
    ON raw_logs (
        chain_id,
        lower(topics[1]),
        block_number,
        transaction_index,
        log_index
    )
    WHERE cardinality(topics) > 0;

CREATE INDEX IF NOT EXISTS raw_logs_transaction_idx
    ON raw_logs (chain_id, transaction_hash, log_index);

COMMENT ON TABLE raw_transactions IS
    'This table stores immutable transactions for selected blocks.';
COMMENT ON COLUMN raw_transactions.chain_id IS
    'This value identifies the chain.';
COMMENT ON COLUMN raw_transactions.block_hash IS
    'This value identifies the block.';
COMMENT ON COLUMN raw_transactions.block_number IS
    'This value is the block height.';
COMMENT ON COLUMN raw_transactions.transaction_hash IS
    'This value identifies the transaction.';
COMMENT ON COLUMN raw_transactions.transaction_index IS
    'This value orders the transaction in the block.';
COMMENT ON COLUMN raw_transactions.from_address IS
    'This value is the transaction sender.';
COMMENT ON COLUMN raw_transactions.to_address IS
    'This value is the transaction target.';
COMMENT ON COLUMN raw_transactions.input IS
    'This value is the transaction input.';
COMMENT ON COLUMN raw_transactions.value IS
    'This value is the transferred wei amount.';
COMMENT ON COLUMN raw_transactions.observed_at IS
    'This time records the stored observation.';

COMMENT ON TABLE raw_receipts IS
    'This table stores immutable receipts for selected transactions.';
COMMENT ON COLUMN raw_receipts.chain_id IS
    'This value identifies the chain.';
COMMENT ON COLUMN raw_receipts.block_hash IS
    'This value identifies the block.';
COMMENT ON COLUMN raw_receipts.block_number IS
    'This value is the block height.';
COMMENT ON COLUMN raw_receipts.transaction_hash IS
    'This value identifies the transaction.';
COMMENT ON COLUMN raw_receipts.transaction_index IS
    'This value orders the transaction in the block.';
COMMENT ON COLUMN raw_receipts.contract_address IS
    'This value is the created contract address.';
COMMENT ON COLUMN raw_receipts.status IS
    'This value states whether the transaction succeeded.';
COMMENT ON COLUMN raw_receipts.gas_used IS
    'This value is the gas used by the transaction.';
COMMENT ON COLUMN raw_receipts.cumulative_gas_used IS
    'This value is the gas used through this receipt.';
COMMENT ON COLUMN raw_receipts.logs_bloom IS
    'This value is the receipt log bloom.';
COMMENT ON COLUMN raw_receipts.observed_at IS
    'This time records the stored observation.';

COMMENT ON TABLE raw_logs IS
    'This table stores immutable logs for selected blocks.';
COMMENT ON COLUMN raw_logs.chain_id IS
    'This value identifies the chain.';
COMMENT ON COLUMN raw_logs.block_hash IS
    'This value identifies the block.';
COMMENT ON COLUMN raw_logs.block_number IS
    'This value is the block height.';
COMMENT ON COLUMN raw_logs.transaction_hash IS
    'This value identifies the transaction.';
COMMENT ON COLUMN raw_logs.transaction_index IS
    'This value orders the transaction in the block.';
COMMENT ON COLUMN raw_logs.log_index IS
    'This value orders the log in the block.';
COMMENT ON COLUMN raw_logs.emitting_address IS
    'This value is the contract that emitted the log.';
COMMENT ON COLUMN raw_logs.topics IS
    'This array stores the log topics.';
COMMENT ON COLUMN raw_logs.data IS
    'This value stores the log data.';
COMMENT ON COLUMN raw_logs.observed_at IS
    'This time records the stored observation.';
