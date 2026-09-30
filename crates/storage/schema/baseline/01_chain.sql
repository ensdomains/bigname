DO $$
BEGIN
    CREATE TYPE canonicality_state AS ENUM (
        'observed',
        'canonical',
        'safe',
        'finalized',
        'orphaned'
    );
EXCEPTION
    WHEN duplicate_object THEN NULL;
END
$$;

CREATE TABLE IF NOT EXISTS chain_lineage (
    chain_id text NOT NULL,
    block_hash text NOT NULL,
    parent_hash text,
    block_number bigint NOT NULL,
    block_timestamp timestamptz NOT NULL,
    canonicality_state canonicality_state NOT NULL DEFAULT 'observed',
    first_observed_at timestamptz NOT NULL DEFAULT now(),
    canonicality_updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (chain_id, block_hash),
    UNIQUE (chain_id, block_hash, block_number),
    CHECK (btrim(chain_id) <> ''),
    CHECK (btrim(block_hash) <> ''),
    CHECK (parent_hash IS NULL OR btrim(parent_hash) <> ''),
    CHECK (block_number >= 0),
    CHECK (canonicality_updated_at >= first_observed_at)
);

CREATE INDEX IF NOT EXISTS chain_lineage_number_idx
    ON chain_lineage (chain_id, block_number, block_hash);

CREATE UNIQUE INDEX IF NOT EXISTS chain_lineage_readable_height_idx
    ON chain_lineage (chain_id, block_number)
    WHERE canonicality_state IN ('canonical', 'safe', 'finalized');

CREATE INDEX IF NOT EXISTS chain_lineage_state_idx
    ON chain_lineage (chain_id, canonicality_state, block_number DESC);

CREATE TABLE IF NOT EXISTS chain_header_audit (
    chain_id text NOT NULL,
    block_hash text NOT NULL,
    logs_bloom bytea,
    transactions_root text,
    receipts_root text,
    state_root text,
    observed_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (chain_id, block_hash),
    FOREIGN KEY (chain_id, block_hash)
        REFERENCES chain_lineage (chain_id, block_hash),
    CHECK (
        logs_bloom IS NOT NULL
        OR transactions_root IS NOT NULL
        OR receipts_root IS NOT NULL
        OR state_root IS NOT NULL
    )
);

CREATE TABLE IF NOT EXISTS chain_heads (
    chain_id text PRIMARY KEY,
    latest_block_hash text NOT NULL,
    latest_block_number bigint NOT NULL,
    safe_block_hash text,
    safe_block_number bigint,
    finalized_block_hash text,
    finalized_block_number bigint,
    lineage_orphaning_epoch bigint NOT NULL DEFAULT 0,
    updated_at timestamptz NOT NULL DEFAULT now(),
    FOREIGN KEY (chain_id, latest_block_hash, latest_block_number)
        REFERENCES chain_lineage (chain_id, block_hash, block_number),
    FOREIGN KEY (chain_id, safe_block_hash, safe_block_number)
        REFERENCES chain_lineage (chain_id, block_hash, block_number),
    FOREIGN KEY (chain_id, finalized_block_hash, finalized_block_number)
        REFERENCES chain_lineage (chain_id, block_hash, block_number),
    CHECK (btrim(chain_id) <> ''),
    CHECK (latest_block_number >= 0),
    CHECK ((safe_block_hash IS NULL) = (safe_block_number IS NULL)),
    CHECK ((finalized_block_hash IS NULL) = (finalized_block_number IS NULL)),
    CONSTRAINT chain_heads_lineage_orphaning_epoch_check
        CHECK (lineage_orphaning_epoch >= 0),
    CHECK (safe_block_number IS NULL OR safe_block_number <= latest_block_number),
    CHECK (
        finalized_block_number IS NULL
        OR (
            safe_block_number IS NOT NULL
            AND finalized_block_number <= safe_block_number
        )
    )
);

CREATE OR REPLACE FUNCTION protect_chain_lineage_identity()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF NEW.chain_id IS DISTINCT FROM OLD.chain_id
        OR NEW.block_hash IS DISTINCT FROM OLD.block_hash
        OR NEW.parent_hash IS DISTINCT FROM OLD.parent_hash
        OR NEW.block_number IS DISTINCT FROM OLD.block_number
        OR NEW.block_timestamp IS DISTINCT FROM OLD.block_timestamp
    THEN
        RAISE EXCEPTION
            'chain lineage block identity is immutable'
            USING ERRCODE = '23514';
    END IF;

    RETURN NEW;
END
$$;

CREATE OR REPLACE FUNCTION enforce_chain_lineage_canonicality_transition()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF NEW.canonicality_state IS NOT DISTINCT FROM OLD.canonicality_state THEN
        RETURN NEW;
    END IF;

    IF NOT (
        (
            OLD.canonicality_state = 'observed'
            AND NEW.canonicality_state IN ('canonical', 'orphaned')
        )
        OR (
            OLD.canonicality_state = 'canonical'
            AND NEW.canonicality_state IN ('safe', 'orphaned')
        )
        OR (
            OLD.canonicality_state = 'safe'
            AND NEW.canonicality_state IN ('finalized', 'orphaned')
        )
        OR (
            OLD.canonicality_state = 'orphaned'
            AND NEW.canonicality_state = 'canonical'
        )
    ) THEN
        RAISE EXCEPTION
            'illegal chain lineage canonicality transition: % -> %',
            OLD.canonicality_state,
            NEW.canonicality_state
            USING ERRCODE = '23514';
    END IF;

    NEW.canonicality_updated_at := now();
    RETURN NEW;
END
$$;

CREATE OR REPLACE FUNCTION enforce_chain_head_state()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    PERFORM 1
    FROM chain_lineage
    WHERE chain_id = NEW.chain_id
      AND block_hash = NEW.latest_block_hash
      AND block_number = NEW.latest_block_number
      AND canonicality_state IN ('canonical', 'safe', 'finalized')
    FOR SHARE;

    IF NOT FOUND THEN
        RAISE EXCEPTION
            'latest head must reference a canonical chain block'
            USING ERRCODE = '23503';
    END IF;

    IF NEW.safe_block_hash IS NOT NULL THEN
        PERFORM 1
        FROM chain_lineage
        WHERE chain_id = NEW.chain_id
          AND block_hash = NEW.safe_block_hash
          AND block_number = NEW.safe_block_number
          AND canonicality_state IN ('safe', 'finalized')
        FOR SHARE;

        IF NOT FOUND THEN
            RAISE EXCEPTION
                'safe head must reference a safe or finalized chain block'
                USING ERRCODE = '23503';
        END IF;
    END IF;

    IF NEW.finalized_block_hash IS NOT NULL THEN
        PERFORM 1
        FROM chain_lineage
        WHERE chain_id = NEW.chain_id
          AND block_hash = NEW.finalized_block_hash
          AND block_number = NEW.finalized_block_number
          AND canonicality_state = 'finalized'
        FOR SHARE;

        IF NOT FOUND THEN
            RAISE EXCEPTION
                'finalized head must reference a finalized chain block'
                USING ERRCODE = '23503';
        END IF;
    END IF;

    RETURN NEW;
END
$$;

CREATE OR REPLACE FUNCTION protect_chain_head_state()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM chain_heads
        WHERE chain_id = OLD.chain_id
          AND (
              (
                  latest_block_hash = OLD.block_hash
                  AND latest_block_number = OLD.block_number
                  AND NEW.canonicality_state
                      NOT IN ('canonical', 'safe', 'finalized')
              )
              OR (
                  safe_block_hash = OLD.block_hash
                  AND safe_block_number = OLD.block_number
                  AND NEW.canonicality_state NOT IN ('safe', 'finalized')
              )
              OR (
                  finalized_block_hash = OLD.block_hash
                  AND finalized_block_number = OLD.block_number
                  AND NEW.canonicality_state <> 'finalized'
              )
          )
    ) THEN
        RAISE EXCEPTION
            'a chain head still references this block state'
            USING ERRCODE = '23503';
    END IF;

    RETURN NEW;
END
$$;

DROP TRIGGER IF EXISTS chain_lineage_protect_identity ON chain_lineage;
CREATE TRIGGER chain_lineage_protect_identity
BEFORE UPDATE OF
    chain_id,
    block_hash,
    parent_hash,
    block_number,
    block_timestamp
ON chain_lineage
FOR EACH ROW
EXECUTE FUNCTION protect_chain_lineage_identity();

DROP TRIGGER IF EXISTS chain_lineage_enforce_canonicality_transition
    ON chain_lineage;
CREATE TRIGGER chain_lineage_enforce_canonicality_transition
BEFORE UPDATE OF canonicality_state ON chain_lineage
FOR EACH ROW
EXECUTE FUNCTION enforce_chain_lineage_canonicality_transition();

DROP TRIGGER IF EXISTS chain_heads_enforce_state ON chain_heads;
CREATE TRIGGER chain_heads_enforce_state
BEFORE INSERT OR UPDATE ON chain_heads
FOR EACH ROW
EXECUTE FUNCTION enforce_chain_head_state();

DROP TRIGGER IF EXISTS chain_lineage_protect_head_state ON chain_lineage;
CREATE TRIGGER chain_lineage_protect_head_state
BEFORE UPDATE OF canonicality_state ON chain_lineage
FOR EACH ROW
EXECUTE FUNCTION protect_chain_head_state();

COMMENT ON TABLE chain_lineage IS
    'This table stores each observed block and its chain state.';
COMMENT ON COLUMN chain_lineage.chain_id IS
    'This value identifies the chain.';
COMMENT ON COLUMN chain_lineage.block_hash IS
    'This value identifies the block.';
COMMENT ON COLUMN chain_lineage.parent_hash IS
    'This value identifies the parent block.';
COMMENT ON COLUMN chain_lineage.block_number IS
    'This value is the block height.';
COMMENT ON COLUMN chain_lineage.block_timestamp IS
    'This value is the block time.';
COMMENT ON COLUMN chain_lineage.canonicality_state IS
    'This value states how the chain treats the block.';
COMMENT ON COLUMN chain_lineage.first_observed_at IS
    'This time records the first stored observation.';
COMMENT ON COLUMN chain_lineage.canonicality_updated_at IS
    'This time records the last chain-state change.';

COMMENT ON TABLE chain_header_audit IS
    'This table stores optional header fields for block inspection.';
COMMENT ON COLUMN chain_header_audit.chain_id IS
    'This value identifies the chain.';
COMMENT ON COLUMN chain_header_audit.block_hash IS
    'This value identifies the block.';
COMMENT ON COLUMN chain_header_audit.logs_bloom IS
    'This value is the header log bloom.';
COMMENT ON COLUMN chain_header_audit.transactions_root IS
    'This value is the header transaction root.';
COMMENT ON COLUMN chain_header_audit.receipts_root IS
    'This value is the header receipt root.';
COMMENT ON COLUMN chain_header_audit.state_root IS
    'This value is the header state root.';
COMMENT ON COLUMN chain_header_audit.observed_at IS
    'This time records the stored observation.';

COMMENT ON TABLE chain_heads IS
    'This table stores the current head markers for each chain.';
COMMENT ON COLUMN chain_heads.chain_id IS
    'This value identifies the chain.';
COMMENT ON COLUMN chain_heads.latest_block_hash IS
    'This value identifies the latest block.';
COMMENT ON COLUMN chain_heads.latest_block_number IS
    'This value is the latest block height.';
COMMENT ON COLUMN chain_heads.safe_block_hash IS
    'This value identifies the safe block.';
COMMENT ON COLUMN chain_heads.safe_block_number IS
    'This value is the safe block height.';
COMMENT ON COLUMN chain_heads.finalized_block_hash IS
    'This value identifies the finalized block.';
COMMENT ON COLUMN chain_heads.finalized_block_number IS
    'This value is the finalized block height.';
COMMENT ON COLUMN chain_heads.lineage_orphaning_epoch IS
    'This value increases whenever head publication orphans readable lineage for this chain.';
COMMENT ON COLUMN chain_heads.updated_at IS
    'This time records the last marker change.';
