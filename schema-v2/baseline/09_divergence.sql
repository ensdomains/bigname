CREATE TABLE IF NOT EXISTS resolution_divergences (
    logical_name_id text NOT NULL
        REFERENCES name_surfaces (logical_name_id),
    resolver_chain_id text NOT NULL,
    resolver_address text NOT NULL,
    request_kind text NOT NULL,
    observed_positions jsonb NOT NULL,
    indexed_result jsonb NOT NULL,
    live_result jsonb NOT NULL,
    first_observed_at timestamptz NOT NULL DEFAULT now(),
    last_observed_at timestamptz NOT NULL DEFAULT now(),
    cleared_at timestamptz,
    PRIMARY KEY (
        logical_name_id,
        resolver_chain_id,
        resolver_address,
        request_kind,
        observed_positions
    ),
    CHECK (btrim(resolver_chain_id) <> ''),
    CHECK (btrim(resolver_address) <> ''),
    CHECK (btrim(request_kind) <> ''),
    CHECK (jsonb_typeof(observed_positions) = 'object'),
    CHECK (observed_positions <> '{}'::jsonb),
    CHECK (indexed_result <> live_result),
    CHECK (last_observed_at >= first_observed_at),
    CONSTRAINT resolution_divergences_clearing_time_check
        CHECK (cleared_at IS NULL OR cleared_at >= last_observed_at)
);

CREATE INDEX IF NOT EXISTS resolution_divergences_active_name_idx
    ON resolution_divergences (logical_name_id, last_observed_at DESC)
    WHERE cleared_at IS NULL;

CREATE INDEX IF NOT EXISTS resolution_divergences_active_resolver_idx
    ON resolution_divergences (
        resolver_chain_id,
        lower(resolver_address),
        last_observed_at DESC
    )
    WHERE cleared_at IS NULL;

CREATE UNIQUE INDEX IF NOT EXISTS resolution_divergences_one_active_request_idx
    ON resolution_divergences (
        logical_name_id,
        resolver_chain_id,
        lower(resolver_address),
        request_kind
    )
    WHERE cleared_at IS NULL;

-- Authorized by simplification-build-plan-20260730.md § B6, lines 100-104.
-- The compared projection row is locked until the caller's transaction ends.
CREATE OR REPLACE FUNCTION write_resolution_divergence(
    compared_resource_id uuid,
    compared_boundary_key text,
    compared_row_xmin text,
    requested_logical_name_id text,
    requested_resolver_chain_id text,
    requested_resolver_address text,
    requested_record_key text,
    compared_positions jsonb,
    indexed_answer jsonb,
    live_answer jsonb,
    used_ccip_read boolean
)
RETURNS text
LANGUAGE plpgsql
AS $$
DECLARE
    position_slot text;
    position_value jsonb;
BEGIN
    IF used_ccip_read THEN
        RETURN 'ccip_skipped';
    END IF;

    PERFORM 1
    FROM record_inventory_current
    WHERE resource_id = compared_resource_id
      AND record_version_boundary_key = compared_boundary_key
      AND xmin::text = compared_row_xmin
    FOR SHARE;

    IF NOT FOUND THEN
        RETURN 'guard_rejected';
    END IF;

    IF jsonb_typeof(compared_positions) IS DISTINCT FROM 'object'
        OR compared_positions = '{}'::jsonb
    THEN
        RETURN 'guard_rejected';
    END IF;

    FOR position_slot, position_value IN
        SELECT key, value
        FROM jsonb_each(compared_positions)
        ORDER BY key
    LOOP
        BEGIN
            PERFORM 1
            FROM chain_lineage
            WHERE chain_id = position_value ->> 'chain_id'
              AND block_hash = position_value ->> 'block_hash'
              AND block_number =
                  (position_value ->> 'block_number')::bigint
              AND block_timestamp =
                  (position_value ->> 'timestamp')::timestamptz
              AND canonicality_state IN (
                  'canonical',
                  'safe',
                  'finalized'
              )
            FOR SHARE;
        EXCEPTION
            WHEN data_exception THEN
                RETURN 'guard_rejected';
        END;

        IF NOT FOUND THEN
            RETURN 'guard_rejected';
        END IF;
    END LOOP;

    IF indexed_answer = live_answer THEN
        UPDATE resolution_divergences
        SET cleared_at = GREATEST(statement_timestamp(), last_observed_at)
        WHERE logical_name_id = requested_logical_name_id
          AND resolver_chain_id = requested_resolver_chain_id
          AND lower(resolver_address) = lower(requested_resolver_address)
          AND request_kind = requested_record_key
          AND cleared_at IS NULL;
        IF FOUND THEN
            RETURN 'cleared';
        END IF;
        RETURN 'agreement';
    END IF;

    UPDATE resolution_divergences
    SET cleared_at = GREATEST(statement_timestamp(), last_observed_at)
    WHERE logical_name_id = requested_logical_name_id
      AND resolver_chain_id = requested_resolver_chain_id
      AND lower(resolver_address) = lower(requested_resolver_address)
      AND request_kind = requested_record_key
      AND observed_positions <> compared_positions
      AND cleared_at IS NULL;

    INSERT INTO resolution_divergences (
        logical_name_id,
        resolver_chain_id,
        resolver_address,
        request_kind,
        observed_positions,
        indexed_result,
        live_result
    ) VALUES (
        requested_logical_name_id,
        requested_resolver_chain_id,
        lower(requested_resolver_address),
        requested_record_key,
        compared_positions,
        indexed_answer,
        live_answer
    )
    ON CONFLICT (
        logical_name_id,
        resolver_chain_id,
        resolver_address,
        request_kind,
        observed_positions
    ) DO UPDATE
    SET indexed_result = EXCLUDED.indexed_result,
        live_result = EXCLUDED.live_result,
        last_observed_at = GREATEST(
            resolution_divergences.last_observed_at,
            statement_timestamp()
        ),
        cleared_at = NULL;

    RETURN 'written';
END
$$;

CREATE OR REPLACE FUNCTION validate_resolution_divergence_positions()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    position_slot text;
    position_value jsonb;
    position_chain_id text;
    position_block_hash text;
    position_block_number bigint;
    position_timestamp timestamptz;
BEGIN
    IF jsonb_typeof(NEW.observed_positions) <> 'object'
        OR NEW.observed_positions = '{}'::jsonb
    THEN
        RAISE EXCEPTION
            'observed positions must be a nonempty ChainPositions object'
            USING ERRCODE = '23514';
    END IF;

    FOR position_slot, position_value IN
        SELECT key, value
        FROM jsonb_each(NEW.observed_positions)
    LOOP
        IF btrim(position_slot) = ''
            OR jsonb_typeof(position_value)
                IS DISTINCT FROM 'object'
            OR jsonb_typeof(position_value -> 'chain_id')
                IS DISTINCT FROM 'string'
            OR jsonb_typeof(position_value -> 'block_hash')
                IS DISTINCT FROM 'string'
            OR jsonb_typeof(position_value -> 'block_number')
                IS DISTINCT FROM 'number'
            OR jsonb_typeof(position_value -> 'timestamp')
                IS DISTINCT FROM 'string'
            OR btrim(position_value ->> 'chain_id') = ''
            OR btrim(position_value ->> 'block_hash') = ''
            OR btrim(position_value ->> 'timestamp') = ''
            OR (position_value ->> 'block_number') !~ '^[0-9]+$'
        THEN
            RAISE EXCEPTION
                'observed position % is not a valid ChainPosition',
                position_slot
                USING ERRCODE = '23514';
        END IF;

        BEGIN
            position_chain_id := position_value ->> 'chain_id';
            position_block_hash := position_value ->> 'block_hash';
            position_block_number :=
                (position_value ->> 'block_number')::bigint;
            position_timestamp :=
                (position_value ->> 'timestamp')::timestamptz;
        EXCEPTION
            WHEN data_exception THEN
                RAISE EXCEPTION
                    'observed position % is not a valid ChainPosition',
                    position_slot
                    USING ERRCODE = '23514';
        END;

        IF NEW.cleared_at IS NULL THEN
            PERFORM 1
            FROM chain_lineage
            WHERE chain_id = position_chain_id
              AND block_hash = position_block_hash
              AND block_number = position_block_number
              AND block_timestamp = position_timestamp
              AND canonicality_state IN (
                  'canonical',
                  'safe',
                  'finalized'
              )
            FOR SHARE;

            IF NOT FOUND THEN
                RAISE EXCEPTION
                    'active resolution difference position % is not canonical',
                    position_slot
                    USING ERRCODE = '23503';
            END IF;
        END IF;
    END LOOP;

    RETURN NEW;
END
$$;

DROP TRIGGER IF EXISTS resolution_divergences_validate_positions
    ON resolution_divergences;
CREATE TRIGGER resolution_divergences_validate_positions
BEFORE INSERT OR UPDATE ON resolution_divergences
FOR EACH ROW
EXECUTE FUNCTION validate_resolution_divergence_positions();

CREATE OR REPLACE FUNCTION clear_resolution_divergences_for_block()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF TG_OP = 'DELETE'
        OR (
            OLD.canonicality_state IN ('canonical', 'safe', 'finalized')
            AND NEW.canonicality_state
                NOT IN ('canonical', 'safe', 'finalized')
        )
    THEN
        UPDATE resolution_divergences AS divergence
        SET cleared_at =
            GREATEST(statement_timestamp(), divergence.last_observed_at)
        WHERE divergence.cleared_at IS NULL
          AND EXISTS (
              SELECT 1
              FROM jsonb_each(divergence.observed_positions)
                  AS position(slot, value)
              WHERE position.value ->> 'chain_id' = OLD.chain_id
                AND position.value ->> 'block_hash' = OLD.block_hash
                AND (position.value ->> 'block_number')::bigint =
                    OLD.block_number
          );
    END IF;

    RETURN NULL;
END
$$;

DROP TRIGGER IF EXISTS chain_lineage_clear_resolution_divergences
    ON chain_lineage;
CREATE TRIGGER chain_lineage_clear_resolution_divergences
AFTER UPDATE OF canonicality_state OR DELETE ON chain_lineage
FOR EACH ROW
EXECUTE FUNCTION clear_resolution_divergences_for_block();

COMMENT ON TABLE resolution_divergences IS
    'This table stores disagreements between indexed and live answers.';
COMMENT ON COLUMN resolution_divergences.logical_name_id IS
    'This value identifies the requested name.';
COMMENT ON COLUMN resolution_divergences.resolver_chain_id IS
    'This value identifies the resolver chain.';
COMMENT ON COLUMN resolution_divergences.resolver_address IS
    'This value is the resolver address.';
COMMENT ON COLUMN resolution_divergences.request_kind IS
    'This value identifies the requested record.';
COMMENT ON COLUMN resolution_divergences.observed_positions IS
    'This ChainPositions object identifies the compared canonical blocks.';
COMMENT ON COLUMN resolution_divergences.indexed_result IS
    'This value stores the indexed answer.';
COMMENT ON COLUMN resolution_divergences.live_result IS
    'This value stores the live answer.';
COMMENT ON COLUMN resolution_divergences.first_observed_at IS
    'This time records the first disagreement.';
COMMENT ON COLUMN resolution_divergences.last_observed_at IS
    'This time records the latest disagreement.';
COMMENT ON COLUMN resolution_divergences.cleared_at IS
    'This time records agreement restoration.';
COMMENT ON FUNCTION write_resolution_divergence(
    uuid, text, text, text, text, text, text, jsonb, jsonb, jsonb, boolean
) IS
    'Mutates direct-resolution disagreements only while the compared inventory row and observed canonical lineage are unchanged.';
