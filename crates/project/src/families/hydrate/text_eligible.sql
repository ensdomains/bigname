/* project:families.hydrate.text.eligible */
-- Eligible for a read: an event-less text value of an admitted text resolver, with a
-- non-blank key (Rust's `str::trim` white space, listed below) and a namehash, written
-- after the partition's record version in the canonical event order (families/position.rs).
SELECT COALESCE(value.status = 'unsupported'
    AND value.record_key = 'text:' || COALESCE(value.selector_key, '')
    AND btrim(COALESCE(value.selector_key, ''), U&'\0009\000A\000B\000C\000D\0020\0085\00A0\1680\2000\2001\2002\2003\2004\2005\2006\2007\2008\2009\200A\2028\2029\202F\205F\3000') <> ''
    AND value.resolver_address = ANY($6::text[])
    AND admission.support_status = 'supported'
    AND CASE WHEN value.arm = 'named' THEN surface.namehash ELSE value.node END IS NOT NULL
    AND (boundary.block_number IS NULL OR (
        value.block_number, COALESCE(value.transaction_index, -1),
        COALESCE(value.log_index, -1),
        {value_emission_ordinal},
        value.event_identity COLLATE "C"
    ) > (
        boundary.block_number, COALESCE(boundary.transaction_index, -1),
        COALESCE(boundary.log_index, -1),
        {boundary_emission_ordinal},
        boundary.event_identity COLLATE "C"
    )), false) AS active
FROM (SELECT partition.version_position AS v) version
    -- The record version as a position, when it is one (Position::from_map). A part reads as
    -- serde_json's `as_i64` reads it: an integer JSON number in the i64 range, else absent, so
    -- a fractional or out-of-range block number leaves no boundary.
LEFT JOIN LATERAL (
    SELECT parts.block_number, parts.transaction_index, parts.log_index,
        version.v ->> 'event_identity' AS event_identity
    FROM LATERAL (
        SELECT
            CASE WHEN jsonb_typeof(version.v -> 'block_number') = 'number'
                    AND (version.v ->> 'block_number') ~ '^-?[0-9]{1,19}$'
                    AND (version.v ->> 'block_number')::numeric
                        BETWEEN -9223372036854775808 AND 9223372036854775807
                THEN (version.v ->> 'block_number')::bigint END AS block_number,
            CASE WHEN jsonb_typeof(version.v -> 'transaction_index') = 'number'
                    AND (version.v ->> 'transaction_index') ~ '^-?[0-9]{1,19}$'
                    AND (version.v ->> 'transaction_index')::numeric
                        BETWEEN -9223372036854775808 AND 9223372036854775807
                THEN (version.v ->> 'transaction_index')::bigint END AS transaction_index,
            CASE WHEN jsonb_typeof(version.v -> 'log_index') = 'number'
                    AND (version.v ->> 'log_index') ~ '^-?[0-9]{1,19}$'
                    AND (version.v ->> 'log_index')::numeric
                        BETWEEN -9223372036854775808 AND 9223372036854775807
                THEN (version.v ->> 'log_index')::bigint END AS log_index
    ) parts
    WHERE parts.block_number IS NOT NULL
      AND jsonb_typeof(version.v -> 'event_identity') = 'string'
) boundary ON true
