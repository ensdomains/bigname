/* project:families.hydrate.text.select */
-- Preview only the owned rows changed in this block; all other keys read their stored image.
-- The query decides which selectors need work and cuts the block's share, so a block transfers
-- at most $7 rows however many selectors are already current.
WITH value_changes AS (
    SELECT * FROM jsonb_populate_recordset(NULL::project_node_record_value, $3)
), record_values AS (
    SELECT value.* FROM project_node_record_value value WHERE value.chain_id = $1
      AND NOT EXISTS (SELECT 1 FROM value_changes change
          WHERE (change.chain_id, change.resolver_address, change.arm, change.arm_identity,
                 change.record_key) = (value.chain_id, value.resolver_address, value.arm,
                                      value.arm_identity, value.record_key))
    UNION ALL SELECT * FROM value_changes
), partition_changes AS (
    SELECT * FROM jsonb_populate_recordset(NULL::project_node_record_partition, $4)
), partitions AS (
    SELECT partition.* FROM project_node_record_partition partition WHERE partition.chain_id = $1
      AND NOT EXISTS (SELECT 1 FROM partition_changes change
          WHERE (change.chain_id, change.resolver_address, change.arm, change.arm_identity) =
                (partition.chain_id, partition.resolver_address, partition.arm, partition.arm_identity))
    UNION ALL SELECT * FROM partition_changes
), classification_changes AS (
    SELECT * FROM jsonb_populate_recordset(NULL::project_resolver_classification, $5)
), classifications AS (
    SELECT classification.* FROM project_resolver_classification classification
    WHERE classification.chain_id = $1 AND NOT EXISTS (
        SELECT 1 FROM classification_changes change
        WHERE (change.chain_id, change.resolver_address) =
              (classification.chain_id, classification.resolver_address))
    UNION ALL SELECT * FROM classification_changes
), admissions AS MATERIALIZED (
    -- One verdict per resolver, built once rather than for every selector it serves.
    SELECT chain_id, resolver_address, support_status,
        jsonb_build_object('classification', classification, 'support_status', support_status,
            'unsupported_reason', unsupported_reason, 'manifest_id', manifest_id) AS admission
    FROM classifications
), selected AS (
    SELECT value.*,
        -- What an overlay is read for, part by part; `_selector` below assembles it.
        jsonb_build_object('block_number', value.block_number,
            'transaction_index', value.transaction_index, 'log_index', value.log_index,
            'event_identity', value.event_identity) AS _source_position,
        COALESCE(partition.version_position, 'null') AS _version_position,
        COALESCE(admission.admission, jsonb_build_object('classification', NULL,
            'support_status', NULL, 'unsupported_reason', NULL, 'manifest_id', NULL)) AS _admission,
        COALESCE(to_jsonb(CASE WHEN value.arm = 'named' THEN surface.namehash ELSE value.node END),
            'null') AS _namehash,
        -- Eligible for a read: an event-less text value of an admitted text resolver, with a
        -- non-blank key (Rust's `str::trim` white space, listed below) and a namehash, written
        -- after the partition's record version in the canonical event order (families/position.rs).
        COALESCE(value.status = 'unsupported'
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
            )), false) AS _active,
        EXISTS (SELECT 1 FROM chain_lineage lineage
            WHERE lineage.chain_id = value.chain_id
              AND lineage.block_number = value.hydrated_at_block
              AND lineage.block_number <= $2
              AND lineage.block_hash = value.hydrated_value ->> 'block_hash'
              AND lineage.canonicality_state IN ('canonical', 'safe', 'finalized')) AS _readable,
        -- The block changed this selector's value, record version or admission: hydrate it first.
        -- Stored rows are still the prior block's, in preparation and publication alike.
        EXISTS (SELECT 1 FROM value_changes change
            WHERE (change.resolver_address, change.arm, change.arm_identity, change.record_key) =
                  (value.resolver_address, value.arm, value.arm_identity, value.record_key))
        OR EXISTS (SELECT 1 FROM partition_changes change
            LEFT JOIN project_node_record_partition stored
              ON (stored.chain_id, stored.resolver_address, stored.arm, stored.arm_identity) =
                 (change.chain_id, change.resolver_address, change.arm, change.arm_identity)
            WHERE (change.resolver_address, change.arm, change.arm_identity) =
                  (value.resolver_address, value.arm, value.arm_identity)
              AND change.version_position IS DISTINCT FROM stored.version_position)
        OR EXISTS (SELECT 1 FROM classification_changes change
            LEFT JOIN project_resolver_classification stored
              ON (stored.chain_id, stored.resolver_address) =
                 (change.chain_id, change.resolver_address)
            WHERE change.resolver_address = value.resolver_address
              AND (change.classification, change.support_status, change.unsupported_reason,
                   change.manifest_id) IS DISTINCT FROM (stored.classification,
                   stored.support_status, stored.unsupported_reason, stored.manifest_id))
            AS _delta
    FROM record_values value
    LEFT JOIN partitions partition USING (chain_id, resolver_address, arm, arm_identity)
    LEFT JOIN admissions admission USING (chain_id, resolver_address)
    LEFT JOIN name_surfaces surface ON surface.logical_name_id = value.logical_name_id
    -- The record version as a position, when it is one (Position::from_map). A part reads as
    -- serde_json's `as_i64` reads it: an integer JSON number in the i64 range, else absent, so
    -- a fractional or out-of-range block number leaves no boundary.
    LEFT JOIN LATERAL (
        SELECT parts.block_number, parts.transaction_index, parts.log_index,
            version.v ->> 'event_identity' AS event_identity
        FROM (SELECT partition.version_position AS v) version
        CROSS JOIN LATERAL (
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
    WHERE value.namespace = 'ens' AND value.record_family = 'text'
      AND (value.status = 'unsupported' OR value.hydrated_value IS NOT NULL)
      AND (value.resolver_address = ANY($6::text[]) OR value.hydrated_value IS NOT NULL)
), work AS (
    -- Work is an eligible selector whose overlay is not current (a read is current while it is
    -- readable and its overlay records every part of the selector), or an ineligible one that
    -- still carries an overlay to clear.
    SELECT * FROM selected
    WHERE CASE WHEN _active THEN NOT (_readable
            AND COALESCE(hydrated_value -> 'namehash', 'null') = _namehash
            AND COALESCE(hydrated_value -> 'version_position', 'null') = _version_position
            AND COALESCE(hydrated_value -> 'source_position', 'null') = _source_position
            AND COALESCE(hydrated_value -> 'admission', 'null') = _admission)
        ELSE COALESCE(hydrated_value <> 'null', false) END
)
-- The block's share: its own changes first, then never-read selectors, then the oldest attempts,
-- each in key order.
SELECT to_jsonb(work.*) || jsonb_build_object('_selector', jsonb_build_object(
    'source_position', _source_position, 'version_position', _version_position,
    'admission', _admission, 'namehash', _namehash))
FROM work
ORDER BY NOT _delta, hydrated_at_block NULLS FIRST,
    resolver_address, arm, arm_identity, record_key
LIMIT $7
