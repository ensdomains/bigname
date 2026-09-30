/* project:families.hydrate.reverse.select */
-- The preview and the real reducer use identical owned rows. Replace only changed keys with
-- their RowSet images; no preview is written to the database.
WITH tuple_changes AS (
    SELECT * FROM jsonb_populate_recordset(NULL::project_reverse_tuple, $4)
), tuples AS (
    SELECT t.* FROM project_reverse_tuple t
    WHERE t.chain_id = $1 AND NOT EXISTS (
        SELECT 1 FROM tuple_changes c
        WHERE (c.address, c.coin_type, c.namespace) = (t.address, t.coin_type, t.namespace)
    )
    UNION ALL SELECT * FROM tuple_changes
), registry_changes AS (
    SELECT * FROM jsonb_populate_recordset(NULL::project_registry_pointer, $5)
), registry AS (
    SELECT p.* FROM project_registry_pointer p WHERE p.chain_id = $1 AND NOT EXISTS (
        SELECT 1 FROM registry_changes c
        WHERE (c.chain_id, c.namespace, c.node) = (p.chain_id, p.namespace, p.node)
    )
    UNION ALL SELECT * FROM registry_changes
), resource_changes AS (
    SELECT * FROM jsonb_populate_recordset(NULL::project_resource_pointer, $6)
), resources AS (
    SELECT p.* FROM project_resource_pointer p WHERE p.chain_id = $1 AND NOT EXISTS (
        SELECT 1 FROM resource_changes c
        WHERE (c.chain_id, c.resource_id) = (p.chain_id, p.resource_id)
    )
    UNION ALL SELECT * FROM resource_changes
), node_changes AS (
    SELECT * FROM jsonb_populate_recordset(NULL::project_reverse_node_claim, $7)
), nodes AS (
    SELECT p.* FROM project_reverse_node_claim p WHERE p.chain_id = $1 AND NOT EXISTS (
        SELECT 1 FROM node_changes c
        WHERE (c.namespace, c.reverse_node, c.resolver_address) =
              (p.namespace, p.reverse_node, p.resolver_address)
    )
    UNION ALL SELECT * FROM node_changes
), normalization_changes AS (
    SELECT * FROM jsonb_populate_recordset(NULL::project_claim_normalization, $8)
), normalization AS (
    SELECT p.* FROM project_claim_normalization p WHERE p.chain_id = $1 AND NOT EXISTS (
        SELECT 1 FROM normalization_changes c
        WHERE (c.chain_id, c.claim_event_identity) = (p.chain_id, p.claim_event_identity)
    )
    UNION ALL SELECT * FROM normalization_changes
), selected AS (
    SELECT t.*, pointer.resolver_address AS selected_resolver,
           COALESCE(pointer.resolver_address = ANY($9), false)
               AND t.reverse_node IS NOT NULL AND t.reverse_position IS NOT NULL AS eligible,
           COALESCE((t.reverse_position->>'block_number')::bigint = $2
               OR (t.claim_position->>'block_number')::bigint = $2
               OR pointer.block_number = $2 OR node.block_number = $2, false) AS delta,
           jsonb_build_object(
               'reverse_node', t.reverse_node,
               'resolver_address', pointer.resolver_address,
               'claim_status', COALESCE(claim.status, 'not_found'),
               'raw_claim_name', CASE WHEN claim.status IN ('success', 'invalid_name')
                   THEN claim.raw_name END,
               'claim_name_is_normalized', COALESCE(claim.status = 'success'
                   AND claim.normalized_name = (claim.raw_name #>> '{}'), false),
               'unsupported_reason', claim.reason
           ) AS selected_baseline
    FROM tuples t
    LEFT JOIN LATERAL (
        SELECT p.* FROM (
            SELECT p.block_number, p.transaction_index, p.log_index, p.event_identity,
                   NULLIF(p.resolver_address, '') AS resolver_address
            FROM registry p WHERE p.namespace = t.namespace AND p.node = t.reverse_node
            UNION ALL
            SELECT (p.pointer_position->>'block_number')::bigint,
                   (p.pointer_position->>'transaction_index')::bigint,
                   (p.pointer_position->>'log_index')::bigint,
                   p.pointer_position->>'event_identity', p.resolver_address
            FROM resources p WHERE p.namespace = t.namespace AND p.namehash = t.reverse_node
              AND p.pointer_position IS NOT NULL
        ) p
        ORDER BY p.block_number DESC, p.transaction_index DESC NULLS LAST,
                 p.log_index DESC NULLS LAST,
                 {pointer_emission_ordinal} DESC, p.event_identity COLLATE "C" DESC
        LIMIT 1
    ) pointer ON true
    LEFT JOIN nodes node ON t.source_event = 'ReverseClaimed'
        AND node.namespace = t.namespace AND node.reverse_node = t.reverse_node
        AND node.resolver_address = pointer.resolver_address
    LEFT JOIN normalization claim ON claim.claim_event_identity = CASE
        WHEN t.source_event = 'ReverseClaimed' THEN node.event_identity
        ELSE t.claim_event_identity END
    WHERE t.namespace = 'ens' AND t.coin_type = '60'
), active AS (
    SELECT * FROM selected WHERE eligible
        AND NOT COALESCE(attempt_block = $2 AND attempt_hash = $3, false)
), priority AS (
    SELECT attempt_ordinal FROM active WHERE NOT delta
    ORDER BY attempt_ordinal NULLS FIRST,
        CASE WHEN hydrated_name IS NOT NULL THEN attempt_block END NULLS FIRST, address LIMIT 1
), rolling AS (
    SELECT a.* FROM active a JOIN priority p
        ON a.attempt_ordinal IS NOT DISTINCT FROM p.attempt_ordinal
    WHERE NOT a.delta ORDER BY
        CASE WHEN a.hydrated_name IS NOT NULL THEN a.attempt_block END NULLS FIRST, a.address LIMIT 250
), stale AS (
    SELECT * FROM selected WHERE NOT eligible AND attempt_block IS NOT NULL
), stale_rolling AS (
    SELECT * FROM stale WHERE NOT delta ORDER BY attempt_block, address LIMIT 250
), work AS (
    SELECT * FROM active WHERE delta
    UNION ALL SELECT * FROM rolling
    UNION ALL SELECT * FROM stale WHERE delta
    UNION ALL SELECT * FROM stale_rolling
)
SELECT to_jsonb(work) FROM work ORDER BY address, coin_type, namespace
