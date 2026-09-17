-- Keep lineage validation parameterized by each selected event. A normal join can
-- hash all 25M+ lineage rows when per-name/resource history fanout is overestimated.
-- LIMIT prevents lateral pull-up. It cannot discard a matching lineage row because
-- (chain_id, block_hash) is the chain_lineage primary key; exact height is also checked.
WITH candidates AS MATERIALIZED (
    SELECT event.normalized_event_id,
           event.raw_fact_ref ? 'interpreter_state_key' AS has_key,
           COALESCE(event.raw_fact_ref ->> 'interpreter_state_key', event.event_identity) AS state_key,
           event.after_state ? 'subregistry_invalidated_token_ids' AS clear_marker
    FROM unnest($3::text[]) requested(name)
    JOIN normalized_events event ON COALESCE(event.namespace || ':' || lower(COALESCE(event.after_state ->> 'child_node', event.after_state ->> 'namehash', event.after_state ->> 'node', event.after_state #>> '{grant_source,node}', event.after_state #>> '{revocation_source,node}')), event.logical_name_id) = requested.name
    JOIN LATERAL (
        SELECT 1
        FROM chain_lineage lineage
        WHERE lineage.chain_id = event.chain_id
          AND lineage.block_number = event.block_number AND lineage.block_hash = event.block_hash
          AND lineage.canonicality_state IN ('canonical','safe','finalized')
        LIMIT 1
    ) readable ON TRUE
    WHERE event.chain_id = $1 AND event.block_number < $2
      AND event.source_family LIKE 'ens\_v1\_%'
      AND event.canonicality_state IN ('canonical','safe','finalized')
    UNION
    SELECT event.normalized_event_id,
           event.raw_fact_ref ? 'interpreter_state_key',
           COALESCE(event.raw_fact_ref ->> 'interpreter_state_key', event.event_identity),
           event.after_state ? 'subregistry_invalidated_token_ids'
    FROM unnest($4::uuid[]) requested(resource)
    JOIN normalized_events event ON event.resource_id = requested.resource
      AND (COALESCE(event.namespace || ':' || lower(COALESCE(event.after_state ->> 'child_node', event.after_state ->> 'namehash', event.after_state ->> 'node', event.after_state #>> '{grant_source,node}', event.after_state #>> '{revocation_source,node}')), event.logical_name_id) IS NULL
           OR COALESCE(event.namespace || ':' || lower(COALESCE(event.after_state ->> 'child_node', event.after_state ->> 'namehash', event.after_state ->> 'node', event.after_state #>> '{grant_source,node}', event.after_state #>> '{revocation_source,node}')), event.logical_name_id) = ANY($3::text[]))
    JOIN LATERAL (
        SELECT 1
        FROM chain_lineage lineage
        WHERE lineage.chain_id = event.chain_id
          AND lineage.block_number = event.block_number AND lineage.block_hash = event.block_hash
          AND lineage.canonicality_state IN ('canonical','safe','finalized')
        LIMIT 1
    ) readable ON TRUE
    WHERE event.chain_id = $1 AND event.block_number < $2
      AND event.source_family LIKE 'ens\_v1\_%'
      AND event.canonicality_state IN ('canonical','safe','finalized')
), keys AS MATERIALIZED (
    SELECT DISTINCT has_key, state_key, clear_marker FROM candidates
), winners AS MATERIALIZED (
    SELECT chosen.normalized_event_id, chosen.block_number
    FROM keys request
    JOIN LATERAL (
        SELECT event.block_number
        FROM normalized_events event
        JOIN LATERAL (
            SELECT 1
            FROM chain_lineage lineage
            WHERE lineage.chain_id = event.chain_id
              AND lineage.block_number = event.block_number AND lineage.block_hash = event.block_hash
              AND lineage.canonicality_state IN ('canonical','safe','finalized')
            LIMIT 1
        ) readable ON TRUE
        WHERE event.chain_id = $1 AND event.block_number < $2
          AND event.canonicality_state IN ('canonical','safe','finalized')
          AND (event.raw_fact_ref ? 'interpreter_state_key') = request.has_key
          AND public.digest(COALESCE(event.raw_fact_ref ->> 'interpreter_state_key', event.event_identity), 'sha256') = public.digest(request.state_key,'sha256')
          AND COALESCE(event.raw_fact_ref ->> 'interpreter_state_key',event.event_identity) = request.state_key
          AND (event.after_state ? 'subregistry_invalidated_token_ids') = request.clear_marker
        ORDER BY event.block_number DESC LIMIT 1
    ) latest ON TRUE
    JOIN LATERAL (
        SELECT event.normalized_event_id, event.block_number
        FROM normalized_events event
        JOIN LATERAL (
            SELECT 1
            FROM chain_lineage lineage
            WHERE lineage.chain_id = event.chain_id
              AND lineage.block_number = event.block_number AND lineage.block_hash = event.block_hash
              AND lineage.canonicality_state IN ('canonical','safe','finalized')
            LIMIT 1
        ) readable ON TRUE
        WHERE event.chain_id = $1 AND event.block_number = latest.block_number
          AND event.canonicality_state IN ('canonical','safe','finalized')
          AND (event.raw_fact_ref ? 'interpreter_state_key') = request.has_key
          AND public.digest(COALESCE(event.raw_fact_ref ->> 'interpreter_state_key', event.event_identity), 'sha256') = public.digest(request.state_key,'sha256')
          AND COALESCE(event.raw_fact_ref ->> 'interpreter_state_key',event.event_identity) = request.state_key
          AND (event.after_state ? 'subregistry_invalidated_token_ids') = request.clear_marker
        ORDER BY event.transaction_index DESC NULLS LAST,
          event.log_index DESC NULLS LAST, event.normalized_event_id DESC LIMIT 1
    ) chosen ON TRUE
)
SELECT CASE WHEN payload.bytes <= $6 THEN payload.body END AS body, payload.bytes, lineage.block_timestamp
FROM winners
JOIN normalized_events event ON event.normalized_event_id = winners.normalized_event_id
JOIN LATERAL (
    SELECT lineage.block_timestamp
    FROM chain_lineage lineage
    WHERE lineage.chain_id = event.chain_id
      AND lineage.block_number = event.block_number AND lineage.block_hash = event.block_hash
      AND lineage.canonicality_state IN ('canonical','safe','finalized')
    LIMIT 1
) lineage ON TRUE
CROSS JOIN LATERAL (
    SELECT jsonb_build_object(
        'chain_id', event.chain_id, 'namespace', event.namespace,
        'logical_name_id', event.logical_name_id, 'resource_id', event.resource_id,
        'event_kind', event.event_kind, 'source_family', event.source_family,
        'manifest_version', event.manifest_version, 'source_manifest_id', event.source_manifest_id,
        'emitting_address', event.raw_fact_ref ->> 'emitting_address',
        'interpreter_state_key', event.raw_fact_ref ->> 'interpreter_state_key',
        'event_identity', event.event_identity, 'state_scope', event.raw_fact_ref ->> 'state_scope',
        'after_state', event.after_state
    ) AS body
) value
CROSS JOIN LATERAL (
    SELECT value.body, octet_length(value.body::text)::bigint AS bytes
) payload
ORDER BY winners.block_number,winners.normalized_event_id
LIMIT $5
