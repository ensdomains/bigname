-- Lineage is checked where a row is selected: twice per interpreter state key in `winners`, and
-- for each restored event's timestamp. `winners` matches a key across the whole chain, so a key
-- with no readable event on the chain returns nothing. The name and resource arms of
-- `candidates` skip the check, so they may list a key from an event on an orphaned block. That
-- changes no output: a state key embeds its logical name and resource, and its state scope the
-- node the event is filed under (adapters state_key.rs), so every event of one key is filed
-- under the same name or resource. An orphaned candidate therefore only lists a key whose
-- readable winner, if any, is itself a candidate. If that ever breaks, probe lineage on one
-- candidate per key before `winners`. The ENSv2 key arm keeps its check, once per event: a
-- registry may emit LabelRegistered for one token under a second label, and the adapter keys
-- the token's state by (registry, token) with the token in the state scope (v2_registry.rs,
-- state_v2.rs, protocol.rs), while v2_keys.sql also files the event under its labelhash, so one
-- key can be reached through several ENSv2 state keys. Each probe stays parameterized by its
-- event: a normal join can hash all 25M+ lineage rows.
-- LIMIT prevents lateral pull-up. It cannot discard a matching lineage row because
-- (chain_id, block_hash) is the chain_lineage primary key; exact height is also checked.
WITH candidates AS MATERIALIZED (
    SELECT event.normalized_event_id,
           event.raw_fact_ref ? '{state_key}' AS has_key,
           COALESCE(event.raw_fact_ref ->> '{state_key}', event.event_identity) AS state_key,
           event.after_state ? '{clear_marker}' AS clear_marker
    FROM unnest($3::text[]) requested(name)
    JOIN LATERAL (
        -- One arm per probe index: the ENSv1, Basenames Base and ENSv2 families each have a
        -- partial index, and a family test the planner cannot prove against one predicate
        -- would scan instead.
        (SELECT event.* FROM normalized_events event
        WHERE COALESCE(event.namespace || ':' || lower(COALESCE(event.after_state ->> 'child_node', event.after_state ->> 'namehash', event.after_state ->> 'node', event.after_state #>> '{grant_source,node}', event.after_state #>> '{revocation_source,node}')), event.logical_name_id) = requested.name
          AND event.chain_id = $1 AND event.block_number < $2
          AND event.source_family LIKE 'ens\_v1\_%'
          AND event.canonicality_state IN ('canonical','safe','finalized')
        -- Keep the node index probe parameterized; OFFSET 0 prevents pull-up without
        -- truncating history when stale expression statistics overestimate fanout.
        OFFSET 0)
        UNION ALL
        (SELECT event.* FROM normalized_events event
        WHERE COALESCE(event.namespace || ':' || lower(COALESCE(event.after_state ->> 'child_node', event.after_state ->> 'namehash', event.after_state ->> 'node', event.after_state #>> '{grant_source,node}', event.after_state #>> '{revocation_source,node}')), event.logical_name_id) = requested.name
          AND event.chain_id = $1 AND event.block_number < $2
          AND event.source_family LIKE 'basenames\_base\_%'
          AND event.canonicality_state IN ('canonical','safe','finalized')
        OFFSET 0)
        UNION ALL
        (SELECT event.* FROM normalized_events event
        WHERE COALESCE(event.namespace || ':' || lower(COALESCE(event.after_state ->> 'child_node', event.after_state ->> 'namehash', event.after_state ->> 'node', event.after_state #>> '{grant_source,node}', event.after_state #>> '{revocation_source,node}')), event.logical_name_id) = requested.name
          AND event.chain_id = $1 AND event.block_number < $2
          AND event.source_family LIKE 'ens\_v2\_%'
          AND event.canonicality_state IN ('canonical','safe','finalized')
        OFFSET 0)
    ) event ON TRUE
    UNION ALL
    SELECT event.normalized_event_id,
           event.raw_fact_ref ? '{state_key}',
           COALESCE(event.raw_fact_ref ->> '{state_key}', event.event_identity),
           event.after_state ? '{clear_marker}'
    FROM unnest($4::uuid[]) requested(resource)
    JOIN LATERAL (
        SELECT event.* FROM normalized_events event
        WHERE event.resource_id = requested.resource
          -- Named facts are already covered by the direct node branch above.
          AND COALESCE(event.namespace || ':' || lower(COALESCE(event.after_state ->> 'child_node', event.after_state ->> 'namehash', event.after_state ->> 'node', event.after_state #>> '{grant_source,node}', event.after_state #>> '{revocation_source,node}')), event.logical_name_id) IS NULL
          AND event.chain_id = $1 AND event.block_number < $2
          AND (event.source_family LIKE 'ens\_v1\_%' OR event.source_family LIKE 'basenames\_base\_%')
          AND event.canonicality_state IN ('canonical','safe','finalized')
        OFFSET 0
    ) event ON TRUE
    UNION ALL
    -- ENSv2 events filed under a requested ENSv2 state key, one inverted-index probe per key.
    -- The array must stay identical to normalized_events_v2_key_probe_idx and to
    -- `v2_event_keys` in the adapter crate. One overlap test for all keys is costed by the key
    -- count alone, because the planner does not use the statistics of a partial expression
    -- index, so above a few hundred keys it reads the chain's ENSv2 history by block instead.
    -- The probe holds only the array test and the index's own predicate. OFFSET 0 keeps the
    -- chain and block tests outside it: inside, the planner may pair every probe with a bitmap
    -- over a (chain_id, block_number) index, which reads that history once per key. An event
    -- under several requested keys is listed once before its lineage probe.
    SELECT hit.normalized_event_id, hit.has_key, hit.state_key, hit.clear_marker
    FROM (
        SELECT DISTINCT event.normalized_event_id, event.chain_id, event.block_number, event.block_hash,
               event.raw_fact_ref ? '{state_key}' AS has_key,
               COALESCE(event.raw_fact_ref ->> '{state_key}', event.event_identity) AS state_key,
               event.after_state ? '{clear_marker}' AS clear_marker
        FROM unnest($5::text[]) requested(key)
        JOIN LATERAL (
            SELECT event.* FROM normalized_events event
            WHERE {v2_keys} @> ARRAY[requested.key]
              AND event.source_family LIKE 'ens\_v2\_%'
              AND event.canonicality_state IN ('canonical','safe','finalized')
            OFFSET 0
        ) event ON event.chain_id = $1 AND event.block_number < $2
    ) hit
    JOIN LATERAL (
        SELECT 1 FROM chain_lineage lineage
        WHERE lineage.chain_id = hit.chain_id
          AND lineage.block_number = hit.block_number AND lineage.block_hash = hit.block_hash
          AND lineage.canonicality_state IN ('canonical','safe','finalized')
        LIMIT 1
    ) readable ON TRUE
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
          AND (event.raw_fact_ref ? '{state_key}') = request.has_key
          AND public.digest(COALESCE(event.raw_fact_ref ->> '{state_key}', event.event_identity), 'sha256') = public.digest(request.state_key,'sha256')
          AND COALESCE(event.raw_fact_ref ->> '{state_key}',event.event_identity) = request.state_key
          AND (event.after_state ? '{clear_marker}') = request.clear_marker
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
          AND (event.raw_fact_ref ? '{state_key}') = request.has_key
          AND public.digest(COALESCE(event.raw_fact_ref ->> '{state_key}', event.event_identity), 'sha256') = public.digest(request.state_key,'sha256')
          AND COALESCE(event.raw_fact_ref ->> '{state_key}',event.event_identity) = request.state_key
          AND (event.after_state ? '{clear_marker}') = request.clear_marker
        ORDER BY event.transaction_index DESC NULLS LAST,
          event.log_index DESC NULLS LAST, event.normalized_event_id DESC LIMIT 1
    ) chosen ON TRUE
)
-- No LIMIT anywhere below: a truncated result would restore partial state.
SELECT value.body, lineage.block_timestamp
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
        '{state_key}', event.raw_fact_ref ->> '{state_key}',
        'event_identity', event.event_identity, '{state_scope}', event.raw_fact_ref ->> '{state_scope}',
        'block_number', event.block_number, 'normalized_event_id', event.normalized_event_id,
        '{transaction_index}', event.transaction_index,
        '{log_index}', event.log_index,
        'after_state', event.after_state
    ) AS body
) value
ORDER BY winners.block_number,winners.normalized_event_id
