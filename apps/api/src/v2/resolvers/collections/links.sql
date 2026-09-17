WITH latest_links AS (
    SELECT DISTINCT ON (lower(event.after_state ->> 'node'))
        lower(event.after_state ->> 'node') AS node,
        event.after_state ->> 'resolver_record_id' AS record_id,
        event.normalized_event_id, event.chain_id, event.block_number, event.block_hash,
        event.transaction_hash, event.log_index, lineage.block_timestamp
    FROM bigname_phase.normalized_events event
    LEFT JOIN bigname_phase.chain_lineage lineage
      ON lineage.chain_id = event.chain_id AND lineage.block_hash = event.block_hash
     AND lineage.block_number = event.block_number
    WHERE event.chain_id = $1 AND event.event_kind = 'ResolverRecordLinked'
      AND event.after_state ->> 'storage_model' = 'resolver_record_id'
      AND lower(event.after_state ->> 'resolver') = $2
      AND event.consumer_visibility = 'activated'
      AND event.canonicality_state IN ('canonical', 'safe', 'finalized')
      AND event.block_number <= $3
      AND lineage.canonicality_state IN ('canonical', 'safe', 'finalized')
    ORDER BY lower(event.after_state ->> 'node'), event.block_number DESC NULLS LAST,
        event.transaction_index DESC NULLS LAST, event.log_index DESC NULLS LAST,
        event.normalized_event_id DESC
), named_nodes AS (
    SELECT DISTINCT ON (lower(surface.namehash))
        lower(surface.namehash) AS node, surface.logical_name_id, surface.raw_name, surface.namespace
    FROM bigname_phase.name_surfaces surface
    JOIN bigname_phase.chain_lineage lineage
      ON lineage.chain_id = surface.chain_id AND lineage.block_hash = surface.block_hash
     AND lineage.block_number = surface.block_number
    WHERE surface.chain_id = $1 AND surface.block_number <= $3
      AND surface.canonicality_state IN ('canonical', 'safe', 'finalized')
      AND lineage.canonicality_state IN ('canonical', 'safe', 'finalized')
    ORDER BY lower(surface.namehash), surface.namespace, surface.logical_name_id
), items AS (
    SELECT lpad(link.record_id, 78, '0') AS key1, link.node AS key2,
        jsonb_strip_nulls(jsonb_build_object(
            'record_id', link.record_id, 'namehash', link.node,
            'default', link.node = '0x0000000000000000000000000000000000000000000000000000000000000000',
            'logical_name_id', named.logical_name_id, 'name', named.raw_name,
            'namespace', named.namespace, 'normalized_event_id', link.normalized_event_id,
            'chain_position', jsonb_strip_nulls(jsonb_build_object(
                'chain_id', link.chain_id, 'block_number', link.block_number,
                'block_hash', link.block_hash, 'transaction_hash', link.transaction_hash,
                'log_index', link.log_index,
                'timestamp', to_char(link.block_timestamp AT TIME ZONE 'UTC',
                                     'YYYY-MM-DD"T"HH24:MI:SS"Z"'))))) AS item
    FROM latest_links link
    LEFT JOIN named_nodes named USING (node)
    WHERE link.record_id <> '0'
)
