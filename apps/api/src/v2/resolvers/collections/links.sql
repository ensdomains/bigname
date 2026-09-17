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
    -- The emitter predicate is the indexed one (normalized_events_emitter_history_idx);
    -- a record-ID resolver emits its own Linked logs.
    WHERE event.chain_id = $1 AND event.event_kind = 'ResolverRecordLinked'
      AND event.after_state ->> 'storage_model' = 'resolver_record_id'
      AND lower(event.raw_fact_ref ->> 'emitting_address') = $2
      AND lower(event.after_state ->> 'resolver') = $2
      AND event.consumer_visibility = 'activated'
      AND event.canonicality_state IN ('canonical', 'safe', 'finalized')
      AND event.block_number <= $3
      AND lineage.canonicality_state IN ('canonical', 'safe', 'finalized')
    ORDER BY lower(event.after_state ->> 'node'), event.block_number DESC NULLS LAST,
        event.transaction_index DESC NULLS LAST, event.log_index DESC NULLS LAST,
        event.normalized_event_id DESC
), named_nodes AS (
    -- Only an active surface is a name to show; a shadow surface is withheld from readers,
    -- and the default node's surface is the root name, not a name.
    SELECT link.node, surface.logical_name_id, surface.raw_name, surface.namespace
    FROM (SELECT DISTINCT node FROM latest_links
          WHERE node <> '0x0000000000000000000000000000000000000000000000000000000000000000') link
    JOIN bigname_phase.name_surfaces surface
      ON surface.logical_name_id = $7 || ':' || link.node
     AND surface.chain_id = $1 AND surface.block_number <= $3
     AND surface.visibility_state = 'active'
     AND surface.canonicality_state IN ('canonical', 'safe', 'finalized')
    JOIN bigname_phase.chain_lineage lineage
      ON lineage.chain_id = surface.chain_id AND lineage.block_hash = surface.block_hash
     AND lineage.block_number = surface.block_number
     AND lineage.canonicality_state IN ('canonical', 'safe', 'finalized')
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
