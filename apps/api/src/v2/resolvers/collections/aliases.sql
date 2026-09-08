WITH alias_candidates AS (
    SELECT event.*,
        COALESCE(event.logical_name_id, event.after_state ->> 'from_logical_name_id',
            event.before_state ->> 'from_logical_name_id', event.after_state ->> 'from_namehash',
            event.before_state ->> 'from_namehash', event.after_state ->> 'from_dns_encoded_name',
            event.before_state ->> 'from_dns_encoded_name', event.after_state ->> 'from_name',
            event.before_state ->> 'from_name', event.event_identity) AS alias_identity
    FROM bigname_phase.normalized_events event
    LEFT JOIN bigname_phase.chain_lineage lineage
      ON lineage.chain_id = event.chain_id AND lineage.block_hash = event.block_hash
     AND lineage.block_number = event.block_number
    WHERE event.chain_id = $1 AND event.event_kind = 'AliasChanged'
      AND event.consumer_visibility = 'activated'
      AND event.canonicality_state IN ('canonical', 'safe', 'finalized')
      AND ((event.block_number IS NULL AND event.block_hash IS NULL)
        OR (event.block_number <= $3 AND lineage.canonicality_state IN ('canonical', 'safe', 'finalized')))
      AND lower(COALESCE(event.after_state ->> 'resolver', event.before_state ->> 'resolver',
          event.raw_fact_ref ->> 'emitting_address')) = $2
), latest_aliases AS (
    SELECT alias_candidates.*, row_number() OVER (PARTITION BY alias_identity
        ORDER BY block_number DESC NULLS LAST, transaction_index DESC NULLS LAST,
            log_index DESC NULLS LAST, normalized_event_id DESC) AS latest_rank
    FROM alias_candidates
), items AS (
    SELECT 'binding'::text AS key1, nc.logical_name_id AS key2,
        jsonb_build_object('logical_name_id', nc.logical_name_id,
            'normalized_name', nc.raw_name, 'namehash', nc.namehash) AS item
    FROM bigname_phase.name_current nc
    JOIN bigname_phase.name_surfaces surface ON surface.logical_name_id = nc.logical_name_id
    LEFT JOIN bigname_phase.resources resource ON resource.resource_id = nc.resource_id
    LEFT JOIN bigname_phase.surface_bindings binding ON binding.surface_binding_id = nc.surface_binding_id
    LEFT JOIN bigname_phase.token_lineages token_lineage ON token_lineage.token_lineage_id = nc.token_lineage_id
    {{name_lineage_joins}}
    WHERE nc.binding_kind = 'resolver_alias_path'
      AND nc.declared_summary #>> '{resolver,chain_id}' = $1
      AND lower(nc.declared_summary #>> '{resolver,address}') = $2
      AND nc.provenance ->> 'resolver_pointer_source_family' IS NOT NULL
      {{name_read_filter}}
    UNION ALL
    SELECT 'event' AS key1, alias_identity AS key2,
        jsonb_strip_nulls(jsonb_build_object('logical_name_id', logical_name_id,
            'alias_state', COALESCE(after_state -> 'alias_state', '"active"'::jsonb),
            'chain_id', chain_id, 'resolver_address', $2::text,
            'from_name', after_state -> 'from_name', 'to_name', after_state -> 'to_name',
            'from_dns_encoded_name', after_state -> 'from_dns_encoded_name',
            'to_dns_encoded_name', after_state -> 'to_dns_encoded_name',
            'to_logical_name_id', after_state -> 'to_logical_name_id',
            'to_resource_id', after_state -> 'to_resource_id')) AS item
    FROM latest_aliases WHERE latest_rank = 1 AND COALESCE((after_state ->> 'active')::boolean, true)
)
