pub(super) const MANIFEST_CURRENT_INVALIDATIONS_PREFIX: &str = r#"
WITH readable_lineage AS (
    SELECT chain_id, block_hash
    FROM chain_lineage
    WHERE canonicality_state IN (
        'canonical'::canonicality_state,
        'safe'::canonicality_state,
        'finalized'::canonicality_state
    )
),
changed_events AS (
    SELECT ne.*, change.change_id, change.changed_at
    FROM projection_normalized_event_changes change
    JOIN normalized_events ne
      ON ne.normalized_event_id = change.normalized_event_id
    WHERE change.change_id > $1
      AND change.change_id <= $2
),
manifest_events AS (
    SELECT *
    FROM changed_events
    WHERE derivation_kind = 'manifest_sync'
      AND event_kind IN (
          'SourceManifestUpdated',
          'CapabilityChanged',
          'ProxyImplementationChanged'
      )
      AND namespace IS NOT NULL
),
record_inventory_targets AS (
    SELECT DISTINCT
        target.namespace,
        target.resource_id
    FROM normalized_events target
    JOIN resources resource
      ON resource.resource_id = target.resource_id
     AND resource.canonicality_state IN (
          'canonical'::canonicality_state,
          'safe'::canonicality_state,
          'finalized'::canonicality_state
     )
    LEFT JOIN readable_lineage target_lineage
      ON target_lineage.chain_id = target.chain_id
     AND target_lineage.block_hash = target.block_hash
    LEFT JOIN readable_lineage resource_lineage
      ON resource_lineage.chain_id = resource.chain_id
     AND resource_lineage.block_hash = resource.block_hash
    WHERE target.event_kind IN (
          'RecordChanged',
          'RecordVersionChanged',
          'ResolverChanged'
      )
      AND target.resource_id IS NOT NULL
      AND target.logical_name_id IS NOT NULL
      AND target.canonicality_state IN (
          'canonical'::canonicality_state,
          'safe'::canonicality_state,
          'finalized'::canonicality_state
      )
      AND (
          target.block_hash IS NULL
          OR target_lineage.block_hash IS NOT NULL
      )
      AND (
          resource.block_hash IS NULL
          OR resource_lineage.block_hash IS NOT NULL
      )
),
resolver_targets AS (
    SELECT DISTINCT
        target.namespace,
        target.chain_id,
        lower(resolver.resolver_address) AS resolver_address
    FROM normalized_events target
    LEFT JOIN readable_lineage target_lineage
      ON target_lineage.chain_id = target.chain_id
     AND target_lineage.block_hash = target.block_hash
    CROSS JOIN LATERAL (
        VALUES
            (target.after_state ->> 'resolver'),
            (target.before_state ->> 'resolver')
    ) AS resolver(resolver_address)
    WHERE target.event_kind IN ('ResolverChanged', 'AliasChanged')
      AND target.chain_id IS NOT NULL
      AND resolver.resolver_address IS NOT NULL
      AND resolver.resolver_address <> ''
      AND lower(resolver.resolver_address) <> '0x0000000000000000000000000000000000000000'
      AND target.canonicality_state IN (
          'canonical'::canonicality_state,
          'safe'::canonicality_state,
          'finalized'::canonicality_state
      )
      AND (
          target.block_hash IS NULL
          OR target_lineage.block_hash IS NOT NULL
      )

    UNION

    SELECT DISTINCT
        NULL::TEXT AS namespace,
        pc.scope_detail ->> 'chain_id' AS chain_id,
        lower(pc.scope_detail ->> 'resolver_address') AS resolver_address
    FROM permissions_current pc
    WHERE pc.scope_kind = 'resolver'
      AND pc.scope_detail ->> 'chain_id' IS NOT NULL
      AND pc.scope_detail ->> 'resolver_address' IS NOT NULL
      AND pc.scope_detail ->> 'resolver_address' <> ''
),
candidate_keys AS (
    SELECT
        'name_current'::TEXT AS projection,
        ns.logical_name_id AS projection_key,
        jsonb_build_object('logical_name_id', ns.logical_name_id) AS key_payload,
        me.normalized_event_id,
        me.change_id,
        me.changed_at
    FROM manifest_events me
    JOIN name_surfaces ns
      ON ns.namespace = me.namespace
     AND ns.canonicality_state IN (
          'canonical'::canonicality_state,
          'safe'::canonicality_state,
          'finalized'::canonicality_state
     )
    LEFT JOIN readable_lineage surface_lineage
      ON surface_lineage.chain_id = ns.chain_id
     AND surface_lineage.block_hash = ns.block_hash
    WHERE ns.block_hash IS NULL
       OR surface_lineage.block_hash IS NOT NULL

    UNION ALL

    SELECT
        'record_inventory_current'::TEXT AS projection,
        target.resource_id::TEXT AS projection_key,
        jsonb_build_object('resource_id', target.resource_id::TEXT) AS key_payload,
        me.normalized_event_id,
        me.change_id,
        me.changed_at
    FROM manifest_events me
    JOIN record_inventory_targets target
      ON target.namespace = me.namespace

    UNION ALL

    SELECT
        'resolver_current'::TEXT AS projection,
        target.chain_id || ':' || target.resolver_address AS projection_key,
        jsonb_build_object(
            'chain_id', target.chain_id,
            'resolver_address', target.resolver_address
        ) AS key_payload,
        me.normalized_event_id,
        me.change_id,
        me.changed_at
    FROM manifest_events me
    JOIN resolver_targets target
      ON target.namespace = me.namespace
      OR target.namespace IS NULL
)
"#;
