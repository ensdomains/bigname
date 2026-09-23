
        INSERT INTO project_event_ids
        SELECT event.normalized_event_id
        FROM normalized_events event
        WHERE event.chain_id = $1
          AND event.event_kind = 'SourceManifestUpdated'
          AND (event.block_number IS NULL OR event.block_number <= $2)
        UNION
        SELECT event.normalized_event_id
        FROM project_scope_names scope
        JOIN normalized_events event USING (logical_name_id)
        WHERE event.chain_id = $1 AND event.block_number <= $2
          AND event.canonicality_state IN ('canonical', 'safe', 'finalized')
        UNION
        SELECT event.normalized_event_id
        FROM project_scope_children scope
        JOIN normalized_events event USING (logical_name_id)
        WHERE event.chain_id = $1 AND event.block_number <= $2
          AND event.canonicality_state IN ('canonical', 'safe', 'finalized')
        UNION
        -- An ancestor reached only through a changed child's edge stages its own events (migration
        -- boundary, subregistry pointer, ownership) as parent evidence; its other children stay out
        -- of scope.
        SELECT event.normalized_event_id
        FROM project_scope_ancestors scope
        JOIN normalized_events event USING (logical_name_id)
        WHERE event.chain_id = $1 AND event.block_number <= $2
          AND event.canonicality_state IN ('canonical', 'safe', 'finalized')
        UNION
        -- ENSv2 registration stores the entry's subregistry and emits the label registration
        -- separately. (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L462 @ ens_v2@a971bd64)
        -- (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L467 @ ens_v2@a971bd64)
        -- The child-edge projection combines those inputs, so rebuilding a scoped parent's row
        -- family stages each current sibling's registrations without widening projection scope.
        SELECT event.normalized_event_id
        FROM project_scope_children scope
        JOIN children_current child
          ON child.parent_logical_name_id = scope.logical_name_id
         AND child.provenance ->> 'chain_id' = $1
        JOIN normalized_events event
          ON event.logical_name_id = child.child_logical_name_id
        WHERE event.chain_id = $1 AND event.block_number <= $2
          AND event.event_kind IN (
              'RegistrationGranted', 'RegistrationRenewed', 'RegistrationReleased'
          )
          AND event.canonicality_state IN ('canonical', 'safe', 'finalized')
        UNION
        SELECT event.normalized_event_id
        FROM project_scope_resources scope
        JOIN normalized_events event USING (resource_id)
        WHERE event.chain_id = $1 AND event.block_number <= $2
          AND event.canonicality_state IN ('canonical', 'safe', 'finalized')
        UNION
        SELECT event.normalized_event_id
        FROM project_scope_account_permissions scope
        JOIN normalized_events event
          ON event.chain_id = scope.chain_id
         AND event.after_state #>> '{scope,authority_kind}' = scope.authority_kind
         AND lower(event.after_state #>> '{scope,authority_contract}') = scope.authority_contract
         AND lower(event.after_state #>> '{scope,owner}') = scope.owner
         AND lower(event.after_state ->> 'subject') = scope.subject
         AND event.after_state ->> 'relation_kind' = scope.relation_kind
        WHERE event.event_kind = 'AccountPermissionChanged'
          AND event.block_number <= $2
          AND event.canonicality_state IN ('canonical', 'safe', 'finalized')
        UNION
        -- Defensive symmetry with create_identity_views; inventory closure guarantees the names.
        
SELECT record.normalized_event_id
FROM (
    SELECT logical_name_id FROM project_scope_names
    UNION
    SELECT logical_name_id FROM project_scope_children
) scope
JOIN name_surfaces surface USING (logical_name_id)
JOIN chain_lineage surface_lineage
  ON surface_lineage.chain_id = surface.chain_id
 AND (surface_lineage.block_number, surface_lineage.block_hash) =
     (surface.block_number, surface.block_hash)
JOIN (
    SELECT DISTINCT event.resource_id, event.logical_name_id,
           event.source_family AS pointer_source_family, event.namespace,
           lower(event.after_state ->> 'resolver') AS resolver_address
    FROM project_scope_resources resource_scope
    JOIN normalized_events event USING (resource_id)
    JOIN chain_lineage lineage USING (chain_id, block_number, block_hash)
    WHERE event.chain_id = $1 AND event.block_number <= $2
      AND event.resource_id IS NOT NULL AND event.logical_name_id IS NOT NULL
      AND event.event_kind = 'ResolverChanged'
      AND event.consumer_visibility = 'activated'
      AND (
          event.source_family IN (
              'ens_v1_registry_l1',
              'ens_v1_registrar_l1',
              'ens_v1_wrapper_l1',
              'basenames_base_registry'
          ) OR (
              event.source_family IN ('ens_v2_registry_l1', 'ens_v2_root_l1')
              AND EXISTS (
                  SELECT 1
                  FROM project_declared_resolver_addresses declaration
                  WHERE declaration.namespace = event.namespace
                    AND declaration.resolver_address =
                        lower(event.after_state ->> 'resolver')
              )
          )
      )
      AND event.canonicality_state IN ('canonical', 'safe', 'finalized')
      AND lineage.canonicality_state IN ('canonical', 'safe', 'finalized')
) pointer USING (logical_name_id)
JOIN LATERAL (
    SELECT event.normalized_event_id, event.chain_id,
           event.block_number, event.block_hash
    FROM normalized_events event
    WHERE pointer.pointer_source_family IN (
              'ens_v1_registry_l1', 'ens_v1_registrar_l1',
              'ens_v1_wrapper_l1', 'ens_v2_registry_l1', 'ens_v2_root_l1'
          )
      AND event.chain_id = $1
      AND event.logical_name_id IS NULL
      AND event.source_family = 'ens_v1_resolver_l1'
      AND lower(event.after_state ->> 'node') = lower(surface.namehash)
      AND lower(COALESCE(
              NULLIF(event.after_state ->> 'resolver', ''),
              NULLIF(event.raw_fact_ref ->> 'emitting_address', '')
          )) = pointer.resolver_address
      AND event.block_number <= $2
      AND event.consumer_visibility = 'activated'
      AND event.event_kind IN ('RecordChanged', 'RecordVersionChanged')
      AND event.canonicality_state IN ('canonical', 'safe', 'finalized')
    UNION ALL
    SELECT event.normalized_event_id, event.chain_id,
           event.block_number, event.block_hash
    FROM normalized_events event
    JOIN project_declared_resolver_addresses declaration
      ON declaration.namespace = pointer.namespace
     AND declaration.resolver_address = pointer.resolver_address
     AND declaration.source_family = 'ens_v2_resolver_l1'
     AND declaration.classification_role = 'public_resolver_v2'
     AND declaration.manifest_id = event.source_manifest_id
    WHERE pointer.pointer_source_family IN ('ens_v2_registry_l1', 'ens_v2_root_l1')
      AND event.chain_id = $1 AND event.namespace = pointer.namespace
      AND event.logical_name_id IS NULL
      AND event.source_family = 'ens_v2_resolver_l1'
      AND lower(event.after_state ->> 'node') = lower(surface.namehash)
      AND lower(COALESCE(NULLIF(event.after_state ->> 'resolver', ''),
                        NULLIF(event.raw_fact_ref ->> 'emitting_address', ''))) =
          pointer.resolver_address
      AND event.block_number <= $2
      AND event.consumer_visibility = 'activated'
      AND event.event_kind IN ('RecordChanged', 'RecordVersionChanged')
      AND event.canonicality_state IN ('canonical', 'safe', 'finalized')
    UNION ALL
    -- A pointer to a declared ENSv1 mirror resolver is served through whichever declared ENSv1
    -- resolver the mirror finds for the queried node (builders/record_inventory/mirror.rs), so
    -- stage that node's writes on every declared ENSv1 resolver.
    SELECT event.normalized_event_id, event.chain_id,
           event.block_number, event.block_hash
    FROM normalized_events event
    JOIN project_declared_resolver_addresses ensv1
      ON ensv1.source_family = 'ens_v1_resolver_l1'
     AND ensv1.resolver_address = lower(COALESCE(
             NULLIF(event.after_state ->> 'resolver', ''),
             NULLIF(event.raw_fact_ref ->> 'emitting_address', '')
         ))
    WHERE pointer.pointer_source_family IN ('ens_v2_registry_l1', 'ens_v2_root_l1')
      AND EXISTS (
          SELECT 1
          FROM project_declared_resolver_addresses mirror
          WHERE mirror.namespace = pointer.namespace
            AND mirror.resolver_address = pointer.resolver_address
            AND mirror.classification_role = 'ensv1_mirror_resolver'
      )
      AND event.chain_id = $1
      AND event.logical_name_id IS NULL
      AND event.source_family = 'ens_v1_resolver_l1'
      AND lower(event.after_state ->> 'node') = lower(surface.namehash)
      AND event.block_number <= $2
      AND event.consumer_visibility = 'activated'
      AND event.event_kind IN ('RecordChanged', 'RecordVersionChanged')
      AND event.canonicality_state IN ('canonical', 'safe', 'finalized')
    UNION ALL
    SELECT event.normalized_event_id, event.chain_id,
           event.block_number, event.block_hash
    FROM normalized_events event
    WHERE pointer.pointer_source_family = 'basenames_base_registry'
      AND event.chain_id = $1
      AND event.logical_name_id IS NULL
      AND event.source_family = 'basenames_base_resolver'
      AND lower(event.after_state ->> 'node') = lower(surface.namehash)
      AND lower(COALESCE(
              NULLIF(event.after_state ->> 'resolver', ''),
              NULLIF(event.raw_fact_ref ->> 'emitting_address', '')
          )) = pointer.resolver_address
      AND event.block_number <= $2
      AND event.consumer_visibility = 'activated'
      AND event.event_kind IN ('RecordChanged', 'RecordVersionChanged')
      AND event.canonicality_state IN ('canonical', 'safe', 'finalized')
) record ON TRUE
JOIN chain_lineage record_lineage
  ON record_lineage.chain_id = record.chain_id
 AND (record_lineage.block_number, record_lineage.block_hash) =
     (record.block_number, record.block_hash)
WHERE surface.chain_id = $1 AND surface.block_number <= $2
  AND surface.canonicality_state IN ('canonical', 'safe', 'finalized')
  AND surface_lineage.canonicality_state IN ('canonical', 'safe', 'finalized')
  AND pointer.resolver_address NOT IN (
      '0x0000000000000000000000000000000000000000', ''
  )
  AND record_lineage.canonicality_state IN ('canonical', 'safe', 'finalized')
UNION
-- The ENSv1 registry's resolver for a scoped name's node, whether or not the pointer event is
-- linked to the name (a pre-surface pointer keeps null logical_name_id and resource_id). The
-- ENSv1 mirror resolver walk reads these by node (builders/record_inventory/mirror.rs).
SELECT event.normalized_event_id
FROM (
    SELECT logical_name_id FROM project_scope_names
    UNION
    SELECT logical_name_id FROM project_scope_children
) scope
JOIN name_surfaces surface USING (logical_name_id)
JOIN normalized_events event
  ON event.chain_id = surface.chain_id
 AND event.namespace = surface.namespace
 AND lower(event.after_state ->> 'node') = lower(surface.namehash)
WHERE surface.chain_id = $1 AND surface.block_number <= $2
  AND surface.canonicality_state IN ('canonical', 'safe', 'finalized')
  AND event.event_kind = 'ResolverChanged'
  AND event.source_family IN ('ens_v1_registry_l1', 'ens_v1_registrar_l1', 'ens_v1_wrapper_l1')
  AND event.logical_name_id IS NULL
  AND event.block_number <= $2
  AND event.consumer_visibility = 'activated'
  AND event.canonicality_state IN ('canonical', 'safe', 'finalized')

        UNION
        -- Candidate-only resources supply just the resolver evidence consumed by this build.
        -- They remain outside delete-and-publish resource scope.
        SELECT normalized_event_id
        FROM project_scope_resolver_candidate_events
        UNION
        SELECT matched.normalized_event_id
FROM (
    SELECT logical_name_id FROM project_scope_names
    UNION
    SELECT logical_name_id FROM project_scope_children
) scope
CROSS JOIN LATERAL (
    SELECT normalized_event_id
    FROM normalized_events
    WHERE chain_id = $1 AND block_number <= $2
      AND (event_kind IN ('SubregistryChanged', 'AliasChanged')
           OR (event_kind = 'AuthorityTransferred'
               AND source_family IN ('ens_v1_registry_l1', 'basenames_base_registry')))
      AND canonicality_state IN ('canonical', 'safe', 'finalized')
      AND (namespace || ':' || lower(after_state ->> 'node')) IS NOT NULL
      AND (namespace || ':' || lower(after_state ->> 'node')) = scope.logical_name_id
    UNION ALL
    SELECT normalized_event_id
    FROM normalized_events
    WHERE chain_id = $1 AND block_number <= $2
      AND (event_kind IN ('SubregistryChanged', 'AliasChanged')
           OR (event_kind = 'AuthorityTransferred'
               AND source_family IN ('ens_v1_registry_l1', 'basenames_base_registry')))
      AND canonicality_state IN ('canonical', 'safe', 'finalized')
      AND (namespace || ':' || lower(after_state ->> 'child_node')) IS NOT NULL
      AND (namespace || ':' || lower(after_state ->> 'child_node')) = scope.logical_name_id
    UNION ALL
    SELECT normalized_event_id
    FROM normalized_events
    WHERE chain_id = $1 AND block_number <= $2
      AND (event_kind IN ('SubregistryChanged', 'AliasChanged')
           OR (event_kind = 'AuthorityTransferred'
               AND source_family IN ('ens_v1_registry_l1', 'basenames_base_registry')))
      AND canonicality_state IN ('canonical', 'safe', 'finalized')
      AND (after_state ->> 'to_logical_name_id') IS NOT NULL
      AND (after_state ->> 'to_logical_name_id') = scope.logical_name_id
    UNION ALL
    SELECT normalized_event_id
    FROM normalized_events
    WHERE chain_id = $1 AND block_number <= $2
      AND (event_kind IN ('SubregistryChanged', 'AliasChanged')
           OR (event_kind = 'AuthorityTransferred'
               AND source_family IN ('ens_v1_registry_l1', 'basenames_base_registry')))
      AND canonicality_state IN ('canonical', 'safe', 'finalized')
      AND (before_state ->> 'to_logical_name_id') IS NOT NULL
      AND (before_state ->> 'to_logical_name_id') = scope.logical_name_id
    OFFSET 0
) matched

        UNION
        SELECT event.normalized_event_id
        FROM normalized_events event
        CROSS JOIN LATERAL (
            VALUES (event.after_state ->> 'to_resource_id'),
                   (event.before_state ->> 'to_resource_id')
        ) candidate(resource_id)
        JOIN project_scope_resources scope
          ON scope.resource_id::text = candidate.resource_id
        WHERE event.chain_id = $1 AND event.block_number <= $2
          AND event.event_kind = 'AliasChanged'
          AND event.canonicality_state IN ('canonical', 'safe', 'finalized')
        UNION
        SELECT event.normalized_event_id
        FROM project_changed_events event
        CROSS JOIN LATERAL (VALUES
            (CASE WHEN event.event_kind = 'ResolverChanged'
                  THEN event.after_state ->> 'resolver' END),
            (CASE WHEN event.event_kind = 'ResolverChanged'
                  THEN event.before_state ->> 'resolver' END),
            (CASE WHEN event.event_kind = 'PermissionChanged'
                       AND event.after_state #>> '{scope,kind}' = 'resolver'
                  THEN event.after_state #>> '{scope,resolver_address}' END),
            (CASE WHEN event.event_kind = 'PermissionChanged'
                       AND event.before_state #>> '{scope,kind}' = 'resolver'
                  THEN event.before_state #>> '{scope,resolver_address}' END)
        ) candidate(resolver_address)
        JOIN project_scope_resolvers scope
          ON lower(candidate.resolver_address) = lower(scope.resolver_address)
        WHERE event.event_kind IN ('ResolverChanged', 'PermissionChanged')
          AND NOT EXISTS (
              SELECT 1 FROM project_scope_resolver_passthrough passthrough
              WHERE lower(passthrough.resolver_address) =
                    lower(scope.resolver_address)
          )
        UNION
        SELECT event.normalized_event_id
        FROM project_scope_resolvers scope
        JOIN normalized_events event
          ON lower(COALESCE(
                 event.after_state ->> 'resolver',
                 event.before_state ->> 'resolver',
                 event.raw_fact_ref ->> 'emitting_address'
             )) = lower(scope.resolver_address)
        WHERE event.chain_id = $1 AND event.block_number <= $2
          AND event.event_kind = 'AliasChanged'
          AND NOT EXISTS (
              SELECT 1 FROM project_scope_resolver_passthrough passthrough
              WHERE lower(passthrough.resolver_address) =
                    lower(scope.resolver_address)
          )
          AND event.canonicality_state IN ('canonical', 'safe', 'finalized')
        UNION
        SELECT event.normalized_event_id
        FROM project_scope_resolvers scope
        JOIN normalized_events event
          ON lower(event.after_state ->> 'proxy_address') =
             lower(scope.resolver_address)
        WHERE event.chain_id = $1 AND event.block_number <= $2
          AND event.event_kind = 'Upgraded'
          AND NOT EXISTS (
              SELECT 1 FROM project_scope_resolver_passthrough passthrough
              WHERE lower(passthrough.resolver_address) =
                    lower(scope.resolver_address)
          )
          AND event.canonicality_state IN ('canonical', 'safe', 'finalized')
        UNION
        SELECT matched.normalized_event_id
FROM project_scope_primary scope
CROSS JOIN LATERAL (
    SELECT normalized_event_id
    FROM normalized_events
    WHERE chain_id = $1 AND block_number <= $2
      AND event_kind IN ('ReverseChanged', 'RecordChanged')
      AND canonicality_state IN ('canonical', 'safe', 'finalized')
      AND (lower(after_state ->> 'address')) IS NOT NULL
      AND (after_state ->> 'coin_type') IS NOT NULL
      AND (after_state ->> 'namespace') IS NOT NULL
      AND lower(after_state ->> 'address') = scope.address
      AND after_state ->> 'coin_type' = scope.coin_type
      AND after_state ->> 'namespace' = scope.namespace
    UNION ALL
    SELECT normalized_event_id
    FROM normalized_events
    WHERE chain_id = $1 AND block_number <= $2
      AND event_kind IN ('ReverseChanged', 'RecordChanged')
      AND canonicality_state IN ('canonical', 'safe', 'finalized')
      AND (lower(before_state ->> 'address')) IS NOT NULL
      AND (before_state ->> 'coin_type') IS NOT NULL
      AND (before_state ->> 'namespace') IS NOT NULL
      AND lower(before_state ->> 'address') = scope.address
      AND before_state ->> 'coin_type' = scope.coin_type
      AND before_state ->> 'namespace' = scope.namespace
    UNION ALL
    SELECT normalized_event_id
    FROM normalized_events
    WHERE chain_id = $1 AND block_number <= $2
      AND event_kind IN ('ReverseChanged', 'RecordChanged')
      AND canonicality_state IN ('canonical', 'safe', 'finalized')
      AND (lower(after_state -> 'primary_claim_source' ->> 'address')) IS NOT NULL
      AND (after_state -> 'primary_claim_source' ->> 'coin_type') IS NOT NULL
      AND (after_state -> 'primary_claim_source' ->> 'namespace') IS NOT NULL
      AND lower(after_state -> 'primary_claim_source' ->> 'address') = scope.address
      AND after_state -> 'primary_claim_source' ->> 'coin_type' = scope.coin_type
      AND after_state -> 'primary_claim_source' ->> 'namespace' = scope.namespace
    UNION ALL
    SELECT normalized_event_id
    FROM normalized_events
    WHERE chain_id = $1 AND block_number <= $2
      AND event_kind IN ('ReverseChanged', 'RecordChanged')
      AND canonicality_state IN ('canonical', 'safe', 'finalized')
      AND (lower(before_state -> 'primary_claim_source' ->> 'address')) IS NOT NULL
      AND (before_state -> 'primary_claim_source' ->> 'coin_type') IS NOT NULL
      AND (before_state -> 'primary_claim_source' ->> 'namespace') IS NOT NULL
      AND lower(before_state -> 'primary_claim_source' ->> 'address') = scope.address
      AND before_state -> 'primary_claim_source' ->> 'coin_type' = scope.coin_type
      AND before_state -> 'primary_claim_source' ->> 'namespace' = scope.namespace
    OFFSET 0
) matched

        UNION
        SELECT resolver.normalized_event_id
        FROM project_scope_primary scope
        JOIN normalized_events reverse
          ON reverse.chain_id = $1
         AND reverse.block_number <= $2
         AND reverse.event_kind = 'ReverseChanged'
         AND reverse.canonicality_state IN ('canonical', 'safe', 'finalized')
         AND lower(reverse.after_state ->> 'address') = scope.address
         AND reverse.after_state ->> 'coin_type' = scope.coin_type
         AND reverse.after_state ->> 'namespace' = scope.namespace
        JOIN normalized_events resolver
          ON resolver.chain_id = $1
         AND resolver.block_number <= $2
         AND (resolver.event_kind IN ('ResolverChanged', 'RecordVersionChanged')
              OR (resolver.event_kind = 'RecordChanged'
                  AND resolver.after_state ->> 'source_event' = 'NameChanged'))
         AND resolver.canonicality_state IN ('canonical', 'safe', 'finalized')
         AND lower(resolver.after_state ->> 'node') =
             lower(reverse.after_state ->> 'reverse_node')
        ON CONFLICT DO NOTHING
        