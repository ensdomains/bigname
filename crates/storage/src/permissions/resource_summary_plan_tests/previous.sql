WITH eligible AS NOT MATERIALIZED (
            SELECT ne.*,
                lower(COALESCE(ne.after_state ->> 'child_node',
                    ne.after_state ->> 'namehash', ne.after_state ->> 'node')) AS node,
                COALESCE(ne.namespace || ':' || lower(COALESCE(
                    ne.after_state ->> 'child_node', ne.after_state ->> 'namehash',
                    ne.after_state ->> 'node', ne.after_state #>> '{grant_source,node}',
                    ne.after_state #>> '{revocation_source,node}')), ne.logical_name_id) AS node_key
            FROM bigname_phase.normalized_events ne
            JOIN bigname_phase.chain_lineage lineage
              ON lineage.chain_id = ne.chain_id AND lineage.block_hash = ne.block_hash
             AND lineage.block_number = ne.block_number
            WHERE ne.block_number <= ($3::jsonb ->> ne.chain_id)::bigint
              AND ne.consumer_visibility = 'activated'
              AND ne.canonicality_state IN ('canonical', 'safe', 'finalized')
              AND lineage.canonicality_state IN ('canonical', 'safe', 'finalized')
        ), anchors AS (
            SELECT DISTINCT anchor.namespace, anchor.chain_id,
                COALESCE(anchor.node, recovered.node) AS node, anchor.logical_name_id,
                (anchor.source_family LIKE 'basenames\_%') AS basenames
            FROM eligible anchor
            LEFT JOIN LATERAL (SELECT min(companion.node) AS node FROM eligible companion
         WHERE anchor.event_kind = 'AuthorityEpochChanged'
           AND anchor.source_family IN ('ens_v1_registrar_l1', 'basenames_base_registrar')
           AND anchor.after_state ->> 'source_event' = 'Transfer'
           AND anchor.node_key IS NULL
           AND companion.event_kind = 'TokenControlTransferred'
           AND companion.after_state ->> 'source_event' = 'Transfer'
           AND companion.node IS NOT NULL AND anchor.chain_id = companion.chain_id AND anchor.namespace = companion.namespace
         AND anchor.source_family = companion.source_family
         AND anchor.manifest_version = companion.manifest_version
         AND anchor.source_manifest_id IS NOT DISTINCT FROM companion.source_manifest_id
         AND anchor.block_hash = companion.block_hash AND anchor.block_number = companion.block_number
         AND anchor.transaction_hash = companion.transaction_hash
         AND anchor.transaction_index = companion.transaction_index AND anchor.log_index = companion.log_index
         AND lower(anchor.raw_fact_ref ->> 'emitting_address') = lower(companion.raw_fact_ref ->> 'emitting_address')
         AND anchor.raw_fact_ref @> jsonb_build_object('kind','raw_log','chain_id',anchor.chain_id,
             'block_hash',anchor.block_hash,'block_number',anchor.block_number,
             'transaction_hash',anchor.transaction_hash,'transaction_index',anchor.transaction_index,'log_index',anchor.log_index)
         AND companion.raw_fact_ref @> jsonb_build_object('kind','raw_log','chain_id',companion.chain_id,
             'block_hash',companion.block_hash,'block_number',companion.block_number,
             'transaction_hash',companion.transaction_hash,'transaction_index',companion.transaction_index,'log_index',companion.log_index)
         HAVING count(DISTINCT companion.node) = 1) recovered ON true
            WHERE (anchor.resource_id = ANY($1::uuid[])
                AND anchor.source_family IN ('ens_v1_registry_l1', 'ens_v1_registrar_l1',
                    'basenames_base_registry', 'basenames_base_registrar')
                AND anchor.event_kind IN ('AuthorityTransferred', 'AuthorityEpochChanged', 'SurfaceBound')
                AND anchor.after_state ->> 'authority_kind' = 'registry_only')
              OR (anchor.resource_id = $2::uuid AND anchor.event_kind = 'RegistrationGranted'
                AND anchor.source_family IN ('ens_v1_registrar_l1', 'basenames_base_registrar'))
        ), candidates AS (
            SELECT anchor.*,
                array_remove(ARRAY[anchor.namespace || ':' || anchor.node, anchor.logical_name_id]
                    || ARRAY(SELECT anchor.namespace || ':' || lower(surface.namehash)
                        FROM bigname_phase.name_surfaces surface
                        WHERE surface.namespace = anchor.namespace AND surface.chain_id = anchor.chain_id
                          AND surface.logical_name_id = anchor.logical_name_id), NULL) AS node_keys,
                array_remove(ARRAY[anchor.logical_name_id]
                    || ARRAY(SELECT surface.logical_name_id FROM bigname_phase.name_surfaces surface
                        WHERE surface.namespace = anchor.namespace AND surface.chain_id = anchor.chain_id
                          AND lower(surface.namehash) = anchor.node), NULL) AS logical_ids
            FROM anchors anchor
        )
        SELECT DISTINCT control.resource_id, lease.resource_id AS registration_id
        FROM candidates candidate
        JOIN LATERAL (
            SELECT authority.resource_id, authority.after_state, authority.event_kind
            FROM (SELECT probe.resource_id,probe.after_state,probe.event_kind,probe.namespace,probe.chain_id,probe.source_family,probe.block_number,probe.log_index,probe.normalized_event_id,probe.logical_name_id, probe.node FROM (SELECT probe.* FROM eligible probe
       WHERE NOT candidate.basenames
         AND probe.chain_id = candidate.chain_id AND probe.namespace = candidate.namespace
         AND probe.source_family LIKE 'ens\_v1\_%'
         AND probe.node_key = ANY(candidate.node_keys)
       UNION
       SELECT probe.* FROM eligible probe
       WHERE candidate.basenames
         AND probe.chain_id = candidate.chain_id AND probe.namespace = candidate.namespace
         AND probe.source_family LIKE 'basenames\_%'
         AND probe.node_key = ANY(candidate.node_keys)
       UNION
       SELECT probe.* FROM eligible probe
       WHERE probe.chain_id = candidate.chain_id AND probe.namespace = candidate.namespace
         AND probe.logical_name_id = ANY(candidate.logical_ids)) probe
         UNION ALL
         SELECT epoch.resource_id,epoch.after_state,epoch.event_kind,epoch.namespace,epoch.chain_id,epoch.source_family,epoch.block_number,epoch.log_index,epoch.normalized_event_id,epoch.logical_name_id, recovered.node FROM (SELECT probe.* FROM eligible probe
       WHERE NOT candidate.basenames
         AND probe.chain_id = candidate.chain_id AND probe.namespace = candidate.namespace
         AND probe.source_family LIKE 'ens\_v1\_%'
         AND probe.node_key = ANY(candidate.node_keys)
       UNION
       SELECT probe.* FROM eligible probe
       WHERE candidate.basenames
         AND probe.chain_id = candidate.chain_id AND probe.namespace = candidate.namespace
         AND probe.source_family LIKE 'basenames\_%'
         AND probe.node_key = ANY(candidate.node_keys)
       UNION
       SELECT probe.* FROM eligible probe
       WHERE probe.chain_id = candidate.chain_id AND probe.namespace = candidate.namespace
         AND probe.logical_name_id = ANY(candidate.logical_ids)) token
         JOIN eligible epoch ON token.chain_id = epoch.chain_id AND token.namespace = epoch.namespace
         AND token.source_family = epoch.source_family
         AND token.manifest_version = epoch.manifest_version
         AND token.source_manifest_id IS NOT DISTINCT FROM epoch.source_manifest_id
         AND token.block_hash = epoch.block_hash AND token.block_number = epoch.block_number
         AND token.transaction_hash = epoch.transaction_hash
         AND token.transaction_index = epoch.transaction_index AND token.log_index = epoch.log_index
         AND lower(token.raw_fact_ref ->> 'emitting_address') = lower(epoch.raw_fact_ref ->> 'emitting_address')
         AND token.raw_fact_ref @> jsonb_build_object('kind','raw_log','chain_id',token.chain_id,
             'block_hash',token.block_hash,'block_number',token.block_number,
             'transaction_hash',token.transaction_hash,'transaction_index',token.transaction_index,'log_index',token.log_index)
         AND epoch.raw_fact_ref @> jsonb_build_object('kind','raw_log','chain_id',epoch.chain_id,
             'block_hash',epoch.block_hash,'block_number',epoch.block_number,
             'transaction_hash',epoch.transaction_hash,'transaction_index',epoch.transaction_index,'log_index',epoch.log_index)
         JOIN LATERAL (SELECT min(companion.node) AS node FROM eligible companion
         WHERE epoch.event_kind = 'AuthorityEpochChanged'
           AND epoch.source_family IN ('ens_v1_registrar_l1', 'basenames_base_registrar')
           AND epoch.after_state ->> 'source_event' = 'Transfer'
           AND epoch.node_key IS NULL
           AND companion.event_kind = 'TokenControlTransferred'
           AND companion.after_state ->> 'source_event' = 'Transfer'
           AND companion.node IS NOT NULL AND epoch.chain_id = companion.chain_id AND epoch.namespace = companion.namespace
         AND epoch.source_family = companion.source_family
         AND epoch.manifest_version = companion.manifest_version
         AND epoch.source_manifest_id IS NOT DISTINCT FROM companion.source_manifest_id
         AND epoch.block_hash = companion.block_hash AND epoch.block_number = companion.block_number
         AND epoch.transaction_hash = companion.transaction_hash
         AND epoch.transaction_index = companion.transaction_index AND epoch.log_index = companion.log_index
         AND lower(epoch.raw_fact_ref ->> 'emitting_address') = lower(companion.raw_fact_ref ->> 'emitting_address')
         AND epoch.raw_fact_ref @> jsonb_build_object('kind','raw_log','chain_id',epoch.chain_id,
             'block_hash',epoch.block_hash,'block_number',epoch.block_number,
             'transaction_hash',epoch.transaction_hash,'transaction_index',epoch.transaction_index,'log_index',epoch.log_index)
         AND companion.raw_fact_ref @> jsonb_build_object('kind','raw_log','chain_id',companion.chain_id,
             'block_hash',companion.block_hash,'block_number',companion.block_number,
             'transaction_hash',companion.transaction_hash,'transaction_index',companion.transaction_index,'log_index',companion.log_index)
         HAVING count(DISTINCT companion.node) = 1) recovered ON recovered.node = token.node
         WHERE token.event_kind = 'TokenControlTransferred'
           AND token.source_family IN ('ens_v1_registrar_l1', 'basenames_base_registrar')) authority
            WHERE candidate.namespace = authority.namespace AND candidate.chain_id = authority.chain_id
         AND CASE WHEN candidate.node IS NOT NULL AND authority.node IS NOT NULL
             THEN candidate.node = authority.node
             ELSE candidate.logical_name_id = authority.logical_name_id OR EXISTS (
                 SELECT 1 FROM bigname_phase.name_surfaces surface
                 WHERE surface.logical_name_id = candidate.logical_name_id
                   AND surface.namespace = candidate.namespace AND surface.chain_id = candidate.chain_id
                   AND lower(surface.namehash) = authority.node
             ) OR EXISTS (
                 SELECT 1 FROM bigname_phase.name_surfaces surface
                 WHERE surface.logical_name_id = authority.logical_name_id
                   AND surface.namespace = authority.namespace AND surface.chain_id = authority.chain_id
                   AND lower(surface.namehash) = candidate.node
             ) END
              AND authority.source_family IN ('ens_v1_registry_l1', 'ens_v1_registrar_l1',
                  'ens_v1_wrapper_l1', 'basenames_base_registry', 'basenames_base_registrar')
              AND authority.event_kind IN ('AuthorityTransferred', 'AuthorityEpochChanged',
                  'SurfaceBound', 'SurfaceUnbound')
              AND NOT (COALESCE(authority.after_state ->> 'surface_materialization', 'false') = 'true'
                  AND authority.after_state ->> 'authority_key' IS NULL)
            ORDER BY authority.block_number DESC, authority.log_index DESC NULLS LAST,
                authority.normalized_event_id DESC
            LIMIT 1
        ) control ON control.after_state ->> 'authority_kind' = 'registry_only'
            AND control.event_kind <> 'SurfaceUnbound'
            AND ($2::uuid IS NOT NULL OR control.resource_id = ANY($1::uuid[]))
        JOIN LATERAL (
            SELECT grant_event.* FROM (SELECT probe.* FROM eligible probe
       WHERE NOT candidate.basenames
         AND probe.chain_id = candidate.chain_id AND probe.namespace = candidate.namespace
         AND probe.source_family LIKE 'ens\_v1\_%'
         AND probe.node_key = ANY(candidate.node_keys)
       UNION
       SELECT probe.* FROM eligible probe
       WHERE candidate.basenames
         AND probe.chain_id = candidate.chain_id AND probe.namespace = candidate.namespace
         AND probe.source_family LIKE 'basenames\_%'
         AND probe.node_key = ANY(candidate.node_keys)
       UNION
       SELECT probe.* FROM eligible probe
       WHERE probe.chain_id = candidate.chain_id AND probe.namespace = candidate.namespace
         AND probe.logical_name_id = ANY(candidate.logical_ids)) grant_event
            WHERE candidate.namespace = grant_event.namespace AND candidate.chain_id = grant_event.chain_id
         AND CASE WHEN candidate.node IS NOT NULL AND grant_event.node IS NOT NULL
             THEN candidate.node = grant_event.node
             ELSE candidate.logical_name_id = grant_event.logical_name_id OR EXISTS (
                 SELECT 1 FROM bigname_phase.name_surfaces surface
                 WHERE surface.logical_name_id = candidate.logical_name_id
                   AND surface.namespace = candidate.namespace AND surface.chain_id = candidate.chain_id
                   AND lower(surface.namehash) = grant_event.node
             ) OR EXISTS (
                 SELECT 1 FROM bigname_phase.name_surfaces surface
                 WHERE surface.logical_name_id = grant_event.logical_name_id
                   AND surface.namespace = grant_event.namespace AND surface.chain_id = grant_event.chain_id
                   AND lower(surface.namehash) = candidate.node
             ) END
              AND grant_event.event_kind = 'RegistrationGranted'
              AND grant_event.source_family IN ('ens_v1_registrar_l1', 'basenames_base_registrar')
            ORDER BY grant_event.block_number DESC, grant_event.log_index DESC NULLS LAST,
                grant_event.normalized_event_id DESC
            LIMIT 1
        ) lease ON lease.resource_id <> control.resource_id
        WHERE ($2::uuid IS NULL OR lease.resource_id = $2::uuid)
          AND NOT EXISTS (
              SELECT 1 FROM eligible release
              WHERE release.resource_id = lease.resource_id
                AND release.namespace = lease.namespace AND release.chain_id = lease.chain_id
                AND release.event_kind = 'RegistrationReleased'
                AND (release.block_number, COALESCE(release.log_index, -1))
                    >= (lease.block_number, COALESCE(lease.log_index, -1))
          )
