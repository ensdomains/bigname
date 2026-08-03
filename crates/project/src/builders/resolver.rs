use sqlx::{Postgres, Transaction};

use crate::{Marker, ProjectError, Result};

pub(super) async fn build(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    target: &Marker,
    full_rebuild: bool,
) -> Result<()> {
    sqlx::query(
        r#"
        WITH discovered AS (
            SELECT lower(address.address) AS resolver_address,
                   CASE origin.source_family
                       WHEN 'ens_v1_registry_l1' THEN 'ens_v1_resolver_l1'
                       WHEN 'ens_v1_resolver_l1' THEN 'ens_v1_resolver_l1'
                       WHEN 'ens_v2_registry_l1' THEN 'ens_v2_resolver_l1'
                       WHEN 'ens_v2_resolver_l1' THEN 'ens_v2_resolver_l1'
                       WHEN 'basenames_base_registry' THEN 'basenames_base_resolver'
                       WHEN 'basenames_base_resolver' THEN 'basenames_base_resolver'
                   END AS source_family,
                   NULL::text AS classification_role,
                   1 AS priority
            FROM discovery_edges edge
            JOIN contract_instance_addresses address
              ON address.contract_instance_id = edge.to_contract_instance_id
             AND address.chain_id = edge.chain_id
            LEFT JOIN project_manifests origin
              ON origin.manifest_id = edge.source_manifest_id
            WHERE edge.chain_id = $1
              AND edge.edge_kind = 'resolver'
              AND edge.canonicality_state IN ('canonical', 'safe', 'finalized')
              AND (edge.active_from_block_number IS NULL OR edge.active_from_block_number <= $2)
              AND (edge.active_to_block_number IS NULL OR edge.active_to_block_number > $2)
              AND edge.deactivated_at IS NULL
              AND (
                  edge.active_from_block_hash IS NULL
                  OR EXISTS (
                      SELECT 1 FROM chain_lineage lineage
                      WHERE lineage.chain_id = edge.chain_id
                        AND lineage.block_number = edge.active_from_block_number
                        AND lineage.block_hash = edge.active_from_block_hash
                        AND lineage.canonicality_state IN ('canonical', 'safe', 'finalized')
                  )
              )
              AND (address.active_from_block_number IS NULL OR address.active_from_block_number <= $2)
              AND (address.active_to_block_number IS NULL OR address.active_to_block_number > $2)
              AND address.deactivated_at IS NULL
              AND (
                  address.active_from_block_hash IS NULL
                  OR EXISTS (
                      SELECT 1 FROM chain_lineage lineage
                      WHERE lineage.chain_id = address.chain_id
                        AND lineage.block_number = address.active_from_block_number
                        AND lineage.block_hash = address.active_from_block_hash
                        AND lineage.canonicality_state IN ('canonical', 'safe', 'finalized')
                  )
              )
        ),
        latest_bindings AS (
            SELECT DISTINCT ON (event.logical_name_id, event.resource_id)
                   event.*
            FROM project_events event
            WHERE event.event_kind = 'ResolverChanged'
              AND event.logical_name_id IS NOT NULL
              AND event.resource_id IS NOT NULL
            ORDER BY event.logical_name_id,
                     event.resource_id,
                     event.block_number DESC NULLS LAST,
                     event.transaction_index DESC NULLS LAST,
                     event.log_index DESC NULLS LAST,
                     event.normalized_event_id DESC
        ),
        current_resolvers AS (
            SELECT lower(event.after_state ->> 'resolver') AS resolver_address,
                   CASE
                       WHEN event.source_family LIKE 'ens_v2_%' THEN 'ens_v2_resolver_l1'
                       WHEN event.source_family LIKE 'basenames_%' THEN 'basenames_base_resolver'
                       ELSE 'ens_v1_resolver_l1'
                   END AS source_family,
                   NULL::text AS classification_role,
                   2 AS priority
            FROM latest_bindings event
            JOIN project_bindings binding
              ON binding.logical_name_id = event.logical_name_id
             AND binding.resource_id = event.resource_id
            WHERE event.after_state ->> 'resolver' IS NOT NULL
              AND btrim(event.after_state ->> 'resolver') <> ''
        ),
        resolver_events AS (
            SELECT lower(candidate.resolver_address) AS resolver_address,
                   event.source_family,
                   NULL::text AS classification_role,
                   3 AS priority
            FROM project_events event
            CROSS JOIN LATERAL (
                SELECT CASE
                    WHEN event.event_kind = 'Upgraded'
                        THEN event.after_state ->> 'proxy_address'
                    WHEN event.event_kind = 'AliasChanged'
                        THEN event.after_state ->> 'resolver'
                    WHEN event.event_kind = 'PermissionChanged'
                        THEN event.raw_fact_ref ->> 'emitting_address'
                END AS resolver_address
            ) candidate
            WHERE candidate.resolver_address IS NOT NULL
              AND btrim(candidate.resolver_address) <> ''
              AND (
                  (
                      event.event_kind = 'Upgraded'
                      AND event.source_family = 'ens_v2_resolver_l1'
                  )
                  OR (
                      event.event_kind IN ('AliasChanged', 'PermissionChanged')
                      AND event.source_family IN (
                          'ens_v1_resolver_l1',
                          'ens_v2_resolver_l1',
                          'basenames_base_resolver'
                      )
                  )
              )
        ),
        observed AS (
            SELECT * FROM current_resolvers
            UNION ALL
            SELECT * FROM resolver_events
        ),
        candidates AS (
            SELECT DISTINCT ON (resolver_address)
                   resolver_address, source_family, classification_role
            FROM (
                SELECT * FROM discovered WHERE source_family IS NOT NULL
                UNION ALL SELECT * FROM observed
            ) combined
            WHERE resolver_address <>
                  '0x0000000000000000000000000000000000000000'
              AND (
                  $4 OR EXISTS (
                      SELECT 1 FROM project_scope_resolvers scope
                      WHERE lower(scope.resolver_address) =
                            lower(combined.resolver_address)
                  )
              )
            ORDER BY resolver_address, priority, source_family
        ),
        classified AS (
            SELECT candidate.resolver_address,
                   candidate.source_family,
                   manifest.manifest_id,
                   manifest.manifest_version,
                   manifest.manifest_payload,
                   manifest.manifest_event_id,
                   upgrade.normalized_event_id AS upgrade_event_id,
                   upgrade.block_number AS upgrade_block_number,
                   upgrade.block_hash AS upgrade_block_hash,
                   upgrade.after_state ->> 'implementation' AS implementation,
                   COALESCE(
                       candidate.classification_role,
                       (
                           SELECT declaration ->> 'role'
                           FROM jsonb_array_elements(
                               COALESCE(
                                   manifest.manifest_payload -> 'contracts',
                                   '[]'::jsonb
                               )
                           ) declaration
                           WHERE lower(declaration ->> 'address') =
                                 candidate.resolver_address
                             AND (
                                 declaration ->> 'start_block' IS NULL
                                 OR (declaration ->> 'start_block')::bigint <= $2
                             )
                           LIMIT 1
                       ),
                       (
                           SELECT implementation ->> 'role'
                           FROM jsonb_array_elements(
                               COALESCE(
                                   manifest.manifest_payload
                                       -> 'resolver_implementations',
                                   '[]'::jsonb
                               )
                           ) implementation
                           WHERE lower(implementation ->> 'address') =
                                 lower(upgrade.after_state ->> 'implementation')
                           LIMIT 1
                       )
                   ) AS classification_role,
                   EXISTS (
                       SELECT 1
                       FROM jsonb_array_elements(
                           COALESCE(
                               manifest.manifest_payload -> 'contracts',
                               '[]'::jsonb
                           )
                       ) declaration
                       WHERE lower(declaration ->> 'address') = candidate.resolver_address
                         AND (
                             declaration ->> 'start_block' IS NULL
                             OR (declaration ->> 'start_block')::bigint <= $2
                         )
                   ) AS exact_declared,
                   EXISTS (
                       SELECT 1
                       FROM jsonb_array_elements(
                           COALESCE(
                               manifest.manifest_payload -> 'resolver_implementations',
                               '[]'::jsonb
                           )
                       ) implementation
                       WHERE lower(implementation ->> 'address') =
                             lower(upgrade.after_state ->> 'implementation')
                   ) AS upgraded_to_declared
            FROM candidates candidate
            JOIN project_manifests manifest
              ON manifest.source_family = candidate.source_family
            LEFT JOIN LATERAL (
                SELECT event.*
                FROM project_events event
                WHERE event.event_kind = 'Upgraded'
                  AND event.source_family = candidate.source_family
                  AND lower(event.after_state ->> 'proxy_address') =
                      candidate.resolver_address
                ORDER BY event.block_number DESC NULLS LAST,
                         event.transaction_index DESC NULLS LAST,
                         event.log_index DESC NULLS LAST,
                         event.normalized_event_id DESC
                LIMIT 1
            ) upgrade ON TRUE
        )
        INSERT INTO project_stage_resolver_current (
            chain_id,
            resolver_address,
            declared_summary,
            support_status,
            unsupported_reason,
            provenance,
            chain_positions,
            canonicality_summary,
            manifest_version
        )
        SELECT $1,
               resolver_address,
               jsonb_build_object(
                   'classification', jsonb_strip_nulls(jsonb_build_object(
                       'source_family', source_family,
                       'role', classification_role,
                       'basis', CASE
                           WHEN source_family = 'ens_v2_resolver_l1'
                               THEN 'erc1967_upgraded_history'
                           ELSE 'manifest_declared_address'
                       END,
                       'implementation', implementation
                   )),
                   'bindings', CASE WHEN enumeration.supported THEN jsonb_build_object(
                       'status', 'supported',
                       'count', binding_summary.item_count,
                       'items', binding_summary.items
                   ) ELSE jsonb_build_object(
                       'status', 'unsupported',
                       'unsupported_reason', enumeration.unsupported_reason
                   ) END,
                   'aliases', CASE WHEN enumeration.supported THEN jsonb_build_object(
                       'status', 'supported',
                       'count', jsonb_array_length(
                           binding_summary.alias_items || alias_summary.items
                       ),
                       'items', binding_summary.alias_items || alias_summary.items
                   ) ELSE jsonb_build_object(
                       'status', 'unsupported',
                       'unsupported_reason', enumeration.unsupported_reason
                   ) END,
                   'permissions', CASE WHEN enumeration.supported THEN jsonb_build_object(
                       'status', 'supported',
                       'count', permission_summary.item_count,
                       'items', permission_summary.items
                   ) ELSE jsonb_build_object(
                       'status', 'unsupported',
                       'unsupported_reason', enumeration.unsupported_reason
                   ) END,
                   'role_holders', CASE WHEN enumeration.supported THEN jsonb_build_object(
                       'status', 'supported',
                       'count', role_summary.item_count,
                       'items', role_summary.items
                   ) ELSE jsonb_build_object(
                       'status', 'unsupported',
                       'unsupported_reason', enumeration.unsupported_reason
                   ) END,
                   'event_summary', CASE WHEN enumeration.supported THEN jsonb_build_object(
                       'status', 'supported',
                       'count', binding_summary.item_count +
                                alias_summary.event_count +
                                permission_summary.event_count,
                       'by_kind', jsonb_strip_nulls(jsonb_build_object(
                           'ResolverChanged', NULLIF(binding_summary.item_count, 0),
                           'AliasChanged', NULLIF(alias_summary.event_count, 0),
                           'PermissionChanged', NULLIF(
                               permission_summary.event_count, 0
                           )
                       ))
                   ) ELSE jsonb_build_object(
                       'status', 'unsupported',
                       'unsupported_reason', enumeration.unsupported_reason
                   ) END,
                   'coverage', jsonb_build_object(
                       'status', 'projected',
                       'exhaustiveness', 'not_asserted'
                   )
               ),
               CASE WHEN support.supported THEN 'supported' ELSE 'unsupported' END,
               support.unsupported_reason,
               jsonb_strip_nulls(jsonb_build_object(
                   'chain_id', $1,
                   'manifest_id', manifest_id,
                   'manifest_event_id', manifest_event_id,
                   'upgrade_event_id', upgrade_event_id
               )),
               jsonb_strip_nulls(jsonb_build_object(
                   'block_number', upgrade_block_number,
                   'block_hash', upgrade_block_hash,
                   'target_block_number', $2,
                   'target_block_hash', $3
               )),
               jsonb_build_object(
                   'state', 'canonical_lineage',
                   'target_block_number', $2,
                   'target_block_hash', $3
               ),
               manifest_version
        FROM classified
        CROSS JOIN LATERAL (
            SELECT CASE
                       WHEN source_family = 'ens_v2_resolver_l1'
                           THEN upgraded_to_declared
                       ELSE exact_declared
                   END AS supported,
                   CASE
                       WHEN source_family = 'ens_v2_resolver_l1'
                        AND upgrade_event_id IS NULL
                           THEN 'resolver_upgrade_not_observed'
                       WHEN source_family = 'ens_v2_resolver_l1'
                        AND NOT upgraded_to_declared
                           THEN 'resolver_implementation_not_declared'
                       WHEN source_family <> 'ens_v2_resolver_l1'
                        AND NOT exact_declared
                           THEN 'resolver_not_declared'
                       ELSE NULL
                   END AS unsupported_reason
        ) support
        CROSS JOIN LATERAL (
            SELECT support.supported
                       AND source_family <> 'ens_v1_resolver_l1' AS supported,
                   CASE
                       WHEN support.supported
                        AND source_family = 'ens_v1_resolver_l1'
                           THEN 'resolver_binding_enumeration_not_projected'
                       ELSE support.unsupported_reason
                   END AS unsupported_reason
        ) enumeration
        LEFT JOIN LATERAL (
            SELECT count(*)::integer AS item_count,
                   COALESCE(jsonb_agg(item ORDER BY raw_name, logical_name_id),
                            '[]'::jsonb) AS items,
                   COALESCE(jsonb_agg(item ORDER BY raw_name, logical_name_id)
                            FILTER (WHERE binding_kind = 'resolver_alias_path'),
                            '[]'::jsonb) AS alias_items
            FROM (
                SELECT surface.logical_name_id,
                       surface.raw_name,
                       binding.binding_kind,
                       jsonb_build_object(
                           'logical_name_id', surface.logical_name_id,
                           'canonical_display_name', surface.raw_name,
                           'normalized_name', surface.raw_name,
                           'raw_name', surface.raw_name,
                           'namehash', surface.namehash,
                           'resource_id', binding.resource_id,
                           'surface_binding_id', binding.surface_binding_id,
                           'binding_kind', binding.binding_kind
                       ) AS item
                FROM latest_bindings event
                JOIN project_bindings binding
                  ON binding.logical_name_id = event.logical_name_id
                 AND binding.resource_id = event.resource_id
                JOIN project_surfaces surface
                  ON surface.logical_name_id = binding.logical_name_id
                WHERE event.chain_id = $1
                  AND lower(event.after_state ->> 'resolver') =
                      classified.resolver_address
            ) binding_items
        ) binding_summary ON TRUE
        LEFT JOIN LATERAL (
            SELECT count(*)::integer AS event_count,
                   COALESCE(jsonb_agg(jsonb_strip_nulls(jsonb_build_object(
                       'logical_name_id', event.logical_name_id,
                       'resource_id', event.resource_id,
                       'binding_kind', 'resolver_alias_path',
                       'alias_state', COALESCE(
                           event.after_state -> 'alias_state', '"active"'::jsonb
                       ),
                       'active', COALESCE(
                           event.after_state -> 'active', 'true'::jsonb
                       ),
                       'chain_id', event.chain_id,
                       'resolver_address', classified.resolver_address,
                       'from_dns_encoded_name',
                           event.after_state -> 'from_dns_encoded_name',
                       'to_dns_encoded_name',
                           event.after_state -> 'to_dns_encoded_name',
                       'from_name', event.after_state -> 'from_name',
                       'to_name', event.after_state -> 'to_name',
                       'to_logical_name_id',
                           event.after_state -> 'to_logical_name_id',
                       'to_resource_id', event.after_state -> 'to_resource_id',
                       'latest_event_kind', 'AliasChanged'
                   )) ORDER BY event.logical_name_id, event.normalized_event_id),
                   '[]'::jsonb) AS items
            FROM (
                SELECT DISTINCT ON (alias_identity.value) candidate.*
                FROM project_events candidate
                CROSS JOIN LATERAL (
                    SELECT COALESCE(
                               candidate.logical_name_id,
                               candidate.after_state ->> 'from_logical_name_id',
                               candidate.before_state ->> 'from_logical_name_id',
                               candidate.after_state ->> 'from_namehash',
                               candidate.before_state ->> 'from_namehash',
                               candidate.after_state ->> 'from_dns_encoded_name',
                               candidate.before_state ->> 'from_dns_encoded_name',
                               candidate.after_state ->> 'from_name',
                               candidate.before_state ->> 'from_name',
                               candidate.event_identity
                           ) AS value
                ) alias_identity
                WHERE candidate.event_kind = 'AliasChanged'
                  AND candidate.chain_id = $1
                  AND lower(COALESCE(
                        candidate.after_state ->> 'resolver',
                        candidate.before_state ->> 'resolver',
                        candidate.raw_fact_ref ->> 'emitting_address'
                      )) = classified.resolver_address
                ORDER BY alias_identity.value,
                         candidate.block_number DESC NULLS LAST,
                         candidate.transaction_index DESC NULLS LAST,
                         candidate.log_index DESC NULLS LAST,
                         candidate.normalized_event_id DESC
            ) event
            WHERE COALESCE((event.after_state ->> 'active')::boolean, true)
        ) alias_summary ON TRUE
        LEFT JOIN LATERAL (
            SELECT count(*)::integer AS item_count,
                   COALESCE(sum(jsonb_array_length(COALESCE(
                       permission.provenance -> 'normalized_event_ids', '[]'::jsonb
                   )))::integer, 0) AS event_count,
                   COALESCE(jsonb_agg(jsonb_build_object(
                       'resource_id', permission.resource_id,
                       'subject', permission.subject,
                       'effective_powers', permission.effective_powers,
                       'grant_source', permission.grant_source,
                       'revocation_source', permission.revocation_source
                   ) ORDER BY permission.subject, permission.resource_id),
                   '[]'::jsonb) AS items
            FROM project_stage_permissions_current permission
            WHERE permission.scope_kind = 'resolver'
              AND permission.scope_detail ->> 'chain_id' = $1
              AND lower(permission.scope_detail ->> 'resolver_address') =
                  classified.resolver_address
        ) permission_summary ON TRUE
        LEFT JOIN LATERAL (
            SELECT count(*)::integer AS item_count,
                   COALESCE(jsonb_agg(jsonb_build_object(
                       'subject', subject,
                       'resource_count', resource_count,
                       'permission_row_count', permission_row_count,
                       'effective_powers', effective_powers,
                       'resource_ids', resource_ids
                   ) ORDER BY subject), '[]'::jsonb) AS items
            FROM (
                SELECT permission.subject,
                       count(DISTINCT permission.resource_id)::integer AS resource_count,
                       count(*)::integer AS permission_row_count,
                       jsonb_agg(DISTINCT power.value) AS effective_powers,
                       jsonb_agg(DISTINCT permission.resource_id) AS resource_ids
                FROM project_stage_permissions_current permission
                CROSS JOIN LATERAL jsonb_array_elements_text(
                    permission.effective_powers
                ) power(value)
                WHERE permission.scope_kind = 'resolver'
                  AND permission.scope_detail ->> 'chain_id' = $1
                  AND lower(permission.scope_detail ->> 'resolver_address') =
                      classified.resolver_address
                GROUP BY permission.subject
            ) holders
        ) role_summary ON TRUE
        ORDER BY resolver_address
        "#,
    )
    .bind(chain_id)
    .bind(target.number)
    .bind(&target.hash)
    .bind(full_rebuild)
    .execute(&mut **transaction)
    .await
    .map_err(|error| ProjectError::database("failed to build resolver_current", error))?;
    Ok(())
}
