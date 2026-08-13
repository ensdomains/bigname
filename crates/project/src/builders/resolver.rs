mod alias_summary;
mod binding_summary;
mod permission_summary;

use sqlx::{Postgres, Transaction};

use crate::{
    Marker, ProjectError, Result, resolver_address::PERMISSION_CHANGED_RESOLVER_ADDRESS_VALUES,
};

const SUMMARY_SAMPLE_LIMIT: i32 = 100;

pub(super) async fn build(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    target: &Marker,
    full_rebuild: bool,
) -> Result<()> {
    binding_summary::stage(transaction, chain_id, SUMMARY_SAMPLE_LIMIT, full_rebuild).await?;
    alias_summary::stage(transaction, chain_id, SUMMARY_SAMPLE_LIMIT).await?;
    permission_summary::stage(transaction, chain_id, SUMMARY_SAMPLE_LIMIT, full_rebuild).await?;

    let resolver_build = format!(
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
        resolver_event_candidates AS (
            SELECT lower(CASE
                       WHEN event.event_kind = 'Upgraded'
                           THEN event.after_state ->> 'proxy_address'
                       WHEN event.event_kind = 'AliasChanged'
                           THEN COALESCE(
                               event.after_state ->> 'resolver',
                               event.before_state ->> 'resolver',
                               event.raw_fact_ref ->> 'emitting_address'
                           )
                   END) AS resolver_address,
                   event.source_family,
                   NULL::text AS classification_role,
                   3 AS priority
            FROM project_events event
            WHERE (
                      event.event_kind = 'Upgraded'
                  AND event.source_family = 'ens_v2_resolver_l1'
                  AND event.after_state ->> 'proxy_address' IS NOT NULL
                  AND btrim(event.after_state ->> 'proxy_address') <> ''
              ) OR (
                      event.event_kind = 'AliasChanged'
                  AND event.source_family IN (
                      'ens_v1_resolver_l1', 'ens_v2_resolver_l1',
                      'basenames_base_resolver'
                  )
                  AND COALESCE(
                      event.after_state ->> 'resolver',
                      event.before_state ->> 'resolver',
                      event.raw_fact_ref ->> 'emitting_address'
                  ) IS NOT NULL
                  AND btrim(COALESCE(
                      event.after_state ->> 'resolver',
                      event.before_state ->> 'resolver',
                      event.raw_fact_ref ->> 'emitting_address'
                  )) <> ''
              )
            UNION ALL
            SELECT lower(candidate.resolver_address),
                   CASE
                       WHEN event.source_family LIKE 'ens_v2_%'
                           THEN 'ens_v2_resolver_l1'
                       WHEN event.source_family LIKE 'basenames_%'
                           THEN 'basenames_base_resolver'
                       ELSE 'ens_v1_resolver_l1'
                   END,
                   NULL::text,
                   4
            FROM project_events event
            CROSS JOIN LATERAL (VALUES
                {PERMISSION_CHANGED_RESOLVER_ADDRESS_VALUES},
                (CASE WHEN event.source_family IN (
                    'ens_v1_resolver_l1', 'ens_v2_resolver_l1',
                    'basenames_base_resolver'
                ) THEN event.raw_fact_ref ->> 'emitting_address' END)
            ) candidate(resolver_address)
            WHERE event.event_kind = 'PermissionChanged'
              AND candidate.resolver_address IS NOT NULL
              AND btrim(candidate.resolver_address) <> ''
            UNION ALL
            SELECT lower(candidate.resolver_address),
                   CASE
                       WHEN event.source_family LIKE 'ens_v2_%'
                           THEN 'ens_v2_resolver_l1'
                       WHEN event.source_family LIKE 'basenames_%'
                           THEN 'basenames_base_resolver'
                       ELSE 'ens_v1_resolver_l1'
                   END,
                   NULL::text,
                   3
            FROM project_events event
            CROSS JOIN LATERAL (
                VALUES (event.after_state ->> 'resolver'),
                       (event.before_state ->> 'resolver')
            ) candidate(resolver_address)
            WHERE event.event_kind = 'ResolverChanged'
              AND candidate.resolver_address IS NOT NULL
              AND btrim(candidate.resolver_address) <> ''
        ),
        observed AS (
            SELECT resolver_address, source_family,
                   NULL::text AS classification_role, 2 AS priority
            FROM project_resolver_binding_summary
            UNION ALL
            SELECT * FROM resolver_event_candidates
        ),
        retained_permission_candidates AS (
            SELECT permission.resolver_address,
                   CASE
                       WHEN evidence.item ->> 'source_family' LIKE 'ens_v2_%'
                           THEN 'ens_v2_resolver_l1'
                       WHEN evidence.item ->> 'source_family' LIKE 'basenames_%'
                           THEN 'basenames_base_resolver'
                       ELSE 'ens_v1_resolver_l1'
                   END AS source_family,
                   NULL::text AS classification_role,
                   4 AS priority
            FROM project_resolver_permission_rows permission
            CROSS JOIN LATERAL jsonb_array_elements(COALESCE(
                permission.provenance -> 'permission_manifest_versions', '[]'::jsonb
            )) evidence(item)
            JOIN project_resolver_permission_summary summary
              ON summary.resolver_address = permission.resolver_address
             AND summary.item_count > 0
            WHERE NOT $4
              AND NOT EXISTS (
                  SELECT 1 FROM project_scope_resources scope
                  WHERE scope.resource_id = permission.resource_id
              )
            -- A retained permission may be the only surviving resolver evidence during redo.
            -- Reconstruct family candidates from its PermissionChanged provenance; current
            -- manifests and readable upgrade history below determine the role without copying
            -- prior classification.
        ),
        candidates AS (
            SELECT DISTINCT ON (combined.resolver_address)
                   combined.resolver_address,
                   combined.source_family,
                   combined.classification_role
            FROM (
                SELECT * FROM discovered WHERE source_family IS NOT NULL
                UNION ALL SELECT * FROM observed
                UNION ALL SELECT * FROM retained_permission_candidates
            ) combined
            WHERE combined.resolver_address <>
                  '0x0000000000000000000000000000000000000000'
              AND (
                  $4 OR EXISTS (
                      SELECT 1 FROM project_scope_resolvers scope
                      WHERE lower(scope.resolver_address) =
                            lower(combined.resolver_address)
                  )
              )
              AND NOT EXISTS (
                  SELECT 1 FROM project_scope_resolver_passthrough passthrough
                  WHERE lower(passthrough.resolver_address) =
                        lower(combined.resolver_address)
              )
            ORDER BY combined.resolver_address,
                     combined.priority,
                     combined.source_family
        ),
        upgrade_ranked AS (
            SELECT event.*,
                   lower(event.after_state ->> 'proxy_address') AS resolver_address,
                   row_number() OVER (
                       PARTITION BY event.source_family,
                                    lower(event.after_state ->> 'proxy_address')
                       ORDER BY event.block_number DESC NULLS LAST,
                                event.transaction_index DESC NULLS LAST,
                                event.log_index DESC NULLS LAST,
                                event.normalized_event_id DESC
                   ) AS latest_rank
            FROM project_events event
            WHERE event.event_kind = 'Upgraded'
              AND event.after_state ->> 'proxy_address' IS NOT NULL
        ),
        latest_upgrades AS (
            SELECT * FROM upgrade_ranked WHERE latest_rank = 1
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
                           FROM jsonb_array_elements(COALESCE(
                               manifest.manifest_payload -> 'contracts', '[]'::jsonb
                           )) declaration
                           WHERE lower(declaration ->> 'address') = candidate.resolver_address
                             AND (
                                 declaration ->> 'start_block' IS NULL
                                 OR (declaration ->> 'start_block')::bigint <= $2
                             )
                           LIMIT 1
                       ),
                       (
                           SELECT implementation ->> 'role'
                           FROM jsonb_array_elements(COALESCE(
                               manifest.manifest_payload -> 'resolver_implementations',
                               '[]'::jsonb
                           )) implementation
                           WHERE lower(implementation ->> 'address') =
                                 lower(upgrade.after_state ->> 'implementation')
                           LIMIT 1
                       )
                   ) AS classification_role,
                   EXISTS (
                       SELECT 1
                       FROM jsonb_array_elements(COALESCE(
                           manifest.manifest_payload -> 'contracts', '[]'::jsonb
                       )) declaration
                       WHERE lower(declaration ->> 'address') = candidate.resolver_address
                         AND (
                             declaration ->> 'start_block' IS NULL
                             OR (declaration ->> 'start_block')::bigint <= $2
                         )
                   ) AS exact_declared,
                   EXISTS (
                       SELECT 1
                       FROM jsonb_array_elements(COALESCE(
                           manifest.manifest_payload -> 'resolver_implementations',
                           '[]'::jsonb
                       )) implementation
                       WHERE lower(implementation ->> 'address') =
                             lower(upgrade.after_state ->> 'implementation')
                   ) AS upgraded_to_declared
            FROM candidates candidate
            JOIN project_manifests manifest
              ON manifest.source_family = candidate.source_family
            LEFT JOIN latest_upgrades upgrade
              ON upgrade.source_family = candidate.source_family
             AND upgrade.resolver_address = candidate.resolver_address
        ),
        supported AS (
            SELECT classified.*,
                   CASE WHEN source_family = 'ens_v2_resolver_l1'
                        THEN upgraded_to_declared ELSE exact_declared END AS supported,
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
                   END AS support_reason
            FROM classified
        ),
        summarized AS (
            SELECT supported.*,
                   supported.supported
                       AND supported.source_family <> 'ens_v1_resolver_l1'
                           AS enumeration_supported,
                   CASE
                       WHEN supported.supported
                        AND supported.source_family = 'ens_v1_resolver_l1'
                           THEN 'resolver_binding_enumeration_not_projected'
                       ELSE support_reason
                   END AS enumeration_reason,
                   COALESCE(binding.item_count, 0) AS binding_count,
                   COALESCE(binding.items, '[]'::jsonb) AS binding_items,
                   COALESCE(binding.alias_item_count, 0) +
                       COALESCE(alias.event_count, 0) AS alias_count,
                   jsonb_path_query_array(
                       COALESCE(binding.alias_items, '[]'::jsonb) ||
                           COALESCE(alias.items, '[]'::jsonb),
                       format('$[0 to %s]', $5::integer - 1)::jsonpath
                   ) AS alias_items,
                   COALESCE(permission.item_count, 0) AS permission_count,
                   COALESCE(permission.items, '[]'::jsonb) AS permission_items,
                   COALESCE(permission.role_count, 0) AS role_count,
                   COALESCE(permission.role_items, '[]'::jsonb) AS role_items,
                   COALESCE(alias.event_count, 0) AS alias_event_count,
                   COALESCE(permission.event_count, 0) AS permission_event_count
            FROM supported
            LEFT JOIN project_resolver_binding_summary binding USING (resolver_address)
            LEFT JOIN project_resolver_alias_summary alias USING (resolver_address)
            LEFT JOIN project_resolver_permission_summary permission USING (resolver_address)
        )
        INSERT INTO project_stage_resolver_current (
            chain_id, resolver_address, declared_summary, support_status,
            unsupported_reason, provenance, chain_positions,
            canonicality_summary, manifest_version
        )
        SELECT $1,
               resolver_address,
               jsonb_build_object(
                   'classification', jsonb_strip_nulls(jsonb_build_object(
                       'source_family', source_family,
                       'role', classification_role,
                       'basis', CASE WHEN source_family = 'ens_v2_resolver_l1'
                           THEN 'erc1967_upgraded_history'
                           ELSE 'manifest_declared_address' END,
                       'implementation', implementation
                   )),
                   'bindings', CASE WHEN enumeration_supported THEN jsonb_build_object(
                       'status', 'supported', 'count', binding_count,
                       'total_count', binding_count, 'sample_limit', $5,
                       'sample_count', jsonb_array_length(binding_items),
                       'truncated', binding_count > jsonb_array_length(binding_items),
                       'items', binding_items
                   ) ELSE jsonb_build_object(
                       'status', 'unsupported', 'unsupported_reason', enumeration_reason
                   ) END,
                   'aliases', CASE WHEN enumeration_supported THEN jsonb_build_object(
                       'status', 'supported', 'count', alias_count,
                       'total_count', alias_count, 'sample_limit', $5,
                       'sample_count', jsonb_array_length(alias_items),
                       'truncated', alias_count > jsonb_array_length(alias_items),
                       'items', alias_items
                   ) ELSE jsonb_build_object(
                       'status', 'unsupported', 'unsupported_reason', enumeration_reason
                   ) END,
                   'permissions', CASE WHEN enumeration_supported THEN jsonb_build_object(
                       'status', 'supported', 'count', permission_count,
                       'total_count', permission_count, 'sample_limit', $5,
                       'sample_count', jsonb_array_length(permission_items),
                       'truncated', permission_count > jsonb_array_length(permission_items),
                       'items', permission_items
                   ) ELSE jsonb_build_object(
                       'status', 'unsupported', 'unsupported_reason', enumeration_reason
                   ) END,
                   'role_holders', CASE WHEN enumeration_supported THEN jsonb_build_object(
                       'status', 'supported', 'count', role_count,
                       'total_count', role_count, 'sample_limit', $5,
                       'sample_count', jsonb_array_length(role_items),
                       'truncated', role_count > jsonb_array_length(role_items),
                       'items', role_items
                   ) ELSE jsonb_build_object(
                       'status', 'unsupported', 'unsupported_reason', enumeration_reason
                   ) END,
                   'event_summary', CASE WHEN enumeration_supported THEN jsonb_build_object(
                       'status', 'supported',
                       'count', binding_count + alias_event_count + permission_event_count,
                       'by_kind', jsonb_strip_nulls(jsonb_build_object(
                           'ResolverChanged', NULLIF(binding_count, 0),
                           'AliasChanged', NULLIF(alias_event_count, 0),
                           'PermissionChanged', NULLIF(permission_event_count, 0)
                       ))
                   ) ELSE jsonb_build_object(
                       'status', 'unsupported', 'unsupported_reason', enumeration_reason
                   ) END,
                   'coverage', jsonb_build_object(
                       'status', 'projected', 'exhaustiveness', 'not_asserted'
                   )
               ),
               CASE WHEN supported THEN 'supported' ELSE 'unsupported' END,
               support_reason,
               jsonb_strip_nulls(jsonb_build_object(
                   'chain_id', $1, 'manifest_id', manifest_id,
                   'manifest_event_id', manifest_event_id,
                   'upgrade_event_id', upgrade_event_id
               )),
               jsonb_strip_nulls(jsonb_build_object(
                   'block_number', upgrade_block_number,
                   'block_hash', upgrade_block_hash,
                   'target_block_number', $2, 'target_block_hash', $3
               )),
               jsonb_build_object(
                   'state', 'canonical_lineage',
                   'target_block_number', $2, 'target_block_hash', $3
               ),
               manifest_version
        FROM summarized
        UNION ALL
        SELECT current.chain_id,
               current.resolver_address,
               current.declared_summary,
               current.support_status,
               current.unsupported_reason,
               current.provenance,
               current.chain_positions || jsonb_build_object(
                   'target_block_number', $2,
                   'target_block_hash', $3
               ),
               current.canonicality_summary || jsonb_build_object(
                   'state', 'canonical_lineage',
                   'target_block_number', $2,
                   'target_block_hash', $3
               ),
               current.manifest_version
        FROM resolver_current current
        JOIN project_scope_resolver_passthrough passthrough
          ON lower(passthrough.resolver_address) = lower(current.resolver_address)
        WHERE current.chain_id = $1
        ORDER BY resolver_address
        "#,
    );
    sqlx::query(&resolver_build)
        .bind(chain_id)
        .bind(target.number)
        .bind(&target.hash)
        .bind(full_rebuild)
        .bind(SUMMARY_SAMPLE_LIMIT)
        .execute(&mut **transaction)
        .await
        .map_err(|error| ProjectError::database("failed to build resolver_current", error))?;
    Ok(())
}
