use sqlx::{Postgres, Transaction};

use crate::{ProjectError, Result};

pub(super) async fn stage(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    sample_limit: i32,
    full_rebuild: bool,
) -> Result<()> {
    // Resolver bindings depend on each name's current pointer, not its historical pointers.
    // Merge untouched name_current rows with the rebuilt name slice; both expose that pointer in
    // declared_summary, so a resolver row never needs every dependent's ResolverChanged history.
    sqlx::query(
        r#"
        CREATE TEMP TABLE project_resolver_binding_summary ON COMMIT DROP AS
        WITH retained_names AS (
            SELECT current.logical_name_id,
                   current.resource_id,
                   current.binding_kind,
                   current.raw_name,
                   current.namehash,
                   current.surface_binding_id,
                   lower(current.declared_summary #>> '{resolver,address}')
                       AS resolver_address,
                   resolver.declared_summary #>> '{classification,source_family}'
                       AS source_family
            FROM name_current current
            LEFT JOIN resolver_current resolver
              ON resolver.chain_id = current.declared_summary #>> '{resolver,chain_id}'
             AND lower(resolver.resolver_address) =
                 lower(current.declared_summary #>> '{resolver,address}')
            WHERE NOT $2
              AND current.declared_summary #>> '{resolver,chain_id}' = $3
              AND EXISTS (
                  SELECT 1 FROM project_scope_resolvers scope
                  WHERE lower(scope.resolver_address) = lower(
                      current.declared_summary #>> '{resolver,address}'
                  )
              )
              AND NOT EXISTS (
                  SELECT 1 FROM project_scope_names scope
                  WHERE scope.logical_name_id = current.logical_name_id
              )
              AND NOT EXISTS (
                  SELECT 1 FROM project_scope_resources scope
                  WHERE scope.resource_id = current.resource_id
              )
              AND NOT EXISTS (
                  SELECT 1 FROM project_scope_resolver_passthrough passthrough
                  WHERE lower(passthrough.resolver_address) =
                        lower(current.declared_summary #>> '{resolver,address}')
              )
        ), staged_names AS (
            SELECT staged.logical_name_id,
                   staged.resource_id,
                   staged.binding_kind,
                   staged.raw_name,
                   staged.namehash,
                   staged.surface_binding_id,
                   lower(staged.declared_summary #>> '{resolver,address}')
                       AS resolver_address,
                   COALESCE(
                       resolver.declared_summary #>> '{classification,source_family}',
                       pointer.source_family
                   ) AS source_family
            FROM project_stage_name_current staged
            LEFT JOIN resolver_current resolver
              ON resolver.chain_id = staged.declared_summary #>> '{resolver,chain_id}'
             AND lower(resolver.resolver_address) =
                 lower(staged.declared_summary #>> '{resolver,address}')
            LEFT JOIN LATERAL (
                SELECT CASE
                           WHEN event.source_family LIKE 'ens_v2_%'
                               THEN 'ens_v2_resolver_l1'
                           WHEN event.source_family LIKE 'basenames_%'
                               THEN 'basenames_base_resolver'
                           ELSE 'ens_v1_resolver_l1'
                       END AS source_family
                FROM project_events event
                WHERE event.logical_name_id = staged.logical_name_id
                  AND event.event_kind = 'ResolverChanged'
                  AND (
                      staged.resource_id IS NULL
                      OR event.resource_id = staged.resource_id
                  )
                ORDER BY event.block_number DESC NULLS LAST,
                         event.transaction_index DESC NULLS LAST,
                         event.log_index DESC NULLS LAST,
                         event.normalized_event_id DESC
                LIMIT 1
            ) pointer ON TRUE
            WHERE staged.declared_summary #>> '{resolver,chain_id}' = $3
              AND (
                    $2 OR EXISTS (
                        SELECT 1 FROM project_scope_resolvers scope
                        WHERE lower(scope.resolver_address) = lower(
                            staged.declared_summary #>> '{resolver,address}'
                        )
                    )
                  )
              AND NOT EXISTS (
                  SELECT 1 FROM project_scope_resolver_passthrough passthrough
                  WHERE lower(passthrough.resolver_address) =
                        lower(staged.declared_summary #>> '{resolver,address}')
              )
        ), projected_names AS (
            SELECT * FROM retained_names
            UNION ALL
            SELECT * FROM staged_names
        ), binding_items AS (
            SELECT name.*,
                   jsonb_build_object(
                       'logical_name_id', name.logical_name_id,
                       'canonical_display_name', name.raw_name,
                       'normalized_name', name.raw_name,
                       'raw_name', name.raw_name,
                       'namehash', name.namehash,
                       'resource_id', name.resource_id,
                       'surface_binding_id', name.surface_binding_id,
                       'binding_kind', name.binding_kind
                   ) AS item
            FROM projected_names name
            WHERE name.resolver_address IS NOT NULL
              AND btrim(name.resolver_address) <> ''
              AND name.resolver_address <>
                  '0x0000000000000000000000000000000000000000'
        ), ranked AS (
            SELECT binding_items.*,
                   row_number() OVER (
                       PARTITION BY resolver_address
                       ORDER BY raw_name, logical_name_id, resource_id
                   ) AS item_rank,
                   row_number() OVER (
                       PARTITION BY resolver_address,
                                    binding_kind = 'resolver_alias_path'
                       ORDER BY raw_name, logical_name_id, resource_id
                   ) AS alias_item_rank
            FROM binding_items
        )
        SELECT resolver_address,
               min(source_family) AS source_family,
               count(*)::integer AS item_count,
               COALESCE(jsonb_agg(item ORDER BY raw_name, logical_name_id, resource_id)
                   FILTER (WHERE item_rank <= $1), '[]'::jsonb) AS items,
               count(*) FILTER (
                   WHERE binding_kind = 'resolver_alias_path'
               )::integer AS alias_item_count,
               COALESCE(jsonb_agg(item ORDER BY raw_name, logical_name_id, resource_id)
                   FILTER (
                       WHERE binding_kind = 'resolver_alias_path'
                         AND alias_item_rank <= $1
                   ), '[]'::jsonb) AS alias_items
        FROM ranked
        GROUP BY resolver_address
        "#,
    )
    .bind(sample_limit)
    .bind(full_rebuild)
    .bind(chain_id)
    .execute(&mut **transaction)
    .await
    .map_err(|error| ProjectError::database("failed to group resolver bindings", error))?;
    Ok(())
}
