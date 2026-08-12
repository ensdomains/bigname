use sqlx::{Postgres, Transaction};

use crate::{ProjectError, Result};

pub(super) async fn stage(
    transaction: &mut Transaction<'_, Postgres>,
    sample_limit: i32,
) -> Result<()> {
    sqlx::query(
        r#"
        CREATE TEMP TABLE project_resolver_binding_summary ON COMMIT DROP AS
        WITH binding_events AS (
            SELECT event.*,
                   row_number() OVER (
                       PARTITION BY event.logical_name_id, event.resource_id
                       ORDER BY event.block_number DESC NULLS LAST,
                                event.transaction_index DESC NULLS LAST,
                                event.log_index DESC NULLS LAST,
                                event.normalized_event_id DESC
                   ) AS latest_rank
            FROM project_events event
            WHERE event.event_kind = 'ResolverChanged'
              AND event.logical_name_id IS NOT NULL
              AND event.resource_id IS NOT NULL
        ),
        binding_items AS (
            SELECT lower(event.after_state ->> 'resolver') AS resolver_address,
                   CASE
                       WHEN event.source_family LIKE 'ens_v2_%'
                           THEN 'ens_v2_resolver_l1'
                       WHEN event.source_family LIKE 'basenames_%'
                           THEN 'basenames_base_resolver'
                       ELSE 'ens_v1_resolver_l1'
                   END AS source_family,
                   surface.logical_name_id IS NOT NULL AS has_item,
                   surface.raw_name,
                   event.logical_name_id,
                   event.resource_id,
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
            FROM binding_events event
            JOIN project_bindings binding
              ON binding.logical_name_id = event.logical_name_id
             AND binding.resource_id = event.resource_id
            LEFT JOIN project_surfaces surface
              ON surface.logical_name_id = binding.logical_name_id
            WHERE event.latest_rank = 1
              AND event.after_state ->> 'resolver' IS NOT NULL
              AND btrim(event.after_state ->> 'resolver') <> ''
        ),
        ranked AS (
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
               count(*) FILTER (WHERE has_item)::integer AS item_count,
               COALESCE(jsonb_agg(item ORDER BY raw_name, logical_name_id, resource_id)
                   FILTER (WHERE has_item AND item_rank <= $1), '[]'::jsonb) AS items,
               count(*) FILTER (
                   WHERE has_item AND binding_kind = 'resolver_alias_path'
               )::integer AS alias_item_count,
               COALESCE(jsonb_agg(item ORDER BY raw_name, logical_name_id, resource_id)
                   FILTER (
                       WHERE has_item
                         AND binding_kind = 'resolver_alias_path'
                         AND alias_item_rank <= $1
                   ), '[]'::jsonb) AS alias_items
        FROM ranked
        GROUP BY resolver_address
        "#,
    )
    .bind(sample_limit)
    .execute(&mut **transaction)
    .await
    .map_err(|error| ProjectError::database("failed to group resolver bindings", error))?;
    Ok(())
}
