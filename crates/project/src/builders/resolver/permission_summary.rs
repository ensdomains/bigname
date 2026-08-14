use sqlx::{Postgres, Transaction};

use crate::{ProjectError, Result};

pub(super) async fn stage(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    sample_limit: i32,
    full_rebuild: bool,
) -> Result<()> {
    // Resolver summaries depend on the current resolver-scoped permission rows, not on every
    // resource that historically pointed at the resolver. Merge untouched current rows with the
    // rebuilt resource slice so rebuilding the resolver itself never requires dependent scope.
    sqlx::query(
        "CREATE TEMP TABLE project_resolver_permission_rows ON COMMIT DROP AS
         WITH retained_permissions AS (
             SELECT current.*
             FROM permissions_current current
             WHERE NOT $2
               AND EXISTS (
                   SELECT 1 FROM project_scope_resolvers resolver_scope
                   WHERE lower(resolver_scope.resolver_address) =
                         lower(current.scope_detail ->> 'resolver_address')
               )
               AND NOT EXISTS (
                   SELECT 1 FROM project_scope_resources scope
                   WHERE scope.resource_id = current.resource_id
               )
               AND NOT EXISTS (
                   SELECT 1 FROM project_scope_resolver_passthrough passthrough
                   WHERE lower(passthrough.resolver_address) =
                         lower(current.scope_detail ->> 'resolver_address')
               )
         ), projected_permissions AS (
             SELECT * FROM retained_permissions
             UNION ALL
             SELECT staged.*
             FROM project_stage_permissions_current staged
             WHERE $2 OR EXISTS (
                 SELECT 1 FROM project_scope_resources scope
                 WHERE scope.resource_id = staged.resource_id
             )
         )
         SELECT permission.*,
                lower(permission.scope_detail ->> 'resolver_address') AS resolver_address
         FROM projected_permissions permission
         WHERE permission.scope_kind = 'resolver'
           AND permission.scope_detail ->> 'chain_id' = $1
           AND permission.scope_detail ->> 'resolver_address' IS NOT NULL
           AND (
               $2 OR EXISTS (
                   SELECT 1 FROM project_scope_resolvers resolver_scope
                   WHERE lower(resolver_scope.resolver_address) = lower(
                       permission.scope_detail ->> 'resolver_address'
                   )
               )
           )",
    )
    .bind(chain_id)
    .bind(full_rebuild)
    .execute(&mut **transaction)
    .await
    .map_err(|error| ProjectError::database("failed to stage resolver permissions", error))?;

    sqlx::query(
        r#"
        CREATE TEMP TABLE project_resolver_permission_summary ON COMMIT DROP AS
        WITH permission_items AS (
            SELECT permission.*,
                   jsonb_build_object(
                       'resource_id', permission.resource_id,
                       'subject', permission.subject,
                       'effective_powers', permission.effective_powers,
                       'grant_source', permission.grant_source,
                       'revocation_source', permission.revocation_source
                   ) AS item,
                   row_number() OVER (
                       PARTITION BY permission.resolver_address
                       ORDER BY permission.subject, permission.resource_id
                   ) AS sample_rank
            FROM project_resolver_permission_rows permission
        ),
        permission_groups AS (
            SELECT resolver_address,
                   count(*)::integer AS item_count,
                   COALESCE(sum(jsonb_array_length(COALESCE(
                       provenance -> 'normalized_event_ids', '[]'::jsonb
                   )))::integer, 0) AS event_count,
                   COALESCE(jsonb_agg(item ORDER BY subject, resource_id)
                       FILTER (WHERE sample_rank <= $1), '[]'::jsonb) AS items
            FROM permission_items
            GROUP BY resolver_address
        ),
        role_holders AS (
            SELECT permission.resolver_address,
                   permission.subject,
                   count(DISTINCT permission.resource_id)::integer AS resource_count,
                   count(*)::integer AS permission_row_count,
                   jsonb_agg(DISTINCT power.value) AS effective_powers
            FROM project_resolver_permission_rows permission
            CROSS JOIN LATERAL jsonb_array_elements_text(
                permission.effective_powers
            ) power(value)
            GROUP BY permission.resolver_address, permission.subject
        ),
        role_items AS (
            SELECT role_holders.*,
                   jsonb_build_object(
                       'subject', subject,
                       'resource_count', resource_count,
                       'permission_row_count', permission_row_count,
                       'effective_powers', effective_powers
                   ) AS item,
                   row_number() OVER (
                       PARTITION BY resolver_address ORDER BY subject
                   ) AS sample_rank
            FROM role_holders
        ),
        role_groups AS (
            SELECT resolver_address,
                   count(*)::integer AS item_count,
                   COALESCE(jsonb_agg(item ORDER BY subject)
                       FILTER (WHERE sample_rank <= $1), '[]'::jsonb) AS items
            FROM role_items
            GROUP BY resolver_address
        )
        SELECT COALESCE(permission.resolver_address, role.resolver_address)
                   AS resolver_address,
               COALESCE(permission.item_count, 0) AS item_count,
               COALESCE(permission.event_count, 0) AS event_count,
               COALESCE(permission.items, '[]'::jsonb) AS items,
               COALESCE(role.item_count, 0) AS role_count,
               COALESCE(role.items, '[]'::jsonb) AS role_items
        FROM permission_groups permission
        FULL JOIN role_groups role USING (resolver_address)
        "#,
    )
    .bind(sample_limit)
    .execute(&mut **transaction)
    .await
    .map_err(|error| ProjectError::database("failed to group resolver permissions", error))?;
    Ok(())
}
