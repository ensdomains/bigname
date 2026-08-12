use sqlx::{Postgres, Transaction};

use crate::{ProjectError, Result};

pub(super) async fn stage(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    sample_limit: i32,
) -> Result<()> {
    sqlx::query(
        "CREATE TEMP TABLE project_resolver_permission_rows ON COMMIT DROP AS
         SELECT permission.*,
                lower(permission.scope_detail ->> 'resolver_address') AS resolver_address
         FROM project_stage_permissions_current permission
         WHERE permission.scope_kind = 'resolver'
           AND permission.scope_detail ->> 'chain_id' = $1
           AND permission.scope_detail ->> 'resolver_address' IS NOT NULL",
    )
    .bind(chain_id)
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
