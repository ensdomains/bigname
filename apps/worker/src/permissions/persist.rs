use anyhow::{Context, Result};
use bigname_storage::{
    PermissionsCurrentRow, clear_permissions_current, delete_permissions_current,
};
use sqlx::PgPool;
use uuid::Uuid;

pub(super) async fn delete_stale_permissions_current_rows(
    pool: &PgPool,
    rows: &[PermissionsCurrentRow],
) -> Result<u64> {
    if rows.is_empty() {
        return clear_permissions_current(pool).await;
    }

    let resource_ids = rows.iter().map(|row| row.resource_id).collect::<Vec<_>>();
    let subjects = rows
        .iter()
        .map(|row| row.subject.clone())
        .collect::<Vec<_>>();
    let scopes = rows
        .iter()
        .map(|row| row.scope.storage_key())
        .collect::<Vec<_>>();

    sqlx::query(
        r#"
        DELETE FROM permissions_current current
        WHERE NOT EXISTS (
            SELECT 1
            FROM UNNEST($1::UUID[], $2::TEXT[], $3::TEXT[]) AS replacement(
                resource_id,
                subject,
                scope
            )
            WHERE replacement.resource_id = current.resource_id
              AND replacement.subject = current.subject
              AND replacement.scope = current.scope
        )
        "#,
    )
    .bind(&resource_ids)
    .bind(&subjects)
    .bind(&scopes)
    .execute(pool)
    .await
    .context("failed to delete stale permissions_current rows after rebuild")
    .map(|result| result.rows_affected())
}

pub(super) async fn delete_stale_permissions_current_rows_for_resource(
    pool: &PgPool,
    resource_id: Uuid,
    rows: &[PermissionsCurrentRow],
) -> Result<u64> {
    if rows.is_empty() {
        return delete_permissions_current(pool, resource_id).await;
    }

    let subjects = rows
        .iter()
        .map(|row| row.subject.clone())
        .collect::<Vec<_>>();
    let scopes = rows
        .iter()
        .map(|row| row.scope.storage_key())
        .collect::<Vec<_>>();

    sqlx::query(
        r#"
        DELETE FROM permissions_current current
        WHERE current.resource_id = $1
          AND NOT EXISTS (
            SELECT 1
            FROM UNNEST($2::TEXT[], $3::TEXT[]) AS replacement(subject, scope)
            WHERE replacement.subject = current.subject
              AND replacement.scope = current.scope
          )
        "#,
    )
    .bind(resource_id)
    .bind(&subjects)
    .bind(&scopes)
    .execute(pool)
    .await
    .with_context(|| {
        format!("failed to delete stale permissions_current rows for resource_id {resource_id}")
    })
    .map(|result| result.rows_affected())
}
