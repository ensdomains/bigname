use anyhow::{Context, Result};
use bigname_storage::clear_name_current;
use sqlx::PgPool;

pub(super) async fn delete_stale_name_current_rows(
    pool: &PgPool,
    logical_name_ids: &[String],
) -> Result<u64> {
    if logical_name_ids.is_empty() {
        return clear_name_current(pool).await;
    }

    sqlx::query(
        r#"
        DELETE FROM name_current current
        WHERE NOT EXISTS (
            SELECT 1
            FROM UNNEST($1::TEXT[]) AS replacement(logical_name_id)
            WHERE replacement.logical_name_id = current.logical_name_id
        )
        "#,
    )
    .bind(logical_name_ids)
    .execute(pool)
    .await
    .context("failed to delete stale name_current rows after rebuild")
    .map(|result| result.rows_affected())
}
