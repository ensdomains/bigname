use anyhow::{Context, Result};
use bigname_storage::{
    AddressNameCurrentRow, clear_address_names_current, delete_address_names_current,
};
use sqlx::PgPool;

pub(super) async fn delete_stale_address_names_current_rows(
    pool: &PgPool,
    rows: &[AddressNameCurrentRow],
) -> Result<u64> {
    if rows.is_empty() {
        return clear_address_names_current(pool).await;
    }

    let addresses = rows
        .iter()
        .map(|row| row.address.clone())
        .collect::<Vec<_>>();
    let logical_name_ids = rows
        .iter()
        .map(|row| row.logical_name_id.clone())
        .collect::<Vec<_>>();
    let relations = rows
        .iter()
        .map(|row| row.relation.as_str().to_owned())
        .collect::<Vec<_>>();

    sqlx::query(
        r#"
        DELETE FROM address_names_current current
        WHERE NOT EXISTS (
            SELECT 1
            FROM UNNEST($1::TEXT[], $2::TEXT[], $3::TEXT[]) AS replacement(
                address,
                logical_name_id,
                relation
            )
            WHERE replacement.address = current.address
              AND replacement.logical_name_id = current.logical_name_id
              AND replacement.relation = current.relation
        )
        "#,
    )
    .bind(&addresses)
    .bind(&logical_name_ids)
    .bind(&relations)
    .execute(pool)
    .await
    .context("failed to delete stale address_names_current rows after rebuild")
    .map(|result| result.rows_affected())
}

pub(super) async fn delete_stale_address_names_current_rows_for_address(
    pool: &PgPool,
    address: &str,
    rows: &[AddressNameCurrentRow],
) -> Result<u64> {
    if rows.is_empty() {
        return delete_address_names_current(pool, address).await;
    }

    let logical_name_ids = rows
        .iter()
        .map(|row| row.logical_name_id.clone())
        .collect::<Vec<_>>();
    let relations = rows
        .iter()
        .map(|row| row.relation.as_str().to_owned())
        .collect::<Vec<_>>();

    sqlx::query(
        r#"
        DELETE FROM address_names_current current
        WHERE current.address = $1
          AND NOT EXISTS (
            SELECT 1
            FROM UNNEST($2::TEXT[], $3::TEXT[]) AS replacement(logical_name_id, relation)
            WHERE replacement.logical_name_id = current.logical_name_id
              AND replacement.relation = current.relation
          )
        "#,
    )
    .bind(address)
    .bind(&logical_name_ids)
    .bind(&relations)
    .execute(pool)
    .await
    .with_context(|| {
        format!("failed to delete stale address_names_current rows for address {address}")
    })
    .map(|result| result.rows_affected())
}
