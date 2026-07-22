use anyhow::{Context, Result};
use sqlx::{PgPool, Postgres, Transaction};

use super::lock::{
    invalidate_all_verified_primary_name_outcomes_in_transaction,
    lock_primary_name_tuple_in_transaction, lock_primary_names_current_replacement_in_transaction,
};
use super::rows::decode_primary_name_current_snapshot;
use super::types::{PrimaryNameCurrentRow, PrimaryNameCurrentSnapshot, normalize_address};

/// Load one declared primary-name claim-state row by exact address, namespace, and coin_type.
pub async fn load_primary_name_current(
    pool: &PgPool,
    address: &str,
    namespace: &str,
    coin_type: &str,
) -> Result<Option<PrimaryNameCurrentRow>> {
    load_primary_name_current_snapshot(pool, address, namespace, coin_type)
        .await
        .map(|snapshot| snapshot.map(|snapshot| snapshot.row))
}

/// Load one declared primary-name claim snapshot by exact address, namespace, and coin_type.
pub async fn load_primary_name_current_snapshot(
    pool: &PgPool,
    address: &str,
    namespace: &str,
    coin_type: &str,
) -> Result<Option<PrimaryNameCurrentSnapshot>> {
    let normalized_address = normalize_address(address);
    let row = sqlx::query(
        r#"
        SELECT
            address,
            namespace,
            coin_type,
            claim_status,
            raw_claim_name,
            normalized_claim_name,
            claim_name_is_normalized,
            claim_provenance
        FROM primary_names_current
        WHERE address = $1
          AND namespace = $2
          AND coin_type = $3
        "#,
    )
    .bind(&normalized_address)
    .bind(namespace)
    .bind(coin_type)
    .fetch_optional(pool)
    .await
    .with_context(|| {
        format!(
            "failed to load primary_names_current snapshot for address {normalized_address} namespace {namespace} coin_type {coin_type}"
        )
    })?;

    row.map(decode_primary_name_current_snapshot).transpose()
}

/// Load and row-lock one declared primary-name claim snapshot inside a caller-owned transaction.
pub async fn load_primary_name_current_snapshot_for_update_in_transaction(
    transaction: &mut Transaction<'_, Postgres>,
    address: &str,
    namespace: &str,
    coin_type: &str,
) -> Result<Option<PrimaryNameCurrentSnapshot>> {
    let normalized_address = normalize_address(address);
    let row = sqlx::query(
        r#"
        SELECT
            address,
            namespace,
            coin_type,
            claim_status,
            raw_claim_name,
            normalized_claim_name,
            claim_name_is_normalized,
            claim_provenance
        FROM primary_names_current
        WHERE address = $1
          AND namespace = $2
          AND coin_type = $3
        FOR UPDATE
        "#,
    )
    .bind(&normalized_address)
    .bind(namespace)
    .bind(coin_type)
    .fetch_optional(&mut **transaction)
    .await
    .with_context(|| {
        format!(
            "failed to lock primary_names_current snapshot for address {normalized_address} namespace {namespace} coin_type {coin_type}"
        )
    })?;

    row.map(decode_primary_name_current_snapshot).transpose()
}

/// Delete one declared primary-name claim-state row so a worker can rebuild that exact key.
pub async fn delete_primary_name_current(
    pool: &PgPool,
    address: &str,
    namespace: &str,
    coin_type: &str,
) -> Result<u64> {
    let mut transaction = pool
        .begin()
        .await
        .context("failed to open primary_names_current row delete transaction")?;
    let deleted =
        delete_primary_name_current_in_transaction(&mut transaction, address, namespace, coin_type)
            .await?;
    transaction
        .commit()
        .await
        .context("failed to commit primary_names_current row delete")?;
    Ok(deleted)
}

/// Delete one declared primary-name claim-state row inside a caller-owned transaction.
pub async fn delete_primary_name_current_in_transaction(
    transaction: &mut Transaction<'_, Postgres>,
    address: &str,
    namespace: &str,
    coin_type: &str,
) -> Result<u64> {
    let normalized_address = normalize_address(address);
    lock_primary_name_tuple_in_transaction(transaction, &normalized_address, namespace, coin_type)
        .await?;
    sqlx::query(
        r#"
        DELETE FROM primary_names_current
        WHERE address = $1
          AND namespace = $2
          AND coin_type = $3
        "#,
    )
    .bind(&normalized_address)
    .bind(namespace)
    .bind(coin_type)
    .execute(&mut **transaction)
    .await
    .with_context(|| {
        format!(
            "failed to delete primary_names_current row for address {normalized_address} namespace {namespace} coin_type {coin_type}"
        )
    })
    .map(|result| result.rows_affected())
}

/// Clear the primary-name claim-state projection so a worker can perform a one-shot rebuild.
pub async fn clear_primary_names_current(pool: &PgPool) -> Result<u64> {
    let mut transaction = pool
        .begin()
        .await
        .context("failed to open primary_names_current clear transaction")?;
    lock_primary_names_current_replacement_in_transaction(&mut transaction).await?;
    invalidate_all_verified_primary_name_outcomes_in_transaction(&mut transaction).await?;
    let deleted = sqlx::query("DELETE FROM primary_names_current")
        .execute(&mut *transaction)
        .await
        .context("failed to clear primary_names_current rows")?
        .rows_affected();
    transaction
        .commit()
        .await
        .context("failed to commit primary_names_current clear")?;
    Ok(deleted)
}
