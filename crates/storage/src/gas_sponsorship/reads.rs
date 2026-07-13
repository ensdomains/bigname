use anyhow::{Context, Result};
use sqlx::PgPool;

use super::rows::{
    GasSponsorshipCurrentRow, GasSponsorshipGlobalCurrentRow, decode_gas_sponsorship_current_row,
    decode_gas_sponsorship_global_current_row,
};

const GAS_SPONSORSHIP_CURRENT_COLUMNS: &str = r#"
    logical_name_id,
    namespace,
    normalized_name,
    namehash,
    lease_start_at,
    registered_seconds_total,
    earned_updates,
    spent_updates,
    last_sponsored_write_at,
    provenance,
    coverage,
    chain_positions,
    canonicality_summary,
    manifest_version,
    last_recomputed_at
"#;

pub async fn load_gas_sponsorship_current(
    pool: &PgPool,
    logical_name_id: &str,
) -> Result<Option<GasSponsorshipCurrentRow>> {
    let row = sqlx::query(&format!(
        r#"
        SELECT {GAS_SPONSORSHIP_CURRENT_COLUMNS}
        FROM gas_sponsorship_current
        WHERE logical_name_id = $1
        "#,
    ))
    .bind(logical_name_id)
    .fetch_optional(pool)
    .await
    .with_context(|| format!("failed to load gas_sponsorship_current for {logical_name_id}"))?;
    row.map(decode_gas_sponsorship_current_row).transpose()
}

pub async fn load_gas_sponsorship_global_current(
    pool: &PgPool,
    namespace: &str,
) -> Result<Option<GasSponsorshipGlobalCurrentRow>> {
    let row = sqlx::query(
        r#"
        SELECT
            namespace,
            sponsored_op_count,
            attributed_op_count,
            failed_op_count,
            gas_wei_total::text AS gas_wei_total,
            failed_gas_wei_total::text AS failed_gas_wei_total,
            usd_e8_total::text AS usd_e8_total,
            unpriced_wei_total::text AS unpriced_wei_total,
            provenance,
            coverage,
            chain_positions,
            canonicality_summary,
            manifest_version,
            last_recomputed_at
        FROM gas_sponsorship_global_current
        WHERE namespace = $1
        "#,
    )
    .bind(namespace)
    .fetch_optional(pool)
    .await
    .with_context(|| {
        format!("failed to load gas_sponsorship_global_current for namespace {namespace}")
    })?;
    row.map(decode_gas_sponsorship_global_current_row)
        .transpose()
}

pub async fn delete_gas_sponsorship_current(pool: &PgPool, logical_name_id: &str) -> Result<u64> {
    let result = sqlx::query("DELETE FROM gas_sponsorship_current WHERE logical_name_id = $1")
        .bind(logical_name_id)
        .execute(pool)
        .await
        .with_context(|| {
            format!("failed to delete gas_sponsorship_current for {logical_name_id}")
        })?;
    Ok(result.rows_affected())
}

pub async fn clear_gas_sponsorship_current(pool: &PgPool) -> Result<u64> {
    let result = sqlx::query("DELETE FROM gas_sponsorship_current")
        .execute(pool)
        .await
        .context("failed to clear gas_sponsorship_current")?;
    Ok(result.rows_affected())
}

pub async fn clear_gas_sponsorship_global_current(pool: &PgPool) -> Result<u64> {
    let result = sqlx::query("DELETE FROM gas_sponsorship_global_current")
        .execute(pool)
        .await
        .context("failed to clear gas_sponsorship_global_current")?;
    Ok(result.rows_affected())
}
