use anyhow::{Context, Result};
use sqlx::{PgPool, Postgres, QueryBuilder};

use crate::projection_helpers::{POSTGRES_MAX_BIND_PARAMETERS, serialize_jsonb_field};

use super::rows::{
    GasSponsorshipCurrentRow, GasSponsorshipGlobalCurrentRow, validate_gas_sponsorship_current_row,
    validate_gas_sponsorship_global_current_row,
};

const GAS_SPONSORSHIP_CURRENT_UPSERT_COLUMN_COUNT: usize = 15;
const GAS_SPONSORSHIP_CURRENT_UPSERT_MAX_ROWS: usize =
    (POSTGRES_MAX_BIND_PARAMETERS - 1) / GAS_SPONSORSHIP_CURRENT_UPSERT_COLUMN_COUNT;

/// Insert or replace per-name gas-sponsorship projection rows.
pub async fn upsert_gas_sponsorship_current_rows(
    pool: &PgPool,
    rows: &[GasSponsorshipCurrentRow],
) -> Result<usize> {
    if rows.is_empty() {
        return Ok(0);
    }
    for row in rows {
        validate_gas_sponsorship_current_row(row)?;
    }

    let mut transaction = pool
        .begin()
        .await
        .context("failed to open transaction for gas_sponsorship_current upsert")?;
    let mut upserted_row_count = 0usize;
    for batch in rows.chunks(GAS_SPONSORSHIP_CURRENT_UPSERT_MAX_ROWS) {
        upserted_row_count +=
            upsert_gas_sponsorship_current_row_batch(&mut transaction, batch).await?;
    }
    transaction
        .commit()
        .await
        .context("failed to commit gas_sponsorship_current upsert")?;
    Ok(upserted_row_count)
}

async fn upsert_gas_sponsorship_current_row_batch(
    transaction: &mut sqlx::Transaction<'_, Postgres>,
    rows: &[GasSponsorshipCurrentRow],
) -> Result<usize> {
    let serialized = rows
        .iter()
        .map(|row| {
            Ok((
                serialize_jsonb_field(
                    &row.provenance,
                    "failed to serialize gas_sponsorship_current provenance",
                )?,
                serialize_jsonb_field(
                    &row.coverage,
                    "failed to serialize gas_sponsorship_current coverage",
                )?,
                serialize_jsonb_field(
                    &row.chain_positions,
                    "failed to serialize gas_sponsorship_current chain_positions",
                )?,
                serialize_jsonb_field(
                    &row.canonicality_summary,
                    "failed to serialize gas_sponsorship_current canonicality_summary",
                )?,
            ))
        })
        .collect::<Result<Vec<_>>>()?;

    let mut builder = QueryBuilder::<Postgres>::new(
        r#"
        INSERT INTO gas_sponsorship_current (
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
        )
        "#,
    );
    builder.push_values(rows.iter().zip(&serialized), |mut values, (row, json)| {
        values.push_bind(&row.logical_name_id);
        values.push_bind(&row.namespace);
        values.push_bind(&row.normalized_name);
        values.push_bind(&row.namehash);
        values.push_bind(row.lease_start_at);
        values.push_bind(row.registered_seconds_total);
        values.push_bind(row.earned_updates);
        values.push_bind(row.spent_updates);
        values.push_bind(row.last_sponsored_write_at);
        values.push_bind(&json.0).push_unseparated("::jsonb");
        values.push_bind(&json.1).push_unseparated("::jsonb");
        values.push_bind(&json.2).push_unseparated("::jsonb");
        values.push_bind(&json.3).push_unseparated("::jsonb");
        values.push_bind(row.manifest_version);
        values.push_bind(row.last_recomputed_at);
    });
    builder.push(
        r#"
        ON CONFLICT (logical_name_id) DO UPDATE
        SET
            namespace = EXCLUDED.namespace,
            normalized_name = EXCLUDED.normalized_name,
            namehash = EXCLUDED.namehash,
            lease_start_at = EXCLUDED.lease_start_at,
            registered_seconds_total = EXCLUDED.registered_seconds_total,
            earned_updates = EXCLUDED.earned_updates,
            spent_updates = EXCLUDED.spent_updates,
            last_sponsored_write_at = EXCLUDED.last_sponsored_write_at,
            provenance = EXCLUDED.provenance,
            coverage = EXCLUDED.coverage,
            chain_positions = EXCLUDED.chain_positions,
            canonicality_summary = EXCLUDED.canonicality_summary,
            manifest_version = EXCLUDED.manifest_version,
            last_recomputed_at = EXCLUDED.last_recomputed_at
        "#,
    );

    let result = builder
        .build()
        .execute(&mut **transaction)
        .await
        .context("failed to upsert gas_sponsorship_current rows")?;
    Ok(result.rows_affected() as usize)
}

/// Insert or replace the namespace-wide gas-sponsorship totals row.
pub async fn upsert_gas_sponsorship_global_current_row(
    pool: &PgPool,
    row: &GasSponsorshipGlobalCurrentRow,
) -> Result<()> {
    validate_gas_sponsorship_global_current_row(row)?;

    let provenance = serialize_jsonb_field(
        &row.provenance,
        "failed to serialize gas_sponsorship_global_current provenance",
    )?;
    let coverage = serialize_jsonb_field(
        &row.coverage,
        "failed to serialize gas_sponsorship_global_current coverage",
    )?;
    let chain_positions = serialize_jsonb_field(
        &row.chain_positions,
        "failed to serialize gas_sponsorship_global_current chain_positions",
    )?;
    let canonicality_summary = serialize_jsonb_field(
        &row.canonicality_summary,
        "failed to serialize gas_sponsorship_global_current canonicality_summary",
    )?;

    sqlx::query(
        r#"
        INSERT INTO gas_sponsorship_global_current (
            namespace,
            sponsored_op_count,
            attributed_op_count,
            failed_op_count,
            gas_wei_total,
            failed_gas_wei_total,
            usd_e8_total,
            unpriced_wei_total,
            provenance,
            coverage,
            chain_positions,
            canonicality_summary,
            manifest_version,
            last_recomputed_at
        )
        VALUES (
            $1, $2, $3, $4,
            $5::numeric, $6::numeric, $7::numeric, $8::numeric,
            $9::jsonb, $10::jsonb, $11::jsonb, $12::jsonb,
            $13, $14
        )
        ON CONFLICT (namespace) DO UPDATE
        SET
            sponsored_op_count = EXCLUDED.sponsored_op_count,
            attributed_op_count = EXCLUDED.attributed_op_count,
            failed_op_count = EXCLUDED.failed_op_count,
            gas_wei_total = EXCLUDED.gas_wei_total,
            failed_gas_wei_total = EXCLUDED.failed_gas_wei_total,
            usd_e8_total = EXCLUDED.usd_e8_total,
            unpriced_wei_total = EXCLUDED.unpriced_wei_total,
            provenance = EXCLUDED.provenance,
            coverage = EXCLUDED.coverage,
            chain_positions = EXCLUDED.chain_positions,
            canonicality_summary = EXCLUDED.canonicality_summary,
            manifest_version = EXCLUDED.manifest_version,
            last_recomputed_at = EXCLUDED.last_recomputed_at
        "#,
    )
    .bind(&row.namespace)
    .bind(row.sponsored_op_count)
    .bind(row.attributed_op_count)
    .bind(row.failed_op_count)
    .bind(&row.gas_wei_total)
    .bind(&row.failed_gas_wei_total)
    .bind(&row.usd_e8_total)
    .bind(&row.unpriced_wei_total)
    .bind(&provenance)
    .bind(&coverage)
    .bind(&chain_positions)
    .bind(&canonicality_summary)
    .bind(row.manifest_version)
    .bind(row.last_recomputed_at)
    .execute(pool)
    .await
    .with_context(|| {
        format!(
            "failed to upsert gas_sponsorship_global_current row for namespace {}",
            row.namespace
        )
    })?;
    Ok(())
}
