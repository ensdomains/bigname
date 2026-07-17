use anyhow::{Context, Result};
use sqlx::PgPool;
use uuid::Uuid;

use crate::projection_helpers::{serialize_jsonb_field, serialize_optional_jsonb_field};

use super::{
    decode::decode_permissions_current_row, types::PermissionsCurrentRow,
    validation::validate_permissions_current_row,
};

/// Insert or replace resource-centric permission rows.
pub async fn upsert_permissions_current_rows(
    pool: &PgPool,
    rows: &[PermissionsCurrentRow],
) -> Result<Vec<PermissionsCurrentRow>> {
    if rows.is_empty() {
        return Ok(Vec::new());
    }

    let mut transaction = pool
        .begin()
        .await
        .context("failed to open transaction for permissions_current upsert")?;

    let mut snapshots = Vec::with_capacity(rows.len());
    for row in rows {
        validate_permissions_current_row(row)?;
        snapshots.push(upsert_permissions_current_row(&mut transaction, row).await?);
    }

    transaction
        .commit()
        .await
        .context("failed to commit permissions_current upsert")?;

    Ok(snapshots)
}

/// Delete all permission rows for one resource so a worker can rebuild that collection key.
pub async fn delete_permissions_current(pool: &PgPool, resource_id: Uuid) -> Result<u64> {
    sqlx::query(
        r#"
        DELETE FROM permissions_current
        WHERE resource_id = $1
        "#,
    )
    .bind(resource_id)
    .execute(pool)
    .await
    .with_context(|| {
        format!("failed to delete permissions_current rows for resource_id {resource_id}")
    })
    .map(|result| result.rows_affected())
}

/// Clear the resource-centric permissions projection so a worker can perform a one-shot rebuild.
pub async fn clear_permissions_current(pool: &PgPool) -> Result<u64> {
    sqlx::query("DELETE FROM permissions_current")
        .execute(pool)
        .await
        .context("failed to clear permissions_current rows")
        .map(|result| result.rows_affected())
}

pub(super) async fn upsert_permissions_current_row(
    executor: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    row: &PermissionsCurrentRow,
) -> Result<PermissionsCurrentRow> {
    let scope = row.scope.storage_key();
    let scope_kind = row.scope.kind();
    let scope_detail =
        serialize_jsonb_field(&row.scope.detail(), "failed to serialize scope_detail")?;
    let effective_powers = serialize_jsonb_field(
        &row.effective_powers,
        "failed to serialize permissions_current effective_powers",
    )?;
    let grant_source = serialize_jsonb_field(
        &row.grant_source,
        "failed to serialize permissions_current grant_source",
    )?;
    let revocation_source = serialize_optional_jsonb_field(
        row.revocation_source.as_ref(),
        "failed to serialize permissions_current revocation_source",
    )?;
    let inheritance_path = serialize_jsonb_field(
        &row.inheritance_path,
        "failed to serialize permissions_current inheritance_path",
    )?;
    let transfer_behavior = serialize_jsonb_field(
        &row.transfer_behavior,
        "failed to serialize permissions_current transfer_behavior",
    )?;
    let provenance = serialize_jsonb_field(
        &row.provenance,
        "failed to serialize permissions_current provenance",
    )?;
    let coverage = serialize_jsonb_field(
        &row.coverage,
        "failed to serialize permissions_current coverage",
    )?;
    let chain_positions = serialize_jsonb_field(
        &row.chain_positions,
        "failed to serialize permissions_current chain_positions",
    )?;
    let canonicality_summary = serialize_jsonb_field(
        &row.canonicality_summary,
        "failed to serialize permissions_current canonicality_summary",
    )?;

    let snapshot = sqlx::query(
        r#"
        INSERT INTO permissions_current (
            resource_id,
            subject,
            scope,
            scope_kind,
            scope_detail,
            effective_powers,
            grant_source,
            revocation_source,
            inheritance_path,
            transfer_behavior,
            provenance,
            coverage,
            chain_positions,
            canonicality_summary,
            manifest_version,
            last_recomputed_at
        )
        VALUES (
            $1,
            $2,
            $3,
            $4,
            $5::jsonb,
            $6::jsonb,
            $7::jsonb,
            $8::jsonb,
            $9::jsonb,
            $10::jsonb,
            $11::jsonb,
            $12::jsonb,
            $13::jsonb,
            $14::jsonb,
            $15,
            $16
        )
        ON CONFLICT (resource_id, subject, scope) DO UPDATE
        SET
            scope_kind = EXCLUDED.scope_kind,
            scope_detail = EXCLUDED.scope_detail,
            effective_powers = EXCLUDED.effective_powers,
            grant_source = EXCLUDED.grant_source,
            revocation_source = EXCLUDED.revocation_source,
            inheritance_path = EXCLUDED.inheritance_path,
            transfer_behavior = EXCLUDED.transfer_behavior,
            provenance = EXCLUDED.provenance,
            coverage = EXCLUDED.coverage,
            chain_positions = EXCLUDED.chain_positions,
            canonicality_summary = EXCLUDED.canonicality_summary,
            manifest_version = EXCLUDED.manifest_version,
            last_recomputed_at = EXCLUDED.last_recomputed_at
        RETURNING
            resource_id,
            subject,
            scope,
            scope_kind,
            scope_detail,
            effective_powers,
            grant_source,
            revocation_source,
            inheritance_path,
            transfer_behavior,
            provenance,
            coverage,
            chain_positions,
            canonicality_summary,
            manifest_version,
            last_recomputed_at
        "#,
    )
    .bind(row.resource_id)
    .bind(&row.subject)
    .bind(scope)
    .bind(scope_kind)
    .bind(scope_detail)
    .bind(effective_powers)
    .bind(grant_source)
    .bind(revocation_source)
    .bind(inheritance_path)
    .bind(transfer_behavior)
    .bind(provenance)
    .bind(coverage)
    .bind(chain_positions)
    .bind(canonicality_summary)
    .bind(row.manifest_version)
    .bind(row.last_recomputed_at)
    .fetch_one(&mut **executor)
    .await
    .with_context(|| {
        format!(
            "failed to upsert permissions_current row for resource_id {} subject {} scope {}",
            row.resource_id,
            row.subject,
            row.scope.storage_key()
        )
    })?;

    decode_permissions_current_row(snapshot)
}
