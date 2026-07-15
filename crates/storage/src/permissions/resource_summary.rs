use std::collections::BTreeMap;

use anyhow::{Context, Result, bail, ensure};
use sqlx::{PgPool, Postgres, Transaction};
use uuid::Uuid;

use super::{
    types::{PermissionsCurrentResourceSummary, PermissionsCurrentRow},
    validation::validate_permissions_current_row,
    writes::upsert_permissions_current_row,
};

const SUMMARY_SELECT_COLUMNS: &str = r#"
    summary.resource_id,
    summary.authority_kind,
    summary.root_resource_id,
    summary.coverage,
    summary.provenance,
    summary.chain_positions,
    summary.canonicality_summary,
    summary.manifest_version,
    summary.last_recomputed_at
"#;
const SUMMARY_WRITE_COLUMNS: &str = r#"
    resource_id,
    authority_kind,
    root_resource_id,
    coverage,
    provenance,
    chain_positions,
    canonicality_summary,
    manifest_version,
    last_recomputed_at
"#;

const CURRENT_RESOURCE_FILTER: &str = r#"
    resource.canonicality_state IN (
        'canonical'::canonicality_state,
        'safe'::canonicality_state,
        'finalized'::canonicality_state
    )
"#;

/// Load projection-owned authority/support metadata for one permission resource.
pub async fn load_permissions_current_resource_summary(
    pool: &PgPool,
    resource_id: Uuid,
) -> Result<Option<PermissionsCurrentResourceSummary>> {
    sqlx::query_as::<_, PermissionsCurrentResourceSummary>(&format!(
        "SELECT {SUMMARY_SELECT_COLUMNS} \
         FROM permissions_current_resource_summary summary \
         JOIN resources resource ON resource.resource_id = summary.resource_id \
         WHERE summary.resource_id = $1 AND {CURRENT_RESOURCE_FILTER}"
    ))
    .bind(resource_id)
    .fetch_optional(pool)
    .await
    .with_context(|| {
        format!("failed to load permissions_current resource summary for resource_id {resource_id}")
    })
}

/// Load projection-owned authority/support metadata for a bounded set of resources.
pub async fn load_permissions_current_resource_summaries(
    pool: &PgPool,
    resource_ids: &[Uuid],
) -> Result<BTreeMap<Uuid, PermissionsCurrentResourceSummary>> {
    if resource_ids.is_empty() {
        return Ok(BTreeMap::new());
    }

    let rows = sqlx::query_as::<_, PermissionsCurrentResourceSummary>(&format!(
        "SELECT {SUMMARY_SELECT_COLUMNS} \
         FROM permissions_current_resource_summary summary \
         JOIN resources resource ON resource.resource_id = summary.resource_id \
         WHERE summary.resource_id = ANY($1::UUID[]) \
           AND {CURRENT_RESOURCE_FILTER} \
         ORDER BY summary.resource_id"
    ))
    .bind(resource_ids)
    .fetch_all(pool)
    .await
    .with_context(|| {
        format!(
            "failed to load permissions_current resource summaries for {} resource ids",
            resource_ids.len()
        )
    })?;

    Ok(rows.into_iter().map(|row| (row.resource_id, row)).collect())
}

/// Insert or replace one projection-owned permission resource summary.
pub async fn upsert_permissions_current_resource_summary(
    pool: &PgPool,
    summary: &PermissionsCurrentResourceSummary,
) -> Result<PermissionsCurrentResourceSummary> {
    let mut transaction = pool
        .begin()
        .await
        .context("failed to open permissions_current resource summary transaction")?;
    let snapshot = upsert_summary(&mut transaction, summary).await?;
    transaction
        .commit()
        .await
        .context("failed to commit permissions_current resource summary transaction")?;
    Ok(snapshot)
}

/// Atomically replace one resource's permission rows and its support/authority summary.
///
/// `summary=None` removes both families for a resource that is no longer current. A non-empty
/// summary is published even when `rows` is empty, which is required to distinguish a proven
/// empty collection from unsupported wrapper-holder enumeration.
pub async fn replace_permissions_current_resource_projection(
    pool: &PgPool,
    resource_id: Uuid,
    rows: &[PermissionsCurrentRow],
    summary: Option<&PermissionsCurrentResourceSummary>,
) -> Result<(usize, u64)> {
    ensure!(
        rows.iter().all(|row| row.resource_id == resource_id),
        "permissions_current replacement rows must match resource_id {resource_id}"
    );
    if let Some(summary) = summary {
        ensure!(
            summary.resource_id == resource_id,
            "permissions_current replacement summary must match resource_id {resource_id}"
        );
    }

    let mut transaction = pool
        .begin()
        .await
        .context("failed to open permissions_current resource replacement transaction")?;

    for row in rows {
        validate_permissions_current_row(row)?;
        upsert_permissions_current_row(&mut transaction, row).await?;
    }
    let deleted = delete_stale_rows(&mut transaction, resource_id, rows).await?;
    match summary {
        Some(summary) => {
            upsert_summary(&mut transaction, summary).await?;
        }
        None => {
            sqlx::query("DELETE FROM permissions_current_resource_summary WHERE resource_id = $1")
                .bind(resource_id)
                .execute(&mut *transaction)
                .await
                .with_context(|| {
                    format!(
                        "failed to delete permissions_current resource summary for {resource_id}"
                    )
                })?;
        }
    }

    transaction
        .commit()
        .await
        .context("failed to commit permissions_current resource replacement")?;
    Ok((rows.len(), deleted))
}

async fn upsert_summary(
    transaction: &mut Transaction<'_, Postgres>,
    summary: &PermissionsCurrentResourceSummary,
) -> Result<PermissionsCurrentResourceSummary> {
    validate_summary(summary)?;
    sqlx::query_as::<_, PermissionsCurrentResourceSummary>(&format!(
        r#"
        INSERT INTO permissions_current_resource_summary ({SUMMARY_WRITE_COLUMNS})
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
        ON CONFLICT (resource_id) DO UPDATE SET
            authority_kind = EXCLUDED.authority_kind,
            root_resource_id = EXCLUDED.root_resource_id,
            coverage = EXCLUDED.coverage,
            provenance = EXCLUDED.provenance,
            chain_positions = EXCLUDED.chain_positions,
            canonicality_summary = EXCLUDED.canonicality_summary,
            manifest_version = EXCLUDED.manifest_version,
            last_recomputed_at = EXCLUDED.last_recomputed_at
        RETURNING {SUMMARY_WRITE_COLUMNS}
        "#
    ))
    .bind(summary.resource_id)
    .bind(&summary.authority_kind)
    .bind(summary.root_resource_id)
    .bind(&summary.coverage)
    .bind(&summary.provenance)
    .bind(&summary.chain_positions)
    .bind(&summary.canonicality_summary)
    .bind(summary.manifest_version)
    .bind(summary.last_recomputed_at)
    .fetch_one(&mut **transaction)
    .await
    .with_context(|| {
        format!(
            "failed to upsert permissions_current resource summary for {}",
            summary.resource_id
        )
    })
}

async fn delete_stale_rows(
    transaction: &mut Transaction<'_, Postgres>,
    resource_id: Uuid,
    rows: &[PermissionsCurrentRow],
) -> Result<u64> {
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
    .execute(&mut **transaction)
    .await
    .with_context(|| format!("failed to delete stale permissions_current rows for {resource_id}"))
    .map(|result| result.rows_affected())
}

fn validate_summary(summary: &PermissionsCurrentResourceSummary) -> Result<()> {
    if summary.authority_kind.as_deref().is_some_and(str::is_empty) {
        bail!(
            "permissions_current resource summary {} has empty authority_kind",
            summary.resource_id
        );
    }
    ensure!(
        summary.coverage.is_object(),
        "permissions_current resource summary {} coverage must be an object",
        summary.resource_id
    );
    ensure!(
        summary.provenance.is_object(),
        "permissions_current resource summary {} provenance must be an object",
        summary.resource_id
    );
    ensure!(
        summary.chain_positions.is_object(),
        "permissions_current resource summary {} chain_positions must be an object",
        summary.resource_id
    );
    ensure!(
        summary.canonicality_summary.is_object(),
        "permissions_current resource summary {} canonicality_summary must be an object",
        summary.resource_id
    );
    ensure!(
        summary.manifest_version > 0,
        "permissions_current resource summary {} manifest_version must be positive",
        summary.resource_id
    );
    Ok(())
}
