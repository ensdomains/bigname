use anyhow::{Context, Result, bail};
use sqlx::PgPool;

use crate::projection_helpers::{require_json_object, serialize_jsonb_field};

use super::{
    DECLARED_SURFACE_CLASS, reads::decode_children_current_row, types::ChildrenCurrentRow,
};

/// Insert or replace current declared child rows for one or more parents.
pub async fn upsert_children_current_rows(
    pool: &PgPool,
    rows: &[ChildrenCurrentRow],
) -> Result<Vec<ChildrenCurrentRow>> {
    if rows.is_empty() {
        return Ok(Vec::new());
    }

    let mut transaction = pool
        .begin()
        .await
        .context("failed to open transaction for children_current upsert")?;

    let mut snapshots = Vec::with_capacity(rows.len());
    for row in rows {
        validate_children_current_row(row)?;
        snapshots.push(upsert_children_current_row(&mut transaction, row).await?);
    }

    transaction
        .commit()
        .await
        .context("failed to commit children_current upsert")?;

    Ok(snapshots)
}

/// Delete all declared child rows for one parent so a worker can rebuild that collection key.
pub async fn delete_children_current(pool: &PgPool, parent_logical_name_id: &str) -> Result<u64> {
    sqlx::query(
        r#"
        DELETE FROM children_current
        WHERE parent_logical_name_id = $1
          AND surface_class = $2
        "#,
    )
    .bind(parent_logical_name_id)
    .bind(DECLARED_SURFACE_CLASS)
    .execute(pool)
    .await
    .with_context(|| {
        format!(
            "failed to delete children_current rows for parent_logical_name_id {parent_logical_name_id}"
        )
    })
    .map(|result| result.rows_affected())
}

/// Clear the declared direct-child projection so a worker can perform a one-shot rebuild.
pub async fn clear_children_current(pool: &PgPool) -> Result<u64> {
    sqlx::query(
        r#"
        DELETE FROM children_current
        WHERE surface_class = $1
        "#,
    )
    .bind(DECLARED_SURFACE_CLASS)
    .execute(pool)
    .await
    .context("failed to clear children_current rows")
    .map(|result| result.rows_affected())
}

async fn upsert_children_current_row(
    executor: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    row: &ChildrenCurrentRow,
) -> Result<ChildrenCurrentRow> {
    let provenance = serialize_jsonb_field(
        &row.provenance,
        "failed to serialize children_current provenance",
    )?;
    let chain_positions = serialize_jsonb_field(
        &row.chain_positions,
        "failed to serialize children_current chain_positions",
    )?;
    let canonicality_summary = serialize_jsonb_field(
        &row.canonicality_summary,
        "failed to serialize children_current canonicality_summary",
    )?;

    let snapshot = sqlx::query(
        r#"
        INSERT INTO children_current (
            parent_logical_name_id,
            child_logical_name_id,
            surface_class,
            namespace,
            canonical_display_name,
            normalized_name,
            namehash,
            provenance,
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
            $5,
            $6,
            $7,
            $8::jsonb,
            $9::jsonb,
            $10::jsonb,
            $11,
            $12
        )
        ON CONFLICT (parent_logical_name_id, child_logical_name_id, surface_class) DO UPDATE
        SET
            namespace = EXCLUDED.namespace,
            canonical_display_name = EXCLUDED.canonical_display_name,
            normalized_name = EXCLUDED.normalized_name,
            namehash = EXCLUDED.namehash,
            provenance = EXCLUDED.provenance,
            chain_positions = EXCLUDED.chain_positions,
            canonicality_summary = EXCLUDED.canonicality_summary,
            manifest_version = EXCLUDED.manifest_version,
            last_recomputed_at = EXCLUDED.last_recomputed_at
        RETURNING
            parent_logical_name_id,
            child_logical_name_id,
            surface_class,
            namespace,
            canonical_display_name,
            normalized_name,
            namehash,
            provenance,
            chain_positions,
            canonicality_summary,
            manifest_version,
            last_recomputed_at
        "#,
    )
    .bind(&row.parent_logical_name_id)
    .bind(&row.child_logical_name_id)
    .bind(&row.surface_class)
    .bind(&row.namespace)
    .bind(&row.canonical_display_name)
    .bind(&row.normalized_name)
    .bind(&row.namehash)
    .bind(provenance)
    .bind(chain_positions)
    .bind(canonicality_summary)
    .bind(row.manifest_version)
    .bind(row.last_recomputed_at)
    .fetch_one(&mut **executor)
    .await
    .with_context(|| {
        format!(
            "failed to upsert children_current row for parent_logical_name_id {} child_logical_name_id {}",
            row.parent_logical_name_id,
            row.child_logical_name_id
        )
    })?;

    decode_children_current_row(snapshot)
}

fn validate_children_current_row(row: &ChildrenCurrentRow) -> Result<()> {
    if row.parent_logical_name_id.trim().is_empty() {
        bail!("children_current row must include parent_logical_name_id");
    }
    if row.child_logical_name_id.trim().is_empty() {
        bail!("children_current row must include child_logical_name_id");
    }
    if row.parent_logical_name_id == row.child_logical_name_id {
        bail!(
            "children_current row {} cannot target itself as a child",
            row.parent_logical_name_id
        );
    }
    if row.surface_class != DECLARED_SURFACE_CLASS {
        bail!(
            "children_current row {} -> {} must use declared surface_class",
            row.parent_logical_name_id,
            row.child_logical_name_id
        );
    }
    if row.namespace.trim().is_empty() {
        bail!(
            "children_current row {} -> {} must include namespace",
            row.parent_logical_name_id,
            row.child_logical_name_id
        );
    }
    if row.normalized_name.trim().is_empty() {
        bail!(
            "children_current row {} -> {} must include normalized_name",
            row.parent_logical_name_id,
            row.child_logical_name_id
        );
    }
    if row.canonical_display_name.trim().is_empty() {
        bail!(
            "children_current row {} -> {} must include canonical_display_name",
            row.parent_logical_name_id,
            row.child_logical_name_id
        );
    }
    if row.namehash.trim().is_empty() {
        bail!(
            "children_current row {} -> {} must include namehash",
            row.parent_logical_name_id,
            row.child_logical_name_id
        );
    }
    if row.child_logical_name_id != format!("{}:{}", row.namespace, row.normalized_name) {
        bail!(
            "children_current row {} -> {} does not match namespace {} and normalized_name {}",
            row.parent_logical_name_id,
            row.child_logical_name_id,
            row.namespace,
            row.normalized_name
        );
    }
    if row.manifest_version <= 0 {
        bail!(
            "children_current row {} -> {} has non-positive manifest_version {}",
            row.parent_logical_name_id,
            row.child_logical_name_id,
            row.manifest_version
        );
    }

    require_json_object(&row.provenance, || {
        format!(
            "children_current row {} -> {} field provenance must be a JSON object",
            row.parent_logical_name_id, row.child_logical_name_id
        )
    })?;
    require_json_object(&row.chain_positions, || {
        format!(
            "children_current row {} -> {} field chain_positions must be a JSON object",
            row.parent_logical_name_id, row.child_logical_name_id
        )
    })?;
    require_json_object(&row.canonicality_summary, || {
        format!(
            "children_current row {} -> {} field canonicality_summary must be a JSON object",
            row.parent_logical_name_id, row.child_logical_name_id
        )
    })?;

    Ok(())
}
