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
        validate_children_current_child_surface_shape(&mut transaction, row).await?;
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
            labelhash,
            owner,
            registrant,
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
            $8,
            $9,
            $10,
            $11::jsonb,
            $12::jsonb,
            $13::jsonb,
            $14,
            $15
        )
        ON CONFLICT (parent_logical_name_id, child_logical_name_id, surface_class) DO UPDATE
        SET
            namespace = EXCLUDED.namespace,
            canonical_display_name = EXCLUDED.canonical_display_name,
            normalized_name = EXCLUDED.normalized_name,
            namehash = EXCLUDED.namehash,
            labelhash = EXCLUDED.labelhash,
            owner = EXCLUDED.owner,
            registrant = EXCLUDED.registrant,
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
            labelhash,
            owner,
            registrant,
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
    .bind(&row.labelhash)
    .bind(&row.owner)
    .bind(&row.registrant)
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
    if let Some(labelhash) = row.labelhash.as_ref()
        && (labelhash.trim().is_empty() || labelhash != &labelhash.to_ascii_lowercase())
    {
        bail!(
            "children_current row {} -> {} has invalid labelhash {}",
            row.parent_logical_name_id,
            row.child_logical_name_id,
            labelhash
        );
    }
    validate_optional_normalized_evm_address(row, "owner", row.owner.as_deref())?;
    validate_optional_normalized_evm_address(row, "registrant", row.registrant.as_deref())?;
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

async fn validate_children_current_child_surface_shape(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    row: &ChildrenCurrentRow,
) -> Result<()> {
    let child_surface_exists = sqlx::query_scalar::<_, bool>(
        r#"
        SELECT EXISTS (
            SELECT 1
            FROM name_surfaces
            WHERE logical_name_id = $1
        )
        "#,
    )
    .bind(&row.child_logical_name_id)
    .fetch_one(&mut **transaction)
    .await
    .with_context(|| {
        format!(
            "failed to validate children_current child surface for {} -> {}",
            row.parent_logical_name_id, row.child_logical_name_id
        )
    })?;

    if child_surface_exists || has_allowed_missing_child_surface_shape(row) {
        Ok(())
    } else {
        bail!(
            "children_current row {} -> {} is missing child surface without unknown-label or label-preimage provenance",
            row.parent_logical_name_id,
            row.child_logical_name_id
        )
    }
}

fn has_allowed_missing_child_surface_shape(row: &ChildrenCurrentRow) -> bool {
    match label_provenance(row) {
        Some(("label_preimage", "known")) => row.labelhash.is_some() && !is_unknown_label_row(row),
        Some(("unknown", "unknown")) => row.labelhash.is_some() && is_unknown_label_row(row),
        _ => false,
    }
}

fn label_provenance(row: &ChildrenCurrentRow) -> Option<(&str, &str)> {
    let label = row.provenance.get("label")?;
    Some((
        label.get("source")?.as_str()?,
        label.get("status")?.as_str()?,
    ))
}

fn is_unknown_label_row(row: &ChildrenCurrentRow) -> bool {
    let Some(labelhash) = row.labelhash.as_deref() else {
        return false;
    };
    let marker = format!("[{}]", labelhash.trim_start_matches("0x"));
    first_label(&row.normalized_name) == Some(marker.as_str())
        && first_label(&row.canonical_display_name) == Some(marker.as_str())
}

fn first_label(name: &str) -> Option<&str> {
    name.split('.').next().filter(|label| !label.is_empty())
}

fn validate_optional_normalized_evm_address(
    row: &ChildrenCurrentRow,
    field_name: &str,
    value: Option<&str>,
) -> Result<()> {
    let Some(address) = value else {
        return Ok(());
    };
    if is_normalized_evm_address(address) {
        Ok(())
    } else {
        bail!(
            "children_current row {} -> {} has invalid {} address {}",
            row.parent_logical_name_id,
            row.child_logical_name_id,
            field_name,
            address
        )
    }
}

fn is_normalized_evm_address(value: &str) -> bool {
    let Some(payload) = value.strip_prefix("0x") else {
        return false;
    };
    payload.len() == 40
        && payload
            .bytes()
            .all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f'))
}
