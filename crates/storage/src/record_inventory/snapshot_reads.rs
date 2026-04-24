use anyhow::{Context, Result};
use serde_json::Value;
use sqlx::PgPool;
use uuid::Uuid;

use crate::snapshot_selection::{
    ChainPositions, SnapshotProjectionRead, SnapshotSelectionError,
    ensure_projection_chain_positions_match,
};

use super::{
    boundary_key::record_version_boundary_storage_key,
    row_decode::{RecordInventoryCurrentRow, decode_record_inventory_current_row},
};

const DEFAULT_RECORD_INVENTORY_CURRENT_READ_FILTER: &str = r#"
  AND resource.canonicality_state IN (
      'canonical'::canonicality_state,
      'safe'::canonicality_state,
      'finalized'::canonicality_state
  )
"#;

/// Load one record-inventory projection row by resource and exact version boundary.
pub async fn load_record_inventory_current(
    pool: &PgPool,
    resource_id: Uuid,
    record_version_boundary: &Value,
) -> Result<Option<RecordInventoryCurrentRow>> {
    let record_version_boundary_key = record_version_boundary_storage_key(
        record_version_boundary,
        resource_id,
    )
    .with_context(|| {
        format!(
            "failed to derive record_inventory_current boundary key for resource_id {resource_id}"
        )
    })?;

    let row = sqlx::query(&format!(
        r#"
        SELECT
            ric.resource_id,
            ric.record_version_boundary,
            ric.enumeration_basis,
            ric.selectors,
            ric.explicit_gaps,
            ric.unsupported_families,
            ric.last_change,
            ric.entries,
            ric.provenance,
            ric.coverage,
            ric.chain_positions,
            ric.canonicality_summary,
            ric.manifest_version,
            ric.last_recomputed_at
        FROM record_inventory_current ric
        JOIN resources resource
          ON resource.resource_id = ric.resource_id
        WHERE ric.resource_id = $1
          AND ric.record_version_boundary_key = $2
        {DEFAULT_RECORD_INVENTORY_CURRENT_READ_FILTER}
        "#,
    ))
    .bind(resource_id)
    .bind(&record_version_boundary_key)
    .fetch_optional(pool)
    .await
    .with_context(|| {
        format!("failed to load record_inventory_current row for resource_id {resource_id}")
    })?;

    row.map(decode_record_inventory_current_row).transpose()
}

/// Load one record-inventory projection row only if it is eligible for the selected snapshot.
///
/// A present row with different chain-position context is reported as `stale`
/// instead of being joined into an exact-name response for another snapshot.
pub async fn load_record_inventory_current_for_snapshot(
    pool: &PgPool,
    resource_id: Uuid,
    record_version_boundary: &Value,
    selected_chain_positions: &ChainPositions,
) -> std::result::Result<SnapshotProjectionRead<RecordInventoryCurrentRow>, SnapshotSelectionError>
{
    let row = load_record_inventory_current(pool, resource_id, record_version_boundary)
        .await
        .map_err(|error| {
            SnapshotSelectionError::internal(format!(
                "failed to load record_inventory_current row for resource_id {resource_id}: {error}"
            ))
        })?;

    let Some(row) = row else {
        return Ok(SnapshotProjectionRead::NotFound);
    };

    ensure_projection_chain_positions_match(
        "record_inventory_current",
        &row.chain_positions,
        selected_chain_positions,
    )?;
    Ok(SnapshotProjectionRead::Found(row))
}

/// Delete one record-inventory projection row so a worker can rebuild that exact key.
pub async fn delete_record_inventory_current(
    pool: &PgPool,
    resource_id: Uuid,
    record_version_boundary: &Value,
) -> Result<u64> {
    let record_version_boundary_key = record_version_boundary_storage_key(
        record_version_boundary,
        resource_id,
    )
    .with_context(|| {
        format!(
            "failed to derive record_inventory_current delete key for resource_id {resource_id}"
        )
    })?;

    sqlx::query(
        r#"
        DELETE FROM record_inventory_current
        WHERE resource_id = $1
          AND record_version_boundary_key = $2
        "#,
    )
    .bind(resource_id)
    .bind(&record_version_boundary_key)
    .execute(pool)
    .await
    .with_context(|| {
        format!("failed to delete record_inventory_current row for resource_id {resource_id}")
    })
    .map(|result| result.rows_affected())
}

/// Clear the record-inventory projection so a worker can perform a one-shot rebuild.
pub async fn clear_record_inventory_current(pool: &PgPool) -> Result<u64> {
    sqlx::query("DELETE FROM record_inventory_current")
        .execute(pool)
        .await
        .context("failed to clear record_inventory_current rows")
        .map(|result| result.rows_affected())
}
