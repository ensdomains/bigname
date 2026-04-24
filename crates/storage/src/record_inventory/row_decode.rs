use anyhow::{Context, Result, bail};
use serde_json::Value;
use sqlx::types::time::OffsetDateTime;
use sqlx::{Row, postgres::PgRow};
use uuid::Uuid;

use super::{
    boundary_key::record_version_boundary_storage_key,
    validation::validate_record_inventory_current_row,
};

/// Persisted record-inventory and cache projection row keyed by resource and version boundary.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RecordInventoryCurrentRow {
    pub resource_id: Uuid,
    pub record_version_boundary: Value,
    pub enumeration_basis: Value,
    pub selectors: Value,
    pub explicit_gaps: Value,
    pub unsupported_families: Value,
    pub last_change: Option<Value>,
    pub entries: Value,
    pub provenance: Value,
    pub coverage: Value,
    pub chain_positions: Value,
    pub canonicality_summary: Value,
    pub manifest_version: i64,
    pub last_recomputed_at: OffsetDateTime,
}

pub(super) fn decode_record_inventory_current_row(row: PgRow) -> Result<RecordInventoryCurrentRow> {
    let resource_id: Uuid = row
        .try_get("resource_id")
        .context("record_inventory_current row missing resource_id")?;
    let record_version_boundary: Value = row
        .try_get("record_version_boundary")
        .context("record_inventory_current row missing record_version_boundary")?;
    let boundary_key = record_version_boundary_storage_key(&record_version_boundary, resource_id)?;
    let stored_boundary_key: String = row
        .try_get("record_version_boundary_key")
        .unwrap_or_else(|_| boundary_key.clone());
    if stored_boundary_key != boundary_key {
        bail!(
            "record_inventory_current boundary mismatch for resource_id {}: stored {}, decoded {}",
            resource_id,
            stored_boundary_key,
            boundary_key
        );
    }

    let snapshot = RecordInventoryCurrentRow {
        resource_id,
        record_version_boundary,
        enumeration_basis: row
            .try_get("enumeration_basis")
            .context("record_inventory_current row missing enumeration_basis")?,
        selectors: row
            .try_get("selectors")
            .context("record_inventory_current row missing selectors")?,
        explicit_gaps: row
            .try_get("explicit_gaps")
            .context("record_inventory_current row missing explicit_gaps")?,
        unsupported_families: row
            .try_get("unsupported_families")
            .context("record_inventory_current row missing unsupported_families")?,
        last_change: row
            .try_get("last_change")
            .context("record_inventory_current row missing last_change")?,
        entries: row
            .try_get("entries")
            .context("record_inventory_current row missing entries")?,
        provenance: row
            .try_get("provenance")
            .context("record_inventory_current row missing provenance")?,
        coverage: row
            .try_get("coverage")
            .context("record_inventory_current row missing coverage")?,
        chain_positions: row
            .try_get("chain_positions")
            .context("record_inventory_current row missing chain_positions")?,
        canonicality_summary: row
            .try_get("canonicality_summary")
            .context("record_inventory_current row missing canonicality_summary")?,
        manifest_version: row
            .try_get("manifest_version")
            .context("record_inventory_current row missing manifest_version")?,
        last_recomputed_at: row
            .try_get("last_recomputed_at")
            .context("record_inventory_current row missing last_recomputed_at")?,
    };

    validate_record_inventory_current_row(&snapshot)?;
    Ok(snapshot)
}
