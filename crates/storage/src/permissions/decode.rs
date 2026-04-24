use anyhow::{Result, bail};
use serde_json::Value;
use sqlx::{Row, postgres::PgRow};
use uuid::Uuid;

use crate::projection_helpers::take_json_array;

use super::types::{PermissionScope, PermissionsCurrentFullFilterSummary, PermissionsCurrentRow};

pub(super) fn decode_permissions_current_row(row: PgRow) -> Result<PermissionsCurrentRow> {
    let scope_kind: String = row.try_get("scope_kind")?;
    let scope_detail: Value = row.try_get("scope_detail")?;
    let scope = PermissionScope::parse(&scope_kind, &scope_detail)?;
    let stored_scope: String = row.try_get("scope")?;
    let expected_scope = scope.storage_key();
    if stored_scope != expected_scope {
        bail!(
            "permissions_current scope mismatch for resource_id {} subject {}: stored {stored_scope}, decoded {expected_scope}",
            row.try_get::<Uuid, _>("resource_id")?,
            row.try_get::<String, _>("subject")?
        );
    }

    Ok(PermissionsCurrentRow {
        resource_id: row.try_get("resource_id")?,
        subject: row.try_get("subject")?,
        scope,
        effective_powers: row.try_get("effective_powers")?,
        grant_source: row.try_get("grant_source")?,
        revocation_source: row.try_get("revocation_source")?,
        inheritance_path: row.try_get("inheritance_path")?,
        transfer_behavior: row.try_get("transfer_behavior")?,
        provenance: row.try_get("provenance")?,
        coverage: row.try_get("coverage")?,
        chain_positions: row.try_get("chain_positions")?,
        canonicality_summary: row.try_get("canonicality_summary")?,
        manifest_version: row.try_get("manifest_version")?,
        last_recomputed_at: row.try_get("last_recomputed_at")?,
    })
}

pub(super) fn decode_permissions_current_full_filter_summary(
    row: PgRow,
) -> Result<PermissionsCurrentFullFilterSummary> {
    Ok(PermissionsCurrentFullFilterSummary {
        row_count: row.try_get("row_count")?,
        provenance: json_array(row.try_get("provenance")?, "provenance")?,
        coverage: row.try_get("coverage")?,
        chain_positions: json_array(row.try_get("chain_positions")?, "chain_positions")?,
        canonicality_summaries: json_array(
            row.try_get("canonicality_summaries")?,
            "canonicality_summaries",
        )?,
        last_recomputed_at: row.try_get("last_recomputed_at")?,
    })
}

fn json_array(value: Value, field: &str) -> Result<Vec<Value>> {
    take_json_array(value, || {
        format!("permissions_current summary field {field} must be a JSON array")
    })
}
