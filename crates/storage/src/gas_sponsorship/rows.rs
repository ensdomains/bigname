use anyhow::{Context, Result, bail};
use serde_json::Value;
use sqlx::types::time::OffsetDateTime;
use sqlx::{Row, postgres::PgRow};

/// Per-name sponsored-update accounting projection row.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GasSponsorshipCurrentRow {
    pub logical_name_id: String,
    pub namespace: String,
    pub normalized_name: String,
    pub namehash: String,
    pub lease_start_at: Option<OffsetDateTime>,
    pub registered_seconds_total: i64,
    pub earned_updates: i64,
    pub spent_updates: i64,
    pub last_sponsored_write_at: Option<OffsetDateTime>,
    pub provenance: Value,
    pub coverage: Value,
    pub chain_positions: Value,
    pub canonicality_summary: Value,
    pub manifest_version: i64,
    pub last_recomputed_at: OffsetDateTime,
}

/// Namespace-wide sponsored-gas totals projection row. Wei and USD totals are
/// decimal strings backed by `numeric` columns so 256-bit gas sums never
/// truncate.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GasSponsorshipGlobalCurrentRow {
    pub namespace: String,
    pub sponsored_op_count: i64,
    pub attributed_op_count: i64,
    pub failed_op_count: i64,
    pub gas_wei_total: String,
    pub failed_gas_wei_total: String,
    pub usd_e8_total: String,
    pub unpriced_wei_total: String,
    pub provenance: Value,
    pub coverage: Value,
    pub chain_positions: Value,
    pub canonicality_summary: Value,
    pub manifest_version: i64,
    pub last_recomputed_at: OffsetDateTime,
}

pub(super) fn validate_gas_sponsorship_current_row(row: &GasSponsorshipCurrentRow) -> Result<()> {
    if row.logical_name_id != format!("{}:{}", row.namespace, row.normalized_name) {
        bail!(
            "gas_sponsorship_current logical_name_id {} must equal namespace:normalized_name",
            row.logical_name_id
        );
    }
    if row.namehash.len() != 66 || !row.namehash.starts_with("0x") {
        bail!(
            "gas_sponsorship_current namehash must be 0x-prefixed 32-byte hex, got {}",
            row.namehash
        );
    }
    if row.registered_seconds_total < 0 || row.earned_updates < 0 || row.spent_updates < 0 {
        bail!(
            "gas_sponsorship_current counters must be non-negative for {}",
            row.logical_name_id
        );
    }
    Ok(())
}

pub(super) fn validate_gas_sponsorship_global_current_row(
    row: &GasSponsorshipGlobalCurrentRow,
) -> Result<()> {
    if row.namespace.trim().is_empty() {
        bail!("gas_sponsorship_global_current namespace must be nonblank");
    }
    if row.sponsored_op_count < 0
        || row.attributed_op_count < 0
        || row.failed_op_count < 0
        || row.attributed_op_count > row.sponsored_op_count
        || row.failed_op_count > row.sponsored_op_count
    {
        bail!(
            "gas_sponsorship_global_current op counts are inconsistent for namespace {}",
            row.namespace
        );
    }
    for (field, value) in [
        ("gas_wei_total", &row.gas_wei_total),
        ("failed_gas_wei_total", &row.failed_gas_wei_total),
        ("usd_e8_total", &row.usd_e8_total),
        ("unpriced_wei_total", &row.unpriced_wei_total),
    ] {
        if value.is_empty() || !value.bytes().all(|byte| byte.is_ascii_digit()) {
            bail!(
                "gas_sponsorship_global_current {field} must be a non-negative decimal string, got {value}"
            );
        }
    }
    Ok(())
}

pub(super) fn decode_gas_sponsorship_current_row(row: PgRow) -> Result<GasSponsorshipCurrentRow> {
    let snapshot = GasSponsorshipCurrentRow {
        logical_name_id: row
            .try_get("logical_name_id")
            .context("gas_sponsorship_current row missing logical_name_id")?,
        namespace: row
            .try_get("namespace")
            .context("gas_sponsorship_current row missing namespace")?,
        normalized_name: row
            .try_get("normalized_name")
            .context("gas_sponsorship_current row missing normalized_name")?,
        namehash: row
            .try_get("namehash")
            .context("gas_sponsorship_current row missing namehash")?,
        lease_start_at: row
            .try_get("lease_start_at")
            .context("gas_sponsorship_current row missing lease_start_at")?,
        registered_seconds_total: row
            .try_get("registered_seconds_total")
            .context("gas_sponsorship_current row missing registered_seconds_total")?,
        earned_updates: row
            .try_get("earned_updates")
            .context("gas_sponsorship_current row missing earned_updates")?,
        spent_updates: row
            .try_get("spent_updates")
            .context("gas_sponsorship_current row missing spent_updates")?,
        last_sponsored_write_at: row
            .try_get("last_sponsored_write_at")
            .context("gas_sponsorship_current row missing last_sponsored_write_at")?,
        provenance: row
            .try_get("provenance")
            .context("gas_sponsorship_current row missing provenance")?,
        coverage: row
            .try_get("coverage")
            .context("gas_sponsorship_current row missing coverage")?,
        chain_positions: row
            .try_get("chain_positions")
            .context("gas_sponsorship_current row missing chain_positions")?,
        canonicality_summary: row
            .try_get("canonicality_summary")
            .context("gas_sponsorship_current row missing canonicality_summary")?,
        manifest_version: row
            .try_get("manifest_version")
            .context("gas_sponsorship_current row missing manifest_version")?,
        last_recomputed_at: row
            .try_get("last_recomputed_at")
            .context("gas_sponsorship_current row missing last_recomputed_at")?,
    };
    validate_gas_sponsorship_current_row(&snapshot)?;
    Ok(snapshot)
}

pub(super) fn decode_gas_sponsorship_global_current_row(
    row: PgRow,
) -> Result<GasSponsorshipGlobalCurrentRow> {
    let snapshot = GasSponsorshipGlobalCurrentRow {
        namespace: row
            .try_get("namespace")
            .context("gas_sponsorship_global_current row missing namespace")?,
        sponsored_op_count: row
            .try_get("sponsored_op_count")
            .context("gas_sponsorship_global_current row missing sponsored_op_count")?,
        attributed_op_count: row
            .try_get("attributed_op_count")
            .context("gas_sponsorship_global_current row missing attributed_op_count")?,
        failed_op_count: row
            .try_get("failed_op_count")
            .context("gas_sponsorship_global_current row missing failed_op_count")?,
        gas_wei_total: row
            .try_get("gas_wei_total")
            .context("gas_sponsorship_global_current row missing gas_wei_total")?,
        failed_gas_wei_total: row
            .try_get("failed_gas_wei_total")
            .context("gas_sponsorship_global_current row missing failed_gas_wei_total")?,
        usd_e8_total: row
            .try_get("usd_e8_total")
            .context("gas_sponsorship_global_current row missing usd_e8_total")?,
        unpriced_wei_total: row
            .try_get("unpriced_wei_total")
            .context("gas_sponsorship_global_current row missing unpriced_wei_total")?,
        provenance: row
            .try_get("provenance")
            .context("gas_sponsorship_global_current row missing provenance")?,
        coverage: row
            .try_get("coverage")
            .context("gas_sponsorship_global_current row missing coverage")?,
        chain_positions: row
            .try_get("chain_positions")
            .context("gas_sponsorship_global_current row missing chain_positions")?,
        canonicality_summary: row
            .try_get("canonicality_summary")
            .context("gas_sponsorship_global_current row missing canonicality_summary")?,
        manifest_version: row
            .try_get("manifest_version")
            .context("gas_sponsorship_global_current row missing manifest_version")?,
        last_recomputed_at: row
            .try_get("last_recomputed_at")
            .context("gas_sponsorship_global_current row missing last_recomputed_at")?,
    };
    validate_gas_sponsorship_global_current_row(&snapshot)?;
    Ok(snapshot)
}
