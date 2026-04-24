use anyhow::{Context, Result};
use sqlx::Row;

use crate::{CanonicalityState, ChainLineageBlock};

use super::{
    manifest_state::build_manifest_alert_state,
    types::{
        ManifestDriftAlertKind, ManifestDriftAlertLifecycleStatus, ManifestDriftAlertObservation,
        StoredLineageRangeBlock,
    },
};

pub(super) fn decode_stored_lineage_block(
    row: sqlx::postgres::PgRow,
) -> Result<StoredLineageRangeBlock> {
    Ok(ChainLineageBlock {
        chain_id: row.try_get("chain_id").context("missing chain_id")?,
        block_hash: row.try_get("block_hash").context("missing block_hash")?,
        parent_hash: row.try_get("parent_hash").context("missing parent_hash")?,
        block_number: row
            .try_get("block_number")
            .context("missing block_number")?,
        block_timestamp: row
            .try_get("block_timestamp")
            .context("missing block_timestamp")?,
        logs_bloom: row.try_get("logs_bloom").context("missing logs_bloom")?,
        transactions_root: row
            .try_get("transactions_root")
            .context("missing transactions_root")?,
        receipts_root: row
            .try_get("receipts_root")
            .context("missing receipts_root")?,
        state_root: row.try_get("state_root").context("missing state_root")?,
        canonicality_state: CanonicalityState::parse(
            &row.try_get::<String, _>("canonicality_state")
                .context("missing canonicality_state")?,
        )?,
    })
}

pub(super) fn decode_manifest_drift_alert_observation(
    row: sqlx::postgres::PgRow,
) -> Result<ManifestDriftAlertObservation> {
    let observation_kind = row
        .try_get::<String, _>("observation_kind")
        .context("missing observation_kind")?;
    let alert_kind = ManifestDriftAlertKind::parse_observation_kind(&observation_kind)?;
    let lifecycle_status = ManifestDriftAlertLifecycleStatus::parse(
        &row.try_get::<String, _>("lifecycle_status")
            .context("missing lifecycle_status")?,
    )?;
    let observed_canonicality_state = row
        .try_get::<Option<String>, _>("observed_canonicality_state")
        .context("missing observed_canonicality_state")?
        .map(|value| CanonicalityState::parse(&value))
        .transpose()?;
    let last_observed_at = row
        .try_get("last_observed_at")
        .context("missing last_observed_at")?;
    let raw_fact_ref = row
        .try_get("raw_fact_ref")
        .context("missing raw_fact_ref")?;
    let alert_state = build_manifest_alert_state(
        alert_kind,
        lifecycle_status,
        &row,
        observed_canonicality_state,
    )?;

    Ok(ManifestDriftAlertObservation {
        normalized_event_id: row
            .try_get("manifest_alert_observation_id")
            .context("missing manifest_alert_observation_id")?,
        event_identity: row
            .try_get("observation_identity")
            .context("missing observation_identity")?,
        alert_kind,
        namespace: row.try_get("namespace").context("missing namespace")?,
        source_family: row
            .try_get("source_family")
            .context("missing source_family")?,
        manifest_version: row
            .try_get("manifest_version")
            .context("missing manifest_version")?,
        source_manifest_id: row
            .try_get("source_manifest_id")
            .context("missing source_manifest_id")?,
        chain_id: row.try_get("chain_id").context("missing chain_id")?,
        block_number: row
            .try_get("observed_block_number")
            .context("missing observed_block_number")?,
        block_hash: row
            .try_get("observed_block_hash")
            .context("missing observed_block_hash")?,
        raw_fact_ref,
        canonicality_state: observed_canonicality_state.unwrap_or(CanonicalityState::Observed),
        alert_state,
        observed_at: last_observed_at,
    })
}

pub(super) fn decode_count(row: &sqlx::postgres::PgRow, column_name: &str) -> Result<u64> {
    let count = row
        .try_get::<i64, _>(column_name)
        .with_context(|| format!("missing {column_name}"))?;
    u64::try_from(count).with_context(|| format!("{column_name} does not fit in u64"))
}
