use anyhow::{Context, Result, bail};
use serde_json::Value;
use sqlx::{PgPool, Row, types::time::OffsetDateTime};

use crate::{CanonicalityState, ChainLineageBlock, load_chain_lineage_block};

const DERIVATION_KIND_MANIFEST_ALERT: &str = "manifest_alert";
const EVENT_KIND_MANIFEST_CODE_HASH_DRIFT_ALERT: &str = "ManifestCodeHashDriftAlert";
const EVENT_KIND_MANIFEST_PROXY_IMPLEMENTATION_ALERT: &str = "ManifestProxyImplementationAlert";

/// Audit-facing canonicality status for one requested block identity.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CanonicalityInspectionStatus {
    Missing,
    Observed,
    Canonical,
    Safe,
    Finalized,
    Orphaned,
}

impl From<CanonicalityState> for CanonicalityInspectionStatus {
    fn from(value: CanonicalityState) -> Self {
        match value {
            CanonicalityState::Observed => Self::Observed,
            CanonicalityState::Canonical => Self::Canonical,
            CanonicalityState::Safe => Self::Safe,
            CanonicalityState::Finalized => Self::Finalized,
            CanonicalityState::Orphaned => Self::Orphaned,
        }
    }
}

/// Block-scoped raw fact counts by storage family.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct RawFactAuditCounts {
    pub raw_block_count: u64,
    pub raw_code_hash_count: u64,
    pub raw_transaction_count: u64,
    pub raw_receipt_count: u64,
    pub raw_log_count: u64,
    pub raw_call_snapshot_count: u64,
}

impl RawFactAuditCounts {
    pub const fn total(&self) -> u64 {
        self.raw_block_count
            + self.raw_code_hash_count
            + self.raw_transaction_count
            + self.raw_receipt_count
            + self.raw_log_count
            + self.raw_call_snapshot_count
    }
}

/// Read-only canonicality and fact-count inspection for one block hash.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CanonicalityInspection {
    pub chain_id: String,
    pub block_hash: String,
    pub status: CanonicalityInspectionStatus,
    pub lineage_state: Option<CanonicalityState>,
    pub parent_hash: Option<String>,
    pub block_number: Option<i64>,
    pub raw_fact_counts: RawFactAuditCounts,
    pub normalized_event_count: u64,
}

/// Stored lineage row for bounded read-only range inspection.
pub type StoredLineageRangeBlock = ChainLineageBlock;

/// Read-only stored manifest drift/proxy alert inspection.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ManifestDriftAlertInspection {
    pub code_hash_drift_alerts: Vec<ManifestDriftAlertObservation>,
    pub proxy_implementation_alerts: Vec<ManifestDriftAlertObservation>,
}

impl ManifestDriftAlertInspection {
    pub fn total_alert_count(&self) -> usize {
        self.code_hash_drift_alerts.len() + self.proxy_implementation_alerts.len()
    }
}

/// Alert family represented by a stored manifest alert normalized event.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ManifestDriftAlertKind {
    CodeHashDrift,
    ProxyImplementation,
}

impl ManifestDriftAlertKind {
    pub const fn event_kind(self) -> &'static str {
        match self {
            Self::CodeHashDrift => EVENT_KIND_MANIFEST_CODE_HASH_DRIFT_ALERT,
            Self::ProxyImplementation => EVENT_KIND_MANIFEST_PROXY_IMPLEMENTATION_ALERT,
        }
    }

    pub const fn alert_type(self) -> &'static str {
        match self {
            Self::CodeHashDrift => "manifest_code_hash_drift",
            Self::ProxyImplementation => "manifest_proxy_implementation_edge",
        }
    }

    fn parse(event_kind: &str) -> Result<Self> {
        match event_kind {
            EVENT_KIND_MANIFEST_CODE_HASH_DRIFT_ALERT => Ok(Self::CodeHashDrift),
            EVENT_KIND_MANIFEST_PROXY_IMPLEMENTATION_ALERT => Ok(Self::ProxyImplementation),
            _ => bail!("unsupported manifest drift alert event kind {event_kind}"),
        }
    }
}

/// One stored manifest drift/proxy alert observation. This is decoded from
/// normalized events only and intentionally preserves the stored payloads.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ManifestDriftAlertObservation {
    pub normalized_event_id: i64,
    pub event_identity: String,
    pub alert_kind: ManifestDriftAlertKind,
    pub namespace: String,
    pub source_family: String,
    pub manifest_version: i64,
    pub source_manifest_id: Option<i64>,
    pub chain_id: Option<String>,
    pub block_number: Option<i64>,
    pub block_hash: Option<String>,
    pub raw_fact_ref: Value,
    pub canonicality_state: CanonicalityState,
    pub alert_state: Value,
    pub observed_at: OffsetDateTime,
}

/// Inspect one block by hash-first identity without mutating storage.
pub async fn inspect_block_canonicality(
    pool: &PgPool,
    chain_id: &str,
    block_hash: &str,
) -> Result<CanonicalityInspection> {
    validate_block_identity(chain_id, block_hash)?;

    let lineage = load_chain_lineage_block(pool, chain_id, block_hash).await?;
    let raw_fact_counts = load_raw_fact_counts(pool, chain_id, block_hash).await?;
    let normalized_event_count = load_normalized_event_count(pool, chain_id, block_hash).await?;

    Ok(build_inspection(
        chain_id,
        block_hash,
        lineage,
        raw_fact_counts,
        normalized_event_count,
    ))
}

/// Inspect every stored lineage block in a bounded block-number range. Missing
/// heights cannot be inferred without a requested block hash, so this returns
/// only stored lineage identities in range order.
pub async fn inspect_canonicality_range(
    pool: &PgPool,
    chain_id: &str,
    range_start_block_number: i64,
    range_end_block_number: i64,
) -> Result<Vec<CanonicalityInspection>> {
    validate_range(chain_id, range_start_block_number, range_end_block_number)?;

    let rows = sqlx::query(
        r#"
        SELECT block_hash
        FROM chain_lineage
        WHERE chain_id = $1
          AND block_number >= $2
          AND block_number <= $3
        ORDER BY block_number, block_hash
        "#,
    )
    .bind(chain_id)
    .bind(range_start_block_number)
    .bind(range_end_block_number)
    .fetch_all(pool)
    .await
    .with_context(|| {
        format!(
            "failed to load lineage block hashes for chain {chain_id} range {range_start_block_number}..={range_end_block_number}"
        )
    })?;

    let mut inspections = Vec::with_capacity(rows.len());
    for row in rows {
        let block_hash = row
            .try_get::<String, _>("block_hash")
            .context("missing block_hash from canonicality range row")?;
        inspections.push(inspect_block_canonicality(pool, chain_id, &block_hash).await?);
    }

    Ok(inspections)
}

/// List only stored lineage rows in a bounded block-number range. The helper
/// does not infer missing heights, gaps, range completeness, or span-wide
/// canonicality.
pub async fn list_stored_lineage_range(
    pool: &PgPool,
    chain_id: &str,
    range_start_block_number: i64,
    range_end_block_number: i64,
) -> Result<Vec<StoredLineageRangeBlock>> {
    validate_range(chain_id, range_start_block_number, range_end_block_number)?;

    let rows = sqlx::query(
        r#"
        SELECT
            chain_id,
            block_hash,
            parent_hash,
            block_number,
            block_timestamp,
            logs_bloom,
            transactions_root,
            receipts_root,
            state_root,
            canonicality_state::TEXT AS canonicality_state
        FROM chain_lineage
        WHERE chain_id = $1
          AND block_number >= $2
          AND block_number <= $3
        ORDER BY block_number, block_hash
        "#,
    )
    .bind(chain_id)
    .bind(range_start_block_number)
    .bind(range_end_block_number)
    .fetch_all(pool)
    .await
    .with_context(|| {
        format!(
            "failed to list stored lineage rows for chain {chain_id} range {range_start_block_number}..={range_end_block_number}"
        )
    })?;

    rows.into_iter().map(decode_stored_lineage_block).collect()
}

/// List stored manifest drift and proxy implementation alert observations.
/// The helper reads the existing manifest-alert normalized events; it does not
/// compare chain state, create alerts, update alert lifecycle, or mutate
/// manifest/discovery state.
pub async fn list_manifest_drift_alert_observations(
    pool: &PgPool,
) -> Result<ManifestDriftAlertInspection> {
    let rows = sqlx::query(
        r#"
        SELECT
            normalized_event_id,
            event_identity,
            event_kind,
            namespace,
            source_family,
            manifest_version,
            source_manifest_id,
            chain_id,
            block_number,
            block_hash,
            raw_fact_ref,
            canonicality_state::TEXT AS canonicality_state,
            after_state AS alert_state,
            observed_at
        FROM normalized_events
        WHERE derivation_kind = $1
          AND event_kind IN ($2, $3)
        ORDER BY
            event_kind,
            COALESCE(chain_id, after_state ->> 'chain', ''),
            source_family,
            manifest_version,
            event_identity
        "#,
    )
    .bind(DERIVATION_KIND_MANIFEST_ALERT)
    .bind(EVENT_KIND_MANIFEST_CODE_HASH_DRIFT_ALERT)
    .bind(EVENT_KIND_MANIFEST_PROXY_IMPLEMENTATION_ALERT)
    .fetch_all(pool)
    .await
    .context("failed to list stored manifest drift alert observations")?;

    let mut inspection = ManifestDriftAlertInspection::default();
    for row in rows {
        let observation = decode_manifest_drift_alert_observation(row)?;
        match observation.alert_kind {
            ManifestDriftAlertKind::CodeHashDrift => {
                inspection.code_hash_drift_alerts.push(observation);
            }
            ManifestDriftAlertKind::ProxyImplementation => {
                inspection.proxy_implementation_alerts.push(observation);
            }
        }
    }

    Ok(inspection)
}

fn build_inspection(
    chain_id: &str,
    block_hash: &str,
    lineage: Option<ChainLineageBlock>,
    raw_fact_counts: RawFactAuditCounts,
    normalized_event_count: u64,
) -> CanonicalityInspection {
    let status = lineage
        .as_ref()
        .map(|block| CanonicalityInspectionStatus::from(block.canonicality_state))
        .unwrap_or(CanonicalityInspectionStatus::Missing);
    let lineage_state = lineage.as_ref().map(|block| block.canonicality_state);
    let parent_hash = lineage.as_ref().and_then(|block| block.parent_hash.clone());
    let block_number = lineage.as_ref().map(|block| block.block_number);

    CanonicalityInspection {
        chain_id: chain_id.to_owned(),
        block_hash: block_hash.to_owned(),
        status,
        lineage_state,
        parent_hash,
        block_number,
        raw_fact_counts,
        normalized_event_count,
    }
}

async fn load_raw_fact_counts(
    pool: &PgPool,
    chain_id: &str,
    block_hash: &str,
) -> Result<RawFactAuditCounts> {
    let row = sqlx::query(
        r#"
        SELECT
          (SELECT COUNT(*)::BIGINT FROM raw_blocks WHERE chain_id = $1 AND block_hash = $2) AS raw_block_count,
          (SELECT COUNT(*)::BIGINT FROM raw_code_hashes WHERE chain_id = $1 AND block_hash = $2) AS raw_code_hash_count,
          (SELECT COUNT(*)::BIGINT FROM raw_transactions WHERE chain_id = $1 AND block_hash = $2) AS raw_transaction_count,
          (SELECT COUNT(*)::BIGINT FROM raw_receipts WHERE chain_id = $1 AND block_hash = $2) AS raw_receipt_count,
          (SELECT COUNT(*)::BIGINT FROM raw_logs WHERE chain_id = $1 AND block_hash = $2) AS raw_log_count,
          (SELECT COUNT(*)::BIGINT FROM raw_call_snapshots WHERE chain_id = $1 AND block_hash = $2) AS raw_call_snapshot_count
        "#,
    )
    .bind(chain_id)
    .bind(block_hash)
    .fetch_one(pool)
    .await
    .with_context(|| format!("failed to load raw fact audit counts for chain {chain_id} block {block_hash}"))?;

    Ok(RawFactAuditCounts {
        raw_block_count: decode_count(&row, "raw_block_count")?,
        raw_code_hash_count: decode_count(&row, "raw_code_hash_count")?,
        raw_transaction_count: decode_count(&row, "raw_transaction_count")?,
        raw_receipt_count: decode_count(&row, "raw_receipt_count")?,
        raw_log_count: decode_count(&row, "raw_log_count")?,
        raw_call_snapshot_count: decode_count(&row, "raw_call_snapshot_count")?,
    })
}

async fn load_normalized_event_count(
    pool: &PgPool,
    chain_id: &str,
    block_hash: &str,
) -> Result<u64> {
    let row = sqlx::query(
        r#"
        SELECT COUNT(*)::BIGINT AS normalized_event_count
        FROM normalized_events
        WHERE chain_id = $1
          AND block_hash = $2
        "#,
    )
    .bind(chain_id)
    .bind(block_hash)
    .fetch_one(pool)
    .await
    .with_context(|| {
        format!(
            "failed to load normalized-event audit count for chain {chain_id} block {block_hash}"
        )
    })?;

    decode_count(&row, "normalized_event_count")
}

fn decode_stored_lineage_block(row: sqlx::postgres::PgRow) -> Result<StoredLineageRangeBlock> {
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

fn decode_manifest_drift_alert_observation(
    row: sqlx::postgres::PgRow,
) -> Result<ManifestDriftAlertObservation> {
    let event_kind = row
        .try_get::<String, _>("event_kind")
        .context("missing event_kind")?;
    let alert_kind = ManifestDriftAlertKind::parse(&event_kind)?;

    Ok(ManifestDriftAlertObservation {
        normalized_event_id: row
            .try_get("normalized_event_id")
            .context("missing normalized_event_id")?,
        event_identity: row
            .try_get("event_identity")
            .context("missing event_identity")?,
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
            .try_get("block_number")
            .context("missing block_number")?,
        block_hash: row.try_get("block_hash").context("missing block_hash")?,
        raw_fact_ref: row
            .try_get("raw_fact_ref")
            .context("missing raw_fact_ref")?,
        canonicality_state: CanonicalityState::parse(
            &row.try_get::<String, _>("canonicality_state")
                .context("missing canonicality_state")?,
        )?,
        alert_state: row.try_get("alert_state").context("missing alert_state")?,
        observed_at: row.try_get("observed_at").context("missing observed_at")?,
    })
}

fn decode_count(row: &sqlx::postgres::PgRow, column_name: &str) -> Result<u64> {
    let count = row
        .try_get::<i64, _>(column_name)
        .with_context(|| format!("missing {column_name}"))?;
    u64::try_from(count).with_context(|| format!("{column_name} does not fit in u64"))
}

fn validate_block_identity(chain_id: &str, block_hash: &str) -> Result<()> {
    if chain_id.trim().is_empty() {
        bail!("chain_id must not be empty");
    }
    if block_hash.trim().is_empty() {
        bail!("block_hash must not be empty");
    }
    Ok(())
}

fn validate_range(chain_id: &str, start: i64, end: i64) -> Result<()> {
    if chain_id.trim().is_empty() {
        bail!("chain_id must not be empty");
    }
    if start < 0 {
        bail!("canonicality inspection range start {start} is negative");
    }
    if end < start {
        bail!("canonicality inspection range end {end} is before start {start}");
    }
    Ok(())
}

#[cfg(test)]
mod tests;
