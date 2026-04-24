use anyhow::{Result, bail};
use serde_json::Value;
use sqlx::types::time::OffsetDateTime;
use uuid::Uuid;

use crate::{CanonicalityState, ChainLineageBlock};

pub(super) const EVENT_KIND_MANIFEST_CODE_HASH_DRIFT_ALERT: &str = "ManifestCodeHashDriftAlert";
pub(super) const EVENT_KIND_MANIFEST_PROXY_IMPLEMENTATION_ALERT: &str =
    "ManifestProxyImplementationAlert";
pub(super) const MANIFEST_PROXY_IMPLEMENTATION_EDGE_KIND: &str = "proxy_implementation";
pub(super) const MANIFEST_PROXY_IMPLEMENTATION_DISCOVERY_SOURCE: &str = "manifest_declared_proxy";
pub(super) const OBSERVATION_KIND_MANIFEST_DRIFT: &str = "manifest_drift";
pub(super) const OBSERVATION_KIND_PROXY_IMPLEMENTATION_DRIFT: &str = "proxy_implementation_drift";

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

/// Read-only audit summary for retained payload-cache metadata on one block.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RawPayloadCacheAuditMetadata {
    pub payload_kind: String,
    pub digest_algorithm: Option<String>,
    pub retained_digest: Option<String>,
    pub block_number: Option<i64>,
    pub payload_size_bytes: i64,
    pub content_type: Option<String>,
    pub content_encoding: Option<String>,
    pub cache_metadata: Value,
    pub canonicality_state: CanonicalityState,
    pub first_observed_at: OffsetDateTime,
    pub last_observed_at: OffsetDateTime,
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

/// Alert family represented by a stored manifest alert observation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ManifestDriftAlertKind {
    CodeHashDrift,
    ProxyImplementation,
}

impl ManifestDriftAlertKind {
    pub const fn observation_kind(self) -> &'static str {
        match self {
            Self::CodeHashDrift => OBSERVATION_KIND_MANIFEST_DRIFT,
            Self::ProxyImplementation => OBSERVATION_KIND_PROXY_IMPLEMENTATION_DRIFT,
        }
    }

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

    pub(super) fn parse_observation_kind(observation_kind: &str) -> Result<Self> {
        match observation_kind {
            OBSERVATION_KIND_MANIFEST_DRIFT => Ok(Self::CodeHashDrift),
            OBSERVATION_KIND_PROXY_IMPLEMENTATION_DRIFT => Ok(Self::ProxyImplementation),
            _ => bail!("unsupported manifest drift observation kind {observation_kind}"),
        }
    }
}

/// Persisted lifecycle state for a worker-owned manifest alert observation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ManifestDriftAlertLifecycleStatus {
    Active,
    Acknowledged,
    Remediated,
    Dismissed,
}

impl ManifestDriftAlertLifecycleStatus {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Active => "active",
            Self::Acknowledged => "acknowledged",
            Self::Remediated => "remediated",
            Self::Dismissed => "dismissed",
        }
    }

    pub(super) fn parse(value: &str) -> Result<Self> {
        match value {
            "active" => Ok(Self::Active),
            "acknowledged" => Ok(Self::Acknowledged),
            "remediated" => Ok(Self::Remediated),
            "dismissed" => Ok(Self::Dismissed),
            _ => bail!("unsupported manifest drift alert lifecycle status {value}"),
        }
    }
}

/// Immutable creation contract for a worker-owned manifest drift/proxy alert
/// observation. Reusing the same `observation_identity` is idempotent only when
/// all persisted alert material matches the existing row.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ManifestDriftAlertObservationCreate {
    pub observation_identity: String,
    pub alert_kind: ManifestDriftAlertKind,
    pub lifecycle_status: ManifestDriftAlertLifecycleStatus,
    pub namespace: String,
    pub source_family: String,
    pub manifest_version: i64,
    pub source_manifest_id: Option<i64>,
    pub chain_id: String,
    pub contract_instance_id: Uuid,
    pub proxy_contract_instance_id: Option<Uuid>,
    pub expected_implementation_contract_instance_id: Option<Uuid>,
    pub observed_implementation_contract_instance_id: Option<Uuid>,
    pub discovery_edge_id: Option<i64>,
    pub expected_code_hash: Option<String>,
    pub observed_code_hash: Option<String>,
    pub observed_code_byte_length: Option<i64>,
    pub observed_block_number: Option<i64>,
    pub observed_block_hash: Option<String>,
    pub observed_canonicality_state: Option<CanonicalityState>,
    pub raw_fact_ref: Value,
    pub expected_material: Value,
    pub observed_material: Value,
    pub watch_plan_metadata: Value,
    pub alert_metadata: Value,
    pub remediation_status: Option<String>,
    pub remediation_metadata: Option<Value>,
    pub first_observed_at: OffsetDateTime,
    pub last_observed_at: OffsetDateTime,
    pub remediated_at: Option<OffsetDateTime>,
}

/// One stored manifest drift/proxy alert observation. The `normalized_event_id`
/// field is the alert observation row id kept under its historic API name so
/// existing worker inspection rendering remains source-compatible.
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
