mod canonicality;
mod decode;
mod manifest;
mod manifest_json;
mod manifest_live;
mod manifest_state;
mod manifest_validation;
mod types;

pub use canonicality::{
    inspect_block_canonicality, inspect_canonicality_range, list_raw_payload_cache_audit_metadata,
    list_stored_lineage_range,
};
pub use manifest::list_manifest_drift_alert_observations;
pub use types::{
    CanonicalityInspection, CanonicalityInspectionStatus, ManifestDriftAlertInspection,
    ManifestDriftAlertKind, ManifestDriftAlertObservation, RawFactAuditCounts,
    RawPayloadCacheAuditMetadata, StoredLineageRangeBlock,
};

#[cfg(test)]
use crate::{CanonicalityState, ChainLineageBlock};
#[cfg(test)]
use anyhow::Context;
#[cfg(test)]
pub use manifest::upsert_manifest_drift_alert_observation;
#[cfg(test)]
pub use types::{ManifestDriftAlertLifecycleStatus, ManifestDriftAlertObservationCreate};

#[cfg(test)]
mod tests;
