use serde_json::Value;
use sqlx::types::time::OffsetDateTime;
use uuid::Uuid;

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
