use serde_json::Value;
use sqlx::types::time::OffsetDateTime;

/// Current resolver overview decoded from the phase projection.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ResolverCurrentRow {
    pub chain_id: String,
    pub resolver_address: String,
    pub declared_summary: Value,
    pub provenance: Value,
    pub coverage: Value,
    pub chain_positions: Value,
    pub canonicality_summary: Value,
    pub manifest_version: i64,
    pub last_recomputed_at: OffsetDateTime,
}
