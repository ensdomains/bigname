use serde_json::Value;
use sqlx::types::time::OffsetDateTime;

/// Persisted current child-collection row for declared direct children only.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ChildrenCurrentRow {
    pub parent_logical_name_id: String,
    pub child_logical_name_id: String,
    pub surface_class: String,
    pub namespace: String,
    pub canonical_display_name: String,
    pub normalized_name: String,
    pub namehash: String,
    pub labelhash: Option<String>,
    pub owner: Option<String>,
    pub registrant: Option<String>,
    pub provenance: Value,
    pub chain_positions: Value,
    pub canonicality_summary: Value,
    pub manifest_version: i64,
    pub last_recomputed_at: OffsetDateTime,
}

/// Storage-local keyset cursor for declared direct child collection reads.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ChildrenCurrentKeysetCursor {
    pub canonical_display_name: String,
    pub child_logical_name_id: String,
}

impl From<&ChildrenCurrentRow> for ChildrenCurrentKeysetCursor {
    fn from(row: &ChildrenCurrentRow) -> Self {
        Self {
            canonical_display_name: row.canonical_display_name.clone(),
            child_logical_name_id: row.child_logical_name_id.clone(),
        }
    }
}

/// Compact metadata for the full declared direct child filter.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ChildrenCurrentSummary {
    pub parent_logical_name_id: String,
    pub child_count: i64,
    pub provenance_inputs: Vec<Value>,
    pub chain_positions: Vec<Value>,
    pub canonicality_summaries: Vec<Value>,
    pub last_recomputed_at: Option<OffsetDateTime>,
}

/// Bounded declared direct child page plus full-filter summary metadata.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ChildrenCurrentPage {
    pub rows: Vec<ChildrenCurrentRow>,
    pub next_cursor: Option<ChildrenCurrentKeysetCursor>,
    pub summary: ChildrenCurrentSummary,
}

/// Canonical declared-child subregistry event seed for rebuilding declared child rows.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DeclaredChildEventSource {
    pub parent_logical_name_id: String,
    pub child_logical_name_id: String,
    pub namespace: String,
    pub canonical_display_name: String,
    pub normalized_name: String,
    pub namehash: String,
    pub labelhash: Option<String>,
    pub label_source: Option<String>,
    pub owner: Option<String>,
    pub registrant: Option<String>,
    pub normalized_event_id: i64,
    pub event_identity: String,
    pub source_family: String,
    pub manifest_version: i64,
    pub source_manifest_id: Option<i64>,
    pub chain_id: String,
    pub block_number: i64,
    pub block_hash: String,
    pub transaction_hash: String,
    pub log_index: i64,
    pub raw_fact_ref: Value,
    pub normalized_event_ids: Vec<i64>,
    pub raw_fact_refs: Value,
    pub manifest_versions: Value,
}
