use serde_json::Value;
use sqlx::types::time::OffsetDateTime;

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct GasSponsorshipCurrentRebuildSummary {
    pub requested_name_count: usize,
    pub upserted_row_count: usize,
    pub deleted_row_count: u64,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct GasSponsorshipGlobalRebuildSummary {
    pub requested_namespace_count: usize,
    pub upserted_row_count: usize,
    pub deleted_row_count: u64,
}

/// One normalized event contributing to a per-name fold, in canonical chain
/// order.
#[derive(Clone, Debug, sqlx::FromRow)]
pub(super) struct NameFoldEventRow {
    pub(super) normalized_event_id: i64,
    pub(super) event_kind: String,
    pub(super) chain_id: String,
    pub(super) block_number: Option<i64>,
    pub(super) block_hash: Option<String>,
    pub(super) block_timestamp: Option<OffsetDateTime>,
    pub(super) manifest_version: i64,
    pub(super) canonicality_state: String,
    pub(super) before_state: Value,
    pub(super) after_state: Value,
}

/// One normalized event contributing to a namespace-global fold.
#[derive(Clone, Debug, sqlx::FromRow)]
pub(super) struct GlobalFoldEventRow {
    pub(super) normalized_event_id: i64,
    pub(super) event_kind: String,
    pub(super) chain_id: String,
    pub(super) block_number: Option<i64>,
    pub(super) block_hash: Option<String>,
    pub(super) block_timestamp: Option<OffsetDateTime>,
    pub(super) manifest_version: i64,
    pub(super) canonicality_state: String,
    pub(super) after_state: Value,
}
