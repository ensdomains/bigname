use bigname_storage::{
    CanonicalityState, PermissionsCurrentResourceSummary, PermissionsCurrentRow,
};
use serde_json::Value;
use sqlx::types::time::OffsetDateTime;
use uuid::Uuid;

#[derive(Clone, Debug, sqlx::FromRow)]
pub(super) struct ResourceProjectionContext {
    pub(super) resource_id: Uuid,
    pub(super) chain_id: String,
    pub(super) block_number: i64,
    pub(super) block_hash: String,
    pub(super) provenance: Value,
    pub(super) canonicality_state: CanonicalityState,
    pub(super) block_timestamp: Option<OffsetDateTime>,
}

#[derive(Clone, Debug)]
pub(super) struct ProjectedPermissionsResource {
    pub(super) rows: Vec<PermissionsCurrentRow>,
    pub(super) summary: Option<PermissionsCurrentResourceSummary>,
}

#[derive(Clone, Debug, sqlx::FromRow)]
pub(super) struct RelevantEvent {
    pub(super) normalized_event_id: i64,
    pub(super) event_kind: String,
    pub(super) source_family: String,
    pub(super) manifest_version: i64,
    pub(super) source_manifest_id: Option<i64>,
    pub(super) chain_id: String,
    pub(super) block_number: i64,
    pub(super) block_hash: String,
    pub(super) log_index: Option<i64>,
    pub(super) block_timestamp: Option<OffsetDateTime>,
    pub(super) raw_fact_ref: Value,
    pub(super) canonicality_state: CanonicalityState,
    pub(super) after_state: Value,
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub(super) struct PermissionKey {
    pub(super) subject: String,
    pub(super) scope: String,
}

#[derive(Clone, Debug)]
pub(super) struct ChainPositionCandidate {
    pub(super) chain_id: String,
    pub(super) block_number: i64,
    pub(super) block_hash: String,
    pub(super) timestamp: OffsetDateTime,
}
