use serde_json::Value;
use sqlx::types::time::OffsetDateTime;
use uuid::Uuid;

/// Discriminator for an execution cache dependency that has no projected surface/resource.
pub const SELECTED_CHECKPOINT_BOUNDARY_KIND: &str = "selected_checkpoint";

/// Persisted execution trace with request, chain, manifest, and ordered-step context.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExecutionTrace {
    pub execution_trace_id: Uuid,
    pub request_type: String,
    pub request_key: String,
    pub namespace: String,
    pub chain_context: Value,
    pub manifest_context: Value,
    pub contracts_called: Value,
    pub gateway_digests: Value,
    pub final_payload: Option<Value>,
    pub failure_payload: Option<Value>,
    pub request_metadata: Value,
    pub finished_at: Option<OffsetDateTime>,
    pub steps: Vec<ExecutionTraceStep>,
}

/// Persisted ordered execution step for one trace.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExecutionTraceStep {
    pub step_index: i64,
    pub step_kind: String,
    pub input_digest: Option<String>,
    pub output_digest: Option<String>,
    pub latency_ms: Option<i64>,
    pub canonicality_dependency: Value,
    pub step_payload: Value,
}

/// Read-only operational inspection snapshot for one persisted execution trace.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExecutionTraceInspection {
    pub trace: ExecutionTrace,
}

/// Deterministic cache identity for one verified execution outcome snapshot.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExecutionCacheKey {
    pub request_key: String,
    pub requested_chain_positions: Value,
    pub manifest_versions: Value,
    pub topology_version_boundary: Value,
    pub record_version_boundary: Value,
}

/// Persisted verified execution outcome keyed by the frozen cache boundary.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExecutionOutcome {
    pub cache_key: ExecutionCacheKey,
    pub execution_trace_id: Uuid,
    pub request_type: String,
    pub namespace: String,
    pub outcome_payload: Option<Value>,
    pub failure_payload: Option<Value>,
    pub finished_at: OffsetDateTime,
}

/// Exact stale manifest identity/version that should invalidate persisted execution outcomes.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExecutionManifestInvalidation {
    pub request_type: String,
    pub namespace: String,
    pub source_manifest_id: Option<i64>,
    pub source_family: Option<String>,
    pub manifest_version: i64,
}

/// Exact stale topology or record boundary that should invalidate persisted execution outcomes.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExecutionBoundaryInvalidation {
    pub request_type: String,
    pub namespace: String,
    pub boundary: Value,
}

/// Summary of one execution-outcome invalidation pass.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ExecutionOutcomeInvalidationSummary {
    pub deleted_outcome_count: u64,
}
