#![recursion_limit = "256"]

use std::{
    collections::{BTreeMap, BTreeSet},
    fmt::Write as _,
    net::SocketAddr,
};

use anyhow::{Context, Result, bail};
use axum::{
    Json, Router,
    extract::{Path, Query, State},
    http::StatusCode,
    routing::get,
};
use bigname_manifests::{
    ActiveManifestVersion, CapabilityFlag, NamespaceManifestSnapshot,
    load_namespace_manifest_snapshot,
};
use bigname_storage::{
    AddressNameCurrentEntry, AddressNameRelation, AddressNamesCurrentDedupe, ChainPositions,
    ChildrenCurrentRow, DatabaseConfig, ExecutionCacheKey, ExecutionOutcome, ExecutionTrace,
    HistoryEvent, HistoryScope, NameCurrentRow, PermissionScope, PermissionsCurrentRow,
    PrimaryNameClaimStatus, PrimaryNameCurrentRow, RecordInventoryCurrentRow, ResolverCurrentRow,
    SelectedSnapshot, SnapshotAt, SnapshotConsistency, SnapshotPositionRequirement,
    SnapshotProjectionRead, SnapshotSelectionError, SnapshotSelectionErrorKind,
    SnapshotSelectionScope, SnapshotSelectorInput, SurfaceBindingKind,
    VERIFIED_PRIMARY_NAME_INVALIDATION_KEY, VERIFIED_PRIMARY_NAME_LOOKUP_KEY,
    VERIFIED_PRIMARY_NAME_REQUEST_TYPE, load_address_history, load_chain_checkpoint,
    load_execution_outcome, load_execution_trace, load_name_current,
    load_name_current_for_snapshot, load_name_history, load_name_surface,
    load_primary_name_current_snapshot, load_record_inventory_current,
    load_record_inventory_current_for_snapshot, load_resolver_current, load_resource,
    load_resource_history, load_surface_bindings_by_logical_name_id,
    load_surface_bindings_by_resource_id, parse_rfc3339_utc_timestamp,
    resolve_exact_name_snapshot_selection,
};
use clap::{Args, Parser, Subcommand};
use serde::{Deserialize, Serialize};
use serde_json::{Map as JsonMap, json};
use sqlx::{
    PgPool, Row,
    types::{
        JsonValue, Uuid,
        time::{OffsetDateTime, UtcOffset},
    },
};
use tracing::{error, info, warn};
use tracing_subscriber::EnvFilter;

mod errors;
mod pagination;
mod query;
mod routes;
mod state;

use crate::{
    errors::{ApiError, ApiResult},
    pagination::{
        CURSOR_VERSION, CursorEnvelope, CursorSpec, DEFAULT_PAGE_SIZE, HistoryPageResponse,
        MAX_PAGE_SIZE, PaginationRequest, PaginationWindow,
    },
    query::{
        AddressHistoryQuery, AddressNamesIncludeOptions, AddressNamesQuery, ChildrenQuery,
        ExactNameSnapshotQuery, HistoryQuery, InferredResolutionQuery, PermissionsQuery,
        PrimaryNameQuery, ResolutionExecutionExplainQuery, ResolutionMode, ResolutionQuery,
        ResolutionRecordKey,
    },
    routes::{API_ROUTE_DEFINITIONS, ApiRouteDefinition, ApiRouteId},
    state::AppState,
};

#[cfg(test)]
use crate::errors::ErrorResponse;
#[cfg(test)]
use axum::response::Response;

#[derive(Parser, Debug)]
#[command(name = "bigname-api", about = "Bootstrap API process for bigname")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    Serve(ServeArgs),
    PrintOpenapi,
}

#[derive(Args, Debug)]
struct ServeArgs {
    #[arg(long, env = "BIGNAME_API_BIND_ADDR", default_value = "127.0.0.1:3000")]
    bind_addr: SocketAddr,
    #[command(flatten)]
    database: DatabaseConfig,
}

#[derive(Serialize)]
struct HealthResponse {
    service: &'static str,
    phase: &'static str,
    status: &'static str,
    process: HealthProcessResponse,
    database: HealthDatabaseResponse,
}

#[derive(Serialize)]
struct HealthProcessResponse {
    status: &'static str,
}

#[derive(Serialize)]
struct HealthDatabaseResponse {
    status: &'static str,
    reachable: bool,
    check: &'static str,
    error: Option<&'static str>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct NamespaceMetadataResponse {
    data: NamespaceMetadataData,
    declared_state: NamespaceMetadataDeclaredState,
    verified_state: Option<()>,
    provenance: NamespaceMetadataProvenance,
    coverage: CoverageResponse,
    chain_positions: BTreeMap<String, ChainPositionResponse>,
    consistency: String,
    last_updated: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct NamespaceMetadataData {
    namespace: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct NamespaceMetadataDeclaredState {
    active_manifest_count: usize,
    active_source_families: Vec<String>,
    chains: Vec<String>,
    normalizer_versions: Vec<String>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct NamespaceMetadataProvenance {
    normalized_event_ids: Vec<String>,
    raw_fact_refs: Vec<String>,
    manifest_versions: Vec<ManifestVersionRef>,
    execution_trace_id: Option<String>,
    derivation_kind: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct NamespaceManifestsResponse {
    data: NamespaceManifestsData,
    declared_state: NamespaceManifestsDeclaredState,
    verified_state: Option<()>,
    provenance: NamespaceManifestsProvenance,
    coverage: CoverageResponse,
    chain_positions: BTreeMap<String, ChainPositionResponse>,
    consistency: String,
    last_updated: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct NamespaceManifestsData {
    namespace: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct NamespaceManifestsDeclaredState {
    manifests: Vec<NamespaceManifestEntry>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct NamespaceManifestEntry {
    manifest_version: u64,
    source_family: String,
    chain: String,
    deployment_epoch: String,
    normalizer_version: String,
    capability_flags: BTreeMap<String, CapabilityFlag>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct ManifestVersionRef {
    manifest_version: u64,
    source_family: String,
    chain: String,
    deployment_epoch: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct NamespaceManifestsProvenance {
    normalized_event_ids: Vec<String>,
    raw_fact_refs: Vec<String>,
    manifest_versions: Vec<ManifestVersionRef>,
    execution_trace_id: Option<String>,
    derivation_kind: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct NameResponse {
    data: JsonValue,
    declared_state: JsonValue,
    verified_state: Option<()>,
    provenance: JsonValue,
    coverage: JsonValue,
    chain_positions: JsonValue,
    consistency: String,
    last_updated: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct ResolutionResponse {
    data: JsonValue,
    declared_state: Option<JsonValue>,
    verified_state: Option<JsonValue>,
    provenance: JsonValue,
    coverage: JsonValue,
    chain_positions: JsonValue,
    consistency: String,
    last_updated: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct PrimaryNameResponse {
    data: JsonValue,
    declared_state: Option<JsonValue>,
    verified_state: Option<JsonValue>,
    provenance: JsonValue,
    coverage: JsonValue,
    chain_positions: JsonValue,
    consistency: String,
    last_updated: String,
}

type ResolverResponse = NameResponse;

#[derive(Clone, Debug, Eq, PartialEq)]
enum PrimaryNameTupleState {
    ProjectionUnavailable,
    TupleMissing,
    TuplePresent(PrimaryNameCurrentRow),
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct PrimaryNameLookupState {
    tuple_state: PrimaryNameTupleState,
    normalized_claim_name: Option<String>,
    persisted_verified: Option<PersistedPrimaryNameVerifiedReadback>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct PersistedPrimaryNameVerifiedReadback {
    verified_primary_name: JsonValue,
    provenance: JsonValue,
    finished_at: OffsetDateTime,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct HistoryResponse {
    data: Vec<JsonValue>,
    declared_state: JsonValue,
    verified_state: Option<()>,
    provenance: JsonValue,
    coverage: CoverageResponse,
    chain_positions: JsonValue,
    page: HistoryPageResponse,
    consistency: String,
    last_updated: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct ChildrenResponse {
    data: Vec<JsonValue>,
    declared_state: JsonValue,
    verified_state: Option<()>,
    provenance: JsonValue,
    coverage: CoverageResponse,
    chain_positions: JsonValue,
    page: HistoryPageResponse,
    consistency: String,
    last_updated: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct AddressNamesResponse {
    data: Vec<JsonValue>,
    declared_state: JsonValue,
    verified_state: Option<()>,
    provenance: JsonValue,
    coverage: CoverageResponse,
    chain_positions: JsonValue,
    page: HistoryPageResponse,
    consistency: String,
    last_updated: String,
}

#[derive(Clone, Debug, Default)]
struct AddressNamesResponseSupplement {
    provenances: Vec<JsonValue>,
    chain_positions: Vec<JsonValue>,
    canonicality_summaries: Vec<JsonValue>,
    last_recomputed_at: Vec<OffsetDateTime>,
}

#[derive(Clone, Debug)]
struct AddressNameExpansionFacts {
    status: JsonValue,
    expiry: JsonValue,
    record_count: JsonValue,
}

impl Default for AddressNameExpansionFacts {
    fn default() -> Self {
        Self {
            status: JsonValue::Null,
            expiry: JsonValue::Null,
            record_count: JsonValue::Null,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct ResourcePermissionsResponse {
    data: Vec<JsonValue>,
    declared_state: JsonValue,
    verified_state: Option<()>,
    provenance: JsonValue,
    coverage: CoverageResponse,
    chain_positions: JsonValue,
    page: HistoryPageResponse,
    consistency: String,
    last_updated: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct CoverageResponse {
    status: String,
    exhaustiveness: String,
    source_classes_considered: Vec<String>,
    enumeration_basis: String,
    unsupported_reason: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct ChainPositionResponse {
    chain_id: String,
    block_number: i64,
    block_hash: String,
    timestamp: String,
}

impl From<ActiveManifestVersion> for NamespaceManifestEntry {
    fn from(value: ActiveManifestVersion) -> Self {
        Self {
            manifest_version: value.manifest_version,
            source_family: value.source_family,
            chain: value.chain,
            deployment_epoch: value.deployment_epoch,
            normalizer_version: value.normalizer_version,
            capability_flags: value.capability_flags,
        }
    }
}

impl From<&NamespaceManifestEntry> for ManifestVersionRef {
    fn from(value: &NamespaceManifestEntry) -> Self {
        Self {
            manifest_version: value.manifest_version,
            source_family: value.source_family.clone(),
            chain: value.chain.clone(),
            deployment_epoch: value.deployment_epoch.clone(),
        }
    }
}

impl From<&ActiveManifestVersion> for ManifestVersionRef {
    fn from(value: &ActiveManifestVersion) -> Self {
        Self {
            manifest_version: value.manifest_version,
            source_family: value.source_family.clone(),
            chain: value.chain.clone(),
            deployment_epoch: value.deployment_epoch.clone(),
        }
    }
}

const PUBLIC_NAMESPACES: &[&str] = &["ens", "basenames"];
const VERIFIED_RESOLUTION_REQUEST_TYPE: &str = "verified_resolution";

#[tokio::main]
async fn main() -> Result<()> {
    init_tracing("bigname-api");

    match Cli::parse().command {
        Command::Serve(args) => serve(args).await,
        Command::PrintOpenapi => {
            print!("{}", render_openapi_document());
            Ok(())
        }
    }
}

include!("openapi.rs");

include!("handlers.rs");

include!("responses.rs");

include!("support.rs");

#[cfg(test)]
mod tests;
