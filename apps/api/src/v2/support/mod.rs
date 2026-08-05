use std::collections::{BTreeMap, BTreeSet};

use anyhow::{Context, Result, bail};
use axum::http::StatusCode;
use bigname_storage::{
    AddressNameRelation, AddressNamesCurrentDedupe, ChainPosition, ChainPositions,
    ExecutionCacheKey, ExecutionOutcome, ExecutionTrace, HistoryScope, HistorySummaryMode,
    NameCurrentRow, PermissionScope, PrimaryNameClaimStatus, RecordInventoryCurrentRow,
    SelectedSnapshot, SnapshotAt, SnapshotConsistency, SnapshotPositionRequirement,
    SnapshotProjectionRead, SnapshotSelectionError, SnapshotSelectionErrorKind,
    SnapshotSelectionScope, SnapshotSelectorInput, SurfaceBindingKind,
    VERIFIED_PRIMARY_NAME_REQUEST_TYPE, load_chain_checkpoint, load_execution_outcome,
    load_execution_trace_from_connection, load_name_current_for_snapshot,
    load_primary_name_current_snapshot, load_record_inventory_current,
    load_record_inventory_current_for_snapshot, load_surface_bindings_by_logical_name_id,
    load_surface_bindings_by_resource_id, parse_rfc3339_utc_timestamp,
    resolve_exact_name_snapshot_selection,
};
use serde_json::json;
use sqlx::{
    PgConnection, PgPool, Row,
    types::{JsonValue, Uuid},
};
use tracing::{error, warn};

use crate::{
    errors::{ApiError, ApiResult},
    query::*,
    state::AppState,
    types::*,
    *,
};

#[cfg(test)]
use sqlx::types::time::OffsetDateTime;

pub(crate) const BASENAMES_NAMESPACE: &str = bigname_storage::BASENAMES_NAMESPACE;
const BASENAMES_COMPAT_SOURCE_CHAIN_ID: &str = bigname_storage::BASE_MAINNET_CHAIN_ID;
const BASENAMES_COMPAT_TARGET_CHAIN_ID: &str = bigname_storage::ETHEREUM_MAINNET_CHAIN_ID;
const BASENAMES_COMPAT_CONTRACT_ADDRESS: &str = bigname_storage::BASENAMES_L1_RESOLVER_ADDRESS;

mod history;
mod identity_facade;
pub(crate) mod permissions_support;
mod primary_name_execution;
mod primary_name_live;
pub(crate) mod primary_name_lookup;
mod primary_name_readback;
mod primary_name_readback_fence;
mod primary_name_response;
mod primary_name_rpc;
mod primary_name_types;
mod projections;
mod query_parsing;
mod record_json;
mod record_keys;
mod records;
mod resolution_diagnostics;
mod resolution_lookup;
pub(crate) mod resolution_on_demand;
mod resolution_verified;
mod snapshots;
pub(crate) mod status_freshness;

pub(crate) use history::*;
#[cfg(test)]
pub(crate) use identity_facade::test_hooks as identity_facade_count_test_hooks;
pub(crate) use identity_facade::*;
pub(crate) use primary_name_execution::*;
pub(crate) use primary_name_live::*;
pub(crate) use primary_name_lookup::*;
pub(crate) use primary_name_readback::*;
pub(crate) use primary_name_readback_fence::*;
pub(crate) use primary_name_response::*;
pub(crate) use primary_name_rpc::*;
pub(crate) use primary_name_types::*;
pub(crate) use projections::*;
pub(crate) use query_parsing::*;
pub(crate) use record_json::*;
pub(crate) use record_keys::*;
pub(crate) use records::*;
pub(crate) use resolution_diagnostics::*;
pub(crate) use resolution_lookup::*;
pub(crate) use resolution_verified::*;
pub(crate) use snapshots::*;
