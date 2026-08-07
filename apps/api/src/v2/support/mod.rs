use std::collections::{BTreeMap, BTreeSet};

use anyhow::{Context, Result};
use axum::http::StatusCode;
use bigname_storage::{
    ChainPosition, ChainPositions, NameCurrentRow, PrimaryNameClaimStatus,
    RecordInventoryCurrentRow, SelectedSnapshot, SnapshotConsistency, SnapshotPositionRequirement,
    SnapshotProjectionRead, SnapshotSelectionError, SnapshotSelectionErrorKind,
    SnapshotSelectionScope, SnapshotSelectorInput, load_name_current_for_snapshot,
    load_record_inventory_current_for_snapshot, parse_rfc3339_utc_timestamp,
    resolve_exact_name_snapshot_selection, snapshot_chain_has_head,
};
use serde_json::json;
use sqlx::{
    PgPool, Row,
    types::{JsonValue, Uuid},
};
use tracing::{error, warn};

use crate::{
    errors::{ApiError, ApiResult},
    state::AppState,
    *,
};

pub(crate) const BASENAMES_NAMESPACE: &str = bigname_storage::BASENAMES_NAMESPACE;
const BASENAMES_COMPAT_SOURCE_CHAIN_ID: &str = bigname_storage::BASE_MAINNET_CHAIN_ID;
const BASENAMES_COMPAT_TARGET_CHAIN_ID: &str = bigname_storage::ETHEREUM_MAINNET_CHAIN_ID;

mod identity_facade;
mod json;
mod primary_name_live;
mod primary_name_lookup;
mod primary_name_response;
mod primary_name_types;
mod projections;
mod query_parsing;
mod record_json;
mod record_keys;
mod records;
mod resolution_lookup;
mod resolution_verified;
mod snapshots;
pub(crate) mod status_freshness;

#[cfg(test)]
pub(crate) use identity_facade::test_hooks as identity_facade_count_test_hooks;
pub(crate) use identity_facade::*;
pub(crate) use json::*;
pub(crate) use primary_name_live::*;
pub(crate) use primary_name_lookup::*;
pub(crate) use primary_name_response::*;
pub(crate) use primary_name_types::*;
pub(crate) use projections::*;
pub(crate) use query_parsing::*;
pub(crate) use record_json::*;
pub(crate) use record_keys::*;
pub(crate) use records::*;
pub(crate) use resolution_lookup::*;
pub(crate) use resolution_verified::*;
pub(crate) use snapshots::*;

use super::format_timestamp;
