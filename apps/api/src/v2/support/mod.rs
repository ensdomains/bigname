use std::collections::{BTreeMap, BTreeSet};

use anyhow::Result;
use axum::http::StatusCode;
use bigname_storage::{
    ChainPosition, ChainPositions, NameCurrentRow, PrimaryNameClaimStatus,
    RecordInventoryCurrentRow, SelectedSnapshot, SnapshotConsistency, SnapshotPositionRequirement,
    SnapshotProjectionRead, SnapshotSelectionError, SnapshotSelectionErrorKind,
    SnapshotSelectionScope, load_name_current_for_snapshot, parse_rfc3339_utc_timestamp,
    resolve_exact_name_snapshot_selection, snapshot_chain_has_head,
};
use serde_json::json;
use sqlx::{PgPool, types::JsonValue};
use tracing::{error, warn};

use crate::{
    errors::{ApiError, ApiResult},
    state::AppState,
};

pub(crate) const BASENAMES_NAMESPACE: &str = bigname_storage::BASENAMES_NAMESPACE;
const BASENAMES_COMPAT_SOURCE_CHAIN_ID: &str = bigname_storage::BASE_MAINNET_CHAIN_ID;
const BASENAMES_COMPAT_TARGET_CHAIN_ID: &str = bigname_storage::ETHEREUM_MAINNET_CHAIN_ID;

mod alias_paths;
mod json;
mod legacy_lease;
mod legacy_lease_rows;
mod primary_name_claim_gate;
mod primary_name_live;
mod primary_name_lookup;
mod primary_name_response;
mod primary_name_types;
mod projections;
mod query_parsing;
mod record_json;
mod record_keys;
mod records;
mod registry_read;
mod resolution_lookup;
mod resolution_verified;
mod reverse_identity;
mod snapshot_disclosure;
mod snapshots;
pub(crate) mod status_freshness;

pub(crate) use alias_paths::*;
pub(crate) use json::*;
pub(crate) use legacy_lease::*;
pub(crate) use legacy_lease_rows::*;
pub(crate) use primary_name_live::*;
pub(crate) use primary_name_lookup::*;
pub(crate) use primary_name_response::*;
pub(crate) use primary_name_types::*;
pub(crate) use projections::*;
pub(crate) use query_parsing::*;
pub(crate) use record_json::*;
pub(crate) use record_keys::*;
pub(crate) use records::*;
pub(crate) use registry_read::*;
pub(crate) use resolution_lookup::*;
pub(crate) use resolution_verified::*;
#[cfg(test)]
pub(crate) use reverse_identity::test_hooks as identity_facade_count_test_hooks;
pub(crate) use reverse_identity::*;
pub(crate) use snapshot_disclosure::*;
pub(crate) use snapshots::*;

/// The test database a hook is keyed by, read on `db` so a hook inside a request's read
/// snapshot takes no second connection.
#[cfg(test)]
pub(crate) async fn test_database_on(db: &mut bigname_storage::ReadDb<'_>) -> Result<String> {
    match db {
        bigname_storage::ReadDb::Pool(pool) => {
            bigname_test_support::current_test_database(pool).await
        }
        bigname_storage::ReadDb::Snapshot(conn) => {
            Ok(sqlx::query_scalar("SELECT current_database()")
                .fetch_one(&mut **conn)
                .await?)
        }
    }
}
