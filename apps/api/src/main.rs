#![recursion_limit = "256"]

use std::collections::{BTreeMap, BTreeSet};

use anyhow::{Context, Result, bail};
use axum::{
    Json, Router,
    extract::{Path, Query, State, rejection::QueryRejection},
    http::StatusCode,
    routing::{get, post},
};
use bigname_manifests::{NamespaceManifestSnapshot, load_namespace_manifest_snapshot};
use bigname_storage::{
    AddressNameCurrentEntry, AddressNameRelation, AddressNamesCurrentDedupe, ChainPositions,
    ChildrenCurrentRow, EventHistoryAddressFilter, EventHistoryFilter, ExecutionCacheKey,
    ExecutionOutcome, ExecutionTrace, HistoryEvent, HistoryScope, HistorySummary,
    HistorySummaryMode, NameCurrentRow, PermissionScope, PermissionsCurrentRow,
    PrimaryNameClaimStatus, PrimaryNameCurrentRow, RecordInventoryCurrentRow, ResolverCurrentRow,
    SelectedSnapshot, SnapshotAt, SnapshotConsistency, SnapshotPositionRequirement,
    SnapshotProjectionRead, SnapshotSelectionError, SnapshotSelectionErrorKind,
    SnapshotSelectionScope, SnapshotSelectorInput, SurfaceBindingKind,
    VERIFIED_PRIMARY_NAME_INVALIDATION_KEY, VERIFIED_PRIMARY_NAME_LOOKUP_KEY,
    VERIFIED_PRIMARY_NAME_REQUEST_TYPE, load_address_history_page, load_chain_checkpoint,
    load_event_history_page, load_execution_outcome, load_execution_trace,
    load_execution_trace_from_connection, load_name_current, load_name_current_for_snapshot,
    load_name_history_page, load_name_surface, load_primary_name_current_snapshot,
    load_record_inventory_current, load_record_inventory_current_for_snapshot,
    load_resolver_current, load_resource, load_resource_history_page,
    load_surface_bindings_by_logical_name_id, load_surface_bindings_by_resource_id,
    parse_rfc3339_utc_timestamp, resolve_exact_name_snapshot_selection,
};
use clap::Parser;
use serde_json::{Map as JsonMap, json};
use sqlx::{
    PgConnection, PgPool, Row,
    types::{JsonValue, Uuid, time::OffsetDateTime},
};
use tracing::{error, info, warn};
use tracing_subscriber::EnvFilter;

mod bounds;
mod cli;
mod errors;
mod graphql;
mod pagination;
mod query;
#[cfg(test)]
mod replay {
    pub(crate) use super::replay_staging as staging;
}
#[cfg(test)]
#[allow(dead_code)]
#[path = "../../worker/src/replay/staging.rs"]
pub(crate) mod replay_staging;
#[cfg(test)]
mod projection_apply {
    use anyhow::{Context, Result};
    use serde_json::Value;
    use sqlx::{Postgres, Transaction};

    #[derive(Clone, Copy)]
    pub(crate) enum CompletedProjectionSourceRange<'a> {
        Through(&'a Value),
        Full,
    }

    #[derive(Clone, Copy)]
    pub(crate) struct ProjectionStagingInputWatermark {
        pub(crate) normalized_change_id: i64,
        pub(crate) direct_invalidation_revision: i64,
    }

    pub(crate) async fn capture_projection_staging_input_watermark_in_transaction(
        transaction: &mut Transaction<'_, Postgres>,
    ) -> Result<ProjectionStagingInputWatermark> {
        let normalized_change_id = sqlx::query_scalar::<_, i64>(
            "SELECT public.capture_projection_normalized_event_change_watermark()",
        )
        .fetch_one(&mut **transaction)
        .await
        .context("failed to capture complete normalized-event projection change watermark")?;
        let direct_invalidation_revision = sqlx::query_scalar::<_, i64>(
            "SELECT public.capture_projection_direct_invalidation_watermark()",
        )
        .fetch_one(&mut **transaction)
        .await
        .context("failed to capture complete direct projection invalidation watermark")?;
        Ok(ProjectionStagingInputWatermark {
            normalized_change_id,
            direct_invalidation_revision,
        })
    }

    pub(crate) async fn completed_projection_sources_changed(
        transaction: &mut Transaction<'_, Postgres>,
        projection: &str,
        lower: ProjectionStagingInputWatermark,
        upper: ProjectionStagingInputWatermark,
        completed_range: CompletedProjectionSourceRange<'_>,
    ) -> Result<bool> {
        if let CompletedProjectionSourceRange::Through(last_source_key) = completed_range {
            let _ = last_source_key;
        }
        if upper.normalized_change_id <= lower.normalized_change_id
            && upper.direct_invalidation_revision <= lower.direct_invalidation_revision
        {
            return Ok(false);
        }
        sqlx::query_scalar::<_, bool>(
            r#"
            SELECT EXISTS (
                SELECT 1
                FROM projection_normalized_event_changes
                WHERE change_id > $1
                  AND change_id <= $2
            )
            OR EXISTS (
                SELECT 1
                FROM projection_direct_invalidation_revisions
                WHERE projection = $3
                  AND revision > $4
                  AND revision <= $5
            )
            "#,
        )
        .bind(lower.normalized_change_id)
        .bind(upper.normalized_change_id)
        .bind(projection)
        .bind(lower.direct_invalidation_revision)
        .bind(upper.direct_invalidation_revision)
        .fetch_one(&mut **transaction)
        .await
        .context("failed to conservatively fence API fixture projection staging")
    }
}
mod routes;
mod state;
mod status_freshness;
mod types;
mod v2;

use crate::{
    bounds::ApiBoundsConfig,
    cli::*,
    errors::{ApiError, ApiResult},
    pagination::{
        CURSOR_VERSION, CursorEnvelope, CursorSpec, DEFAULT_PAGE_SIZE, HistoryPageResponse,
        MAX_PAGE_SIZE, PaginationRequest,
    },
    query::{
        AddressHistoryQuery, AddressNamesIncludeOptions, AddressNamesQuery, ChildrenQuery,
        EventsQuery, ExactNameSnapshotQuery, HistoryQuery, MetaMode, NameProfileQuery,
        NameRecordsQuery, NameRolesQuery, NamesQuery, PermissionsQuery, PrimaryNameQuery,
        ResolutionExecutionExplainQuery, ResolutionMode, ResolutionRecordKey,
        ResolverOverviewQuery, ResourceLookupQuery, ResponseView, RolesQuery,
    },
    routes::API_ROUTE_DEFINITIONS,
    state::AppState,
    types::*,
};

#[cfg(test)]
use crate::errors::ErrorResponse;
#[cfg(test)]
use axum::response::Response;

pub(crate) const PUBLIC_NAMESPACES: &[&str] = &["ens", "basenames"];
pub(crate) const SOFTWARE_VERSION: &str = env!("CARGO_PKG_VERSION");
pub(crate) const BUILD_SHA: &str = match option_env!("BIGNAME_BUILD_SHA") {
    Some(build_sha) => build_sha,
    None => "unknown",
};
const VERIFIED_RESOLUTION_REQUEST_TYPE: &str = "verified_resolution";

#[tokio::main]
async fn main() -> Result<()> {
    match Cli::parse().command {
        Command::Serve(args) => {
            init_tracing("bigname-api");
            serve(*args).await
        }
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
