//! Shared PostgreSQL storage and migration utilities.
use std::{str::FromStr, time::Duration};
mod address_names;
mod audit;
mod backfill_jobs;
mod checkpoints;
mod children;
mod connection;
mod evm_primitives;
mod execution;
mod history;
mod identity;
mod identity_facade;
mod label_preimages;
mod lineage;
mod migration_indexes;
mod name_current;
mod normalized_events;
mod permissions;
mod primary_name;
mod projection_helpers;
pub mod projection_staging;
mod raw;
mod raw_calls;
mod raw_children;
mod raw_code;
mod raw_payload_cache;
mod record_inventory;
mod resolution_support;
mod resolver;
mod service_heartbeats;
mod snapshot_selection;
pub mod sql_row;
mod time;
mod versions;
pub use address_names::{
    AddressNameCurrentEntry, AddressNameCurrentRow, AddressNameRelation,
    AddressNamesCurrentAddressReplacement, AddressNamesCurrentCountFilter,
    AddressNamesCurrentCursor, AddressNamesCurrentDedupe, AddressNamesCurrentFullRebuild,
    AddressNamesCurrentOrder, AddressNamesCurrentPage, AddressNamesCurrentProvenanceSummary,
    AddressNamesCurrentSort, AddressNamesCurrentSortedCursor, AddressNamesCurrentSortedCursorValue,
    AddressNamesCurrentSortedPage, AddressNamesCurrentSummary,
    begin_address_names_current_address_replacement, begin_address_names_current_full_rebuild,
    clear_address_names_current, collapse_address_name_current_rows,
    count_address_names_current_for_app_filter, delete_address_names_current,
    drop_address_names_current_address_replacement, drop_address_names_current_full_rebuild,
    insert_address_names_current_address_replacement_rows,
    insert_address_names_current_full_rebuild_rows, load_address_names_current,
    load_address_names_current_for_relations, load_address_names_current_including_noncanonical,
    load_address_names_current_including_noncanonical_for_relations,
    load_address_names_current_page, load_address_names_current_page_sorted_for_relations,
    publish_address_names_current_address_replacement, publish_address_names_current_full_rebuild,
    rebuild_address_names_current_identity_sidecars, replace_address_names_current_logical_names,
    upsert_address_names_current_rows,
};
use anyhow::{Context, Result, ensure};
pub use audit::{
    CanonicalityInspection, CanonicalityInspectionStatus, ManifestDriftAlertInspection,
    ManifestDriftAlertKind, ManifestDriftAlertObservation, RawFactAuditCounts,
    RawPayloadCacheAuditMetadata, StoredLineageRangeBlock, inspect_block_canonicality,
    inspect_canonicality_range, list_manifest_drift_alert_observations,
    list_raw_payload_cache_audit_metadata, list_stored_lineage_range,
};
mod long_operation_exports;
pub use checkpoints::{
    ChainCheckpoint, ChainCheckpointUpdate, CheckpointBlockRef, advance_chain_checkpoints,
    advance_chain_checkpoints_rejecting_non_orphaned_lineage_forks,
    advance_chain_checkpoints_rejecting_non_orphaned_lineage_forks_in_transaction,
    load_chain_checkpoint, load_chain_checkpoint_snapshots, rewind_chain_checkpoints_to_ancestor,
    sync_chain_checkpoints,
};
pub use children::{
    ChildrenCurrentKeysetCursor, ChildrenCurrentPage, ChildrenCurrentRow, ChildrenCurrentSummary,
    DeclaredChildEventSource, clear_children_current, delete_children_current,
    load_canonical_declared_child_sources, load_canonical_ens_v1_declared_child_sources,
    load_children_current, load_children_current_including_noncanonical,
    load_children_current_page, load_children_current_summaries,
    stream_canonical_declared_child_sources, upsert_children_current_rows,
};
use clap::Args;
pub use connection::{PROJECTION_REPLAY_VERSION_SETTING, stamp_projection_replay_version};
pub use evm_primitives::{ens_namehash_label_bytes, normalize_evm_address, normalize_evm_b256};
pub use execution::{
    ExecutionBoundaryInvalidation, ExecutionCacheKey, ExecutionManifestInvalidation,
    ExecutionOutcome, ExecutionOutcomeInvalidationSummary, ExecutionTrace,
    ExecutionTraceInspection, ExecutionTraceStep, PrimaryNameRouteCachePruneSummary,
    SELECTED_CHECKPOINT_BOUNDARY_KIND, invalidate_execution_outcomes_for_manifest_version,
    invalidate_execution_outcomes_for_manifest_version_and_request_key,
    invalidate_execution_outcomes_for_orphaned_blocks_in_transaction,
    invalidate_execution_outcomes_for_record_boundary,
    invalidate_execution_outcomes_for_record_boundary_and_request_key,
    invalidate_execution_outcomes_for_topology_boundary,
    invalidate_execution_outcomes_for_topology_boundary_and_request_key, load_execution_outcome,
    load_execution_outcome_for_update_in_transaction, load_execution_trace,
    load_execution_trace_from_connection, load_execution_trace_inspection,
    load_resolution_execution_outcome_at_snapshot, prune_route_local_primary_name_execution,
    upsert_execution_outcome, upsert_execution_outcome_in_transaction, upsert_execution_trace,
    upsert_execution_trace_in_transaction,
};
pub use history::{
    EventHistoryAddressFilter, EventHistoryFilter, HistoryChainPositionSample, HistoryCursor,
    HistoryEvent, HistoryPage, HistoryScope, HistorySummary, HistorySummaryMode,
    InvalidHistoryCursor, load_address_history, load_address_history_for_relations,
    load_address_history_page, load_address_history_page_for_relations, load_event_history,
    load_event_history_page, load_name_history, load_name_history_head, load_name_history_page,
    load_resource_history, load_resource_history_page,
};
pub use identity::{
    NameSurface, Resource, SurfaceBinding, SurfaceBindingKind, TokenLineage,
    ens_v2_registry_resource_id, load_name_surface, load_name_surface_including_noncanonical,
    load_name_surfaces_by_logical_name_ids, load_resource, load_resource_including_noncanonical,
    load_surface_binding, load_surface_binding_including_noncanonical,
    load_surface_bindings_by_logical_name_id,
    load_surface_bindings_by_logical_name_id_including_noncanonical,
    load_surface_bindings_by_resource_id,
    load_surface_bindings_by_resource_id_including_noncanonical, load_token_lineage,
    load_token_lineage_including_noncanonical, upsert_name_surfaces,
    upsert_name_surfaces_without_snapshots, upsert_resources, upsert_resources_without_snapshots,
    upsert_surface_bindings, upsert_surface_bindings_without_snapshots, upsert_token_lineages,
    upsert_token_lineages_without_snapshots,
};
pub use identity_facade::{
    IdentityAddressRelationRow, IdentityNameCurrentRow, IdentityNameRecordRow,
    IdentityPrimaryNameSnapshot, IdentityRecordInventoryRow, IndexingStatusChainRow,
    IndexingStatusRead, PENDING_INVALIDATION_COUNT_CAP, ReverseIdentityCursor,
    ReverseIdentityFeedGroup, ReverseIdentityFeedInput, ReverseIdentityFeedRecordRow,
    ReverseIdentityGroup, ReverseIdentityRecordRow, ReverseIdentityRoles,
    ReverseIdentityStorageInput, load_expected_status_chain_ids,
    load_identity_name_feed_records_by_names, load_identity_records_by_names, load_indexing_status,
    load_reverse_identity_feed_records, load_reverse_identity_records,
};
#[cfg(any(test, feature = "test-support"))]
pub use label_preimages::upsert_label_preimages_from_normalized_events;
pub use label_preimages::{
    LabelPreimage, LabelPreimageImportSummary, import_label_preimages_from_ens_names_table,
    label_preimage_from_label, upsert_label_preimages, upsert_label_preimages_in_transaction,
};
pub use lineage::{
    CanonicalityState, ChainLineageBlock, chain_lineage_contains_ancestor,
    chain_lineage_contains_ancestor_at_block, chain_lineage_contains_canonical_ancestor_position,
    load_chain_lineage_block, load_chain_lineage_canonical_child_path,
    load_highest_canonical_chain_lineage_block, upsert_chain_lineage_blocks,
    upsert_chain_lineage_blocks_without_snapshots,
};
pub use long_operation_exports::*;
pub use migration_indexes::{
    DEFERRED_NORMALIZED_EVENT_INDEXES, NormalizedReplayIndexDdlGuard,
    RECORD_INVENTORY_REPLAY_INDEX, TEMPORARY_NORMALIZED_REPLAY_INDEXES,
    acquire_normalized_replay_index_ddl_guard, count_unready_normalized_event_indexes,
};
pub use name_current::{
    NameCurrentAddressFilter, NameCurrentAddressRelationFilter, NameCurrentListCursor,
    NameCurrentListCursorValue, NameCurrentListFilter, NameCurrentListOrder, NameCurrentListPage,
    NameCurrentListRow, NameCurrentListSort, NameCurrentReplacement, NameCurrentRow,
    clear_name_current, count_name_current_list, delete_name_current,
    load_current_names_by_resource_ids, load_name_current, load_name_current_by_logical_name_ids,
    load_name_current_for_snapshot, load_name_current_list_page,
    load_name_current_list_page_offset, load_name_current_list_row_by_name,
    load_name_current_list_row_by_namehash, name_current_list_cursor_from_row,
    replace_name_current_rows, upsert_name_current_rows,
};
pub use normalized_events::*;
pub use permissions::{
    PERMISSIONS_CURRENT_PUBLICATION_VERSION, PermissionCoverageExhaustiveness,
    PermissionCoverageStatus, PermissionCoverageUnsupportedReason, PermissionScope,
    PermissionsCurrentAccountResourceCursor, PermissionsCurrentAccountResourcePage,
    PermissionsCurrentFullFilterSummary, PermissionsCurrentKeysetCursor, PermissionsCurrentPage,
    PermissionsCurrentResourceSummary, PermissionsCurrentRow, ResourcePermissionCoverage,
    clear_permissions_current, delete_permissions_current, load_permissions_current,
    load_permissions_current_account_resource_page,
    load_permissions_current_account_resource_page_count_summary,
    load_permissions_current_by_resource_ids, load_permissions_current_for_resolver_scope,
    load_permissions_current_page, load_permissions_current_resolver_targets,
    load_permissions_current_resource_summaries, load_permissions_current_resource_summary,
    publish_permissions_current_compatibility_in_transaction,
    replace_permissions_current_resource_projection, upsert_permissions_current_resource_summary,
    upsert_permissions_current_rows,
};
pub use primary_name::{
    PrimaryNameClaimStatus, PrimaryNameCurrentRow, PrimaryNameCurrentSnapshot,
    VERIFIED_PRIMARY_NAME_INVALIDATION_KEY, VERIFIED_PRIMARY_NAME_LOOKUP_KEY,
    VERIFIED_PRIMARY_NAME_REQUEST_TYPE, VerifiedPrimaryNameClaimHooks,
    VerifiedPrimaryNameInvalidationHook, VerifiedPrimaryNameLookupHook,
    clear_primary_names_current, delete_primary_name_current,
    delete_primary_name_current_in_transaction, fallback as primary_name_fallback,
    load_primary_name_current, load_primary_name_current_snapshot,
    load_primary_name_current_snapshot_for_update_in_transaction,
    lock_primary_name_tuple_in_transaction, lock_primary_names_current_replacement_in_transaction,
    publish_primary_names_current_full_rebuild,
    publish_primary_names_current_full_rebuild_in_transaction, upsert_primary_name_current_rows,
    upsert_primary_name_current_snapshots, upsert_primary_name_current_snapshots_in_transaction,
    verified_primary_name_claim_hooks,
};
pub use raw::{
    RawBlock, RawLogReplayInput, list_canonical_raw_log_replay_inputs,
    list_canonical_raw_log_replay_inputs_for_block_hashes, load_raw_block,
    load_raw_blocks_by_hashes, upsert_raw_blocks, upsert_raw_blocks_without_snapshots,
};
pub use raw_calls::{
    RawCallSnapshot, load_raw_call_snapshots_by_block_hash, upsert_raw_call_snapshots,
    upsert_raw_call_snapshots_in_transaction,
};
pub use raw_children::{
    RawLog, RawReceipt, RawTransaction, upsert_raw_logs, upsert_raw_logs_without_snapshots,
    upsert_raw_receipts, upsert_raw_receipts_without_snapshots, upsert_raw_transactions,
    upsert_raw_transactions_without_snapshots,
};
pub use raw_code::{
    RawCodeHash, load_raw_code_hash_counts_by_block_hashes, upsert_raw_code_hashes,
};
pub use raw_payload_cache::{
    RawPayloadCacheDigestVerification, RawPayloadCacheMetadata, RawPayloadCacheMetadataUpsert,
    list_raw_payload_cache_metadata_by_block_hash, load_raw_payload_cache_metadata,
    upsert_raw_payload_cache_metadata, verify_raw_payload_cache_digest,
};
pub use record_inventory::{
    RecordInventoryCurrentRow, clear_record_inventory_current,
    count_record_inventory_selectors_by_lookup_keys, delete_record_inventory_current,
    load_record_inventory_current, load_record_inventory_current_batch,
    load_record_inventory_current_for_snapshot, load_record_inventory_current_with_anchor_fallback,
    upsert_record_inventory_current_rows,
};
pub use resolution_support::{
    BASE_MAINNET_CHAIN_ID, BASENAMES_L1_RESOLVER_ADDRESS, BASENAMES_NAMESPACE,
    ENS_LEGACY_EVENT_SILENT_REVERSE_RESOLVER_ADDRESSES, ENS_NAMESPACE, ETHEREUM_MAINNET_CHAIN_ID,
    SupportedVerifiedResolutionRecordKey, VerifiedResolutionPathClass, VerifiedResolutionRecord,
    VerifiedResolutionRequestedChainPosition, VerifiedResolutionSupportBoundary,
    build_resolution_execution_cache_key, build_resolution_requested_chain_positions,
    canonical_addr_coin_type, classify_supported_resolution_topology, is_resolution_avatar_record,
    normalized_resolution_request_key, normalized_resolution_request_key_from_record_keys,
    parse_supported_verified_resolution_record_key, projected_resolution_boundaries_from_topology,
    projected_resolution_topology, record_version_boundary_has_pointer,
    resolution_execution_cache_lookup_records, resolution_record_inventory_lookup_key,
    resolution_record_inventory_lookup_key_any_chain,
    resolution_record_inventory_lookup_key_for_revalidation, resolution_record_version_boundary,
    resolution_record_version_boundary_for_revalidation,
    resolution_requested_chain_positions_from_projection, resolution_supports_avatar_readback,
    resolution_verified_support_boundary, row_has_basenames_supported_chain_positions,
    supported_resolution_verified_lookup_records, supported_resolution_verified_readback_records,
    supports_resolution_verified_lookup_record, try_classify_supported_resolution_topology,
    try_resolution_verified_support_boundary,
};
pub use resolver::{
    ResolverCurrentRow, clear_resolver_current, delete_resolver_current, load_resolver_current,
    upsert_resolver_current_rows,
};
pub use service_heartbeats::{
    DEFAULT_INDEXER_CHAIN_HEARTBEAT_MAX_AGE_SECS, DEFAULT_WORKER_REBUILD_PHASE_MAX_AGE_SECS,
    INDEXER_SERVICE_NAME, ServiceLoopChainHeartbeat, ServiceLoopHeartbeat,
    ServiceLoopPhaseHeartbeat, WORKER_SERVICE_NAME, begin_service_loop_phase,
    deregister_service_loop, ensure_service_loop_heartbeat_recent,
    ensure_service_loop_heartbeat_recent_with_phase,
    ensure_service_loop_heartbeat_recent_with_phase_and_chain, finish_service_loop_phase,
    load_preferred_service_loop_heartbeats,
    load_preferred_service_loop_heartbeats_with_indexer_chain_max_age, load_service_loop_heartbeat,
    record_service_loop_heartbeat, register_service_loop, resolve_service_instance_id,
};
pub use snapshot_selection::{
    ChainPosition, ChainPositions, SelectedSnapshot, SnapshotAt, SnapshotConsistency,
    SnapshotPositionRequirement, SnapshotProjectionRead, SnapshotSelectionError,
    SnapshotSelectionErrorKind, SnapshotSelectionResult, SnapshotSelectionScope,
    SnapshotSelectorInput, ensure_projection_chain_positions_match, parse_rfc3339_utc_timestamp,
    resolve_exact_name_snapshot_selection, snapshot_chain_has_head,
};
use sqlx::PgPool;
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use tracing::info;
pub use versions::{CURRENT_PROJECTION_REPLAY_VERSION, latest_migration_version};
/// Checked-in database migrations.
pub const MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!("../../migrations");

/// Common database settings shared by the services.
#[derive(Args, Clone, Debug)]
pub struct DatabaseConfig {
    #[arg(long, env = "BIGNAME_DATABASE_URL")]
    pub database_url: Option<String>,
    #[arg(
        long,
        env = "BIGNAME_DATABASE_MAX_CONNECTIONS",
        default_value_t = 10_u32
    )]
    pub max_connections: u32,
}

impl Default for DatabaseConfig {
    fn default() -> Self {
        Self {
            database_url: Some(default_database_url().to_owned()),
            max_connections: 10,
        }
    }
}

/// Default database URL for local development.
pub const fn default_database_url() -> &'static str {
    "postgres://bigname:bigname@127.0.0.1:5432/bigname"
}

/// Open a PostgreSQL connection pool using the shared settings.
pub async fn connect(config: &DatabaseConfig) -> Result<PgPool> {
    connect_inner(config, None, None).await
}

/// Open a PostgreSQL connection pool with an application name visible in `pg_stat_activity`.
pub async fn connect_with_application_name(
    config: &DatabaseConfig,
    application_name: &str,
) -> Result<PgPool> {
    connect_inner(config, Some(application_name), None).await
}

/// Open a named PostgreSQL pool whose every connection has a statement timeout.
pub async fn connect_with_application_name_and_statement_timeout(
    config: &DatabaseConfig,
    application_name: &str,
    statement_timeout: Duration,
) -> Result<PgPool> {
    ensure!(
        !statement_timeout.is_zero(),
        "PostgreSQL statement timeout must be greater than zero"
    );
    connect_inner(config, Some(application_name), Some(statement_timeout)).await
}

/// Open a named, single-connection pool reserved for a bounded readiness check.
pub async fn connect_reserved_readiness_pool(
    config: &DatabaseConfig,
    application_name: &str,
    check_timeout: Duration,
) -> Result<PgPool> {
    ensure!(
        !check_timeout.is_zero(),
        "PostgreSQL readiness check timeout must be greater than zero"
    );
    let options = connect_options(config, Some(application_name), Some(check_timeout))?;
    PgPoolOptions::new()
        .min_connections(1)
        .max_connections(1)
        .acquire_timeout(check_timeout)
        .idle_timeout(None)
        .max_lifetime(None)
        .connect_with(stamp_projection_replay_version(options))
        .await
        .context("failed to connect reserved PostgreSQL readiness pool")
}

async fn connect_inner(
    config: &DatabaseConfig,
    application_name: Option<&str>,
    statement_timeout: Option<Duration>,
) -> Result<PgPool> {
    let options = connect_options(config, application_name, statement_timeout)?;
    PgPoolOptions::new()
        .max_connections(config.max_connections)
        .connect_with(stamp_projection_replay_version(options))
        .await
        .context("failed to connect to PostgreSQL")
}

fn connect_options(
    config: &DatabaseConfig,
    application_name: Option<&str>,
    statement_timeout: Option<Duration>,
) -> Result<PgConnectOptions> {
    let database_url = config
        .database_url
        .clone()
        .or_else(|| std::env::var("DATABASE_URL").ok())
        .unwrap_or_else(|| default_database_url().to_owned());

    let mut options = stamp_projection_replay_version(
        PgConnectOptions::from_str(&database_url)
            .context("failed to parse PostgreSQL database URL")?,
    );
    if let Some(application_name) = application_name {
        options = options.application_name(application_name);
    }
    if let Some(statement_timeout) = statement_timeout {
        options = options.options([(
            "statement_timeout",
            format!("{}ms", statement_timeout.as_millis()),
        )]);
    }
    Ok(options)
}

/// Apply all checked-in migrations.
pub async fn migrate(pool: &PgPool) -> Result<()> {
    migration_indexes::run_migrations_and_ensure_required_indexes_ready(pool, &MIGRATOR).await?;
    info!("checked-in migrations applied");
    Ok(())
}
