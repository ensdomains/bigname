//! PostgreSQL reads for the phase schema, the verified rainbow label-preimage
//! import, and shared test migration utilities.

use std::{str::FromStr, time::Duration};

use anyhow::{Context, Result, ensure};
use clap::Args;
use sqlx::{
    PgPool,
    postgres::{PgConnectOptions, PgPoolOptions},
};

mod address_names;
mod children;
mod evm_primitives;
mod history;
mod identity;
mod identity_facade;
mod label_preimages;
mod lineage;
mod name_current;
mod normalized_events;
mod permissions;
mod phase_projection_reads;
mod primary_name;
mod projection_helpers;
mod record_inventory;
mod resolution_support;
mod resolver;
mod snapshot_selection;
pub mod sql_row;
mod time;

pub use address_names::{
    AddressNameCurrentEntry, AddressNameCurrentRow, AddressNameRelation,
    AddressNamesCurrentCountFilter, AddressNamesCurrentCursor, AddressNamesCurrentDedupe,
    AddressNamesCurrentOrder, AddressNamesCurrentPage, AddressNamesCurrentProvenanceSummary,
    AddressNamesCurrentSort, AddressNamesCurrentSortedCursor, AddressNamesCurrentSortedCursorValue,
    AddressNamesCurrentSortedPage, AddressNamesCurrentSummary,
    count_address_names_current_for_app_filter, load_address_names_current,
    load_address_names_current_for_relations, load_address_names_current_including_noncanonical,
    load_address_names_current_including_noncanonical_for_relations,
    load_address_names_current_page, load_address_names_current_page_sorted_for_relations,
};
pub use children::{
    ChildrenCurrentKeysetCursor, ChildrenCurrentPage, ChildrenCurrentRow, ChildrenCurrentSummary,
    load_children_current, load_children_current_including_noncanonical,
    load_children_current_page, load_children_current_summaries,
};
pub use evm_primitives::{
    ens_namehash_label_bytes, logical_name_id_for_name, normalize_evm_address, normalize_evm_b256,
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
    load_token_lineage_including_noncanonical,
};
pub use identity_facade::{
    IdentityAddressRelationRow, IdentityNameCurrentRow, IdentityNameRecordRow,
    IdentityPrimaryNameSnapshot, IdentityRecordInventoryRow, IndexingStatusChainRow,
    IndexingStatusRead, ReverseIdentityCursor, ReverseIdentityGroup, ReverseIdentityRecordRow,
    ReverseIdentityRoles, ReverseIdentityStorageInput,
};
pub use label_preimages::{
    ENS_RAINBOW_SOURCE_KIND, LabelPreimageImportSummary,
    import_label_preimages_from_ens_names_table,
};
pub use lineage::{
    CanonicalityState, ChainLineageBlock, chain_lineage_contains_ancestor,
    chain_lineage_contains_ancestor_at_block, chain_lineage_contains_canonical_ancestor_position,
    load_chain_lineage_block, load_chain_lineage_canonical_child_path,
    load_highest_canonical_chain_lineage_block,
};
pub use name_current::{
    NameCurrentAddressFilter, NameCurrentAddressRelationFilter, NameCurrentListCursor,
    NameCurrentListCursorValue, NameCurrentListFilter, NameCurrentListOrder, NameCurrentListPage,
    NameCurrentListRow, NameCurrentListSort, NameCurrentRow, count_name_current_list,
    load_current_names_by_resource_ids, load_name_current, load_name_current_by_logical_name_ids,
    load_name_current_for_snapshot, load_name_current_list_page,
    load_name_current_list_page_offset, load_name_current_list_row_by_name,
    load_name_current_list_row_by_namehash, name_current_list_cursor_from_row,
};
pub use normalized_events::*;
pub use permissions::{
    PermissionCoverageExhaustiveness, PermissionCoverageStatus,
    PermissionCoverageUnsupportedReason, PermissionScope, PermissionsCurrentAccountResourceCursor,
    PermissionsCurrentAccountResourcePage, PermissionsCurrentFullFilterSummary,
    PermissionsCurrentKeysetCursor, PermissionsCurrentPage, PermissionsCurrentResourceSummary,
    PermissionsCurrentRow, ResourcePermissionCoverage, load_permissions_current,
    load_permissions_current_account_resource_page,
    load_permissions_current_account_resource_page_count_summary,
    load_permissions_current_by_resource_ids, load_permissions_current_for_resolver_scope,
    load_permissions_current_page, load_permissions_current_resolver_targets,
    load_permissions_current_resource_summaries, load_permissions_current_resource_summary,
};
pub use phase_projection_reads::{
    PHASE_EXPECTED_CHAIN_IDS_SELECT, PhaseGraphqlNameCount, PhaseGraphqlNameCountTarget,
    PhaseGraphqlNameListRow, PhaseGraphqlRecordInventoryKey, PhaseGraphqlRecordInventoryRow,
    count_phase_graphql_name_list, load_phase_expected_status_chain_ids,
    load_phase_graphql_name_list_page_offset, load_phase_graphql_name_row_by_name,
    load_phase_graphql_name_row_by_namehash, load_phase_graphql_record_inventory_batch,
    load_phase_identity_name_feed_records_by_ids, load_phase_identity_records_by_ids,
    load_phase_indexing_status, load_phase_name_current_rows_by_ids,
    load_phase_resolver_bound_name_rows, load_phase_resolver_current,
};
pub use primary_name::{
    PrimaryNameClaimStatus, PrimaryNameCurrentRow, PrimaryNameCurrentSnapshot,
    load_primary_name_current, load_primary_name_current_snapshot, normalized_claim_name,
};
pub use record_inventory::{
    RecordInventoryCurrentRow, count_record_inventory_selectors_by_lookup_keys,
    load_record_inventory_current, load_record_inventory_current_batch,
    load_record_inventory_current_for_snapshot, load_record_inventory_current_with_anchor_fallback,
    record_version_boundary_storage_key,
};
pub use resolution_support::{
    BASE_MAINNET_CHAIN_ID, BASENAMES_L1_RESOLVER_ADDRESS, BASENAMES_NAMESPACE, ENS_NAMESPACE,
    ETHEREUM_MAINNET_CHAIN_ID, SupportedVerifiedResolutionRecordKey, VerifiedResolutionPathClass,
    VerifiedResolutionRecord, VerifiedResolutionRequestedChainPosition,
    VerifiedResolutionSupportBoundary, canonical_addr_coin_type,
    classify_supported_resolution_topology, is_resolution_avatar_record,
    parse_supported_verified_resolution_record_key, projected_resolution_boundaries_from_topology,
    projected_resolution_topology, record_version_boundary_has_pointer,
    resolution_record_inventory_lookup_key, resolution_record_inventory_lookup_key_any_chain,
    resolution_record_inventory_lookup_key_for_revalidation, resolution_record_version_boundary,
    resolution_record_version_boundary_for_revalidation, resolution_supports_avatar_readback,
    resolution_verified_support_boundary, row_has_basenames_supported_chain_positions,
    supported_resolution_verified_lookup_records, supported_resolution_verified_readback_records,
    supports_resolution_verified_lookup_record, try_classify_supported_resolution_topology,
    try_resolution_verified_support_boundary,
};
pub use resolver::ResolverCurrentRow;
pub use snapshot_selection::{
    ChainPosition, ChainPositions, SelectedSnapshot, SnapshotAt, SnapshotConsistency,
    SnapshotPositionRequirement, SnapshotProjectionRead, SnapshotSelectionError,
    SnapshotSelectionErrorKind, SnapshotSelectionResult, SnapshotSelectionScope,
    SnapshotSelectorInput, ensure_projection_chain_positions_match, parse_rfc3339_utc_timestamp,
    resolve_exact_name_snapshot_selection, snapshot_chain_has_head,
};

/// Checked-in migrations retained for migration validation and test database construction.
pub const MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!("../../migrations");

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

pub const fn default_database_url() -> &'static str {
    "postgres://bigname:bigname@127.0.0.1:5432/bigname"
}

pub async fn connect_phase_with_application_name_and_statement_timeout(
    config: &DatabaseConfig,
    application_name: &str,
    statement_timeout: Duration,
) -> Result<PgPool> {
    ensure!(
        !statement_timeout.is_zero(),
        "PostgreSQL statement timeout must be greater than zero"
    );
    connect_phase_inner(
        config,
        application_name,
        statement_timeout,
        config.max_connections,
    )
    .await
}

pub async fn connect_phase_reserved_readiness_pool(
    config: &DatabaseConfig,
    application_name: &str,
    check_timeout: Duration,
) -> Result<PgPool> {
    ensure!(
        !check_timeout.is_zero(),
        "PostgreSQL readiness check timeout must be greater than zero"
    );
    connect_phase_inner(config, application_name, check_timeout, 1).await
}

async fn connect_phase_inner(
    config: &DatabaseConfig,
    application_name: &str,
    statement_timeout: Duration,
    max_connections: u32,
) -> Result<PgPool> {
    PgPoolOptions::new()
        .max_connections(max_connections)
        .connect_with(connect_options(
            config,
            application_name,
            statement_timeout,
        )?)
        .await
        .context("failed to connect to the phase PostgreSQL schema")
}

fn connect_options(
    config: &DatabaseConfig,
    application_name: &str,
    statement_timeout: Duration,
) -> Result<PgConnectOptions> {
    let database_url = config
        .database_url
        .clone()
        .or_else(|| std::env::var("DATABASE_URL").ok())
        .unwrap_or_else(|| default_database_url().to_owned());

    Ok(PgConnectOptions::from_str(&database_url)
        .context("failed to parse PostgreSQL database URL")?
        .application_name(application_name)
        .options([
            ("search_path", "bigname_phase".to_owned()),
            (
                "statement_timeout",
                format!("{}ms", statement_timeout.as_millis()),
            ),
        ]))
}
