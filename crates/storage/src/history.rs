mod address_matches;
mod binding_anchors;
mod block_window;
mod decoders;
mod duplicates;
mod event_page;
mod filters;
#[cfg(any(test, feature = "test-support"))]
pub mod history_anchor_read_test_hooks;
mod lineage;
mod options;
mod paging;
#[cfg(any(test, feature = "test-support"))]
mod query_plan;
mod read_filter;
mod redo;
mod registration_identity;
mod selectors;
mod source;
mod summary;
mod wrapped_registrar;

use anyhow::{Context, Result};
use serde_json::Value;
use sqlx::{PgPool, types::time::OffsetDateTime};
use uuid::Uuid;

use crate::{CanonicalityState, address_names::AddressNameRelation};

use address_matches::load_address_history_selector;
use paging::{load_event_history_rows, load_history, load_history_head};
use read_filter::{EventHistoryReadFilter, event_history_read_filter};
use selectors::{name_history_selector, resource_history_selector};

pub use block_window::resolve_chain_block_ranges;
pub use event_page::{load_event_history_page, load_event_history_page_with_redo_policy};
pub use options::{
    ChainBlockRange, HistoryBlockWindow, HistoryOrder, HistoryPageOptions, HistoryScope,
};
pub use redo::{
    InterpretRedoFence, InterpretRedoInProgress, capture_interpret_redo_fence,
    revalidate_interpret_redo_fence,
};
pub use redo::{SelectedInterpretRedoState, load_selected_interpret_redo_state};
pub use wrapped_registrar::load_wrapped_registrar_resource_ids_by_logical_name_id;

/// Replay-stable normalized event exposed to history readers.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HistoryEvent {
    pub normalized_event_id: i64,
    pub event_identity: String,
    pub namespace: String,
    pub logical_name_id: Option<String>,
    pub resource_id: Option<Uuid>,
    pub registration_id: Option<Uuid>,
    pub event_kind: String,
    pub source_family: String,
    pub manifest_version: i64,
    pub source_manifest_id: Option<i64>,
    pub chain_id: Option<String>,
    pub block_number: Option<i64>,
    pub block_hash: Option<String>,
    pub block_timestamp: Option<OffsetDateTime>,
    pub transaction_hash: Option<String>,
    pub log_index: Option<i64>,
    pub raw_fact_ref: Value,
    pub derivation_kind: String,
    pub canonicality_state: CanonicalityState,
    pub before_state: Value,
    pub after_state: Value,
    pub migration_correlation_ids: Vec<String>,
    pub consumer_visibility: String,
    pub migration_associations: Value,
    pub provenance: Value,
    pub coverage: Value,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HistoryCursor {
    pub normalized_event_id: i64,
    pub event_identity: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HistoryChainPositionSample {
    pub chain_id: String,
    pub block_number: i64,
    pub block_hash: String,
    pub block_timestamp: OffsetDateTime,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HistorySummary {
    pub total_count: u64,
    pub normalized_event_ids: Vec<String>,
    pub raw_fact_refs: Vec<Value>,
    pub manifest_versions: Vec<Value>,
    pub chain_position_samples: Vec<HistoryChainPositionSample>,
    pub last_updated: Option<OffsetDateTime>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HistorySummaryMode {
    None,
    Count,
    /// Count at most `cap + 1` matching rows; a `total_count` above the cap
    /// tells the caller the exact count was not computed.
    CappedCount(u64),
    Full,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HistoryPage {
    pub rows: Vec<HistoryEvent>,
    pub next_cursor: Option<HistoryCursor>,
    pub summary: Option<HistorySummary>,
    pub interpret_redo_fence: Option<InterpretRedoFence>,
}

#[derive(Debug)]
pub struct InvalidHistoryCursor;

impl std::fmt::Display for InvalidHistoryCursor {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("history page cursor does not match filtered event history")
    }
}

impl std::error::Error for InvalidHistoryCursor {}

/// Address-derived anchor filter for app-facing event history reads.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EventHistoryAddressFilter {
    pub address: String,
    pub relation: Option<AddressNameRelation>,
}

/// Resolver-contract filter: rows emitted by the resolver on `chain_id`, plus
/// `ResolverChanged` rows whose pointer was set to or cleared from it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EventHistoryResolverFilter {
    pub chain_id: String,
    pub address: String,
}

/// Projection-backed filters for canonical normalized-event history reads.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct EventHistoryFilter {
    pub namespace: Option<String>,
    pub logical_name_id: Option<String>,
    pub resource_id: Option<Uuid>,
    pub address: Option<EventHistoryAddressFilter>,
    pub resolver: Option<EventHistoryResolverFilter>,
    /// Lowercase EVM address of the emitting contract; matches the raw log emitter.
    pub contract_address: Option<String>,
    pub event_kinds: Vec<String>,
    pub bind_cursor_anchor_to_event_kinds: bool,
    pub from_block: Option<i64>,
    pub to_block: Option<i64>,
    pub order: HistoryOrder,
    pub block_window: Option<HistoryBlockWindow>,
    /// Publication upper bounds for expanding bindings and historical ownership anchors.
    pub publication_block_bounds: Option<std::collections::BTreeMap<String, i64>>,
}

/// Load history rows for one logical name anchor.
pub async fn load_name_history(
    pool: &PgPool,
    logical_name_id: &str,
    resource_ids: &[Uuid],
    scope: HistoryScope,
    canonical_only: bool,
) -> Result<Vec<HistoryEvent>> {
    load_history(
        pool,
        name_history_selector(logical_name_id, resource_ids, scope),
        canonical_only,
    )
    .await
    .with_context(|| {
        format!(
            "failed to load history for logical_name_id {logical_name_id} with scope {}",
            scope.as_str()
        )
    })
}

/// Load one SQL-keyset page for one logical name anchor.
#[allow(clippy::too_many_arguments)]
pub async fn load_name_history_page(
    pool: &PgPool,
    logical_name_id: &str,
    resource_ids: &[Uuid],
    scope: HistoryScope,
    canonical_only: bool,
    cursor: Option<&HistoryCursor>,
    page_size: u64,
    summary_mode: HistorySummaryMode,
    options: &HistoryPageOptions,
    interpret_redo_fence: Option<&InterpretRedoFence>,
) -> Result<HistoryPage> {
    #[cfg(any(test, feature = "test-support"))]
    history_anchor_read_test_hooks::run_if(pool, interpret_redo_fence.is_some()).await?;
    paging::load_history_page(
        pool,
        EventHistoryReadFilter {
            selectors: vec![name_history_selector(logical_name_id, resource_ids, scope)],
            ..EventHistoryReadFilter::default()
        }
        .with_page_options(options),
        canonical_only,
        cursor,
        page_size,
        summary_mode,
        false,
        interpret_redo_fence,
    )
    .await
    .with_context(|| {
        format!(
            "failed to load history page for logical_name_id {logical_name_id} with scope {}",
            scope.as_str()
        )
    })
}

/// Load the first history row for one logical name anchor under the shared default sort.
pub async fn load_name_history_head(
    pool: &PgPool,
    logical_name_id: &str,
    resource_ids: &[Uuid],
    scope: HistoryScope,
    canonical_only: bool,
) -> Result<Option<HistoryEvent>> {
    load_history_head(
        pool,
        name_history_selector(logical_name_id, resource_ids, scope),
        canonical_only,
    )
    .await
    .with_context(|| {
        format!(
            "failed to load history head for logical_name_id {logical_name_id} with scope {}",
            scope.as_str()
        )
    })
}

/// Load app-facing event history rows using only canonical normalized-event/history anchors.
pub async fn load_event_history(
    pool: &PgPool,
    filter: EventHistoryFilter,
    canonical_only: bool,
) -> Result<Vec<HistoryEvent>> {
    let read_filter = event_history_read_filter(pool, filter, canonical_only, false).await?;
    load_event_history_rows(pool, read_filter, canonical_only)
        .await
        .context("failed to load app-facing event history")
}

/// Load history rows for one resource anchor.
pub async fn load_resource_history(
    pool: &PgPool,
    resource_id: Uuid,
    logical_name_ids: &[String],
    scope: HistoryScope,
    canonical_only: bool,
) -> Result<Vec<HistoryEvent>> {
    load_history(
        pool,
        resource_history_selector(resource_id, logical_name_ids, scope),
        canonical_only,
    )
    .await
    .with_context(|| {
        format!(
            "failed to load history for resource_id {resource_id} with scope {}",
            scope.as_str()
        )
    })
}

/// Load one SQL-keyset page for one resource anchor.
#[allow(clippy::too_many_arguments)]
pub async fn load_resource_history_page(
    pool: &PgPool,
    resource_id: Uuid,
    logical_name_ids: &[String],
    scope: HistoryScope,
    canonical_only: bool,
    cursor: Option<&HistoryCursor>,
    page_size: u64,
    summary_mode: HistorySummaryMode,
) -> Result<HistoryPage> {
    paging::load_history_page(
        pool,
        EventHistoryReadFilter {
            selectors: vec![resource_history_selector(
                resource_id,
                logical_name_ids,
                scope,
            )],
            ..EventHistoryReadFilter::default()
        },
        canonical_only,
        cursor,
        page_size,
        summary_mode,
        false,
        None,
    )
    .await
    .with_context(|| {
        format!(
            "failed to load history page for resource_id {resource_id} with scope {}",
            scope.as_str()
        )
    })
}

/// Load history rows for one address-derived anchor set.
pub async fn load_address_history(
    pool: &PgPool,
    address: &str,
    namespace: Option<&str>,
    relation: Option<AddressNameRelation>,
    scope: HistoryScope,
    canonical_only: bool,
) -> Result<Vec<HistoryEvent>> {
    let relations = relation.into_iter().collect::<Vec<_>>();
    let relations = (!relations.is_empty()).then_some(relations.as_slice());
    load_address_history_for_relations(pool, address, namespace, relations, scope, canonical_only)
        .await
}

/// Load history rows for one address-derived anchor set.
pub async fn load_address_history_for_relations(
    pool: &PgPool,
    address: &str,
    namespace: Option<&str>,
    relations: Option<&[AddressNameRelation]>,
    scope: HistoryScope,
    canonical_only: bool,
) -> Result<Vec<HistoryEvent>> {
    let normalized_address = address.to_ascii_lowercase();
    let selector = load_address_history_selector(
        pool,
        &normalized_address,
        namespace,
        relations,
        scope,
        canonical_only,
        false,
        None,
    )
    .await?;

    load_history(pool, selector, canonical_only)
        .await
        .with_context(|| {
            let mut parts = vec![format!("address {}", normalized_address)];
            if let Some(namespace) = namespace {
                parts.push(format!("namespace {namespace}"));
            }
            if let Some(relations) = relations.filter(|relations| !relations.is_empty()) {
                parts.push(format!(
                    "relations {}",
                    relations
                        .iter()
                        .map(|relation| relation.as_str())
                        .collect::<Vec<_>>()
                        .join(",")
                ));
            }
            parts.push(format!("scope {}", scope.as_str()));
            format!("failed to load history for {}", parts.join(" "))
        })
}

/// Load one SQL-keyset page for one address-derived anchor set.
#[allow(clippy::too_many_arguments)]
pub async fn load_address_history_page(
    pool: &PgPool,
    address: &str,
    namespace: Option<&str>,
    relation: Option<AddressNameRelation>,
    scope: HistoryScope,
    canonical_only: bool,
    cursor: Option<&HistoryCursor>,
    page_size: u64,
    summary_mode: HistorySummaryMode,
) -> Result<HistoryPage> {
    let relations = relation.into_iter().collect::<Vec<_>>();
    let relations = (!relations.is_empty()).then_some(relations.as_slice());
    load_address_history_page_for_relations(
        pool,
        address,
        namespace,
        relations,
        scope,
        canonical_only,
        cursor,
        page_size,
        summary_mode,
        &HistoryPageOptions::default(),
        false,
    )
    .await
}

/// Load one SQL-keyset page for one address-derived anchor set.
#[allow(clippy::too_many_arguments)]
pub async fn load_address_history_page_for_relations(
    pool: &PgPool,
    address: &str,
    namespace: Option<&str>,
    relations: Option<&[AddressNameRelation]>,
    scope: HistoryScope,
    canonical_only: bool,
    cursor: Option<&HistoryCursor>,
    page_size: u64,
    summary_mode: HistorySummaryMode,
    options: &HistoryPageOptions,
    require_interpret_not_redo: bool,
) -> Result<HistoryPage> {
    let interpret_redo_fence = redo::capture_fence_if(pool, require_interpret_not_redo).await?;
    let normalized_address = address.to_ascii_lowercase();
    let selector = load_address_history_selector(
        pool,
        &normalized_address,
        namespace,
        relations,
        scope,
        canonical_only,
        false,
        options.publication_block_bounds.as_ref(),
    )
    .await?;

    #[cfg(any(test, feature = "test-support"))]
    history_anchor_read_test_hooks::run_if(pool, require_interpret_not_redo).await?;

    paging::load_history_page(
        pool,
        EventHistoryReadFilter {
            selectors: vec![selector],
            ..EventHistoryReadFilter::default()
        }
        .with_page_options(options),
        canonical_only,
        cursor,
        page_size,
        summary_mode,
        false,
        interpret_redo_fence.as_ref(),
    )
    .await
    .with_context(|| {
        let mut parts = vec![format!("address {}", normalized_address)];
        if let Some(namespace) = namespace {
            parts.push(format!("namespace {namespace}"));
        }
        if let Some(relations) = relations.filter(|relations| !relations.is_empty()) {
            parts.push(format!(
                "relations {}",
                relations
                    .iter()
                    .map(|relation| relation.as_str())
                    .collect::<Vec<_>>()
                    .join(",")
            ));
        }
        parts.push(format!("scope {}", scope.as_str()));
        format!("failed to load history page for {}", parts.join(" "))
    })
}

/// Load canonical normalized events by row id in the shared chain-position
/// order, for callers that already hold event ids from projection provenance.
pub async fn load_history_events_by_ids(pool: &PgPool, ids: &[i64]) -> Result<Vec<HistoryEvent>> {
    paging::load_history_events_by_ids(pool, ids)
        .await
        .context("failed to load normalized events by id")
}

/// The exact names a `registration_id` belongs to: the names its resource is bound to, and the
/// names whose `NameWrapped` rows recorded it as the BaseRegistrar lease they wrapped.
pub async fn load_logical_name_ids_for_registration_id(
    pool: &PgPool,
    registration_id: Uuid,
) -> Result<Vec<String>> {
    binding_anchors::load_logical_name_ids_for_resource_id(pool, registration_id, true, None)
        .await
        .with_context(|| format!("failed to load names for registration_id {registration_id}"))
}

#[cfg(any(test, feature = "test-support"))]
pub async fn explain_registration_history_filter_for_test(
    pool: &PgPool,
    registration_id: Uuid,
    logical_name_id: &str,
    chain_id: &str,
    namespace: &str,
    namehash: &str,
) -> Result<String> {
    let filter = event_history_read_filter(
        pool,
        EventHistoryFilter {
            resource_id: Some(registration_id),
            ..EventHistoryFilter::default()
        },
        true,
        false,
    )
    .await?;
    let lookup = query_plan::HistoryPlanLookup {
        logical_name_id,
        registration_id,
        chain_id,
        namespace,
        namehash,
    };
    query_plan::explain_history_filter_for_test(pool, filter, lookup, true).await
}
