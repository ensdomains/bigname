mod address_matches;
mod decoders;
mod paging;
mod selectors;

use anyhow::{Context, Result};
use serde_json::Value;
use sqlx::{PgPool, types::time::OffsetDateTime};
use uuid::Uuid;

use crate::{CanonicalityState, address_names::AddressNameRelation};

use address_matches::load_address_history_selector;
#[cfg(test)]
use address_matches::{ENS_V1_AUTHORITY_DERIVATION_KIND, ENS_V2_REGISTRY_DERIVATION_KIND};
use paging::{load_history, load_history_head};
use selectors::{name_history_selector, resource_history_selector};

/// Anchor selection for normalized-event history reads.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HistoryScope {
    Surface,
    Resource,
    Both,
}

impl HistoryScope {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Surface => "surface",
            Self::Resource => "resource",
            Self::Both => "both",
        }
    }
}

/// Replay-stable normalized event exposed to history readers.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HistoryEvent {
    pub normalized_event_id: i64,
    pub event_identity: String,
    pub namespace: String,
    pub logical_name_id: Option<String>,
    pub resource_id: Option<Uuid>,
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
    pub provenance: Value,
    pub coverage: Value,
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

/// Load history rows for one address-derived anchor set.
pub async fn load_address_history(
    pool: &PgPool,
    address: &str,
    namespace: Option<&str>,
    relation: Option<AddressNameRelation>,
    scope: HistoryScope,
    canonical_only: bool,
) -> Result<Vec<HistoryEvent>> {
    let normalized_address = address.to_ascii_lowercase();
    let selector = load_address_history_selector(
        pool,
        &normalized_address,
        namespace,
        relation,
        scope,
        canonical_only,
    )
    .await?;

    load_history(pool, selector, canonical_only)
        .await
        .with_context(|| {
            let mut parts = vec![format!("address {}", normalized_address)];
            if let Some(namespace) = namespace {
                parts.push(format!("namespace {namespace}"));
            }
            if let Some(relation) = relation {
                parts.push(format!("relation {}", relation.as_str()));
            }
            parts.push(format!("scope {}", scope.as_str()));
            format!("failed to load history for {}", parts.join(" "))
        })
}

#[cfg(test)]
mod tests;
