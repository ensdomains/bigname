//! Basenames-registry scan-all job shape: identity payloads and the adapter
//! sync mode forced for jobs that fetch the whole family by topic.

use anyhow::{Context, Result};
use bigname_manifests::{WatchedSourceSelectorKind, WatchedSourceSelectorPlan};
use serde_json::{Value, json};

use crate::{
    basenames_registry::{
        SOURCE_FAMILY_BASENAMES_BASE_REGISTRY, basenames_registry_scan_all_event_signatures,
        basenames_registry_scan_all_topic0s,
    },
    source_scope::watched_source_plan_uses_basenames_registry_scan_all,
};

use super::super::{BackfillAdapterSyncMode, BackfillTopicPlan};

pub(super) fn coinbase_sql_uses_basenames_registry_scan_all(
    source_plan: &WatchedSourceSelectorPlan,
    topic_plan: &BackfillTopicPlan,
) -> bool {
    source_plan.selector_kind == WatchedSourceSelectorKind::SourceFamily
        && source_plan.source_family.as_deref() == Some("basenames_base_registry")
        && !topic_plan
            .event_signatures_for_source_family("basenames_base_registry")
            .is_empty()
}

/// Identity for the hash-pinned Basenames registry scan-all. Distinct from
/// the Coinbase SQL `basenames_registry_scan_all_event_signatures_v1` shape,
/// and persists the fetched topic0 set verbatim so promotion's topic-drift
/// guard has spans to compare and legacy fact re-derivation stays possible.
pub(super) fn basenames_registry_scan_all_topics_source_identity_payload(
    source_plan: &WatchedSourceSelectorPlan,
) -> Result<Value> {
    let mut payload = json!({
        "selector_kind": source_plan.selector_kind.as_str(),
        "source_family": &source_plan.source_family,
        "requested_watched_targets": &source_plan.requested_watched_targets,
        "source_identity_payload_format": "basenames_registry_scan_all_topics_v1",
        "topic0s_by_source_family": {
            SOURCE_FAMILY_BASENAMES_BASE_REGISTRY: basenames_registry_scan_all_topic0s(),
        },
        "event_signatures_by_source_family": {
            SOURCE_FAMILY_BASENAMES_BASE_REGISTRY: basenames_registry_scan_all_event_signatures(),
        },
    });
    let source_identity_hash = super::keccak256_json_digest(&payload)
        .context("failed to digest Basenames registry scan-all source identity")?;
    payload
        .as_object_mut()
        .expect("Basenames registry scan-all source identity payload must be an object")
        .insert(
            "source_identity_hash".to_owned(),
            Value::String(source_identity_hash),
        );
    Ok(payload)
}

pub(super) fn coinbase_sql_basenames_registry_scan_all_source_identity_payload(
    source_plan: &WatchedSourceSelectorPlan,
) -> Result<Value> {
    Ok(json!({
        "selector_kind": source_plan.selector_kind.as_str(),
        "source_family": &source_plan.source_family,
        "requested_watched_targets": &source_plan.requested_watched_targets,
        "source_identity_payload_format": "basenames_registry_scan_all_event_signatures_v1",
    }))
}

/// The Basenames registry scan-all replays its closure from stored raw logs
/// (like the Coinbase SQL scan-all), so inline adapter sync is forced to
/// raw-only for this job shape.
/// Chunk address vector for the hash-pinned fetch. Scan-all plans fetch
/// address-free, so cloning the ~3.8M active registry addresses per chunk
/// would be pure waste; only address-scoped plans build the vector.
pub(super) fn chunk_addresses_for_plan(
    source_plan: &WatchedSourceSelectorPlan,
    cursor: &mut super::super::selection::SelectedTargetRangeCursor,
    chunk_range: super::super::BackfillBlockRange,
) -> Vec<String> {
    if super::super::fetching::scans_all_source_family_event_emitters(source_plan) {
        Vec::new()
    } else {
        cursor.active_addresses_for_monotonic_range(chunk_range.from_block, chunk_range.to_block)
    }
}

pub(crate) fn effective_hash_pinned_adapter_sync_mode(
    source_plan: &WatchedSourceSelectorPlan,
    requested_mode: BackfillAdapterSyncMode,
) -> BackfillAdapterSyncMode {
    if watched_source_plan_uses_basenames_registry_scan_all(source_plan) {
        BackfillAdapterSyncMode::RawOnly
    } else {
        requested_mode.hash_pinned_backfill_mode()
    }
}
