use anyhow::{Context, Result};
use bigname_adapters::StartupAdapterProgress;
use bigname_manifests::{
    WatchedBackfillTarget, WatchedSourceSelectorKind, WatchedSourceSelectorPlan,
};
use serde_json::{Value, json};

use crate::ens_v1_resolver::{SOURCE_FAMILY_ENS_V1_RESOLVER_L1, generic_resolver_record_topic0s};

use super::{
    COMPACT_SOURCE_IDENTITY_SELECTED_TARGET_THRESHOLD,
    digest::{
        keccak256_json_digest, keccak256_json_value_digest_with_progress,
        keccak256_selected_targets_digest_with_progress,
    },
    requested_watched_targets_value_with_progress,
};

pub(super) async fn generic_topic_scan_source_identity_payload_with_progress(
    pool: &sqlx::PgPool,
    source_plan: &WatchedSourceSelectorPlan,
    progress: &mut dyn StartupAdapterProgress,
) -> Result<Value> {
    const PROGRESS_TARGETS: usize = 1_000;
    let mut selected_count = 0usize;
    let mut selected_first = None;
    let mut selected_last = None;
    for (index, target) in source_plan.selected_targets.iter().enumerate() {
        if target.source_family != SOURCE_FAMILY_ENS_V1_RESOLVER_L1 {
            selected_count += 1;
            selected_first.get_or_insert(target);
            selected_last = Some(target);
        }
        if (index + 1).is_multiple_of(PROGRESS_TARGETS) {
            progress.record(pool).await?;
        }
    }
    if !source_plan.selected_targets.is_empty()
        && !source_plan
            .selected_targets
            .len()
            .is_multiple_of(PROGRESS_TARGETS)
    {
        progress.record(pool).await?;
    }
    let requested_watched_targets =
        requested_watched_targets_value_with_progress(pool, source_plan, progress).await?;
    let generic_topic0s = generic_resolver_record_topic0s();
    let generic_topic_scans = json!([{
        "source_family": SOURCE_FAMILY_ENS_V1_RESOLVER_L1,
        "source_identity_payload_format": "generic_resolver_event_topics_v1"
    }]);

    let mut payload = if source_plan.selector_kind == WatchedSourceSelectorKind::SourceFamily
        && source_plan.source_family.as_deref() == Some(SOURCE_FAMILY_ENS_V1_RESOLVER_L1)
    {
        json!({
            "selector_kind": source_plan.selector_kind.as_str(),
            "source_family": &source_plan.source_family,
            "requested_watched_targets": requested_watched_targets,
            "source_identity_payload_format": "generic_resolver_event_topics_v1",
            "topic0s_by_source_family": {
                SOURCE_FAMILY_ENS_V1_RESOLVER_L1: generic_topic0s,
            },
        })
    } else if selected_count <= COMPACT_SOURCE_IDENTITY_SELECTED_TARGET_THRESHOLD {
        let mut selected_targets = Vec::with_capacity(selected_count);
        for target in &source_plan.selected_targets {
            if target.source_family != SOURCE_FAMILY_ENS_V1_RESOLVER_L1 {
                selected_targets.push(target.clone());
            }
        }
        json!({
            "selector_kind": source_plan.selector_kind.as_str(),
            "source_family": &source_plan.source_family,
            "requested_watched_targets": requested_watched_targets,
            "selected_targets": selected_targets,
            "generic_topic_scans": generic_topic_scans,
            "source_identity_payload_format": "selected_targets_with_generic_topic_scans_v1",
            "topic0s_by_source_family": {
                SOURCE_FAMILY_ENS_V1_RESOLVER_L1: generic_topic0s,
            },
        })
    } else {
        let selected_targets_digest = keccak256_selected_targets_digest_with_progress(
            pool,
            &source_plan.selected_targets,
            Some(SOURCE_FAMILY_ENS_V1_RESOLVER_L1),
            progress,
        )
        .await
        .context("failed to digest compact generic-topic-scan source selected targets")?;
        json!({
            "selector_kind": source_plan.selector_kind.as_str(),
            "source_family": &source_plan.source_family,
            "requested_watched_targets": requested_watched_targets,
            "selected_target_count": selected_count,
            "selected_targets_digest_algorithm": "keccak256",
            "selected_targets_digest": selected_targets_digest,
            "selected_targets_sample": {"first": selected_first, "last": selected_last},
            "generic_topic_scans": generic_topic_scans,
            "source_identity_payload_format": "selected_targets_digest_with_generic_topic_scans_v1",
            "topic0s_by_source_family": {
                SOURCE_FAMILY_ENS_V1_RESOLVER_L1: generic_topic0s,
            },
        })
    };
    let source_identity_hash = keccak256_json_value_digest_with_progress(pool, &payload, progress)
        .await
        .context("failed to digest generic-topic-scan backfill source identity")?;
    payload
        .as_object_mut()
        .expect("generic-topic-scan source identity payload must be an object")
        .insert(
            "source_identity_hash".to_owned(),
            Value::String(source_identity_hash),
        );
    Ok(payload)
}

pub(super) fn generic_topic_scan_source_identity_payload(
    source_plan: &WatchedSourceSelectorPlan,
) -> Result<Value> {
    generic_topic_scan_source_identity_payload_with_topic0s(
        source_plan,
        &generic_resolver_record_topic0s(),
    )
}

fn generic_topic_scan_source_identity_payload_with_topic0s(
    source_plan: &WatchedSourceSelectorPlan,
    generic_resolver_topic0s: &[String],
) -> Result<Value> {
    let selected_targets = source_plan
        .selected_targets
        .iter()
        .filter(|target| target.source_family != SOURCE_FAMILY_ENS_V1_RESOLVER_L1)
        .cloned()
        .collect::<Vec<_>>();
    let requested_watched_targets = source_plan.requested_watched_targets.clone();
    // The hash-pinned fetch uses this same topic helper in
    // `fetching/log_ranges.rs`. Persist the exact fetched set so completed
    // family coverage can be checked against later manifest topic drift.
    let generic_topic_scans = json!([
        {
            "source_family": SOURCE_FAMILY_ENS_V1_RESOLVER_L1,
            "source_identity_payload_format": "generic_resolver_event_topics_v1"
        }
    ]);

    let mut payload = if source_plan.selector_kind == WatchedSourceSelectorKind::SourceFamily
        && source_plan.source_family.as_deref() == Some(SOURCE_FAMILY_ENS_V1_RESOLVER_L1)
    {
        json!({
            "selector_kind": source_plan.selector_kind.as_str(),
            "source_family": &source_plan.source_family,
            "requested_watched_targets": requested_watched_targets,
            "source_identity_payload_format": "generic_resolver_event_topics_v1",
            "topic0s_by_source_family": {
                SOURCE_FAMILY_ENS_V1_RESOLVER_L1: generic_resolver_topic0s,
            },
        })
    } else if selected_targets.len() <= COMPACT_SOURCE_IDENTITY_SELECTED_TARGET_THRESHOLD {
        json!({
            "selector_kind": source_plan.selector_kind.as_str(),
            "source_family": &source_plan.source_family,
            "requested_watched_targets": requested_watched_targets,
            "selected_targets": selected_targets,
            "generic_topic_scans": generic_topic_scans,
            "source_identity_payload_format": "selected_targets_with_generic_topic_scans_v1",
            "topic0s_by_source_family": {
                SOURCE_FAMILY_ENS_V1_RESOLVER_L1: generic_resolver_topic0s,
            },
        })
    } else {
        let selected_targets_digest = keccak256_json_digest(&selected_targets)
            .context("failed to digest compact generic-topic-scan source selected targets")?;
        json!({
            "selector_kind": source_plan.selector_kind.as_str(),
            "source_family": &source_plan.source_family,
            "requested_watched_targets": requested_watched_targets,
            "selected_target_count": selected_targets.len(),
            "selected_targets_digest_algorithm": "keccak256",
            "selected_targets_digest": selected_targets_digest,
            "selected_targets_sample": selected_targets_sample(&selected_targets),
            "generic_topic_scans": generic_topic_scans,
            "source_identity_payload_format": "selected_targets_digest_with_generic_topic_scans_v1",
            "topic0s_by_source_family": {
                SOURCE_FAMILY_ENS_V1_RESOLVER_L1: generic_resolver_topic0s,
            },
        })
    };
    let source_identity_hash = keccak256_json_digest(&payload)
        .context("failed to digest generic-topic-scan backfill source identity")?;
    payload
        .as_object_mut()
        .expect("generic-topic-scan source identity payload must be an object")
        .insert(
            "source_identity_hash".to_owned(),
            Value::String(source_identity_hash),
        );
    Ok(payload)
}

pub(super) fn selected_targets_sample(selected_targets: &[WatchedBackfillTarget]) -> Value {
    json!({
        "first": selected_targets.first(),
        "last": selected_targets.last(),
    })
}

#[cfg(test)]
mod tests {
    use bigname_manifests::{WatchedChainPlan, WatchedSourceSelectorKind};

    use super::*;

    fn generic_resolver_source_plan() -> WatchedSourceSelectorPlan {
        WatchedSourceSelectorPlan {
            chain: "ethereum-mainnet".to_owned(),
            selector_kind: WatchedSourceSelectorKind::SourceFamily,
            source_family: Some(SOURCE_FAMILY_ENS_V1_RESOLVER_L1.to_owned()),
            requested_watched_targets: Vec::new(),
            selected_targets: Vec::new(),
            watched_chain_plan: WatchedChainPlan {
                chain: "ethereum-mainnet".to_owned(),
                addresses: Vec::new(),
                manifest_root_entry_count: 0,
                manifest_contract_entry_count: 0,
                discovery_edge_entry_count: 0,
            },
        }
    }

    #[test]
    fn generic_topic_source_identity_hash_covers_the_fetched_topic_set() -> Result<()> {
        let source_plan = generic_resolver_source_plan();
        let current_topic0s = generic_resolver_record_topic0s();
        let current = generic_topic_scan_source_identity_payload_with_topic0s(
            &source_plan,
            &current_topic0s,
        )?;

        let mut drifted_topic0s = current_topic0s;
        drifted_topic0s.pop();
        let drifted = generic_topic_scan_source_identity_payload_with_topic0s(
            &source_plan,
            &drifted_topic0s,
        )?;

        assert_ne!(
            current.get("source_identity_hash"),
            drifted.get("source_identity_hash"),
            "changing the exact fetched topic set must change the immutable source identity"
        );
        Ok(())
    }
}
