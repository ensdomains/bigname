use std::collections::BTreeSet;

use anyhow::Result;
use bigname_manifests::{
    ManifestBootstrapTarget, WatchedSourceSelector, WatchedTargetIdentity,
    load_historical_watched_source_selector_plan,
    load_historical_watched_source_selector_plan_with_progress,
    load_manifest_declared_watched_source_selector_plan,
};

use crate::{
    backfill::{
        BackfillBlockRange, backfill_job_source_identity_payload,
        backfill_job_source_identity_payload_with_progress,
    },
    run::startup_heartbeat::{StartupAdapterHeartbeat, StartupHeartbeat},
};

use super::planning::{
    BootstrapBackfillSegment, BootstrapBackfillTargetRange, narrow_manifest_bootstrap_source_plan,
    narrow_manifest_bootstrap_source_plan_with_progress, plan_bootstrap_backfill_segments,
    plan_bootstrap_backfill_segments_with_progress,
};

const TARGET_ACCUMULATION_PROGRESS_ROWS: usize = 1_000;

pub(super) async fn load_discovery_bootstrap_targets_with_optional_progress(
    pool: &sqlx::PgPool,
    chain: &str,
    through_block: i64,
    heartbeat: &mut Option<&mut StartupHeartbeat>,
    chain_ids: &[String],
) -> Result<Vec<ManifestBootstrapTarget>> {
    match heartbeat.as_deref_mut() {
        Some(heartbeat) => {
            let mut progress = StartupAdapterHeartbeat::new(heartbeat, chain_ids);
            bigname_manifests::load_ens_v2_authoritative_discovery_bootstrap_targets_with_progress(
                pool,
                chain,
                through_block,
                &mut progress,
            )
            .await
        }
        None => {
            bigname_manifests::load_ens_v2_authoritative_discovery_bootstrap_targets(
                pool,
                chain,
                through_block,
            )
            .await
        }
    }
}

pub(super) async fn bootstrap_source_identity_with_optional_progress(
    pool: &sqlx::PgPool,
    source_plan: &bigname_manifests::WatchedSourceSelectorPlan,
    heartbeat: &mut Option<&mut StartupHeartbeat>,
    chain_ids: &[String],
) -> Result<serde_json::Value> {
    match heartbeat.as_deref_mut() {
        Some(heartbeat) => {
            let mut progress = StartupAdapterHeartbeat::new(heartbeat, chain_ids);
            backfill_job_source_identity_payload_with_progress(pool, source_plan, &mut progress)
                .await
        }
        None => backfill_job_source_identity_payload(source_plan),
    }
}

pub(super) async fn bootstrap_segment_target_ids_with_optional_progress(
    pool: &sqlx::PgPool,
    targets: &[ManifestBootstrapTarget],
    heartbeat: &mut Option<&mut StartupHeartbeat>,
    chain_ids: &[String],
) -> Result<BTreeSet<String>> {
    let mut target_ids = BTreeSet::new();
    for (index, target) in targets.iter().enumerate() {
        target_ids.insert(target.contract_instance_id.to_string());
        if (index + 1).is_multiple_of(TARGET_ACCUMULATION_PROGRESS_ROWS) {
            record_bootstrap_progress(pool, heartbeat, chain_ids).await?;
        }
    }
    if !targets.is_empty()
        && !targets
            .len()
            .is_multiple_of(TARGET_ACCUMULATION_PROGRESS_ROWS)
    {
        record_bootstrap_progress(pool, heartbeat, chain_ids).await?;
    }
    Ok(target_ids)
}

pub(super) async fn narrow_bootstrap_source_plan_with_optional_progress(
    pool: &sqlx::PgPool,
    source_plan: &mut bigname_manifests::WatchedSourceSelectorPlan,
    targets: &[ManifestBootstrapTarget],
    range: BackfillBlockRange,
    heartbeat: &mut Option<&mut StartupHeartbeat>,
    chain_ids: &[String],
) -> Result<()> {
    match heartbeat.as_deref_mut() {
        Some(heartbeat) => {
            let mut progress = StartupAdapterHeartbeat::new(heartbeat, chain_ids);
            narrow_manifest_bootstrap_source_plan_with_progress(
                pool,
                source_plan,
                targets,
                range,
                &mut progress,
            )
            .await
        }
        None => narrow_manifest_bootstrap_source_plan(source_plan, targets, range),
    }
}

pub(super) async fn load_bootstrap_source_plan_with_optional_progress(
    pool: &sqlx::PgPool,
    chain: &str,
    targets: &[ManifestBootstrapTarget],
    range: BackfillBlockRange,
    include_historical_targets: bool,
    heartbeat: &mut Option<&mut StartupHeartbeat>,
    chain_ids: &[String],
) -> Result<bigname_manifests::WatchedSourceSelectorPlan> {
    let selector = WatchedSourceSelector::WatchedTargetSet(
        targets
            .iter()
            .map(|target| WatchedTargetIdentity {
                contract_instance_id: target.contract_instance_id,
            })
            .collect(),
    );
    match heartbeat.as_deref_mut() {
        Some(heartbeat) => {
            let mut progress = StartupAdapterHeartbeat::new(heartbeat, chain_ids);
            if include_historical_targets {
                load_historical_watched_source_selector_plan_with_progress(
                    pool,
                    chain,
                    selector,
                    range.from_block,
                    range.to_block,
                    &mut progress,
                )
                .await
            } else {
                let plan = load_manifest_declared_watched_source_selector_plan(
                    pool,
                    chain,
                    selector,
                    range.from_block,
                    range.to_block,
                )
                .await?;
                bigname_manifests::ManifestRuntimeProgress::record(&mut progress, pool).await?;
                Ok(plan)
            }
        }
        None if include_historical_targets => {
            load_historical_watched_source_selector_plan(
                pool,
                chain,
                selector,
                range.from_block,
                range.to_block,
            )
            .await
        }
        None => {
            load_manifest_declared_watched_source_selector_plan(
                pool,
                chain,
                selector,
                range.from_block,
                range.to_block,
            )
            .await
        }
    }
}

pub(super) async fn plan_bootstrap_segments_with_optional_progress(
    pool: &sqlx::PgPool,
    target_ranges: Vec<BootstrapBackfillTargetRange>,
    heartbeat: &mut Option<&mut StartupHeartbeat>,
    chain_ids: &[String],
) -> Result<Vec<BootstrapBackfillSegment>> {
    match heartbeat.as_deref_mut() {
        Some(heartbeat) => {
            let mut progress = StartupAdapterHeartbeat::new(heartbeat, chain_ids);
            plan_bootstrap_backfill_segments_with_progress(pool, target_ranges, &mut progress).await
        }
        None => plan_bootstrap_backfill_segments(target_ranges),
    }
}

pub(super) async fn extend_bootstrap_targets_with_progress(
    pool: &sqlx::PgPool,
    targets: &mut BTreeSet<ManifestBootstrapTarget>,
    additions: Vec<ManifestBootstrapTarget>,
    heartbeat: &mut Option<&mut StartupHeartbeat>,
    chain_ids: &[String],
) -> Result<()> {
    let addition_count = additions.len();
    for (index, target) in additions.into_iter().enumerate() {
        targets.insert(target);
        if (index + 1).is_multiple_of(TARGET_ACCUMULATION_PROGRESS_ROWS) {
            record_bootstrap_progress(pool, heartbeat, chain_ids).await?;
        }
    }
    if addition_count > 0 && !addition_count.is_multiple_of(TARGET_ACCUMULATION_PROGRESS_ROWS) {
        record_bootstrap_progress(pool, heartbeat, chain_ids).await?;
    }
    Ok(())
}

pub(super) async fn record_bootstrap_progress(
    pool: &sqlx::PgPool,
    heartbeat: &mut Option<&mut StartupHeartbeat>,
    chain_ids: &[String],
) -> Result<()> {
    if let Some(heartbeat) = heartbeat.as_deref_mut() {
        heartbeat.record_if_due(pool, chain_ids).await?;
    }
    Ok(())
}
