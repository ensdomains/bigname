use std::collections::BTreeSet;

use anyhow::{Context, Result};
use bigname_adapters::StartupAdapterProgress;
use bigname_manifests::{
    ManifestBootstrapTarget, WatchedBackfillTarget, WatchedSourceSelectorKind,
    WatchedSourceSelectorPlan,
};

use crate::{backfill::BackfillBlockRange, ens_v1_resolver::SOURCE_FAMILY_ENS_V1_RESOLVER_L1};

use super::{BootstrapBackfillSegment, BootstrapBackfillTargetRange};

const PLANNING_PROGRESS_TARGETS: usize = 1_000;

pub(in crate::bootstrap_backfill) async fn plan_bootstrap_backfill_segments_with_progress(
    pool: &sqlx::PgPool,
    target_ranges: Vec<BootstrapBackfillTargetRange>,
    progress: &mut dyn StartupAdapterProgress,
) -> Result<Vec<BootstrapBackfillSegment>> {
    let mut max_end_block = None::<i64>;
    let mut boundaries = BTreeSet::new();
    let mut resolver_range = None::<BackfillBlockRange>;
    for (index, target_range) in target_ranges.iter().enumerate() {
        max_end_block = Some(
            max_end_block.map_or(target_range.range.to_block, |current| {
                current.max(target_range.range.to_block)
            }),
        );
        if target_range.target.source_family == SOURCE_FAMILY_ENS_V1_RESOLVER_L1 {
            resolver_range = Some(match resolver_range {
                Some(range) => BackfillBlockRange {
                    from_block: range.from_block.min(target_range.range.from_block),
                    to_block: range.to_block.max(target_range.range.to_block),
                },
                None => target_range.range,
            });
        } else {
            boundaries.insert(target_range.range.from_block);
            if target_range.range.to_block < i64::MAX {
                boundaries.insert(target_range.range.to_block.checked_add(1).with_context(
                    || {
                        format!(
                            "bootstrap target range end {} overflowed while planning segments",
                            target_range.range.to_block
                        )
                    },
                )?);
            }
        }
        record_every(pool, progress, index + 1).await?;
    }
    record_tail(pool, progress, target_ranges.len()).await?;
    let Some(max_end_block) = max_end_block else {
        return Ok(Vec::new());
    };

    if let Some(resolver_range) = resolver_range {
        boundaries.insert(resolver_range.from_block);
        if resolver_range.to_block < i64::MAX {
            boundaries.insert(resolver_range.to_block.checked_add(1).with_context(|| {
                format!(
                    "bootstrap resolver range end {} overflowed while planning segments",
                    resolver_range.to_block
                )
            })?);
        }
    }

    let mut boundaries_vec = Vec::with_capacity(boundaries.len());
    for boundary in boundaries {
        boundaries_vec.push(boundary);
        record_every(pool, progress, boundaries_vec.len()).await?;
    }
    record_tail(pool, progress, boundaries_vec.len()).await?;

    let mut segments = Vec::new();
    for (index, segment_start) in boundaries_vec.iter().copied().enumerate() {
        if segment_start > max_end_block {
            break;
        }
        let segment_end = boundaries_vec
            .get(index + 1)
            .map(|next_start| *next_start - 1)
            .unwrap_or(max_end_block)
            .min(max_end_block);
        if segment_start > segment_end {
            continue;
        }

        let mut targets = Vec::new();
        for (target_index, target_range) in target_ranges.iter().enumerate() {
            let selected = if target_range.target.source_family == SOURCE_FAMILY_ENS_V1_RESOLVER_L1
            {
                target_range.range.from_block <= segment_end
                    && segment_start <= target_range.range.to_block
            } else {
                target_range.range.from_block <= segment_start
                    && segment_end <= target_range.range.to_block
            };
            if selected {
                targets.push(target_range.target.clone());
            }
            record_every(pool, progress, target_index + 1).await?;
        }
        record_tail(pool, progress, target_ranges.len()).await?;
        if !targets.is_empty() {
            segments.push(BootstrapBackfillSegment {
                range: BackfillBlockRange::new(segment_start, segment_end)?,
                targets,
            });
        }
    }
    Ok(segments)
}

pub(in crate::bootstrap_backfill) async fn narrow_manifest_bootstrap_source_plan_with_progress(
    pool: &sqlx::PgPool,
    source_plan: &mut WatchedSourceSelectorPlan,
    targets: &[ManifestBootstrapTarget],
    range: BackfillBlockRange,
    progress: &mut dyn StartupAdapterProgress,
) -> Result<()> {
    anyhow::ensure!(
        source_plan.selector_kind == WatchedSourceSelectorKind::WatchedTargetSet,
        "bootstrap source plan for range {}..={} used selector kind {} instead of watched_target_set",
        range.from_block,
        range.to_block,
        source_plan.selector_kind.as_str()
    );

    let mut selected_ids = BTreeSet::new();
    let mut selected_exact = BTreeSet::new();
    for (index, selected) in source_plan.selected_targets.iter().enumerate() {
        selected_ids.insert((
            selected.source_family.clone(),
            selected.contract_instance_id,
        ));
        selected_exact.insert((
            selected.source_family.clone(),
            selected.contract_instance_id,
            selected.address.clone(),
            selected.effective_from_block,
            selected.effective_to_block,
        ));
        record_every(pool, progress, index + 1).await?;
    }
    record_tail(pool, progress, source_plan.selected_targets.len()).await?;

    let mut narrowed = BTreeSet::new();
    for (index, target) in targets.iter().enumerate() {
        let is_resolver = target.source_family == SOURCE_FAMILY_ENS_V1_RESOLVER_L1;
        let selected = if is_resolver {
            selected_ids.contains(&(target.source_family.clone(), target.contract_instance_id))
        } else {
            selected_exact.contains(&(
                target.source_family.clone(),
                target.contract_instance_id,
                target.address.clone(),
                range.from_block,
                range.to_block,
            ))
        };
        anyhow::ensure!(
            selected || is_resolver,
            "bootstrap source plan for range {}..={} did not select authoritative contract_instance_id {} with the segmented address/range",
            range.from_block,
            range.to_block,
            target.contract_instance_id
        );
        narrowed.insert(WatchedBackfillTarget {
            source_family: target.source_family.clone(),
            contract_instance_id: target.contract_instance_id,
            address: target.address.clone(),
            effective_from_block: range.from_block,
            effective_to_block: range.to_block,
        });
        record_every(pool, progress, index + 1).await?;
    }
    record_tail(pool, progress, targets.len()).await?;

    let narrowed_len = narrowed.len();
    let mut narrowed_targets = Vec::with_capacity(narrowed_len);
    for target in narrowed {
        narrowed_targets.push(target);
        record_every(pool, progress, narrowed_targets.len()).await?;
    }
    record_tail(pool, progress, narrowed_len).await?;
    source_plan.selected_targets = narrowed_targets;
    Ok(())
}

async fn record_every(
    pool: &sqlx::PgPool,
    progress: &mut dyn StartupAdapterProgress,
    completed: usize,
) -> Result<()> {
    if completed.is_multiple_of(PLANNING_PROGRESS_TARGETS) {
        progress.record(pool).await?;
    }
    Ok(())
}

async fn record_tail(
    pool: &sqlx::PgPool,
    progress: &mut dyn StartupAdapterProgress,
    completed: usize,
) -> Result<()> {
    if completed > 0 && !completed.is_multiple_of(PLANNING_PROGRESS_TARGETS) {
        progress.record(pool).await?;
    }
    Ok(())
}
