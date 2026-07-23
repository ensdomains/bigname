use std::collections::{BTreeMap, BTreeSet, btree_map, btree_set};

use anyhow::Result;
use bigname_adapters::StartupAdapterProgress;
use bigname_domain::block_interval::{InclusiveBlockInterval, coalesce_inclusive_block_intervals};
use bigname_manifests::{WatchedBackfillTarget, WatchedSourceSelectorPlan};
use bigname_storage::{
    BackfillCoverageFactScope, BackfillCoverageFactStreamItem, BackfillCoverageFactWrite,
    BackfillCoverageProgress, BackfillCoverageProgressFuture, BackfillRange,
    complete_backfill_range_recording_coverage_with_progress,
};

use crate::{
    ens_v1_resolver::SOURCE_FAMILY_ENS_V1_RESOLVER_L1,
    source_scope::watched_source_plan_uses_generic_resolver_scope,
};

use super::{
    BackfillJobRunConfig,
    failure_recording::{ReservedRangeFailure, record_reserved_range_failure},
};

const SOURCE_FAMILY_BASENAMES_BASE_REGISTRY: &str = "basenames_base_registry";
const COVERAGE_PLAN_PROGRESS_TARGETS: usize = 1_000;

struct BackfillCoverageHeartbeat<'a, 'b> {
    pool: &'a sqlx::PgPool,
    progress_sender: Option<&'a tokio::sync::mpsc::UnboundedSender<()>>,
    service_progress: &'a mut Option<&'b mut dyn StartupAdapterProgress>,
}

impl BackfillCoverageProgress for BackfillCoverageHeartbeat<'_, '_> {
    fn record<'a>(&'a mut self) -> BackfillCoverageProgressFuture<'a> {
        Box::pin(async move {
            if let Some(progress_sender) = self.progress_sender {
                let _ = progress_sender.send(());
            }
            if let Some(progress) = self.service_progress.as_deref_mut() {
                progress.record(self.pool).await?;
            }
            Ok(())
        })
    }
}

/// Complete a reserved range, recording plan-derived coverage facts in the
/// job-flip transaction when this range completion also completes the job.
/// Completion failures are persisted as reserved-range failure state, matching
/// the other reserved-range phases.
#[expect(clippy::too_many_arguments)]
pub(super) async fn complete_reserved_range_recording_plan_coverage(
    pool: &sqlx::PgPool,
    active_range: &BackfillRange,
    config: &BackfillJobRunConfig,
    source_plan: &WatchedSourceSelectorPlan,
    uses_basenames_registry_scan_all: bool,
    failure_reason: &'static str,
    progress_sender: Option<&tokio::sync::mpsc::UnboundedSender<()>>,
    service_progress: &mut Option<&mut dyn StartupAdapterProgress>,
) -> Result<()> {
    let completion = {
        let mut progress = BackfillCoverageHeartbeat {
            pool,
            progress_sender,
            service_progress,
        };
        complete_backfill_range_recording_coverage_with_progress(
            pool,
            active_range.backfill_range_id,
            &config.lease_token,
            |job| {
                job_completion_coverage_fact_stream(
                    source_plan,
                    uses_basenames_registry_scan_all,
                    job.range_start_block_number,
                    job.range_end_block_number,
                )
            },
            &mut progress,
        )
        .await
    };
    if let Err(error) = completion {
        return Err(record_reserved_range_failure(ReservedRangeFailure {
            pool,
            reserved_range: active_range,
            config,
            failure_reason,
            block_number: None,
            attempted_range: None,
            phase: "range_completion",
            error,
        })
        .await);
    }
    Ok(())
}

/// Coverage facts recorded when a backfill job completes, derived from the
/// executor's own in-memory selector plan so the recorded coverage can never
/// drift from what the job actually fetched. Families fetched via
/// topics-complete scans (the ENSv1 generic resolver scope and the Basenames
/// registry scan-all shape) yield one family-scope fact per merged union
/// segment of their targets' clamped effective windows — the scan-all planner
/// skips windows holding no selected targets, so neither blocks outside every
/// window nor gaps between disjoint windows were fetched — and their
/// per-address targets are excluded because the fetch was not
/// address-enumerated. Every other selected target yields an address-scope
/// fact clamped to the job range, skipping empty intersections.
#[cfg(test)]
fn job_completion_coverage_facts<'a>(
    source_plan: &'a WatchedSourceSelectorPlan,
    uses_basenames_registry_scan_all: bool,
    job_start_block: i64,
    job_end_block: i64,
) -> impl Iterator<Item = BackfillCoverageFactWrite> + 'a {
    job_completion_coverage_fact_stream(
        source_plan,
        uses_basenames_registry_scan_all,
        job_start_block,
        job_end_block,
    )
    .filter_map(|item| match item {
        BackfillCoverageFactStreamItem::Fact(fact) => Some(fact),
        BackfillCoverageFactStreamItem::Progress => None,
    })
}

fn job_completion_coverage_fact_stream(
    source_plan: &WatchedSourceSelectorPlan,
    uses_basenames_registry_scan_all: bool,
    job_start_block: i64,
    job_end_block: i64,
) -> JobCompletionCoverageFactStream<'_> {
    let mut family_scan_source_families = BTreeSet::new();
    if watched_source_plan_uses_generic_resolver_scope(source_plan) {
        family_scan_source_families.insert(SOURCE_FAMILY_ENS_V1_RESOLVER_L1);
    }
    if uses_basenames_registry_scan_all {
        family_scan_source_families.insert(SOURCE_FAMILY_BASENAMES_BASE_REGISTRY);
    }

    JobCompletionCoverageFactStream {
        all_targets: &source_plan.selected_targets,
        targets: source_plan.selected_targets.iter(),
        address_targets: None,
        family_scan_source_families,
        family_scan_windows: BTreeMap::new(),
        family_sources: None,
        family_merge: None,
        pending_fact: None,
        source_examined: 0,
        address_examined: 0,
        job_start_block,
        job_end_block,
    }
}

struct FamilyWindowMerge {
    source_family: String,
    windows: btree_set::IntoIter<(i64, i64)>,
    current: Option<(i64, i64)>,
    examined: usize,
}

struct JobCompletionCoverageFactStream<'a> {
    all_targets: &'a [WatchedBackfillTarget],
    targets: std::slice::Iter<'a, WatchedBackfillTarget>,
    address_targets: Option<std::slice::Iter<'a, WatchedBackfillTarget>>,
    family_scan_source_families: BTreeSet<&'static str>,
    family_scan_windows: BTreeMap<String, BTreeSet<(i64, i64)>>,
    family_sources: Option<btree_map::IntoIter<String, BTreeSet<(i64, i64)>>>,
    family_merge: Option<FamilyWindowMerge>,
    pending_fact: Option<BackfillCoverageFactWrite>,
    source_examined: usize,
    address_examined: usize,
    job_start_block: i64,
    job_end_block: i64,
}

impl Iterator for JobCompletionCoverageFactStream<'_> {
    type Item = BackfillCoverageFactStreamItem;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(fact) = self.pending_fact.take() {
            return Some(BackfillCoverageFactStreamItem::Fact(fact));
        }

        if let Some(address_targets) = self.address_targets.as_mut() {
            loop {
                let target = address_targets.next()?;
                self.address_examined += 1;
                let fact = if self
                    .family_scan_source_families
                    .contains(target.source_family.as_str())
                {
                    None
                } else {
                    covered_block_interval(
                        target.effective_from_block,
                        target.effective_to_block,
                        self.job_start_block,
                        self.job_end_block,
                    )
                    .map(|(covered_from_block, covered_to_block)| {
                        BackfillCoverageFactWrite {
                            source_family: target.source_family.clone(),
                            scope: BackfillCoverageFactScope::Address,
                            address: Some(target.address.to_ascii_lowercase()),
                            covered_from_block,
                            covered_to_block,
                        }
                    })
                };
                if self
                    .address_examined
                    .is_multiple_of(COVERAGE_PLAN_PROGRESS_TARGETS)
                {
                    self.pending_fact = fact;
                    return Some(BackfillCoverageFactStreamItem::Progress);
                }
                if let Some(fact) = fact {
                    return Some(BackfillCoverageFactStreamItem::Fact(fact));
                }
            }
        }

        for target in self.targets.by_ref() {
            self.source_examined += 1;
            let interval = covered_block_interval(
                target.effective_from_block,
                target.effective_to_block,
                self.job_start_block,
                self.job_end_block,
            );
            if self
                .family_scan_source_families
                .contains(target.source_family.as_str())
                && let Some(interval) = interval
            {
                self.family_scan_windows
                    .entry(target.source_family.clone())
                    .or_default()
                    .insert(interval);
            }
            if self
                .source_examined
                .is_multiple_of(COVERAGE_PLAN_PROGRESS_TARGETS)
            {
                return Some(BackfillCoverageFactStreamItem::Progress);
            }
        }

        if self.family_sources.is_none() {
            self.family_sources = Some(std::mem::take(&mut self.family_scan_windows).into_iter());
        }
        loop {
            if self.family_merge.is_none() {
                let Some((source_family, windows)) = self.family_sources.as_mut()?.next() else {
                    self.address_targets = Some(self.all_targets.iter());
                    return self.next();
                };
                self.family_merge = Some(FamilyWindowMerge {
                    source_family,
                    windows: windows.into_iter(),
                    current: None,
                    examined: 0,
                });
            }
            let merge = self
                .family_merge
                .as_mut()
                .expect("family merge initialized");
            match merge.windows.next() {
                Some((from_block, to_block)) => {
                    merge.examined += 1;
                    let completed = match merge.current.replace((from_block, to_block)) {
                        Some((current_from, current_to))
                            if from_block <= current_to.saturating_add(1) =>
                        {
                            merge.current = Some((current_from, current_to.max(to_block)));
                            None
                        }
                        previous => previous,
                    };
                    let fact = completed.map(|(covered_from_block, covered_to_block)| {
                        BackfillCoverageFactWrite {
                            source_family: merge.source_family.clone(),
                            scope: BackfillCoverageFactScope::Family,
                            address: None,
                            covered_from_block,
                            covered_to_block,
                        }
                    });
                    if merge
                        .examined
                        .is_multiple_of(COVERAGE_PLAN_PROGRESS_TARGETS)
                    {
                        self.pending_fact = fact;
                        return Some(BackfillCoverageFactStreamItem::Progress);
                    }
                    if let Some(fact) = fact {
                        return Some(BackfillCoverageFactStreamItem::Fact(fact));
                    }
                }
                None => {
                    let final_interval = merge.current.take();
                    let source_family = merge.source_family.clone();
                    self.family_merge = None;
                    if let Some((covered_from_block, covered_to_block)) = final_interval {
                        return Some(BackfillCoverageFactStreamItem::Fact(
                            BackfillCoverageFactWrite {
                                source_family,
                                scope: BackfillCoverageFactScope::Family,
                                address: None,
                                covered_from_block,
                                covered_to_block,
                            },
                        ));
                    }
                }
            }
        }
    }
}

/// Intersect a target's effective window with the job range; `None` when the
/// intersection is empty.
pub(crate) fn covered_block_interval(
    effective_from_block: i64,
    effective_to_block: i64,
    job_start_block: i64,
    job_end_block: i64,
) -> Option<(i64, i64)> {
    let covered_from_block = effective_from_block.max(job_start_block);
    let covered_to_block = effective_to_block.min(job_end_block);
    (covered_from_block <= covered_to_block).then_some((covered_from_block, covered_to_block))
}

/// Clamp each effective window to the job range first, then merge the
/// surviving windows (overlapping or block-adjacent) into union segments.
/// Clamping before folding matters: windows individually disjoint from the
/// job range contribute nothing, and gaps between disjoint windows are never
/// claimed.
pub(crate) fn merged_covered_block_segments(
    windows: impl IntoIterator<Item = (i64, i64)>,
    job_start_block: i64,
    job_end_block: i64,
) -> Vec<(i64, i64)> {
    coalesce_inclusive_block_intervals(windows.into_iter().filter_map(|(from_block, to_block)| {
        covered_block_interval(from_block, to_block, job_start_block, job_end_block).map(
            |(covered_from_block, covered_to_block)| {
                InclusiveBlockInterval::new(covered_from_block, covered_to_block)
                    .expect("clamped covered block interval must not be inverted")
            },
        )
    }))
    .into_iter()
    .map(|interval| (interval.from_block(), interval.through_block()))
    .collect()
}

#[cfg(test)]
#[path = "coverage_facts/tests.rs"]
mod tests;
