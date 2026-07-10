use std::collections::{BTreeMap, BTreeSet};

use anyhow::Result;
use bigname_manifests::WatchedSourceSelectorPlan;
use bigname_storage::{
    BackfillCoverageFactScope, BackfillCoverageFactWrite, BackfillRange,
    complete_backfill_range_recording_coverage,
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

/// Complete a reserved range, recording plan-derived coverage facts in the
/// job-flip transaction when this range completion also completes the job.
/// Completion failures are persisted as reserved-range failure state, matching
/// the other reserved-range phases.
pub(super) async fn complete_reserved_range_recording_plan_coverage(
    pool: &sqlx::PgPool,
    active_range: &BackfillRange,
    config: &BackfillJobRunConfig,
    source_plan: &WatchedSourceSelectorPlan,
    uses_basenames_registry_scan_all: bool,
    failure_reason: &'static str,
) -> Result<()> {
    let completion = complete_backfill_range_recording_coverage(
        pool,
        active_range.backfill_range_id,
        &config.lease_token,
        |job| {
            job_completion_coverage_facts(
                source_plan,
                uses_basenames_registry_scan_all,
                job.range_start_block_number,
                job.range_end_block_number,
            )
        },
    )
    .await;
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
/// registry scan-all shape) yield one family-scope fact clamped to the span of
/// the plan's targets for that family — the scan-all planner skips windows
/// with no selected targets, so blocks outside that span were never fetched —
/// and their per-address targets are excluded because the fetch was not
/// address-enumerated. Every other selected target yields an address-scope
/// fact clamped to the job range, skipping empty intersections.
fn job_completion_coverage_facts<'a>(
    source_plan: &'a WatchedSourceSelectorPlan,
    uses_basenames_registry_scan_all: bool,
    job_start_block: i64,
    job_end_block: i64,
) -> impl Iterator<Item = BackfillCoverageFactWrite> + 'a {
    let mut family_scan_source_families = BTreeSet::new();
    if watched_source_plan_uses_generic_resolver_scope(source_plan) {
        family_scan_source_families.insert(SOURCE_FAMILY_ENS_V1_RESOLVER_L1);
    }
    if uses_basenames_registry_scan_all {
        family_scan_source_families.insert(SOURCE_FAMILY_BASENAMES_BASE_REGISTRY);
    }

    let mut family_scan_spans = BTreeMap::<&'a str, (i64, i64)>::new();
    for target in &source_plan.selected_targets {
        if !family_scan_source_families.contains(target.source_family.as_str()) {
            continue;
        }
        let span = family_scan_spans
            .entry(target.source_family.as_str())
            .or_insert((target.effective_from_block, target.effective_to_block));
        span.0 = span.0.min(target.effective_from_block);
        span.1 = span.1.max(target.effective_to_block);
    }
    let family_facts = family_scan_spans.into_iter().filter_map(
        move |(source_family, (span_from_block, span_to_block))| {
            let (covered_from_block, covered_to_block) = covered_block_interval(
                span_from_block,
                span_to_block,
                job_start_block,
                job_end_block,
            )?;
            Some(BackfillCoverageFactWrite {
                source_family: source_family.to_owned(),
                scope: BackfillCoverageFactScope::Family,
                address: None,
                covered_from_block,
                covered_to_block,
            })
        },
    );
    let address_facts = source_plan
        .selected_targets
        .iter()
        .filter_map(move |target| {
            if family_scan_source_families.contains(target.source_family.as_str()) {
                return None;
            }
            let (covered_from_block, covered_to_block) = covered_block_interval(
                target.effective_from_block,
                target.effective_to_block,
                job_start_block,
                job_end_block,
            )?;
            Some(BackfillCoverageFactWrite {
                source_family: target.source_family.clone(),
                scope: BackfillCoverageFactScope::Address,
                address: Some(target.address.to_ascii_lowercase()),
                covered_from_block,
                covered_to_block,
            })
        });

    family_facts.chain(address_facts)
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

#[cfg(test)]
mod tests {
    use bigname_manifests::{
        WatchedBackfillTarget, WatchedChainPlan, WatchedSourceSelectorKind,
        WatchedSourceSelectorPlan,
    };
    use bigname_storage::BackfillCoverageFactScope;
    use sqlx::types::Uuid;

    use super::*;

    #[test]
    fn covered_block_interval_clamps_and_rejects_empty_intersections() {
        assert_eq!(covered_block_interval(10, 20, 12, 18), Some((12, 18)));
        assert_eq!(covered_block_interval(12, 18, 10, 20), Some((12, 18)));
        assert_eq!(covered_block_interval(10, 20, 15, 30), Some((15, 20)));
        assert_eq!(covered_block_interval(15, 30, 10, 20), Some((15, 20)));
        assert_eq!(covered_block_interval(10, 10, 10, 10), Some((10, 10)));
        assert_eq!(covered_block_interval(10, 20, 20, 25), Some((20, 20)));
        assert_eq!(covered_block_interval(10, 20, 21, 25), None);
        assert_eq!(covered_block_interval(21, 25, 10, 20), None);
        assert_eq!(covered_block_interval(0, 0, 1, 1), None);
    }

    #[test]
    fn job_completion_facts_clamp_targets_and_skip_out_of_range_targets() {
        let source_plan = source_plan(
            "base-mainnet",
            vec![
                target(
                    "basenames_base_registry",
                    1,
                    "0xABCDEFabcdefABCDEFabcdefabcdefABCDEFabcd",
                    10,
                    30,
                ),
                target(
                    "basenames_base_registrar",
                    2,
                    "0x2222222222222222222222222222222222222222",
                    25,
                    60,
                ),
                target(
                    "basenames_base_resolver",
                    3,
                    "0x3333333333333333333333333333333333333333",
                    1,
                    19,
                ),
            ],
        );

        let facts = job_completion_coverage_facts(&source_plan, false, 20, 40).collect::<Vec<_>>();

        assert_eq!(facts.len(), 2);
        assert_eq!(facts[0].source_family, "basenames_base_registry");
        assert_eq!(facts[0].scope, BackfillCoverageFactScope::Address);
        assert_eq!(
            facts[0].address.as_deref(),
            Some("0xabcdefabcdefabcdefabcdefabcdefabcdefabcd")
        );
        assert_eq!(
            (facts[0].covered_from_block, facts[0].covered_to_block),
            (20, 30)
        );
        assert_eq!(facts[1].source_family, "basenames_base_registrar");
        assert_eq!(
            (facts[1].covered_from_block, facts[1].covered_to_block),
            (25, 40)
        );
    }

    #[test]
    fn generic_resolver_scan_yields_family_fact_and_excludes_resolver_targets() {
        let source_plan = source_plan(
            "ethereum-mainnet",
            vec![
                target(
                    "ens_v1_registry_l1",
                    1,
                    "0x1111111111111111111111111111111111111111",
                    5,
                    50,
                ),
                target(
                    "ens_v1_resolver_l1",
                    2,
                    "0x2222222222222222222222222222222222222222",
                    12,
                    18,
                ),
            ],
        );

        let facts = job_completion_coverage_facts(&source_plan, false, 10, 20).collect::<Vec<_>>();

        assert_eq!(facts.len(), 2);
        assert_eq!(facts[0].source_family, "ens_v1_resolver_l1");
        assert_eq!(facts[0].scope, BackfillCoverageFactScope::Family);
        assert_eq!(facts[0].address, None);
        assert_eq!(
            (facts[0].covered_from_block, facts[0].covered_to_block),
            (12, 18),
            "family fact must be clamped to the resolver targets' effective span"
        );
        assert_eq!(facts[1].source_family, "ens_v1_registry_l1");
        assert_eq!(facts[1].scope, BackfillCoverageFactScope::Address);
        assert_eq!(
            (facts[1].covered_from_block, facts[1].covered_to_block),
            (10, 20)
        );
    }

    #[test]
    fn family_scans_without_matching_targets_or_overlap_yield_no_family_fact() {
        let mut no_resolver_targets = source_plan(
            "ethereum-mainnet",
            vec![target(
                "ens_v1_registry_l1",
                1,
                "0x1111111111111111111111111111111111111111",
                5,
                50,
            )],
        );
        no_resolver_targets.selector_kind = WatchedSourceSelectorKind::SourceFamily;
        no_resolver_targets.source_family = Some("ens_v1_resolver_l1".to_owned());
        assert!(
            job_completion_coverage_facts(&no_resolver_targets, false, 10, 20)
                .all(|fact| fact.scope != BackfillCoverageFactScope::Family),
            "a family scan with no selected targets must not claim family coverage"
        );

        let disjoint_span = source_plan(
            "base-mainnet",
            vec![target(
                "basenames_base_registry",
                1,
                "0x1111111111111111111111111111111111111111",
                30,
                40,
            )],
        );
        assert_eq!(
            job_completion_coverage_facts(&disjoint_span, true, 10, 20).count(),
            0,
            "a family span disjoint from the job range must not claim coverage"
        );
    }

    #[test]
    fn basenames_registry_scan_all_yields_only_the_registry_family_fact() {
        let source_plan = source_plan(
            "base-mainnet",
            vec![
                target(
                    "basenames_base_registry",
                    1,
                    "0x1111111111111111111111111111111111111111",
                    12,
                    18,
                ),
                target(
                    "basenames_base_registry",
                    2,
                    "0x2222222222222222222222222222222222222222",
                    14,
                    30,
                ),
            ],
        );

        let facts = job_completion_coverage_facts(&source_plan, true, 10, 20).collect::<Vec<_>>();

        assert_eq!(facts.len(), 1);
        assert_eq!(facts[0].source_family, "basenames_base_registry");
        assert_eq!(facts[0].scope, BackfillCoverageFactScope::Family);
        assert_eq!(facts[0].address, None);
        assert_eq!(
            (facts[0].covered_from_block, facts[0].covered_to_block),
            (12, 20),
            "family fact must span the union of registry target windows clamped to the job range"
        );
    }

    fn target(
        source_family: &str,
        id: u128,
        address: &str,
        effective_from_block: i64,
        effective_to_block: i64,
    ) -> WatchedBackfillTarget {
        WatchedBackfillTarget {
            source_family: source_family.to_owned(),
            contract_instance_id: Uuid::from_u128(id),
            address: address.to_owned(),
            effective_from_block,
            effective_to_block,
        }
    }

    fn source_plan(
        chain: &str,
        selected_targets: Vec<WatchedBackfillTarget>,
    ) -> WatchedSourceSelectorPlan {
        WatchedSourceSelectorPlan {
            chain: chain.to_owned(),
            selector_kind: WatchedSourceSelectorKind::WatchedTargetSet,
            source_family: None,
            requested_watched_targets: Vec::new(),
            selected_targets,
            watched_chain_plan: WatchedChainPlan {
                chain: chain.to_owned(),
                addresses: Vec::new(),
                manifest_root_entry_count: 0,
                manifest_contract_entry_count: 0,
                discovery_edge_entry_count: 0,
            },
        }
    }
}
