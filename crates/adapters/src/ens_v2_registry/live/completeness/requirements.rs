use std::collections::BTreeMap;

use anyhow::{Result, ensure};
use bigname_domain::block_interval::InclusiveBlockInterval;
use bigname_manifests::{
    RequiredWatchedTuple, load_required_watched_tuples, load_required_watched_tuples_with_progress,
};
use sqlx::PgPool;

use crate::{
    checkpoint_context::StartupAdapterProgress, startup_progress::StartupManifestProgress,
};

use crate::ens_v2_registry::constants::{
    SOURCE_FAMILY_ENS_V2_REGISTRY_L1, SOURCE_FAMILY_ENS_V2_RESOLVER_L1,
    SOURCE_FAMILY_ENS_V2_ROOT_L1,
};

mod coverage;
mod witnesses;
pub(super) use coverage::{
    ensure_generation_bound_coverage, ensure_generation_bound_coverage_with_live_selection,
    ensure_generation_bound_coverage_with_live_selection_with_progress,
    ensure_newly_required_generation_bound_coverage,
};
pub(super) use witnesses::{
    ensure_retained_semantic_witnesses, ensure_retained_semantic_witnesses_with_progress,
};

pub(in crate::ens_v2_registry) async fn has_authoritative_ens_v2_closure_through(
    pool: &PgPool,
    chain: &str,
    through_block: i64,
) -> Result<bool> {
    ensure!(
        through_block >= 0,
        "ENSv2 authoritative-closure boundary cannot be negative"
    );
    Ok(!load_required_watched_tuples(
        pool,
        chain,
        0,
        through_block,
        &ens_v2_closure_source_families(),
    )
    .await?
    .is_empty())
}

pub(in crate::ens_v2_registry) async fn has_authoritative_ens_v2_closure_through_with_progress(
    pool: &PgPool,
    chain: &str,
    through_block: i64,
    progress: &mut dyn StartupAdapterProgress,
) -> Result<bool> {
    ensure!(
        through_block >= 0,
        "ENSv2 authoritative-closure boundary cannot be negative"
    );
    let mut manifest_progress = StartupManifestProgress::new(progress);
    Ok(!load_required_watched_tuples_with_progress(
        pool,
        chain,
        0,
        through_block,
        &ens_v2_closure_source_families(),
        &mut manifest_progress,
    )
    .await?
    .is_empty())
}

pub(super) fn ens_v2_closure_source_families() -> Vec<String> {
    vec![
        SOURCE_FAMILY_ENS_V2_ROOT_L1.to_owned(),
        SOURCE_FAMILY_ENS_V2_REGISTRY_L1.to_owned(),
    ]
}

/// Source families whose newly admitted historical intervals must be fetched
/// before a live registry sync may publish its discovery epoch. Resolver
/// history participates in this admission delta without becoming part of the
/// registry adapter's retained-history proof or semantic-witness set.
pub(super) fn ens_v2_discovery_history_source_families() -> Vec<String> {
    vec![
        SOURCE_FAMILY_ENS_V2_ROOT_L1.to_owned(),
        SOURCE_FAMILY_ENS_V2_REGISTRY_L1.to_owned(),
        SOURCE_FAMILY_ENS_V2_RESOLVER_L1.to_owned(),
    ]
}

pub(super) fn requirement_intervals_not_covered_by(
    required: &[RequiredWatchedTuple],
    covered: &[RequiredWatchedTuple],
) -> Vec<RequiredWatchedTuple> {
    let mut covered_by_tuple = BTreeMap::<(String, String), Vec<InclusiveBlockInterval>>::new();
    for requirement in covered {
        let Some(interval) = InclusiveBlockInterval::new(
            requirement.required_from_block,
            requirement.required_to_block,
        ) else {
            continue;
        };
        covered_by_tuple
            .entry((
                requirement.source_family.clone(),
                requirement.address.clone(),
            ))
            .or_default()
            .push(interval);
    }

    let mut gaps = Vec::new();
    for requirement in required {
        let Some(required_interval) = InclusiveBlockInterval::new(
            requirement.required_from_block,
            requirement.required_to_block,
        ) else {
            continue;
        };
        let key = (
            requirement.source_family.clone(),
            requirement.address.clone(),
        );
        gaps.extend(
            required_interval
                .uncovered_by(covered_by_tuple.get(&key).into_iter().flatten().copied())
                .into_iter()
                .map(|gap| RequiredWatchedTuple {
                    source_family: requirement.source_family.clone(),
                    address: requirement.address.clone(),
                    required_from_block: gap.from_block(),
                    required_to_block: gap.through_block(),
                }),
        );
    }
    gaps
}

pub(super) async fn requirement_intervals_not_covered_by_with_progress(
    pool: &PgPool,
    required: &[RequiredWatchedTuple],
    covered: &[RequiredWatchedTuple],
    progress: &mut dyn StartupAdapterProgress,
) -> Result<Vec<RequiredWatchedTuple>> {
    let mut covered_by_tuple = BTreeMap::<(String, String), Vec<InclusiveBlockInterval>>::new();
    for (index, requirement) in covered.iter().enumerate() {
        if let Some(interval) = InclusiveBlockInterval::new(
            requirement.required_from_block,
            requirement.required_to_block,
        ) {
            covered_by_tuple
                .entry((
                    requirement.source_family.clone(),
                    requirement.address.clone(),
                ))
                .or_default()
                .push(interval);
        }
        if (index + 1).is_multiple_of(super::RETAINED_REQUIREMENT_PROGRESS_ROWS) {
            progress.record(pool).await?;
        }
    }
    if !covered.is_empty()
        && !covered
            .len()
            .is_multiple_of(super::RETAINED_REQUIREMENT_PROGRESS_ROWS)
    {
        progress.record(pool).await?;
    }

    let mut gaps = Vec::new();
    for (index, requirement) in required.iter().enumerate() {
        if let Some(required_interval) = InclusiveBlockInterval::new(
            requirement.required_from_block,
            requirement.required_to_block,
        ) {
            let key = (
                requirement.source_family.clone(),
                requirement.address.clone(),
            );
            gaps.extend(
                required_interval
                    .uncovered_by(covered_by_tuple.get(&key).into_iter().flatten().copied())
                    .into_iter()
                    .map(|gap| RequiredWatchedTuple {
                        source_family: requirement.source_family.clone(),
                        address: requirement.address.clone(),
                        required_from_block: gap.from_block(),
                        required_to_block: gap.through_block(),
                    }),
            );
        }
        if (index + 1).is_multiple_of(super::RETAINED_REQUIREMENT_PROGRESS_ROWS) {
            progress.record(pool).await?;
        }
    }
    if !required.is_empty()
        && !required
            .len()
            .is_multiple_of(super::RETAINED_REQUIREMENT_PROGRESS_ROWS)
    {
        progress.record(pool).await?;
    }
    Ok(gaps)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn requirement(family: &str, address: &str, from: i64, to: i64) -> RequiredWatchedTuple {
        RequiredWatchedTuple {
            source_family: family.to_owned(),
            address: address.to_owned(),
            required_from_block: from,
            required_to_block: to,
        }
    }

    #[test]
    fn requirement_intervals_treat_same_open_extension_as_pre_sync_coverage() {
        let required = [requirement("registry", "0xa", 0, 20)];
        let covered = [requirement("registry", "0xa", 0, 20)];

        assert!(requirement_intervals_not_covered_by(&required, &covered).is_empty());
    }

    #[test]
    fn requirement_intervals_ignore_resolver_only_epoch_changes() {
        let required = [
            requirement("root", "0x1", 0, 20),
            requirement("registry", "0x2", 0, 20),
        ];

        assert!(requirement_intervals_not_covered_by(&required, &required).is_empty());
    }

    #[test]
    fn requirement_intervals_return_reopened_gap() {
        let required = [
            requirement("registry", "0xa", 0, 10),
            requirement("registry", "0xa", 20, 30),
        ];
        let covered = [requirement("registry", "0xa", 0, 10)];

        assert_eq!(
            requirement_intervals_not_covered_by(&required, &covered),
            vec![requirement("registry", "0xa", 20, 30)]
        );
    }

    #[test]
    fn requirement_intervals_return_earlier_expansion() {
        let required = [requirement("registry", "0xa", 5, 30)];
        let covered = [requirement("registry", "0xa", 10, 30)];

        assert_eq!(
            requirement_intervals_not_covered_by(&required, &covered),
            vec![requirement("registry", "0xa", 5, 9)]
        );
    }

    #[test]
    fn requirement_intervals_merge_overlapping_covered_ranges() {
        let required = [requirement("registry", "0xa", 0, 25)];
        let covered = [
            requirement("registry", "0xa", 0, 10),
            requirement("registry", "0xa", 5, 20),
        ];

        assert_eq!(
            requirement_intervals_not_covered_by(&required, &covered),
            vec![requirement("registry", "0xa", 21, 25)]
        );
    }

    #[test]
    fn requirement_intervals_do_not_cross_family_or_address_identity() {
        let required = [requirement("registry", "0xa", 0, 10)];
        let covered = [
            requirement("root", "0xa", 0, 10),
            requirement("registry", "0xb", 0, 10),
        ];

        assert_eq!(
            requirement_intervals_not_covered_by(&required, &covered),
            required
        );
    }

    #[test]
    fn requirement_interval_subtraction_matches_point_membership_on_small_ranges() {
        let intervals = (0..=5)
            .flat_map(|from| (from..=5).map(move |to| (from, to)))
            .collect::<Vec<_>>();

        for &(required_from, required_to) in &intervals {
            let required = [requirement("registry", "0xa", required_from, required_to)];
            for &(first_from, first_to) in &intervals {
                for &(second_from, second_to) in &intervals {
                    let covered = [
                        requirement("registry", "0xa", first_from, first_to),
                        requirement("registry", "0xa", second_from, second_to),
                    ];
                    let gaps = requirement_intervals_not_covered_by(&required, &covered);

                    for block in required_from..=required_to {
                        let expected_gap = !covered.iter().any(|interval| {
                            block >= interval.required_from_block
                                && block <= interval.required_to_block
                        });
                        let actual_gap = gaps.iter().any(|interval| {
                            block >= interval.required_from_block
                                && block <= interval.required_to_block
                        });
                        assert_eq!(actual_gap, expected_gap, "block {block}, {covered:?}");
                    }
                }
            }
        }
    }
}
