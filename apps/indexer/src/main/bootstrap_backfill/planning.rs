use std::{collections::BTreeSet, thread};

use anyhow::{Context, Result, bail};
use bigname_manifests::{
    ManifestBootstrapTarget, WatchedBackfillTarget, WatchedSourceSelectorKind,
    WatchedSourceSelectorPlan,
};

use crate::{
    backfill::{BackfillAdapterSyncMode, BackfillBlockRange},
    ens_v1_resolver::SOURCE_FAMILY_ENS_V1_RESOLVER_L1,
    provider::ProviderHeadSnapshot,
};

use super::DEFAULT_BOOTSTRAP_BACKFILL_WORKERS;

const MAX_AUTOMATIC_BOOTSTRAP_BACKFILL_WORKERS: usize = 4;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct BootstrapBackfillTargetRange {
    pub(super) target: ManifestBootstrapTarget,
    pub(super) range: BackfillBlockRange,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct BootstrapBackfillSegment {
    pub(super) range: BackfillBlockRange,
    pub(super) targets: Vec<ManifestBootstrapTarget>,
}

pub(crate) fn bootstrap_finalized_head_block(
    chain: &str,
    heads: &ProviderHeadSnapshot,
) -> Result<i64> {
    let finalized = heads.finalized.as_ref().with_context(|| {
        format!(
            "provider for chain {chain} did not return a finalized head required for automatic bootstrap"
        )
    })?;
    anyhow::ensure!(
        finalized.block_number <= heads.canonical.block_number,
        "provider for chain {chain} returned finalized block {} above canonical block {}",
        finalized.block_number,
        heads.canonical.block_number
    );
    Ok(finalized.block_number)
}

pub(crate) fn resolve_bootstrap_backfill_worker_count(configured_worker_count: usize) -> usize {
    if configured_worker_count != DEFAULT_BOOTSTRAP_BACKFILL_WORKERS {
        return configured_worker_count.max(1);
    }

    thread::available_parallelism()
        .map(usize::from)
        .unwrap_or(1)
        .clamp(1, MAX_AUTOMATIC_BOOTSTRAP_BACKFILL_WORKERS)
}

pub(super) fn effective_bootstrap_backfill_worker_count(
    requested_worker_count: usize,
    adapter_sync_mode: BackfillAdapterSyncMode,
) -> usize {
    if adapter_sync_mode == BackfillAdapterSyncMode::RawOnly {
        requested_worker_count
    } else {
        1
    }
}

pub(super) fn bootstrap_target_range(
    target: &ManifestBootstrapTarget,
    provider_finalized_head_block: i64,
) -> Result<Option<BackfillBlockRange>> {
    let finite_end_block = target
        .effective_to_block
        .map(|effective_to_block| effective_to_block.min(provider_finalized_head_block))
        .unwrap_or(provider_finalized_head_block);
    let finite_start_block = target.effective_from_block;
    if finite_start_block > finite_end_block {
        return Ok(None);
    }

    BackfillBlockRange::new(finite_start_block, finite_end_block).map(Some)
}

pub(super) fn plan_bootstrap_backfill_segments(
    target_ranges: Vec<BootstrapBackfillTargetRange>,
) -> Result<Vec<BootstrapBackfillSegment>> {
    let Some(max_end_block) = target_ranges
        .iter()
        .map(|target_range| target_range.range.to_block)
        .max()
    else {
        return Ok(Vec::new());
    };

    let mut boundaries = BTreeSet::new();
    let mut resolver_range = None::<BackfillBlockRange>;
    for target_range in &target_ranges {
        if target_range.target.source_family == SOURCE_FAMILY_ENS_V1_RESOLVER_L1 {
            resolver_range = Some(match resolver_range {
                Some(range) => BackfillBlockRange {
                    from_block: range.from_block.min(target_range.range.from_block),
                    to_block: range.to_block.max(target_range.range.to_block),
                },
                None => target_range.range,
            });
            continue;
        }

        boundaries.insert(target_range.range.from_block);
        if target_range.range.to_block < i64::MAX {
            boundaries.insert(
                target_range
                    .range
                    .to_block
                    .checked_add(1)
                    .with_context(|| {
                        format!(
                            "bootstrap target range end {} overflowed while planning segments",
                            target_range.range.to_block
                        )
                    })?,
            );
        }
    }
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

    let boundaries = boundaries.into_iter().collect::<Vec<_>>();
    let mut segments = Vec::new();
    for (index, segment_start) in boundaries.iter().copied().enumerate() {
        if segment_start > max_end_block {
            break;
        }

        let segment_end = boundaries
            .get(index + 1)
            .map(|next_start| *next_start - 1)
            .unwrap_or(max_end_block)
            .min(max_end_block);
        if segment_start > segment_end {
            continue;
        }

        let targets = target_ranges
            .iter()
            .filter(|target_range| {
                if target_range.target.source_family == SOURCE_FAMILY_ENS_V1_RESOLVER_L1 {
                    target_range.range.from_block <= segment_end
                        && segment_start <= target_range.range.to_block
                } else {
                    target_range.range.from_block <= segment_start
                        && segment_end <= target_range.range.to_block
                }
            })
            .map(|target_range| target_range.target.clone())
            .collect::<Vec<_>>();
        if targets.is_empty() {
            continue;
        }

        segments.push(BootstrapBackfillSegment {
            range: BackfillBlockRange::new(segment_start, segment_end)?,
            targets,
        });
    }

    Ok(segments)
}

pub(super) fn narrow_manifest_bootstrap_source_plan(
    source_plan: &mut WatchedSourceSelectorPlan,
    targets: &[ManifestBootstrapTarget],
    range: BackfillBlockRange,
) -> Result<()> {
    if source_plan.selector_kind != WatchedSourceSelectorKind::WatchedTargetSet {
        bail!(
            "bootstrap source plan for range {}..={} used selector kind {} instead of watched_target_set",
            range.from_block,
            range.to_block,
            source_plan.selector_kind.as_str()
        );
    }

    let mut narrowed_targets = Vec::with_capacity(targets.len());

    for target in targets {
        let selected_target = source_plan.selected_targets.iter().find(|selected_target| {
            selected_target.source_family == target.source_family
                && selected_target.contract_instance_id == target.contract_instance_id
                && (target.source_family == SOURCE_FAMILY_ENS_V1_RESOLVER_L1
                    || (selected_target.address == target.address
                        && selected_target.effective_from_block == range.from_block
                        && selected_target.effective_to_block == range.to_block))
        });
        let Some(_selected_target) = selected_target else {
            if target.source_family == SOURCE_FAMILY_ENS_V1_RESOLVER_L1 {
                narrowed_targets.push(WatchedBackfillTarget {
                    source_family: target.source_family.clone(),
                    contract_instance_id: target.contract_instance_id,
                    address: target.address.clone(),
                    effective_from_block: range.from_block,
                    effective_to_block: range.to_block,
                });
                continue;
            }
            bail!(
                "bootstrap source plan for range {}..={} did not select authoritative contract_instance_id {} with the segmented address/range",
                range.from_block,
                range.to_block,
                target.contract_instance_id
            );
        };

        if target.source_family == SOURCE_FAMILY_ENS_V1_RESOLVER_L1 {
            narrowed_targets.push(WatchedBackfillTarget {
                source_family: target.source_family.clone(),
                contract_instance_id: target.contract_instance_id,
                address: target.address.clone(),
                effective_from_block: range.from_block,
                effective_to_block: range.to_block,
            });
            continue;
        }

        narrowed_targets.push(WatchedBackfillTarget {
            source_family: target.source_family.clone(),
            contract_instance_id: target.contract_instance_id,
            address: target.address.clone(),
            effective_from_block: range.from_block,
            effective_to_block: range.to_block,
        });
    }

    narrowed_targets.sort();
    narrowed_targets.dedup();
    if narrowed_targets.len() != targets.len() {
        bail!(
            "bootstrap source plan for range {}..={} produced {} unique manifest targets from {} requested targets",
            range.from_block,
            range.to_block,
            narrowed_targets.len(),
            targets.len()
        );
    }

    source_plan.selected_targets = narrowed_targets;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use bigname_manifests::{WatchedChainPlan, WatchedTargetIdentity};
    use sqlx::types::Uuid;

    #[test]
    fn bootstrap_source_plan_is_narrowed_to_manifest_segment_targets() -> Result<()> {
        let target = manifest_target(
            "ens_v1_registry_l1",
            1,
            "0x0000000000000000000000000000000000000001",
        );
        let range = BackfillBlockRange::new(10, 20)?;
        let expected = watched_target(&target, range);
        let mut source_plan = source_plan(vec![
            watched_target(&target, range),
            WatchedBackfillTarget {
                source_family: "ens_v1_resolver_l1".to_owned(),
                contract_instance_id: target.contract_instance_id,
                address: target.address.clone(),
                effective_from_block: range.from_block,
                effective_to_block: range.to_block,
            },
        ]);

        narrow_manifest_bootstrap_source_plan(&mut source_plan, &[target], range)?;

        assert_eq!(source_plan.selected_targets, vec![expected]);
        Ok(())
    }

    #[test]
    fn bootstrap_source_plan_rejects_missing_manifest_segment_target() -> Result<()> {
        let target = manifest_target(
            "ens_v1_registry_l1",
            1,
            "0x0000000000000000000000000000000000000001",
        );
        let range = BackfillBlockRange::new(10, 20)?;
        let mut source_plan = source_plan(Vec::new());

        let error = narrow_manifest_bootstrap_source_plan(&mut source_plan, &[target], range)
            .expect_err("missing target must fail");

        assert!(
            error
                .to_string()
                .contains("did not select authoritative contract_instance_id"),
            "unexpected error: {error:#}"
        );
        Ok(())
    }

    #[test]
    fn bootstrap_segments_coalesce_resolver_boundaries_without_future_profiles() -> Result<()> {
        let registry = target_range(
            "ens_v1_registry_l1",
            1,
            "0x0000000000000000000000000000000000000001",
            1,
            300,
        )?;
        let resolver_a = target_range(
            SOURCE_FAMILY_ENS_V1_RESOLVER_L1,
            2,
            "0x0000000000000000000000000000000000000002",
            10,
            300,
        )?;
        let resolver_b = target_range(
            SOURCE_FAMILY_ENS_V1_RESOLVER_L1,
            3,
            "0x0000000000000000000000000000000000000003",
            200,
            300,
        )?;
        let resolver_c = target_range(
            SOURCE_FAMILY_ENS_V1_RESOLVER_L1,
            4,
            "0x0000000000000000000000000000000000000004",
            400,
            500,
        )?;

        let segments =
            plan_bootstrap_backfill_segments(vec![registry, resolver_a, resolver_b, resolver_c])?;

        assert_eq!(segments.len(), 3);
        assert_eq!(segments[0].range, BackfillBlockRange::new(1, 9)?);
        assert_eq!(segment_target_ids(&segments[0]), vec![Uuid::from_u128(1)]);
        assert_eq!(segments[1].range, BackfillBlockRange::new(10, 300)?);
        assert_eq!(
            segment_target_ids(&segments[1]),
            vec![Uuid::from_u128(1), Uuid::from_u128(2), Uuid::from_u128(3),]
        );
        assert_eq!(segments[2].range, BackfillBlockRange::new(301, 500)?);
        assert_eq!(segment_target_ids(&segments[2]), vec![Uuid::from_u128(4)]);

        Ok(())
    }

    fn manifest_target(source_family: &str, id: u128, address: &str) -> ManifestBootstrapTarget {
        ManifestBootstrapTarget {
            source_family: source_family.to_owned(),
            contract_instance_id: Uuid::from_u128(id),
            address: address.to_owned(),
            effective_from_block: 10,
            effective_to_block: Some(20),
        }
    }

    fn target_range(
        source_family: &str,
        id: u128,
        address: &str,
        from_block: i64,
        to_block: i64,
    ) -> Result<BootstrapBackfillTargetRange> {
        Ok(BootstrapBackfillTargetRange {
            target: ManifestBootstrapTarget {
                source_family: source_family.to_owned(),
                contract_instance_id: Uuid::from_u128(id),
                address: address.to_owned(),
                effective_from_block: from_block,
                effective_to_block: Some(to_block),
            },
            range: BackfillBlockRange::new(from_block, to_block)?,
        })
    }

    fn segment_target_ids(segment: &BootstrapBackfillSegment) -> Vec<Uuid> {
        segment
            .targets
            .iter()
            .map(|target| target.contract_instance_id)
            .collect()
    }

    fn watched_target(
        target: &ManifestBootstrapTarget,
        range: BackfillBlockRange,
    ) -> WatchedBackfillTarget {
        WatchedBackfillTarget {
            source_family: target.source_family.clone(),
            contract_instance_id: target.contract_instance_id,
            address: target.address.clone(),
            effective_from_block: range.from_block,
            effective_to_block: range.to_block,
        }
    }

    fn source_plan(selected_targets: Vec<WatchedBackfillTarget>) -> WatchedSourceSelectorPlan {
        WatchedSourceSelectorPlan {
            chain: "ethereum-mainnet".to_owned(),
            selector_kind: WatchedSourceSelectorKind::WatchedTargetSet,
            source_family: None,
            requested_watched_targets: vec![WatchedTargetIdentity {
                contract_instance_id: Uuid::from_u128(1),
            }],
            selected_targets,
            watched_chain_plan: WatchedChainPlan {
                chain: "ethereum-mainnet".to_owned(),
                addresses: Vec::new(),
                manifest_root_entry_count: 0,
                manifest_contract_entry_count: 0,
                discovery_edge_entry_count: 0,
            },
        }
    }
}
