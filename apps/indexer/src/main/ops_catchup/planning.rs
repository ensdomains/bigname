use std::collections::{BTreeSet, HashSet};
use std::sync::Arc;

#[cfg(test)]
use anyhow::{Context, Result};
use bigname_manifests::WatchedContract;
use sqlx::types::Uuid;

use crate::backfill::BackfillBlockRange;

#[path = "planning/recovery.rs"]
mod recovery;
pub(super) use recovery::merge_retained_history_recovery_targets;
#[path = "planning/chunks.rs"]
mod chunks;
#[cfg(test)]
pub(super) use chunks::plan_catchup_chunks;
pub(super) use chunks::plan_catchup_chunks_reusing_completed;
#[path = "planning/retries.rs"]
mod retries;
pub(super) use retries::{CompletedCatchupPass, retry_required_ranges};
#[path = "planning/source_plan.rs"]
mod source_plan;

#[rustfmt::skip]
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(super) struct CatchupTarget { source_family: String, pub(super) contract_instance_id: Uuid, address: String, from_block: i64, to_block: Option<i64> }

/// A contiguous block range plus the watched targets that cover every block of
/// the range.
///
/// The covering target set is represented as a window over one shared,
/// activation-ordered target arena instead of a per-chunk deep clone. At the
/// post-rederivation discovery-graph scale (1.12M watched targets on
/// ethereum-mainnet, 3.84M on base-mainnet, each with ~1M distinct activation
/// boundaries) per-chunk `Vec<CatchupTarget>` clones made the planned chunk
/// vector quadratic in the target count (observed 69-90GB RSS, three OOM
/// kills); the shared-arena window keeps the whole plan linear in the target
/// count while describing exactly the same per-chunk target sets.
#[derive(Clone, Debug)]
pub(super) struct CatchupChunk {
    pub(super) range: BackfillBlockRange,
    covering: CatchupTargetWindow,
}

#[derive(Clone, Debug)]
struct CatchupTargetWindow {
    /// Every head-clipped target of the planning pass, sorted by ascending
    /// activation block; shared by all chunks of the pass.
    starts: Arc<[CatchupTarget]>,
    /// Indices into `starts`, sorted by ascending head-clipped end block;
    /// shared by all chunks of the pass.
    expiry_order: Arc<[u32]>,
    /// `starts[..start_count]` are exactly the targets with
    /// `from_block <= range.from_block`.
    start_count: u32,
    /// `expiry_order[..expired_count]` are exactly the targets whose clipped
    /// end block lies before `range.from_block`. They are always contained in
    /// `starts[..start_count]` because a target activates no later than it
    /// ends, so the covering set is `starts[..start_count]` minus these
    /// entries.
    expired_count: u32,
}

impl CatchupChunk {
    /// Contract instance ids of the targets covering this chunk. Duplicate ids
    /// across source families are preserved exactly as the previous per-chunk
    /// target vector preserved them; the downstream watched-target-set
    /// selector sorts and dedups identities before hashing.
    #[cfg(test)]
    pub(super) fn target_contract_instance_ids(&self) -> Vec<Uuid> {
        self.covered_targets()
            .into_iter()
            .map(|target| target.contract_instance_id)
            .collect()
    }

    pub(super) fn covered_targets(&self) -> Vec<&CatchupTarget> {
        let start_count = self.covering.start_count as usize;
        let expired_count = self.covering.expired_count as usize;
        let activated = &self.covering.starts[..start_count];
        if expired_count == 0 {
            return activated.iter().collect();
        }

        let expired = self.covering.expiry_order[..expired_count]
            .iter()
            .copied()
            .collect::<HashSet<u32>>();
        activated
            .iter()
            .enumerate()
            .filter(|(index, _)| !expired.contains(&(*index as u32)))
            .map(|(_, target)| target)
            .collect()
    }

    #[cfg(test)]
    pub(super) fn shares_target_arena_with(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.covering.starts, &other.covering.starts)
            && Arc::ptr_eq(&self.covering.expiry_order, &other.covering.expiry_order)
    }

    #[cfg(test)]
    pub(super) fn target_arena_entry_count(&self) -> usize {
        self.covering.starts.len()
    }
}

pub(super) fn catchup_targets_for_chain(
    watched_contracts: &[WatchedContract],
    chain: &str,
) -> (Vec<CatchupTarget>, Vec<CatchupTarget>) {
    let mut targets = BTreeSet::new();
    let mut skipped = BTreeSet::new();
    for contract in watched_contracts
        .iter()
        .filter(|contract| contract.chain == chain)
    {
        let target = CatchupTarget {
            source_family: contract.source_family.clone(),
            contract_instance_id: contract.contract_instance_id,
            address: contract.address.clone(),
            from_block: contract.active_from_block_number.unwrap_or(0),
            to_block: contract.active_to_block_number,
        };
        if contract.active_from_block_number.is_some() {
            targets.insert(target);
        } else {
            skipped.insert(target);
        }
    }

    (targets.into_iter().collect(), skipped.into_iter().collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    type CatchupChunkPlan = Vec<(BackfillBlockRange, Vec<CatchupTarget>)>;

    /// The pre-fix planner, kept verbatim as the semantic oracle: one deep
    /// clone of every covering target per chunk. Memory-quadratic at
    /// discovery-graph scale, but exact on small fixtures.
    fn naive_plan_catchup_chunks(
        targets: &[CatchupTarget],
        finalized_head_block_number: i64,
        chunk_blocks: i64,
    ) -> Result<(CatchupChunkPlan, usize)> {
        let mut target_ranges = Vec::<(CatchupTarget, BackfillBlockRange)>::new();
        let mut skipped_future_target_count = 0;
        for target in targets {
            let end = target
                .to_block
                .map(|to_block| to_block.min(finalized_head_block_number))
                .unwrap_or(finalized_head_block_number);
            if target.from_block > end {
                skipped_future_target_count += 1;
                continue;
            }
            target_ranges.push((
                target.clone(),
                BackfillBlockRange::new(target.from_block, end)?,
            ));
        }

        let mut boundaries = BTreeSet::new();
        for (_, range) in &target_ranges {
            boundaries.insert(range.from_block);
            boundaries.insert(
                range
                    .to_block
                    .checked_add(1)
                    .context("catch-up range overflow")?,
            );
        }
        let boundaries = boundaries.into_iter().collect::<Vec<_>>();
        let mut chunks = Vec::new();
        for pair in boundaries.windows(2) {
            let segment_start = pair[0];
            let segment_end = pair[1] - 1;
            let targets = target_ranges
                .iter()
                .filter(|(_, range)| {
                    range.from_block <= segment_start && segment_end <= range.to_block
                })
                .map(|(target, _)| target.clone())
                .collect::<Vec<_>>();
            if targets.is_empty() {
                continue;
            }

            let mut chunk_start = segment_start;
            while chunk_start <= segment_end {
                let chunk_end = chunk_start
                    .checked_add(chunk_blocks - 1)
                    .unwrap_or(segment_end)
                    .min(segment_end);
                chunks.push((
                    BackfillBlockRange::new(chunk_start, chunk_end)?,
                    targets.clone(),
                ));
                chunk_start = chunk_end
                    .checked_add(1)
                    .context("catch-up chunk start overflow")?;
            }
        }

        Ok((chunks, skipped_future_target_count))
    }

    fn target(
        source_family: &str,
        id: u128,
        address: &str,
        from_block: i64,
        to_block: Option<i64>,
    ) -> CatchupTarget {
        CatchupTarget {
            source_family: source_family.to_owned(),
            contract_instance_id: Uuid::from_u128(id),
            address: address.to_owned(),
            from_block,
            to_block,
        }
    }

    fn assert_compact_plan_matches_naive_plan(
        targets: &[CatchupTarget],
        finalized_head_block_number: i64,
        chunk_blocks: i64,
    ) -> Result<()> {
        let (compact_chunks, compact_skipped) =
            plan_catchup_chunks(targets, finalized_head_block_number, chunk_blocks)?;
        let (naive_chunks, naive_skipped) =
            naive_plan_catchup_chunks(targets, finalized_head_block_number, chunk_blocks)?;

        assert_eq!(
            compact_skipped, naive_skipped,
            "skipped-future counts diverged for head {finalized_head_block_number} chunk_blocks {chunk_blocks}"
        );
        assert_eq!(
            compact_chunks.len(),
            naive_chunks.len(),
            "chunk counts diverged for head {finalized_head_block_number} chunk_blocks {chunk_blocks}"
        );
        for (compact, (naive_range, naive_targets)) in compact_chunks.iter().zip(&naive_chunks) {
            assert_eq!(
                compact.range, *naive_range,
                "chunk ranges diverged for head {finalized_head_block_number} chunk_blocks {chunk_blocks}"
            );

            let compact_targets = compact.covered_targets();
            assert_eq!(
                compact_targets.len(),
                naive_targets.len(),
                "covering target counts diverged for chunk {}..={}",
                compact.range.from_block,
                compact.range.to_block
            );
            let compact_set = compact_targets.into_iter().collect::<BTreeSet<_>>();
            let naive_set = naive_targets.iter().collect::<BTreeSet<_>>();
            assert_eq!(
                compact_set, naive_set,
                "covering target sets diverged for chunk {}..={}",
                compact.range.from_block, compact.range.to_block
            );
        }

        Ok(())
    }

    #[test]
    fn compact_chunks_match_naive_chunks_on_multi_edge_fixture() -> Result<()> {
        // Multi-edge shape: one root watched from genesis, discovery edges
        // activating at staggered blocks (including two edges sharing one
        // contract instance across source families), a bounded-lifetime edge
        // that expires mid-plan, an edge activating exactly at the head, and a
        // future edge past the head.
        let targets = vec![
            target(
                "ens_v1_registry_l1",
                1,
                "0x00000000000000000000000000000000000000aa",
                1,
                None,
            ),
            target(
                "ens_v1_resolver_l1",
                2,
                "0x00000000000000000000000000000000000000bb",
                5,
                None,
            ),
            target(
                "ens_v1_resolver_l1",
                3,
                "0x00000000000000000000000000000000000000cc",
                12,
                None,
            ),
            target(
                "ens_v1_registrar_l1",
                3,
                "0x00000000000000000000000000000000000000cc",
                12,
                None,
            ),
            target(
                "ens_v1_resolver_l1",
                4,
                "0x00000000000000000000000000000000000000dd",
                12,
                Some(40),
            ),
            target(
                "ens_v1_resolver_l1",
                5,
                "0x00000000000000000000000000000000000000ee",
                30,
                Some(64),
            ),
            target(
                "ens_v1_resolver_l1",
                6,
                "0x00000000000000000000000000000000000000ff",
                100,
                None,
            ),
            target(
                "ens_v1_resolver_l1",
                7,
                "0x0000000000000000000000000000000000000011",
                101,
                None,
            ),
        ];

        for chunk_blocks in [1, 7, 32, 10_000] {
            assert_compact_plan_matches_naive_plan(&targets, 100, chunk_blocks)?;
        }
        Ok(())
    }

    #[test]
    fn compact_chunks_match_naive_chunks_on_seeded_random_target_sets() -> Result<()> {
        // Deterministic LCG so the sweep is reproducible without a new
        // property-testing dependency.
        let mut state = 0x2545_f491_4f6c_dd1d_u64;
        let mut next = move |bound: u64| {
            state = state
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            (state >> 33) % bound
        };

        for case in 0..60 {
            let target_count = 1 + next(40) as usize;
            let targets = (0..target_count)
                .map(|_| {
                    let from_block = next(120) as i64;
                    let to_block = match next(3) {
                        0 => None,
                        _ => Some(from_block + next(50) as i64),
                    };
                    target(
                        [
                            "ens_v1_registry_l1",
                            "ens_v1_resolver_l1",
                            "basenames_base_registry",
                        ][next(3) as usize],
                        u128::from(1 + next(16)),
                        &format!("0x{:040x}", next(24)),
                        from_block,
                        to_block,
                    )
                })
                .collect::<BTreeSet<_>>()
                .into_iter()
                .collect::<Vec<_>>();
            let head = [60, 100, 119][next(3) as usize];
            let chunk_blocks = [1, 3, 8, 32][next(4) as usize];

            assert_compact_plan_matches_naive_plan(&targets, head, chunk_blocks)
                .with_context(|| format!("seeded case {case} diverged"))?;
        }
        Ok(())
    }

    #[test]
    fn chunk_covering_windows_share_one_target_arena() -> Result<()> {
        // Memory-shape regression guard for the three OOM kills (exit 137,
        // 69-90GB RSS): the plan must own each covering target once in a
        // shared arena, never once per chunk. Six unique targets with distinct
        // activation blocks over a 1-block chunk size produce far more chunks
        // than targets; the naive representation materialized
        // sum(|covering set| per chunk) target clones.
        let targets = (0..6_u32)
            .map(|index| {
                target(
                    "ens_v1_resolver_l1",
                    u128::from(index + 1),
                    &format!("0x{:040x}", index),
                    i64::from(index) * 10,
                    None,
                )
            })
            .collect::<Vec<_>>();

        let (chunks, skipped) = plan_catchup_chunks(&targets, 99, 1)?;
        assert_eq!(skipped, 0);
        assert_eq!(chunks.len(), 100, "1-block chunks over blocks 0..=99");

        let first = chunks.first().expect("plan must contain chunks");
        for chunk in &chunks {
            assert!(
                chunk.shares_target_arena_with(first),
                "every chunk must borrow the one shared target arena"
            );
            assert_eq!(
                chunk.target_arena_entry_count(),
                targets.len(),
                "the shared arena must hold exactly the unique target entries"
            );
        }

        let materialized_covering_entry_count = chunks
            .iter()
            .map(|chunk| chunk.covered_targets().len())
            .sum::<usize>();
        assert_eq!(
            materialized_covering_entry_count, 450,
            "naive per-chunk clones would have owned 450 targets here; the plan owns 6"
        );

        Ok(())
    }

    #[test]
    fn future_targets_are_skipped_and_counted() -> Result<()> {
        let targets = vec![
            target(
                "ens_v1_registry_l1",
                1,
                "0x00000000000000000000000000000000000000aa",
                1,
                None,
            ),
            target(
                "ens_v1_resolver_l1",
                2,
                "0x00000000000000000000000000000000000000bb",
                51,
                None,
            ),
            target(
                "ens_v1_resolver_l1",
                3,
                "0x00000000000000000000000000000000000000cc",
                200,
                Some(300),
            ),
        ];

        let (chunks, skipped) = plan_catchup_chunks(&targets, 50, 32)?;

        assert_eq!(skipped, 2, "targets past the finalized head are skipped");
        assert_eq!(chunks.len(), 2, "blocks 1..=50 in 32-block chunks");
        assert_eq!(chunks[0].range, BackfillBlockRange::new(1, 32)?);
        assert_eq!(chunks[1].range, BackfillBlockRange::new(33, 50)?);
        for chunk in &chunks {
            assert_eq!(
                chunk.target_contract_instance_ids(),
                vec![Uuid::from_u128(1)]
            );
        }
        Ok(())
    }
}
