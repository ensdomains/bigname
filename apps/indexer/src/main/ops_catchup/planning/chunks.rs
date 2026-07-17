use std::{collections::BTreeSet, sync::Arc};

use anyhow::{Context, Result};

use crate::backfill::BackfillBlockRange;

use super::{CatchupChunk, CatchupTarget, CatchupTargetWindow};

pub(in crate::ops_catchup) struct CatchupChunkPlan {
    pub(in crate::ops_catchup) chunks_to_run: Vec<CatchupChunk>,
    pub(in crate::ops_catchup) skipped_future_target_count: usize,
    pub(in crate::ops_catchup) planned_chunk_count: usize,
    pub(in crate::ops_catchup) reused_completed_chunk_count: usize,
}

#[cfg(test)]
pub(in crate::ops_catchup) fn plan_catchup_chunks(
    targets: &[CatchupTarget],
    finalized_head_block_number: i64,
    chunk_blocks: i64,
) -> Result<(Vec<CatchupChunk>, usize)> {
    let plan = plan_catchup_chunks_inner(targets, finalized_head_block_number, chunk_blocks, None)?;
    Ok((plan.chunks_to_run, plan.skipped_future_target_count))
}

/// Re-plan a completed convergence pass while materializing only chunks whose
/// source set may have changed. `required_ranges` is normalized and inclusive.
pub(in crate::ops_catchup) fn plan_catchup_chunks_reusing_completed(
    targets: &[CatchupTarget],
    finalized_head_block_number: i64,
    chunk_blocks: i64,
    required_ranges: Option<&[BackfillBlockRange]>,
) -> Result<CatchupChunkPlan> {
    plan_catchup_chunks_inner(
        targets,
        finalized_head_block_number,
        chunk_blocks,
        required_ranges,
    )
}

fn plan_catchup_chunks_inner(
    targets: &[CatchupTarget],
    finalized_head_block_number: i64,
    chunk_blocks: i64,
    required_ranges: Option<&[BackfillBlockRange]>,
) -> Result<CatchupChunkPlan> {
    let mut clipped = Vec::with_capacity(targets.len());
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
        clipped.push((
            target.clone(),
            BackfillBlockRange::new(target.from_block, end)?,
        ));
    }

    // Build one shared covering arena. Retry filters jump directly to chunks
    // intersecting changed target ranges instead of walking every historical
    // chunk and opening an idempotent job-creation transaction for each one.
    clipped.sort_by(|left, right| {
        (left.1.from_block, left.1.to_block).cmp(&(right.1.from_block, right.1.to_block))
    });
    let clipped_ranges = clipped.iter().map(|(_, range)| *range).collect::<Vec<_>>();
    let starts: Arc<[CatchupTarget]> = clipped
        .into_iter()
        .map(|(target, _)| target)
        .collect::<Vec<_>>()
        .into();

    let mut expiry_order = (0..u32::try_from(clipped_ranges.len())
        .context("catch-up target count overflowed the covering-window index width")?)
        .collect::<Vec<_>>();
    expiry_order.sort_by_key(|&index| clipped_ranges[index as usize].to_block);
    let expiry_order: Arc<[u32]> = expiry_order.into();

    let mut boundaries = BTreeSet::new();
    for range in &clipped_ranges {
        boundaries.insert(range.from_block);
        boundaries.insert(
            range
                .to_block
                .checked_add(1)
                .context("catch-up range overflow")?,
        );
    }
    let boundaries = boundaries.into_iter().collect::<Vec<_>>();

    let mut chunks_to_run = Vec::new();
    let mut planned_chunk_count = 0_usize;
    let mut start_count = 0_usize;
    let mut expired_count = 0_usize;
    for pair in boundaries.windows(2) {
        let segment_start = pair[0];
        let segment_end = pair[1] - 1;
        while start_count < clipped_ranges.len()
            && clipped_ranges[start_count].from_block <= segment_start
        {
            start_count += 1;
        }
        while expired_count < clipped_ranges.len()
            && clipped_ranges[expiry_order[expired_count] as usize].to_block < segment_start
        {
            expired_count += 1;
        }
        if start_count == expired_count {
            continue;
        }

        let covering = CatchupTargetWindow {
            starts: Arc::clone(&starts),
            expiry_order: Arc::clone(&expiry_order),
            start_count: u32::try_from(start_count)
                .context("catch-up covering start count overflowed the window width")?,
            expired_count: u32::try_from(expired_count)
                .context("catch-up covering expired count overflowed the window width")?,
        };
        let segment_block_count = segment_end - segment_start + 1;
        let segment_chunk_count = usize::try_from((segment_block_count - 1) / chunk_blocks + 1)
            .context("catch-up segment chunk count does not fit in usize")?;
        planned_chunk_count = planned_chunk_count
            .checked_add(segment_chunk_count)
            .context("catch-up planned chunk count overflow")?;

        match required_ranges {
            None => push_chunk_indexes(
                &mut chunks_to_run,
                &covering,
                segment_start,
                segment_end,
                chunk_blocks,
                0,
                segment_chunk_count - 1,
            )?,
            Some(required_ranges) => {
                let mut last_pushed_index = None;
                for required in required_ranges {
                    let overlap_start = required.from_block.max(segment_start);
                    let overlap_end = required.to_block.min(segment_end);
                    if overlap_start > overlap_end {
                        continue;
                    }
                    let mut first_index =
                        usize::try_from((overlap_start - segment_start) / chunk_blocks)
                            .context("catch-up retry first chunk index does not fit in usize")?;
                    let last_index = usize::try_from((overlap_end - segment_start) / chunk_blocks)
                        .context("catch-up retry last chunk index does not fit in usize")?;
                    if let Some(last_pushed_index) = last_pushed_index {
                        first_index = first_index.max(last_pushed_index + 1);
                    }
                    if first_index <= last_index {
                        push_chunk_indexes(
                            &mut chunks_to_run,
                            &covering,
                            segment_start,
                            segment_end,
                            chunk_blocks,
                            first_index,
                            last_index,
                        )?;
                        last_pushed_index = Some(last_index);
                    }
                }
            }
        }
    }

    let reused_completed_chunk_count = planned_chunk_count
        .checked_sub(chunks_to_run.len())
        .expect("selected retry chunks cannot exceed the complete plan");
    Ok(CatchupChunkPlan {
        chunks_to_run,
        skipped_future_target_count,
        planned_chunk_count,
        reused_completed_chunk_count,
    })
}

fn push_chunk_indexes(
    chunks: &mut Vec<CatchupChunk>,
    covering: &CatchupTargetWindow,
    segment_start: i64,
    segment_end: i64,
    chunk_blocks: i64,
    first_index: usize,
    last_index: usize,
) -> Result<()> {
    for index in first_index..=last_index {
        let index = i64::try_from(index).context("catch-up chunk index does not fit in i64")?;
        let chunk_start = segment_start
            .checked_add(
                index
                    .checked_mul(chunk_blocks)
                    .context("catch-up chunk offset overflow")?,
            )
            .context("catch-up chunk start overflow")?;
        let chunk_end = chunk_start
            .checked_add(chunk_blocks - 1)
            .unwrap_or(segment_end)
            .min(segment_end);
        chunks.push(CatchupChunk {
            range: BackfillBlockRange::new(chunk_start, chunk_end)?,
            covering: covering.clone(),
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlx::types::Uuid;

    fn target(id: u128, from_block: i64, to_block: Option<i64>) -> CatchupTarget {
        CatchupTarget {
            source_family: "ens_v2_registry_l1".to_owned(),
            contract_instance_id: Uuid::from_u128(id),
            address: format!("0x{id:040x}"),
            from_block,
            to_block,
        }
    }

    #[test]
    fn retry_plan_materializes_only_chunks_intersecting_changed_ranges() -> Result<()> {
        let previous_targets = vec![target(1, 1, None)];
        let targets = vec![target(1, 1, None), target(2, 40, Some(70))];
        let previous = super::super::CompletedCatchupPass::new(3, false, previous_targets);
        let required =
            super::super::retry_required_ranges(Some(&previous), 3, false, &targets, 100)?
                .expect("unchanged retention authority permits selective retry");

        let plan = plan_catchup_chunks_reusing_completed(&targets, 100, 10, Some(&required))?;
        let full_plan = plan_catchup_chunks_reusing_completed(&targets, 100, 10, None)?;

        assert_eq!(plan.planned_chunk_count, 11);
        assert_eq!(plan.reused_completed_chunk_count, 7);
        assert_eq!(
            plan.reused_completed_chunk_count + plan.chunks_to_run.len(),
            plan.planned_chunk_count,
            "the stable plan must be exactly partitioned into reused and retried chunks"
        );
        assert_eq!(
            plan.chunks_to_run
                .iter()
                .map(|chunk| chunk.range)
                .collect::<Vec<_>>(),
            vec![
                BackfillBlockRange::new(40, 49)?,
                BackfillBlockRange::new(50, 59)?,
                BackfillBlockRange::new(60, 69)?,
                BackfillBlockRange::new(70, 70)?,
            ]
        );
        for retry_chunk in &plan.chunks_to_run {
            let fresh_chunk = full_plan
                .chunks_to_run
                .iter()
                .find(|chunk| chunk.range == retry_chunk.range)
                .expect("every selective retry chunk must exist in a fresh stable plan");
            assert_eq!(
                retry_chunk.source_plan("ethereum-mainnet")?,
                fresh_chunk.source_plan("ethereum-mainnet")?,
                "selective retry must preserve the exact source identity for its range"
            );
        }
        Ok(())
    }
}
