use std::collections::BTreeSet;

use anyhow::{Context, Result};
use bigname_manifests::WatchedContract;
use sqlx::types::Uuid;

use crate::backfill::BackfillBlockRange;

#[rustfmt::skip]
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(super) struct CatchupTarget { source_family: String, pub(super) contract_instance_id: Uuid, address: String, from_block: i64, to_block: Option<i64> }

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct CatchupChunk {
    pub(super) range: BackfillBlockRange,
    pub(super) targets: Vec<CatchupTarget>,
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

pub(super) fn plan_catchup_chunks(
    targets: &[CatchupTarget],
    finalized_head_block_number: i64,
    chunk_blocks: i64,
) -> Result<(Vec<CatchupChunk>, usize)> {
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
            .filter(|(_, range)| range.from_block <= segment_start && segment_end <= range.to_block)
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
            chunks.push(CatchupChunk {
                range: BackfillBlockRange::new(chunk_start, chunk_end)?,
                targets: targets.clone(),
            });
            chunk_start = chunk_end
                .checked_add(1)
                .context("catch-up chunk start overflow")?;
        }
    }

    Ok((chunks, skipped_future_target_count))
}
