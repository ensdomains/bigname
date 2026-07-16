use std::cmp::Ordering;

use anyhow::Result;

use crate::backfill::BackfillBlockRange;

use super::CatchupTarget;

pub(in crate::ops_catchup) struct CompletedCatchupPass {
    retention_generation: i64,
    included_historical_recovery_targets: bool,
    targets: Vec<CatchupTarget>,
}

impl CompletedCatchupPass {
    pub(in crate::ops_catchup) fn new(
        retention_generation: i64,
        included_historical_recovery_targets: bool,
        targets: Vec<CatchupTarget>,
    ) -> Self {
        Self {
            retention_generation,
            included_historical_recovery_targets,
            targets,
        }
    }
}

/// Return the ranges whose selected target set may differ from the preceding
/// completed pass. `None` means prior work cannot be reused (for example after
/// raw-log retention rotation); `Some([])` means the entire plan is reusable.
pub(in crate::ops_catchup) fn retry_required_ranges(
    previous: Option<&CompletedCatchupPass>,
    retention_generation: i64,
    includes_historical_recovery_targets: bool,
    targets: &[CatchupTarget],
    finalized_head_block_number: i64,
) -> Result<Option<Vec<BackfillBlockRange>>> {
    let Some(previous) = previous else {
        return Ok(None);
    };
    if previous.retention_generation != retention_generation
        || previous.included_historical_recovery_targets != includes_historical_recovery_targets
    {
        return Ok(None);
    }

    let mut changed_ranges = Vec::new();
    let mut previous_index = 0;
    let mut current_index = 0;
    while previous_index < previous.targets.len() || current_index < targets.len() {
        match (
            previous.targets.get(previous_index),
            targets.get(current_index),
        ) {
            (Some(previous_target), Some(current_target)) => {
                match previous_target.cmp(current_target) {
                    Ordering::Less => {
                        push_target_range(
                            &mut changed_ranges,
                            previous_target,
                            finalized_head_block_number,
                        )?;
                        previous_index += 1;
                    }
                    Ordering::Greater => {
                        push_target_range(
                            &mut changed_ranges,
                            current_target,
                            finalized_head_block_number,
                        )?;
                        current_index += 1;
                    }
                    Ordering::Equal => {
                        previous_index += 1;
                        current_index += 1;
                    }
                }
            }
            (Some(previous_target), None) => {
                push_target_range(
                    &mut changed_ranges,
                    previous_target,
                    finalized_head_block_number,
                )?;
                previous_index += 1;
            }
            (None, Some(current_target)) => {
                push_target_range(
                    &mut changed_ranges,
                    current_target,
                    finalized_head_block_number,
                )?;
                current_index += 1;
            }
            (None, None) => break,
        }
    }

    changed_ranges.sort_by_key(|range| (range.from_block, range.to_block));
    let mut merged = Vec::<BackfillBlockRange>::new();
    for range in changed_ranges {
        if let Some(last) = merged.last_mut()
            && last
                .to_block
                .checked_add(1)
                .is_some_and(|next| range.from_block <= next)
        {
            last.to_block = last.to_block.max(range.to_block);
        } else {
            merged.push(range);
        }
    }
    Ok(Some(merged))
}

fn push_target_range(
    ranges: &mut Vec<BackfillBlockRange>,
    target: &CatchupTarget,
    finalized_head_block_number: i64,
) -> Result<()> {
    let to_block = target
        .to_block
        .unwrap_or(finalized_head_block_number)
        .min(finalized_head_block_number);
    if target.from_block <= to_block {
        ranges.push(BackfillBlockRange::new(target.from_block, to_block)?);
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
    fn retry_ranges_cover_only_changed_target_intervals() -> Result<()> {
        let previous_targets = vec![target(1, 1, None)];
        let current_targets = vec![target(1, 1, None), target(2, 40, Some(70))];
        let previous = CompletedCatchupPass::new(3, false, previous_targets);

        assert_eq!(
            retry_required_ranges(Some(&previous), 3, false, &current_targets, 100)?,
            Some(vec![BackfillBlockRange::new(40, 70)?])
        );
        assert_eq!(
            retry_required_ranges(Some(&previous), 4, false, &current_targets, 100)?,
            None,
            "retention rotation must force current-generation work for the whole plan"
        );
        Ok(())
    }
}
