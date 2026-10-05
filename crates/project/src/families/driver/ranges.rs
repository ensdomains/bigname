//! The rebuild ranges of a population (range.rs): where the switch point stands for the run, and
//! one range with its share of the budget.
use super::{Budget, Run};
use crate::{
    ProjectError, Result,
    families::{
        FamilyOutcome, RANGE_SAFE_MARGIN, RANGE_TARGET_MARGIN, RebuildRanges, block,
        marker::FamilyMarker, range, reduce,
    },
};

impl Budget {
    /// Spend the blocks of a range beyond the one `take` already spent.
    fn spend(&mut self, blocks: u64) {
        self.left = self.left.saturating_sub(blocks);
    }
}

impl Run<'_> {
    /// The highest work block this run's rebuild applies in a range, read once per run; `None`
    /// when ranges are off. By default it is the chain's safe block minus `RANGE_SAFE_MARGIN`, so
    /// the blocks nearest the head stay one to a transaction, or with no safe block the target
    /// minus `RANGE_TARGET_MARGIN`. The safe block is not final; a reorg whose fork point lies
    /// inside a range undoes the whole range.
    pub(super) async fn switch_point(&self) -> Result<Option<i64>> {
        match self.options.rebuild_ranges {
            RebuildRanges::Off => Ok(None),
            RebuildRanges::Through(block) => Ok(Some(block)),
            RebuildRanges::BelowSafe => {
                let safe: Option<Option<i64>> = sqlx::query_scalar(
                    "/* project:families.range.switch_point */ SELECT safe_block_number
                     FROM chain_heads WHERE chain_id = $1",
                )
                .bind(self.chain_id)
                .fetch_optional(self.pool)
                .await
                .map_err(|error| {
                    ProjectError::database("failed to read the chain's safe block", error)
                })?;
                Ok(Some(match safe.flatten() {
                    Some(safe) => safe - RANGE_SAFE_MARGIN,
                    None => self.target.number - RANGE_TARGET_MARGIN,
                }))
            }
        }
    }

    /// Apply `blocks`, or their first `size`, as one range within the budget, after `take` spent
    /// the range's first block. Returns the marker the range published and how many blocks it
    /// applied, which the event cap can make fewer than planned.
    pub(super) async fn range(
        &mut self,
        blocks: &[i64],
        size: usize,
        plan: &block::Plan<'_>,
        outcome: &mut FamilyOutcome,
    ) -> Result<(FamilyMarker, usize)> {
        let budget = usize::try_from(self.budget.left.saturating_add(1)).unwrap_or(usize::MAX);
        let planned = &blocks[..blocks.len().min(size).min(budget)];
        let (first, last) = (planned[0], planned[planned.len() - 1]);
        let (next, applied, stats) =
            range::apply(self.pool, self.chain_id, planned, plan, self.options)
                .await
                .map_err(reduce::in_family(&format!("range {first} to {last}")))?;
        tracing::debug!(
            target: "bigname_project::families",
            chain_id = self.chain_id,
            first,
            last = planned[applied - 1],
            blocks = applied,
            elapsed_ms = stats.elapsed_ms,
            rows = ?stats.rows,
            undo_rows = stats.undo_rows,
            "applied a family rebuild range"
        );
        self.budget
            .spend(u64::try_from(applied - 1).unwrap_or(u64::MAX));
        outcome.record_range(stats, u64::try_from(applied).unwrap_or(u64::MAX));
        Ok((next, applied))
    }
}
