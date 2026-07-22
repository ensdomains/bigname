use std::collections::BTreeSet;

use anyhow::{Context, Result};

use super::{
    CallRef, HydrationRow, RecordInventoryTextHydrationSummary, TextHydrationCall,
    TextHydrationOutcome, set_entry_not_found, set_entry_success,
    text_hydration_failure_is_skippable,
};

pub(super) fn apply_text_hydration_outcomes(
    rows: &mut [HydrationRow],
    calls_with_refs: &[(CallRef, TextHydrationCall)],
    outcomes: Vec<TextHydrationOutcome>,
    changed_rows: &mut BTreeSet<usize>,
    summary: &mut RecordInventoryTextHydrationSummary,
) -> Result<()> {
    for ((call_ref, _), outcome) in calls_with_refs.iter().zip(outcomes) {
        let row = rows
            .get_mut(call_ref.row_index)
            .context("text hydration row reference is out of bounds")?;
        let entries = row
            .entries
            .as_array_mut()
            .context("record_inventory_current.entries must be an array")?;
        let entry = entries
            .get_mut(call_ref.entry_index)
            .context("text hydration entry reference is out of bounds")?;

        match outcome {
            TextHydrationOutcome::Success(value) => {
                set_entry_success(entry, value);
                changed_rows.insert(call_ref.row_index);
                summary.hydrated_entry_count += 1;
            }
            TextHydrationOutcome::NotFound => {
                set_entry_not_found(entry);
                changed_rows.insert(call_ref.row_index);
                summary.not_found_entry_count += 1;
            }
            TextHydrationOutcome::Failed(message)
                if text_hydration_failure_is_skippable(&message) =>
            {
                summary.skipped_entry_count += 1;
            }
            TextHydrationOutcome::Failed(_) => {
                summary.failed_entry_count += 1;
            }
        }
    }
    Ok(())
}
