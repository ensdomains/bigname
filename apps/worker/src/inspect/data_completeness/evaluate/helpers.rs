use super::report::{ChainFrontier, CursorLag};
use bigname_storage::{ChainCompletenessRow, ReplayCursorRow};

/// Replay is complete for a cursor's target when `next_block_number > target_block_number`.
/// A reorg rewind lowers `next_block_number` below the target, so a stale high
/// `last_completed_block_number` no longer reads as caught up. A missing bound fails closed.
pub(super) fn replay_complete_lag(cursor: &ReplayCursorRow) -> Option<CursorLag> {
    match (cursor.next_block_number, cursor.target_block_number) {
        (Some(next), Some(target)) if next > target => None,
        (next, Some(target)) => Some(CursorLag {
            label: cursor_label(cursor),
            behind_by: target - next.unwrap_or(-1),
        }),
        (_, None) => Some(CursorLag {
            label: cursor_label(cursor),
            behind_by: -1,
        }),
    }
}

pub(super) fn cursor_label(cursor: &ReplayCursorRow) -> String {
    format!(
        "{}/{}/{}",
        cursor.deployment_profile, cursor.chain_id, cursor.cursor_kind
    )
}

pub(super) fn missing_chain_frontier(chain_id: &str) -> ChainFrontier {
    ChainFrontier {
        chain_id: chain_id.to_owned(),
        canonical_block_number: None,
        checkpoint_canonical_lineage_match: false,
        lineage_head_block_number: None,
        head_lag_blocks: None,
        contiguous: false,
        missing_block_count: 0,
        duplicate_canonical_height_count: 0,
        disconnected_canonical_parent_count: 0,
        missing_from_storage: true,
    }
}

pub(super) fn chain_frontier(chain: &ChainCompletenessRow) -> ChainFrontier {
    let head_lag_blocks = chain
        .canonical_block_number
        .zip(chain.lineage_head_block_number)
        .map(|(canonical, lineage_head)| canonical - lineage_head);

    let expected_block_count = chain
        .lineage_head_block_number
        .zip(chain.lineage_floor_block_number)
        .map(|(head, floor)| head - floor + 1);
    let missing_block_count = expected_block_count
        .map(|expected| expected - chain.lineage_canonical_block_count)
        .unwrap_or_default();

    ChainFrontier {
        chain_id: chain.chain_id.clone(),
        canonical_block_number: chain.canonical_block_number,
        checkpoint_canonical_lineage_match: chain.checkpoint_canonical_lineage_match,
        lineage_head_block_number: chain.lineage_head_block_number,
        head_lag_blocks,
        contiguous: expected_block_count.is_some()
            && missing_block_count == 0
            && chain.duplicate_canonical_height_count == 0
            && chain.disconnected_canonical_parent_count == 0,
        missing_block_count,
        duplicate_canonical_height_count: chain.duplicate_canonical_height_count,
        disconnected_canonical_parent_count: chain.disconnected_canonical_parent_count,
        missing_from_storage: false,
    }
}
