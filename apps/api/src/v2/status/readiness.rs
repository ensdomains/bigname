const fn level_rank(level: &str) -> Option<u8> {
    match level.as_bytes() {
        b"quick_synced" => Some(0),
        b"cross_checked" => Some(1),
        b"node_checked" => Some(2),
        _ => None,
    }
}
pub(super) fn meets_floor(level: Option<&str>, floor: &str) -> bool {
    level_rank(floor)
        .zip(level.and_then(level_rank))
        .is_some_and(|(floor_rank, level_rank)| level_rank >= floor_rank)
}
/// The block and time lag of the indexed position behind the stored head, each clamped at 0.
/// An Interpret or Project redo holds both the stored head and the indexed position still, so
/// their difference would read 0 while the chain moves on: both lags are unknown for the redo's
/// duration, with either publication source (the redo already makes the chain `degraded`).
pub(super) fn projection_lags(
    row: &bigname_storage::IndexingStatusChainRow,
) -> (Option<i64>, Option<i64>) {
    if row.interpret_redo_in_progress || row.project_redo_in_progress {
        return (None, None);
    }
    let blocks = row
        .canonical_block
        .zip(row.latest_projected_block)
        .map(|(canonical, projected)| canonical.saturating_sub(projected).max(0));
    let seconds = row
        .canonical_timestamp
        .zip(row.latest_projected_timestamp)
        .map(|(canonical, projected)| (canonical - projected).whole_seconds().max(0));
    (blocks, seconds)
}
#[test]
fn known_levels_meet_the_floor_and_unknowns_fail_closed() {
    for level in ["quick_synced", "cross_checked", "node_checked"] {
        assert!(meets_floor(Some(level), "quick_synced"));
    }
    assert!(!meets_floor(Some("unknown"), "quick_synced"));
    assert!(!meets_floor(Some("node_checked"), "unknown"));
    assert!(!meets_floor(Some("quick_synced"), "cross_checked"));
}
