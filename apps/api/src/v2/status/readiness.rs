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
#[test]
fn known_levels_meet_the_floor_and_unknowns_fail_closed() {
    for level in ["quick_synced", "cross_checked", "node_checked"] {
        assert!(meets_floor(Some(level), "quick_synced"));
    }
    assert!(!meets_floor(Some("unknown"), "quick_synced"));
    assert!(!meets_floor(Some("node_checked"), "unknown"));
    assert!(!meets_floor(Some("quick_synced"), "cross_checked"));
}
