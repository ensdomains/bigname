const LEVELS: [&str; 3] = ["quick_synced", "cross_checked", "node_checked"];
pub(super) fn meets_floor(level: Option<&str>) -> bool {
    level.is_some_and(|value| LEVELS.contains(&value))
}
#[test]
fn known_levels_meet_the_floor_and_unknowns_fail_closed() {
    let ready = meets_floor;
    assert!(LEVELS.into_iter().all(|level| ready(Some(level))) && !ready(Some("unknown")));
}
