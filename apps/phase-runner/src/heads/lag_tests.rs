use super::{readable_hashes, select};
use crate::heads::{BlockMarker, HeadMarkers};

fn marker(number: i64) -> BlockMarker {
    BlockMarker {
        number,
        hash: format!("0x{number:x}"),
    }
}

/// The path from `newest` down to `floor`, newest first.
fn path(newest: i64, floor: i64) -> Vec<(i64, String)> {
    (floor..=newest)
        .rev()
        .map(|number| (number, marker(number).hash))
        .collect()
}

fn observed(newest: i64, safe: Option<i64>, finalized: Option<i64>) -> HeadMarkers {
    HeadMarkers {
        latest: marker(newest),
        safe: safe.map(marker),
        finalized: finalized.map(marker),
    }
}

#[test]
fn no_lag_publishes_the_newest_block_with_the_engine_checkpoints() {
    let observed = observed(6, Some(2), Some(1));
    assert_eq!(
        select("chain", &observed, &path(6, 0), 0, Some(3)).unwrap(),
        observed
    );
}

#[test]
fn the_readable_head_stands_the_lag_below_the_newest_block() {
    let heads = select(
        "chain",
        &observed(6, Some(0), Some(0)),
        &path(6, 0),
        2,
        Some(3),
    )
    .unwrap();
    assert_eq!(heads, observed(4, Some(0), Some(0)));
}

#[test]
fn the_readable_head_never_falls_below_a_block_already_readable_on_the_path() {
    let heads = select("chain", &observed(4, None, None), &path(4, 0), 2, Some(3)).unwrap();
    assert_eq!(heads.latest, marker(3));
}

#[test]
fn fewer_blocks_than_the_lag_clamp_at_the_path_floor_without_underflow() {
    let heads = select("chain", &observed(1, None, None), &path(1, 0), 3, None).unwrap();
    assert_eq!(heads.latest, marker(0));
    let heads = select(
        "chain",
        &observed(0, None, None),
        &path(0, 0),
        u32::MAX,
        None,
    )
    .unwrap();
    assert_eq!(heads.latest, marker(0));
}

#[test]
fn a_deeper_reorg_lands_on_the_lag_or_the_common_ancestor_whichever_is_higher() {
    // The engine resumed from the common ancestor, the highest readable block on the new path.
    let heads = select("chain", &observed(6, None, None), &path(6, 0), 2, Some(3)).unwrap();
    assert_eq!(heads.latest, marker(4));
    let heads = select("chain", &observed(6, None, None), &path(6, 0), 2, Some(5)).unwrap();
    assert_eq!(heads.latest, marker(5));
}

#[test]
fn checkpoints_inside_the_window_are_bounded_to_the_readable_head() {
    let heads = select(
        "chain",
        &observed(6, Some(6), Some(5)),
        &path(6, 0),
        2,
        Some(3),
    )
    .unwrap();
    assert_eq!(heads, observed(4, Some(4), Some(4)));
    let heads = select(
        "chain",
        &observed(6, Some(3), Some(1)),
        &path(6, 0),
        2,
        Some(3),
    )
    .unwrap();
    assert_eq!(heads, observed(4, Some(3), Some(1)));
}

#[test]
fn a_readable_head_missing_from_the_path_is_refused() {
    let gapped = vec![(6, marker(6).hash), (3, marker(3).hash)];
    assert!(select("chain", &observed(6, None, None), &gapped, 2, None).is_err());
}

#[test]
fn only_path_blocks_at_or_below_the_readable_head_become_readable() {
    let path = path(6, 3);
    assert_eq!(readable_hashes(&path, 4), vec!["0x4", "0x3"]);
    assert_eq!(readable_hashes(&path, 6).len(), 4);
}
