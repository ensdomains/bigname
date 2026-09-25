use std::cmp::Ordering;

use serde_json::{Map, Value};

use super::Position;

fn at(tx: Option<i64>, log: Option<i64>, identity: &str) -> Position {
    Position {
        block_number: 12,
        transaction_index: tx,
        log_index: log,
        event_identity: identity.to_owned(),
    }
}

fn logged(identity: &str) -> Position {
    at(Some(0), Some(5), identity)
}

#[test]
fn the_ordinal_is_a_trailing_u32_of_a_logged_event() {
    for (identity, ordinal) in [
        ("a:b:0", Some(0)),
        ("a:b:7", Some(7)),
        ("a:b:007", Some(7)),
        ("a:b:4294967295", Some(u32::MAX)),
        ("a:b:4294967296", None),
        ("a:b:99999999999999999999", None),
        ("a:b:", None),
        ("a:b:x1", None),
        ("a:b:1x", None),
        ("a:b:+1", None),
        ("a:b:-1", None),
        ("a:b: 1", None),
        ("a:b:\u{0661}", None),
        ("123", None),
        ("binding:00000000-0000-0000-0000-000000000001", None),
    ] {
        assert_eq!(logged(identity).emission_ordinal(), ordinal, "{identity}");
    }
    for (tx, log) in [(None, None), (Some(0), None), (None, Some(5))] {
        assert_eq!(
            at(tx, log, "a:b:7").emission_ordinal(),
            None,
            "{tx:?} {log:?}"
        );
    }
}

#[test]
fn ordinals_compare_as_numbers_before_the_identity() {
    assert!(logged("z:x:9") < logged("a:x:10"));
    assert!(logged("a:revoke:1") < logged("a:grant:2"));
    assert!(
        logged("a:b:x1") < logged("a:b:0"),
        "no ordinal sorts before an ordinal"
    );
    assert!(
        logged("a:b:4294967296") < logged("a:b:0"),
        "an overflow has no ordinal"
    );
    assert!(
        logged("m7:x:1") < logged("m9:x:1"),
        "equal ordinals fall back to the bytes"
    );
    assert_eq!(logged("a:b:07").cmp(&logged("a:b:7")), Ordering::Less);
}

#[test]
fn block_transaction_and_log_still_come_first() {
    let later_log = at(Some(0), Some(6), "a:0");
    assert!(logged("a:99") < later_log);
    assert!(
        at(None, None, "z:99") < logged("a:0"),
        "a boundary fact precedes the block's logs"
    );
    let mut later_block = logged("a:0");
    later_block.block_number = 13;
    assert!(logged("a:99") < later_block);
}

#[test]
fn boundary_facts_keep_identity_order() {
    assert!(at(None, None, "p:10") < at(None, None, "p:9"));
}

#[test]
fn equality_is_consistent_with_the_order() {
    let positions = [
        logged("a:b:1"),
        logged("a:b:01"),
        logged("a:c:1"),
        at(None, None, "a:b:1"),
    ];
    for left in &positions {
        for right in &positions {
            assert_eq!(
                left.cmp(right) == Ordering::Equal,
                left == right,
                "{left:?} {right:?}"
            );
        }
    }
}

#[test]
fn stored_positions_order_as_they_did() {
    let mut positions = vec![
        logged("a:revoke:old:1"),
        logged("a:grant:new:2"),
        logged("a:x:10"),
        logged("a:x:9"),
        logged("a:x:nope"),
        at(None, None, "p:10"),
        at(None, None, "p:9"),
        at(Some(0), Some(4), "a:x:3"),
    ];
    positions.sort();
    let through_json: Vec<Position> = positions
        .iter()
        .map(|position| match position.to_json() {
            Value::Object(object) => Position::of_row(&object).expect("a stored position reads"),
            other => panic!("not an object: {other}"),
        })
        .collect();
    let through_columns: Vec<Position> = positions
        .iter()
        .map(|position| {
            let mut row = Map::new();
            position.write_columns(&mut row);
            Position::of_row(&row).expect("stored columns read")
        })
        .collect();
    for mut read in [through_json, through_columns] {
        assert_eq!(read, positions);
        read.reverse();
        read.sort();
        assert_eq!(read, positions);
    }
}
