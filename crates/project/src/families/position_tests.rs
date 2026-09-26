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

// The shared vectors. Their twin, the same three lists asserted against the storage crate's
// comparator and reader (`Ord` and `from_json`), is in
// crates/storage/src/families/control/position.rs; keep the two copies identical, since a drift
// between the comparators moves the shadow read and the canonical excuse read together.
type Place = (i64, Option<i64>, Option<i64>, &'static str);
/// A suffix of 131073 digits, one more than PostgreSQL's numeric type accepts before the
/// decimal point, and far past `u32::MAX`: no ordinal.
const LONG_SUFFIX: &str = match core::str::from_utf8(&LONG_SUFFIX_BYTES) {
    Ok(text) => text,
    Err(_) => panic!("ASCII"),
};
static LONG_SUFFIX_BYTES: [u8; 131075] = {
    let mut bytes = [b'1'; 131075];
    bytes[0] = b't';
    bytes[1] = b':';
    bytes
};
/// Positions in ascending canonical order: block, transaction, log (none first), the
/// emission ordinal when both indexes are present (none first), then identity bytes. It
/// covers synthesised facts, partially absent indexes, a missing or empty suffix, signed and
/// non-ASCII digits, a trailing newline, overflow (at `u32::MAX + 1` and at 131073 digits),
/// `u32::MAX`, leading zeros, 9 against 10 and an equal-ordinal identity tie.
const SHARED_ORDER: [Place; 28] = [
    // Synthesised facts: no ordinal, identity bytes, so ":10" before ":9".
    (5, None, None, "a:10"),
    (5, None, None, "a:9"),
    (5, None, None, "b"),
    // A log index without a transaction index, and the reverse: no ordinal.
    (5, None, Some(0), "a:1"),
    (5, Some(0), None, "a:3"),
    // Both indexes, no ordinal: empty, signed, newline-terminated, non-ASCII, missing,
    // negative, overflowing or non-numeric suffix; identity bytes.
    (5, Some(0), Some(0), "a:"),
    (5, Some(0), Some(0), "a:+1"),
    (5, Some(0), Some(0), "a:7\n"),
    (5, Some(0), Some(0), "a:holder"),
    (5, Some(0), Some(0), "a:\u{663}"),
    (5, Some(0), Some(0), "a:\u{ff13}"),
    (5, Some(0), Some(0), "abc"),
    (5, Some(0), Some(0), "q:-1"),
    (5, Some(0), Some(0), LONG_SUFFIX),
    (5, Some(0), Some(0), "t:4294967296"),
    (5, Some(0), Some(0), "z"),
    // Ordinals, numerically, then identity bytes on a tie.
    (5, Some(0), Some(0), "z:0"),
    (5, Some(0), Some(0), "y:1"),
    (5, Some(0), Some(0), "x:2"),
    (5, Some(0), Some(0), "r:7"),
    (5, Some(0), Some(0), "s:007"),
    (5, Some(0), Some(0), "w:9"),
    (5, Some(0), Some(0), "v:10"),
    (5, Some(0), Some(0), "u:4294967295"),
    // The log, then the transaction, then the block decide before any ordinal.
    (5, Some(0), Some(1), "a:0"),
    (5, Some(1), Some(0), "a:0"),
    (6, None, None, "a"),
    (6, Some(0), Some(0), "a:0"),
];
/// The partial positions: a log index without a transaction index, and the reverse. Each class
/// holds two identities, so the vector fails if either guard is dropped: neither reads an
/// ordinal, and identity bytes put ":10" before ":9".
const SHARED_PARTIAL: [(Place, Place); 2] = [
    ((5, None, Some(0), "a:10"), (5, None, Some(0), "a:9")),
    ((5, Some(0), None, "a:10"), (5, Some(0), None, "a:9")),
];
/// Stored JSON positions and what each reads as.
const SHARED_JSON: [(&str, Option<Place>); 8] = [
    (
        r#"{"block_number": 7, "transaction_index": null, "log_index": null, "event_identity": "e"}"#,
        Some((7, None, None, "e")),
    ),
    (
        r#"{"block_number": 7, "log_index": 3, "event_identity": "e:1"}"#,
        Some((7, None, Some(3), "e:1")),
    ),
    (
        r#"{"block_number": 7, "transaction_index": "1", "log_index": 2, "event_identity": "e"}"#,
        Some((7, None, Some(2), "e")),
    ),
    (
        r#"{"block_number": 7, "event_identity": ""}"#,
        Some((7, None, None, "")),
    ),
    (r#"{"block_number": 7}"#, None),
    (r#"{"block_number": 7, "event_identity": 5}"#, None),
    (r#"{"block_number": "7", "event_identity": "e"}"#, None),
    (r#"{"block_number": 7.5, "event_identity": "e"}"#, None),
];

fn place((block, transaction, log, identity): Place) -> Position {
    Position {
        block_number: block,
        transaction_index: transaction,
        log_index: log,
        event_identity: identity.to_owned(),
    }
}

#[test]
fn the_shared_order_vector_holds() {
    let positions: Vec<Position> = SHARED_ORDER.into_iter().map(place).collect();
    for (i, left) in positions.iter().enumerate() {
        for (j, right) in positions.iter().enumerate() {
            assert_eq!(left.cmp(right), i.cmp(&j), "{left:?} against {right:?}");
        }
    }
    let mut shuffled = positions.clone();
    shuffled.reverse();
    shuffled.sort();
    assert_eq!(shuffled, positions);
}

#[test]
fn the_shared_partial_vector_holds() {
    for (lower, higher) in SHARED_PARTIAL {
        let (lower, higher) = (place(lower), place(higher));
        assert_eq!(lower.emission_ordinal(), None, "{lower:?}");
        assert_eq!(higher.emission_ordinal(), None, "{higher:?}");
        assert!(lower < higher, "{lower:?} against {higher:?}");
    }
}

#[test]
fn the_shared_json_vector_holds() {
    for (text, expected) in SHARED_JSON {
        let value: Value = serde_json::from_str(text).expect("the vector is JSON");
        let row: &Map<String, Value> = value.as_object().expect("the vector holds objects");
        assert_eq!(Position::of_row(row), expected.map(place), "{text}");
    }
}
