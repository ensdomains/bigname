//! The shared order vectors against the shadow readers' comparator and reader
//! (`FamilyPosition`'s `Ord` and `from_json`).
use serde_json::Value;

use super::FamilyPosition;

// The shared vectors, a third literal copy. Its twins assert the same three lists against the
// project crate's comparator and reader, in crates/project/src/families/position_tests.rs (step
// 2, PR 952, bbb7d0ab), and against step 3's storage copy, in
// crates/storage/src/families/control/position.rs (PR 956). Keep the three copies identical,
// since a drift between the comparators moves the shadow reads away from the families they
// read. The three copies collapse into one fixture when the branches meet on main.
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

fn place((block, transaction, log, identity): Place) -> FamilyPosition {
    FamilyPosition {
        block_number: block,
        transaction_index: transaction,
        log_index: log,
        event_identity: identity.to_owned(),
    }
}

#[test]
fn the_shared_order_vector_holds() {
    let positions: Vec<FamilyPosition> = SHARED_ORDER.into_iter().map(place).collect();
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
        assert_eq!(
            FamilyPosition::from_json(&value),
            expected.map(place),
            "{text}"
        );
    }
}
