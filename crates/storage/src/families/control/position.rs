//! The canonical event order (TYR-36 D12 as amended on 2026-09-26, the project crate's
//! families/position.rs): block number, transaction index, log index, then, when the event has
//! both a transaction and a log index, the emission ordinal its identity ends with, then the
//! event identity compared as a byte string. `None` sorts first at each step, so an event with
//! no transaction or log position (a block-boundary event the interpreter synthesises) sorts
//! before every transaction of its block. Generated normalized event ids never take part.
use std::{cmp::Ordering, collections::BTreeMap};

use serde_json::Value;

/// One event's place in the canonical order.
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct Position {
    pub block_number: i64,
    pub transaction_index: Option<i64>,
    pub log_index: Option<i64>,
    pub event_identity: String,
}

impl Ord for Position {
    fn cmp(&self, other: &Self) -> Ordering {
        // `None < Some`, so a synthesised event sorts first within its block.
        self.block_number
            .cmp(&other.block_number)
            .then(self.transaction_index.cmp(&other.transaction_index))
            .then(self.log_index.cmp(&other.log_index))
            .then_with(|| self.emission_ordinal().cmp(&other.emission_ordinal()))
            .then_with(|| {
                self.event_identity
                    .as_bytes()
                    .cmp(other.event_identity.as_bytes())
            })
    }
}

impl PartialOrd for Position {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Position {
    /// The emission ordinal, as the project crate reads it: the identity's final `:`-separated
    /// segment when the event has a transaction and a log index and that segment is a nonempty
    /// run of ASCII digits no greater than `u32::MAX`. The adapter's raw-log identities end with
    /// the fact's index in its log (adapters schema_v2/normalized.rs:118-131); boundary facts
    /// and family-internal identities have none.
    pub fn emission_ordinal(&self) -> Option<u32> {
        self.transaction_index?;
        self.log_index?;
        let (_, tail) = self.event_identity.rsplit_once(':')?;
        if tail.is_empty() || !tail.bytes().all(|byte| byte.is_ascii_digit()) {
            return None;
        }
        tail.parse().ok()
    }

    /// A position stored as one JSON object: the family rows' secondary positions and the
    /// `position` member of their jsonb maxima. Like the project crate's `Position::of_row`, it
    /// needs a numeric block and a string identity, and reads a missing or non-numeric
    /// transaction or log index as none.
    pub fn from_json(value: &Value) -> Option<Self> {
        Some(Self {
            block_number: value.get("block_number")?.as_i64()?,
            transaction_index: value.get("transaction_index").and_then(Value::as_i64),
            log_index: value.get("log_index").and_then(Value::as_i64),
            event_identity: value.get("event_identity")?.as_str()?.to_owned(),
        })
    }

    /// The position a family row carries in its own four columns.
    pub fn of_row(row: &Value) -> Option<Self> {
        Self::from_json(row)
    }

    /// The three-part bound the authority admission compares against (authority_events.sql
    /// :200-217, :264-276): block, then transaction and log with a missing one read as -1. For
    /// nonnegative transaction and log indexes, it agrees with the canonical order except that it
    /// ignores both the emission ordinal and the identity, which only matter between two
    /// positions the bound treats as equal; a negative index would sort after a missing one in
    /// the canonical order but not in the bound.
    pub fn bound(&self) -> (i64, i64, i64) {
        (
            self.block_number,
            self.transaction_index.unwrap_or(-1),
            self.log_index.unwrap_or(-1),
        )
    }
}

/// The order a read takes its "latest" in. The readers read in the canonical order. The
/// shadow harness reads the same facts again in the orders today's builders use, which break a
/// tie inside a block by the generated normalized event id, to show that a same-block difference
/// is only an ordering difference (brief section 4.3). The positions themselves never change,
/// so the admission's three-part bounds read the same in every order.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub enum EventOrder {
    #[default]
    Canonical,
    /// The generated normalized event id of each event identity.
    Generated(BTreeMap<String, i64>),
}

impl EventOrder {
    fn generated(&self, position: &Position) -> Option<i64> {
        match self {
            Self::Canonical => None,
            Self::Generated(ids) => ids.get(&position.event_identity).copied(),
        }
    }

    /// The order ENSv2 membership reads in. Today's builders take block, then generated id,
    /// with no transaction or log (build.sql:322-340, permissions.rs:111-133,
    /// v2_lifecycle_events.sql:19); an event without a generated id falls back to the
    /// canonical order.
    pub fn membership(&self, left: &Position, right: &Position) -> Ordering {
        match (self.generated(left), self.generated(right)) {
            (Some(left_id), Some(right_id)) => left
                .block_number
                .cmp(&right.block_number)
                .then(left_id.cmp(&right_id)),
            _ => left.cmp(right),
        }
    }

    /// The order the summary laterals read in. Today's builders take block, transaction and
    /// log, a missing one first, then the generated id (build.sql:307-308, :388-390).
    pub fn lateral(&self, left: &Position, right: &Position) -> Ordering {
        match (self.generated(left), self.generated(right)) {
            (Some(left_id), Some(right_id)) => left
                .block_number
                .cmp(&right.block_number)
                .then(left.transaction_index.cmp(&right.transaction_index))
                .then(left.log_index.cmp(&right.log_index))
                .then(left_id.cmp(&right_id)),
            _ => left.cmp(right),
        }
    }
}

/// A three-part bound from a JSON object with `block_number`, `transaction_index` and
/// `log_index`, missing parts read as -1.
pub fn bound_of(value: &Value) -> Option<(i64, i64, i64)> {
    let part = |name: &str| value.get(name).and_then(Value::as_i64);
    Some((
        part("block_number")?,
        part("transaction_index").unwrap_or(-1),
        part("log_index").unwrap_or(-1),
    ))
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    fn at(block: i64, place: Option<(i64, i64)>, identity: &str) -> Position {
        Position {
            block_number: block,
            transaction_index: place.map(|(transaction, _)| transaction),
            log_index: place.map(|(_, log)| log),
            event_identity: identity.to_owned(),
        }
    }

    #[test]
    fn a_synthesised_event_sorts_before_the_transactions_of_its_block() {
        assert!(at(5, None, "z") < at(5, Some((0, 0)), "a"));
        assert!(at(4, Some((9, 9)), "z") < at(5, None, "a"));
    }

    #[test]
    fn transaction_and_log_decide_before_the_identity() {
        // Two grants in one transaction with generated ids in the other order (design item 5):
        // log order decides.
        assert!(at(5, Some((2, 5)), "b") < at(5, Some((3, 1)), "a"));
        assert!(at(5, Some((2, 1)), "z") < at(5, Some((2, 2)), "a"));
    }

    #[test]
    fn facts_of_one_log_fold_in_emission_order() {
        // ":10" after ":9" when both are emission ordinals of one log, whatever the text before.
        assert!(
            at(5, Some((1, 1)), "x:holder:revoke:z:9") < at(5, Some((1, 1)), "x:holder:grant:a:10")
        );
        // No ordinal sorts first; a segment past u32::MAX or with a non-digit is no ordinal.
        assert!(at(5, Some((1, 1)), "z:holder") < at(5, Some((1, 1)), "a:0"));
        assert_eq!(at(5, Some((1, 1)), "a:4294967296").emission_ordinal(), None);
        assert_eq!(at(5, Some((1, 1)), "a:007").emission_ordinal(), Some(7));
        assert_eq!(at(5, Some((1, 1)), "a:-1").emission_ordinal(), None);
        assert_eq!(at(5, None, "a:7").emission_ordinal(), None);
    }

    #[test]
    fn synthesised_identities_compare_as_text_not_as_ordinals() {
        // ":10" sorts before ":9" (design, "Order among synthesised events").
        assert!(
            at(5, None, "x:RegistrationReleased:expiry:r:1:10")
                < at(5, None, "x:RegistrationReleased:expiry:r:1:9")
        );
        assert!(at(5, None, "x:RegistrationReleased:a") < at(5, None, "x:ResolverChanged:a"));
    }

    #[test]
    fn the_generated_orders_keep_positions_and_break_ties_by_id() {
        let grant = at(5, Some((0, 3)), "grant");
        let release = at(5, None, "release");
        let ids = EventOrder::Generated(
            [("grant".to_owned(), 1), ("release".to_owned(), 2)]
                .into_iter()
                .collect(),
        );
        // Membership ignores transaction and log; the laterals keep them.
        assert_eq!(ids.membership(&grant, &release), Ordering::Less);
        assert_eq!(ids.lateral(&grant, &release), Ordering::Greater);
        assert_eq!(
            EventOrder::Canonical.membership(&grant, &release),
            Ordering::Greater
        );
        let unknown = at(5, None, "unknown");
        assert_eq!(ids.membership(&grant, &unknown), grant.cmp(&unknown));
    }

    // The shared vectors. Their twin, the same three lists asserted against the project crate's
    // comparator and reader (`Ord` and `of_row`), is in
    // crates/project/src/families/position_tests.rs (step 2, PR 952). At step 2's bbb7d0ab the
    // two copies' literal lists and their order and partial tests are identical; keep them so,
    // since a drift between the comparators moves the shadow read and the canonical excuse read
    // together.
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
            assert_eq!(Position::from_json(&value), expected.map(place), "{text}");
        }
    }

    #[test]
    fn positions_read_from_json() {
        let position = Position::from_json(&json!({
            "block_number": 7, "transaction_index": null, "log_index": null, "event_identity": "e"
        }))
        .expect("a position");
        assert_eq!(position, at(7, None, "e"));
        assert_eq!(position.bound(), (7, -1, -1));
        assert_eq!(
            bound_of(&json!({"block_number": 7, "log_index": 3})),
            Some((7, -1, 3))
        );
    }
}
