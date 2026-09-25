//! The canonical event order (TYR-36 D12): block number, transaction index, log index, then the
//! event identity compared as a byte string. An event with no transaction or log position (a
//! block-boundary event the interpreter synthesises) sorts before every transaction of its
//! block. Generated normalized event ids never take part.
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
    /// A position stored as one JSON object: the family rows' secondary positions and the
    /// `position` member of their jsonb maxima.
    pub fn from_json(value: &Value) -> Option<Self> {
        Some(Self {
            block_number: value.get("block_number")?.as_i64()?,
            transaction_index: value.get("transaction_index").and_then(Value::as_i64),
            log_index: value.get("log_index").and_then(Value::as_i64),
            event_identity: value
                .get("event_identity")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_owned(),
        })
    }

    /// The position a family row carries in its own four columns.
    pub fn of_row(row: &Value) -> Option<Self> {
        Self::from_json(row)
    }

    /// The three-part bound the authority admission compares against (authority_events.sql
    /// :200-217, :264-276): block, then transaction and log with a missing one read as -1. It
    /// agrees with the canonical order except that it ignores the identity, which only matters
    /// between two positions the bound treats as equal.
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
