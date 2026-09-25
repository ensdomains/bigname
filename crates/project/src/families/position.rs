//! The canonical event order (D12 as amended by Tate on 2026-09-26; docs/projections.md, "Owned
//! key families"): block number, transaction index, log index, then, when the event has both a
//! transaction and a log index, the emission ordinal its identity ends with, then the event
//! identity compared as bytes. `None` sorts first at each step. Several facts of one log fold in
//! the order the adapter wrote them, since its raw-log identities end with the fact's index in
//! that log (adapters schema_v2/normalized.rs:118-131). Every family comparison of positions,
//! stored or read, goes through this one comparator.
use std::cmp::Ordering;

use serde_json::{Map, Value, json};

/// A position in the canonical event order. Equality is field equality; the order ends with the
/// full identity, so two positions compare equal exactly when they are equal.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct Position {
    pub(crate) block_number: i64,
    pub(crate) transaction_index: Option<i64>,
    pub(crate) log_index: Option<i64>,
    pub(crate) event_identity: String,
}

impl Position {
    /// The emission ordinal: the identity's final `:`-separated segment when the event has a
    /// transaction and a log index and that segment is a nonempty run of ASCII digits no greater
    /// than `u32::MAX` (leading zeros allowed). Boundary facts, which have neither index, have
    /// none: their trailing number counts earlier same-prefix events of the batch
    /// (adapters normalized.rs:141-144) and is not an emission index. Family-internal identities
    /// (`binding:<uuid>`, `activation:<block>`) have none either.
    pub(crate) fn emission_ordinal(&self) -> Option<u32> {
        self.transaction_index?;
        self.log_index?;
        let (_, tail) = self.event_identity.rsplit_once(':')?;
        if tail.is_empty() || !tail.bytes().all(|byte| byte.is_ascii_digit()) {
            return None;
        }
        tail.parse().ok()
    }

    /// The four position columns every family row carries for its last owning event.
    pub(crate) fn write_columns(&self, row: &mut Map<String, Value>) {
        row.insert("block_number".into(), json!(self.block_number));
        row.insert("transaction_index".into(), json!(self.transaction_index));
        row.insert("log_index".into(), json!(self.log_index));
        row.insert("event_identity".into(), json!(self.event_identity));
    }

    /// A secondary position stored as one JSON object beside the row's own position.
    pub(crate) fn to_json(&self) -> Value {
        json!({
            "block_number": self.block_number,
            "transaction_index": self.transaction_index,
            "log_index": self.log_index,
            "event_identity": self.event_identity,
        })
    }

    /// The row's own position, or a stored JSON position, when it has one. The ordinal is not
    /// stored: it is read back from the identity, so a stored position orders as it did.
    pub(crate) fn of_row(row: &Map<String, Value>) -> Option<Self> {
        Some(Self {
            block_number: row.get("block_number")?.as_i64()?,
            transaction_index: row.get("transaction_index").and_then(Value::as_i64),
            log_index: row.get("log_index").and_then(Value::as_i64),
            event_identity: row.get("event_identity")?.as_str()?.to_owned(),
        })
    }
}

impl Ord for Position {
    fn cmp(&self, other: &Self) -> Ordering {
        self.block_number
            .cmp(&other.block_number)
            .then_with(|| self.transaction_index.cmp(&other.transaction_index))
            .then_with(|| self.log_index.cmp(&other.log_index))
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

#[cfg(test)]
#[path = "position_tests.rs"]
mod tests;
