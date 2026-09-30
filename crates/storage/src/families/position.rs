//! The canonical event order (docs/glossary.md#canonical-event-order): block number,
//! transaction index, log index, then, when the event has both a transaction and a log index, the emission ordinal its identity ends with, then the
//! event identity compared as a byte string. `None` sorts first at each step, so an event with
//! no transaction or log position (a block-boundary event the interpreter synthesises) sorts
//! before every transaction of its block. Generated normalized event ids never take part.
use std::cmp::Ordering;

use serde_json::{Map, Value, json};

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
    /// The emission ordinal of this position (docs/glossary.md#emission-ordinal).
    pub fn emission_ordinal(&self) -> Option<u32> {
        emission_ordinal(self.transaction_index, self.log_index, &self.event_identity)
    }

    /// A secondary position stored as one JSON object. Missing or non-numeric transaction
    /// and log indexes read as none; a numeric block and string identity are required.
    pub fn from_json(value: &Value) -> Option<Self> {
        Self::from_map(value.as_object()?)
    }

    /// The position a family row carries in its own four columns.
    pub fn of_row(row: &Value) -> Option<Self> {
        Self::from_json(row)
    }

    /// The four position columns every family row carries for its last owning event.
    pub fn write_columns(&self, row: &mut Map<String, Value>) {
        row.insert("block_number".into(), json!(self.block_number));
        row.insert("transaction_index".into(), json!(self.transaction_index));
        row.insert("log_index".into(), json!(self.log_index));
        row.insert("event_identity".into(), json!(self.event_identity));
    }

    /// A secondary position stored as one JSON object beside the row's own position.
    pub fn to_json(&self) -> Value {
        json!({
            "block_number": self.block_number,
            "transaction_index": self.transaction_index,
            "log_index": self.log_index,
            "event_identity": self.event_identity,
        })
    }

    /// The row's own position, or a stored JSON position, when it has one. The ordinal is not
    /// stored: it is read back from the identity, so a stored position orders as it did.
    pub fn from_map(row: &Map<String, Value>) -> Option<Self> {
        Some(Self {
            block_number: row.get("block_number")?.as_i64()?,
            transaction_index: row.get("transaction_index").and_then(Value::as_i64),
            log_index: row.get("log_index").and_then(Value::as_i64),
            event_identity: row.get("event_identity")?.as_str()?.to_owned(),
        })
    }
}

/// The emission ordinal (docs/glossary.md#emission-ordinal): the final `:`-separated segment
/// when both indexes exist and the segment is nonempty ASCII digits at most `u32::MAX`.
/// Leading zeros are allowed. Without both indexes, the suffix is not an emission ordinal.
pub fn emission_ordinal(
    transaction_index: Option<i64>,
    log_index: Option<i64>,
    event_identity: &str,
) -> Option<u32> {
    transaction_index?;
    log_index?;
    let (_, tail) = event_identity.rsplit_once(':')?;
    if tail.is_empty() || !tail.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    tail.parse().ok()
}

/// The emission ordinal (docs/glossary.md#emission-ordinal) of an event as a bigint, -1 when it
/// has none: the identity's final `:`-separated segment when the event has both a transaction and
/// a log index and that segment is a nonempty run of ASCII digits no greater than 4294967295,
/// leading zeros allowed. This is the glossary's checked SQL form, which
/// crates/project/tests/families_ordinal_sql.rs checks against the Rust parse in
/// this module: strip leading zeros, check the significant length
/// against the ten-digit bound, and only then cast, so no suffix errors where the Rust parse
/// yields none. Absent is -1 rather than null so the row value
/// stays decisive; every valid ordinal is at least 0, so -1 sorts first as `NULLS FIRST` does.
/// Expressions must come from trusted query source; values belong in query parameters.
pub fn emission_ordinal_sql(identity: &str, transaction: &str, log: &str) -> String {
    format!(
        "COALESCE(CASE WHEN {transaction} IS NOT NULL AND {log} IS NOT NULL THEN (
            SELECT CASE WHEN digits.d = '' THEN 0::bigint
                        WHEN length(digits.d) < 10
                          OR (length(digits.d) = 10
                              AND digits.d COLLATE \"C\" <= '4294967295' COLLATE \"C\")
                            THEN digits.d::bigint END
            FROM (SELECT ltrim(m[1], '0') AS d
                  FROM regexp_match(({identity}) COLLATE \"C\", ':([0-9]+)$') m) digits
        ) END, -1::bigint)"
    )
}
