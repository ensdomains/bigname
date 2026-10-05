//! The scheduling columns a read leaves on a hydrated row beside its observation. They ride on
//! the family's own rows, so publication journals them and undo restores them with the row.
use serde_json::{Value, json};

use super::super::{reduce::set, store::Row};

/// The size of aggregate an earlier read left the row with, if any.
pub(super) fn limit(row: &Row, column: &str) -> Option<usize> {
    row.get(column)
        .and_then(Value::as_u64)
        .and_then(|limit| usize::try_from(limit).ok())
}

/// One more read in a row that observed nothing for the row.
pub(super) fn count_failure(row: &mut Row, column: &'static str) {
    let failures = row.get(column).and_then(Value::as_i64).unwrap_or(0);
    set(
        row,
        column,
        json!(failures.saturating_add(1).min(i64::from(i32::MAX))),
    );
}
