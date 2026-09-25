//! The reducers of one block, in dependency order. Each family loads the rows its block keys name,
//! then folds the block's events into them in the canonical event order: a reducer reads the
//! key's current row and writes the row the event leaves. Clears stay rows; a row goes only when
//! its reducer has no remaining fact. A reducer that needs another family's row reads that
//! family's row for one of the block's own keys, never history.
use serde_json::{Map, Value, json};
use sqlx::{Postgres, Transaction};

use super::manifests::ActiveSet;
use super::{
    input::{BlockEvent, BlockHeader},
    keys::{BlockKeys, Key, Space},
    store::{Row, RowSet},
    tables::TableSpec,
};
use crate::{ProjectError, Result};

pub(crate) struct Context<'a> {
    pub(crate) chain_id: &'a str,
    pub(crate) block: &'a BlockHeader,
    pub(crate) keys: &'a BlockKeys,
    /// The active manifest set at the block, from the run's read of the manifest updates.
    pub(crate) manifests: &'a ActiveSet,
    /// Whether its key differs from the one the previous block recorded.
    pub(crate) manifests_changed: bool,
}

/// Name the family in a reducer error, so a skipped block says which family failed.
pub(crate) fn in_family(family: &str) -> impl Fn(ProjectError) -> ProjectError + '_ {
    move |error| {
        let kind = error.kind();
        let message = format!("family {family}: {error}");
        match kind {
            crate::ErrorKind::Transient => ProjectError::transient(message),
            crate::ErrorKind::DataIntegrity => ProjectError::data_integrity(message),
            crate::ErrorKind::Configuration => ProjectError::configuration(message),
        }
    }
}

pub(crate) async fn apply(
    transaction: &mut Transaction<'_, Postgres>,
    context: &Context<'_>,
    events: &[BlockEvent],
    rows: &mut RowSet,
) -> Result<()> {
    super::identity::apply(transaction, context, events, rows).await?;
    super::registry::apply(transaction, context, events, rows).await?;
    super::resolver::registry_pointers(transaction, context, events, rows).await?;
    super::resolver::resource_pointers(transaction, context, events, rows).await?;
    super::classification::apply(transaction, context, events, rows).await?;
    super::records::apply(transaction, context, events, rows).await?;
    super::wrapper::apply(transaction, context, events, rows).await?;
    super::permissions::apply(transaction, context, events, rows).await?;
    super::topology::apply(transaction, context, events, rows).await?;
    super::reverse::apply(transaction, context, events, rows).await?;
    super::addresses::apply(transaction, context, events, rows).await?;
    super::lifecycle::apply(transaction, context, events, rows).await?;
    Ok(())
}

/// A key object for `table`: its key columns paired with `values` in order.
pub(crate) fn key_of(table: &TableSpec, values: impl IntoIterator<Item = Value>) -> Row {
    table
        .key
        .iter()
        .map(|column| (*column).to_owned())
        .zip(values)
        .collect()
}

/// A key object for `table` from the chain and a block key's text parts.
pub(crate) fn chain_key(table: &TableSpec, chain_id: &str, key: &Key) -> Row {
    key_of(
        table,
        std::iter::once(json!(chain_id)).chain(key.iter().map(|part| json!(part))),
    )
}

/// Load the rows of `table` at every block key of `space`, keyed through `to_key`.
pub(crate) async fn load(
    transaction: &mut Transaction<'_, Postgres>,
    rows: &mut RowSet,
    table: &'static TableSpec,
    keys: &BlockKeys,
    space: Space,
    to_key: impl Fn(&Key) -> Row,
) -> Result<()> {
    rows.load(transaction, table, keys.of(space).map(to_key))
        .await
        .map_err(in_family(table.name))
}

/// Load the rows of `table` at `keys`.
pub(crate) async fn load_rows(
    transaction: &mut Transaction<'_, Postgres>,
    rows: &mut RowSet,
    table: &'static TableSpec,
    keys: Vec<Row>,
) -> Result<()> {
    rows.load(transaction, table, keys)
        .await
        .map_err(in_family(table.name))
}

/// The key's current in-block row, or a new row holding only its key.
pub(crate) fn current(rows: &RowSet, table: &'static TableSpec, key: &Row) -> Row {
    rows.get(table, key).cloned().unwrap_or_else(|| key.clone())
}

/// Stamp the row with the event that last wrote it and put it.
pub(crate) fn put(
    rows: &mut RowSet,
    table: &'static TableSpec,
    mut row: Row,
    event: &BlockEvent,
) -> Result<()> {
    event.write_position(&mut row);
    rows.put(table, row).map_err(in_family(table.name))
}

/// Set `column` to `value` on a row being built.
pub(crate) fn set(row: &mut Map<String, Value>, column: &str, value: impl Into<Value>) {
    row.insert(column.to_owned(), value.into());
}

/// A text value as JSON, null when absent.
pub(crate) fn text_or_null(value: Option<String>) -> Value {
    value.map_or(Value::Null, Value::String)
}

/// A field read the way `->>` reads it: strings as they are (blank included), numbers and
/// booleans as their text, `None` when absent or null.
pub(crate) fn raw_text(value: &Value, field: &str) -> Option<String> {
    match value.get(field)? {
        Value::Null => None,
        Value::String(text) => Some(text.clone()),
        other => Some(other.to_string()),
    }
}

/// `raw_text` lower-cased, as `lower(... ->> field)` reads an address.
pub(crate) fn raw_lower(value: &Value, field: &str) -> Option<String> {
    raw_text(value, field).map(|text| text.to_ascii_lowercase())
}

/// The namehash part of a logical name id (`<namespace>:<namehash>`), lower-cased.
pub(crate) fn namehash_of(logical_name_id: &str) -> Option<String> {
    logical_name_id
        .split_once(':')
        .map(|(_, namehash)| namehash.to_ascii_lowercase())
}

/// A JSON value as `(... ->> field)::boolean` reads it: a JSON boolean as itself, a string or a
/// number through PostgreSQL's boolean input, anything else (null included) as no boolean.
pub(crate) fn json_boolean(value: &Value) -> Option<bool> {
    match value {
        Value::Bool(flag) => Some(*flag),
        Value::String(text) => postgres_boolean(text),
        Value::Number(number) => postgres_boolean(&number.to_string()),
        _ => None,
    }
}

/// PostgreSQL's boolean input: trimmed of ASCII space, tab, line feed, carriage return, vertical
/// tab and form feed only (not Unicode whitespace), case-insensitive, `1` or `0`, or a unique
/// prefix of `true`, `false`, `yes`, `no`, `on` or `off` (`o` alone is ambiguous).
pub(crate) fn postgres_boolean(text: &str) -> Option<bool> {
    let text = text
        .trim_matches([' ', '\t', '\n', '\r', '\u{b}', '\u{c}'])
        .to_ascii_lowercase();
    if text.is_empty() {
        return None;
    }
    match text.as_str() {
        "1" => return Some(true),
        "0" => return Some(false),
        "o" => return None,
        _ => {}
    }
    [
        ("true", true),
        ("false", false),
        ("yes", true),
        ("no", false),
        ("on", true),
        ("off", false),
    ]
    .into_iter()
    .find_map(|(word, flag)| word.starts_with(&text).then_some(flag))
}

#[cfg(test)]
mod tests {
    use super::postgres_boolean;

    #[test]
    fn a_boolean_reads_as_postgresql_reads_it() {
        for (text, flag) in [
            ("t", Some(true)),
            ("TRUE", Some(true)),
            (" y ", Some(true)),
            ("on", Some(true)),
            ("1", Some(true)),
            ("f", Some(false)),
            ("fals", Some(false)),
            ("NO", Some(false)),
            ("of", Some(false)),
            ("0", Some(false)),
            ("o", None),
            ("", None),
            ("2", None),
            ("maybe", None),
            ("truex", None),
            ("\u{b}off\u{c}", Some(false)),
            ("\u{a0}off\u{a0}", None),
        ] {
            assert_eq!(postgres_boolean(text), flag, "{text:?}");
        }
    }
}
