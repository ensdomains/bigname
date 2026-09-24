//! The reducers of one block, in dependency order. Each family loads the rows its block keys name,
//! then folds the block's events into them in the canonical event order: a reducer reads the
//! key's current row and writes the row the event leaves. Clears stay rows; a row goes only when
//! its reducer has no remaining fact. A reducer that needs another family's row reads that
//! family's row for one of the block's own keys, never history.
use serde_json::{Map, Value, json};
use sqlx::{Postgres, Transaction};

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
    super::records::apply(transaction, context, events, rows).await?;
    super::wrapper::apply(transaction, context, events, rows).await?;
    super::permissions::apply(transaction, context, events, rows).await?;
    super::topology::apply(transaction, context, events, rows).await?;
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

/// A JSON value of the event's after state, `Value::Null` when absent.
pub(crate) fn after(event: &BlockEvent, field: &str) -> Value {
    event.after.get(field).cloned().unwrap_or(Value::Null)
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
