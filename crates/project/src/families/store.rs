//! The rows one block reads and writes. Every family table is handled the same way: the block
//! loads the current rows of its derived keys, the reducers change them in memory, and the diff is
//! journalled and written back. A row travels as `to_jsonb(row)` and returns through
//! `jsonb_populate_recordset`, so a before-image restores the row byte for byte.
use std::collections::BTreeMap;

use serde_json::{Map, Value};
use sqlx::{Postgres, Transaction};

use super::tables::{self, TableSpec};
use crate::{ProjectError, Result};

pub(crate) type Row = Map<String, Value>;

/// A primary key as the JSON array of its column values, in key column order. It is also the
/// journal's `key` text.
pub(crate) type RowKey = String;

#[derive(Debug, Default)]
struct Slot {
    before: Option<Row>,
    after: Option<Row>,
}

/// The block's working set: every loaded row with its pre-block image.
#[derive(Debug, Default)]
pub(crate) struct RowSet {
    tables: BTreeMap<&'static str, BTreeMap<RowKey, Slot>>,
}

/// One row the block changed: its table, key and pre-block image (`None` when it did not exist).
pub(crate) struct Change<'a> {
    pub(crate) table: &'static TableSpec,
    pub(crate) key: &'a str,
    pub(crate) before: Option<&'a Row>,
    pub(crate) after: Option<&'a Row>,
}

impl RowSet {
    /// Load the current rows of `keys`, a key object per requested row. Keys already loaded are
    /// skipped; a key with no row is remembered as absent.
    pub(crate) async fn load(
        &mut self,
        transaction: &mut Transaction<'_, Postgres>,
        table: &'static TableSpec,
        keys: impl IntoIterator<Item = Row>,
    ) -> Result<()> {
        let slots = self.tables.entry(table.name).or_default();
        let mut wanted = Vec::new();
        for key in keys {
            let text = key_text(table, &key);
            if let std::collections::btree_map::Entry::Vacant(entry) = slots.entry(text) {
                entry.insert(Slot::default());
                wanted.push(Value::Object(key));
            }
        }
        if wanted.is_empty() {
            return Ok(());
        }
        let columns = table.key.join(", ");
        let rows: Vec<Value> = sqlx::query_scalar(&format!(
            "/* project:families.store.load_{name} */ SELECT to_jsonb(family_row)
             FROM {name} family_row
             WHERE ({columns}) IN (
                 SELECT {columns} FROM jsonb_populate_recordset(NULL::{name}, $1)
             )",
            name = table.name,
        ))
        .bind(Value::Array(wanted))
        .fetch_all(&mut **transaction)
        .await
        .map_err(|error| {
            ProjectError::database(format!("failed to load {} rows", table.name), error)
        })?;
        for row in rows {
            let Value::Object(row) = row else { continue };
            let slot = slots.entry(key_text(table, &row)).or_default();
            slot.before = Some(row.clone());
            slot.after = Some(row);
        }
        Ok(())
    }

    /// The row's current in-block state.
    pub(crate) fn get(&self, table: &'static TableSpec, key: &Row) -> Option<&Row> {
        self.tables
            .get(table.name)?
            .get(&key_text(table, key))?
            .after
            .as_ref()
    }

    /// Replace the row at its key. The key must have been loaded first, so its before-image is
    /// known; writing an unloaded key is a reducer bug.
    pub(crate) fn put(&mut self, table: &'static TableSpec, row: Row) -> Result<()> {
        let key = key_text(table, &row);
        let slot = self.loaded(table, &key)?;
        slot.after = Some(row);
        Ok(())
    }

    /// Remove the row at its key; the reducer has no remaining fact for it.
    pub(crate) fn delete(&mut self, table: &'static TableSpec, key: &Row) -> Result<()> {
        let key = key_text(table, key);
        self.loaded(table, &key)?.after = None;
        Ok(())
    }

    fn loaded(&mut self, table: &'static TableSpec, key: &str) -> Result<&mut Slot> {
        self.tables
            .get_mut(table.name)
            .and_then(|slots| slots.get_mut(key))
            .ok_or_else(|| {
                ProjectError::data_integrity(format!(
                    "family reducer wrote {} key {key} without loading it",
                    table.name
                ))
            })
    }

    /// Every row whose state differs from its pre-block image, by table then key.
    pub(crate) fn changes(&self) -> Vec<Change<'_>> {
        let mut changes = Vec::new();
        for (name, slots) in &self.tables {
            let table = tables::spec(name);
            for (key, slot) in slots {
                if slot.before != slot.after {
                    changes.push(Change {
                        table,
                        key,
                        before: slot.before.as_ref(),
                        after: slot.after.as_ref(),
                    });
                }
            }
        }
        changes
    }
}

/// The journal key text of a row or key object.
pub(crate) fn key_text(table: &TableSpec, row: &Row) -> RowKey {
    Value::Array(
        table
            .key
            .iter()
            .map(|column| row.get(*column).cloned().unwrap_or(Value::Null))
            .collect(),
    )
    .to_string()
}

/// The key object a journal key text names.
pub(crate) fn key_object(table: &TableSpec, key: &str) -> Result<Row> {
    let values: Vec<Value> = serde_json::from_str(key).map_err(|error| {
        ProjectError::data_integrity(format!(
            "unreadable {} journal key {key}: {error}",
            table.name
        ))
    })?;
    if values.len() != table.key.len() {
        return Err(ProjectError::data_integrity(format!(
            "{} journal key {key} does not match its key columns",
            table.name
        )));
    }
    Ok(table
        .key
        .iter()
        .map(|column| (*column).to_owned())
        .zip(values)
        .collect())
}

/// Delete the rows at `keys` and insert `rows`, in one pass per table. Returns the number of
/// rows written or removed.
pub(crate) async fn replace(
    transaction: &mut Transaction<'_, Postgres>,
    table: &TableSpec,
    keys: Vec<Value>,
    rows: Vec<Value>,
) -> Result<u64> {
    let columns = table.key.join(", ");
    let mut touched = 0;
    if !keys.is_empty() {
        touched += sqlx::query(&format!(
            "/* project:families.store.delete_{name} */ DELETE FROM {name}
             WHERE ({columns}) IN (
                 SELECT {columns} FROM jsonb_populate_recordset(NULL::{name}, $1)
             )",
            name = table.name,
        ))
        .bind(Value::Array(keys))
        .execute(&mut **transaction)
        .await
        .map_err(|error| {
            ProjectError::database(format!("failed to delete {} rows", table.name), error)
        })?
        .rows_affected();
    }
    if !rows.is_empty() {
        let inserted = sqlx::query(&format!(
            "/* project:families.store.insert_{name} */ INSERT INTO {name}
             SELECT * FROM jsonb_populate_recordset(NULL::{name}, $1)",
            name = table.name,
        ))
        .bind(Value::Array(rows))
        .execute(&mut **transaction)
        .await
        .map_err(|error| {
            ProjectError::database(format!("failed to write {} rows", table.name), error)
        })?
        .rows_affected();
        touched = touched.max(inserted);
    }
    Ok(touched)
}
