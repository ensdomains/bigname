//! The rows one block reads and writes. Every family table is handled the same way: the block
//! loads the current rows of its derived keys, the reducers change them in memory, and the diff is
//! journalled and written back. A row travels as `to_jsonb(row)` and returns through
//! `jsonb_populate_recordset`, so a before-image restores the row byte for byte.
//!
//! A rebuild range folds several blocks into one working set before it writes. Each row then
//! also keeps its image before the block being folded, so the reducers see what they would see
//! block by block: `changes` is the current block's diff, and a reducer's read of a family table
//! goes through `overlay`, which replaces what the table says for a row an earlier block of the
//! range changed with the row as that block left it. With one block the two images are the same
//! and both are no-ops.
use std::collections::{BTreeMap, BTreeSet};

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
    /// The row before the transaction.
    before: Option<Row>,
    /// The row before the block being folded; `before` until a range ends a block.
    base: Option<Row>,
    after: Option<Row>,
}

/// The transaction's working set: every loaded row with its pre-transaction and pre-block
/// images.
#[derive(Debug, Default)]
pub(crate) struct RowSet {
    tables: BTreeMap<&'static str, BTreeMap<RowKey, Slot>>,
    /// Rows put or deleted in the block being folded.
    dirty: BTreeSet<(&'static str, RowKey)>,
    /// Rows an earlier block of the range left other than the table holds them.
    moved: BTreeMap<&'static str, BTreeSet<RowKey>>,
    /// The moved rows by the value of each `INDEXED` column of their pre-block image.
    moved_by: BTreeMap<(&'static str, &'static str), BTreeMap<String, BTreeSet<RowKey>>>,
}

/// The columns the reads made once per event find moved rows by (lease.rs: a grant's name and
/// a release's resource, a registry-only candidate's name), so each such read looks up its rows
/// rather than scanning every row the range moved.
const INDEXED: [(&str, &str); 3] = [
    ("project_binding_candidate", "logical_name_id"),
    ("project_lifecycle_event", "decoded_logical_name_id"),
    ("project_lifecycle_event", "state_key"),
];

/// One row changed: its table, key and earlier image (`None` when it did not exist), before the
/// block for `changes` and before the transaction for `written`.
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
            slot.base = Some(row.clone());
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
        self.dirty.insert((table.name, key));
        Ok(())
    }

    /// Remove the row at its key; the reducer has no remaining fact for it.
    pub(crate) fn delete(&mut self, table: &'static TableSpec, key: &Row) -> Result<()> {
        let key = key_text(table, key);
        self.loaded(table, &key)?.after = None;
        self.dirty.insert((table.name, key));
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

    /// Every row of `table` the block being folded changed, with its pre-block image, by key.
    pub(crate) fn changes_in(&self, table: &'static TableSpec) -> Vec<Change<'_>> {
        let Some(slots) = self.tables.get(table.name) else {
            return Vec::new();
        };
        self.dirty
            .range((table.name, RowKey::new())..)
            .take_while(|(name, _)| *name == table.name)
            .filter_map(|(_, key)| {
                let (key, slot) = slots.get_key_value(key)?;
                (slot.base != slot.after).then(|| Change {
                    table,
                    key,
                    before: slot.base.as_ref(),
                    after: slot.after.as_ref(),
                })
            })
            .collect()
    }

    /// Every row the transaction changed, with its pre-transaction image, by table then key: what
    /// it journals and writes.
    pub(crate) fn written(&self) -> Vec<Change<'_>> {
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

    /// End the block being folded: its rows become the next block's pre-block images.
    pub(crate) fn end_block(&mut self) {
        for (name, key) in std::mem::take(&mut self.dirty) {
            let Some(slot) = self
                .tables
                .get_mut(name)
                .and_then(|slots| slots.get_mut(&key))
            else {
                continue;
            };
            let moved = self.moved.entry(name).or_default();
            let indexed = INDEXED.iter().filter(|(table, _)| *table == name);
            if moved.contains(&key)
                && let Some(base) = &slot.base
            {
                for &(table, name) in indexed.clone() {
                    if let Some(keys) = column(base, name)
                        .and_then(|value| self.moved_by.get_mut(&(table, name))?.get_mut(value))
                    {
                        keys.remove(&key);
                    }
                }
            }
            slot.base.clone_from(&slot.after);
            if slot.base == slot.before {
                moved.remove(&key);
                continue;
            }
            if let Some(base) = &slot.base {
                for &(table, name) in indexed {
                    if let Some(value) = column(base, name) {
                        self.moved_by
                            .entry((table, name))
                            .or_default()
                            .entry(value.to_owned())
                            .or_default()
                            .insert(key.clone());
                    }
                }
            }
            moved.insert(key);
        }
    }

    /// What a read of `table` returns as the table would stand before the block being folded:
    /// `stored`, the rows the read found, with every row an earlier block of the range changed
    /// replaced by its pre-block image when `selected` (the read's own filter) keeps it, and
    /// dropped when it does not or the row is gone. Block by block, and in a range's first block,
    /// it returns `stored`.
    pub(crate) fn overlay(
        &self,
        table: &'static TableSpec,
        stored: Vec<Row>,
        selected: impl Fn(&Row) -> bool,
    ) -> Vec<Row> {
        self.overlay_keys(table, stored, None, selected)
    }

    /// `overlay` for a read that keeps only rows whose text `column` is one of `values`; an
    /// indexed column finds the moved rows without scanning them all.
    pub(crate) fn overlay_where(
        &self,
        table: &'static TableSpec,
        stored: Vec<Row>,
        (name, values): (&'static str, &[&str]),
        selected: impl Fn(&Row) -> bool,
    ) -> Vec<Row> {
        self.overlay_keys(table, stored, Some((name, values)), |row| {
            column(row, name).is_some_and(|value| values.contains(&value)) && selected(row)
        })
    }

    fn overlay_keys(
        &self,
        table: &'static TableSpec,
        stored: Vec<Row>,
        by: Option<(&'static str, &[&str])>,
        selected: impl Fn(&Row) -> bool,
    ) -> Vec<Row> {
        let (Some(moved), Some(slots)) = (self.moved.get(table.name), self.tables.get(table.name))
        else {
            return stored;
        };
        if moved.is_empty() {
            return stored;
        }
        let mut rows: Vec<Row> = stored
            .into_iter()
            .filter(|row| !moved.contains(&key_text(table, row)))
            .collect();
        let indexed = by.and_then(|(name, values)| {
            let index = self.moved_by.get(&(table.name, name))?;
            Some(
                values
                    .iter()
                    .filter_map(|value| index.get(*value))
                    .flatten()
                    .collect::<BTreeSet<_>>()
                    .into_iter(),
            )
        });
        let keys: Box<dyn Iterator<Item = &RowKey>> = match indexed {
            Some(keys) => Box::new(keys),
            None if by.is_some_and(|(name, _)| INDEXED.contains(&(table.name, name))) => {
                Box::new(std::iter::empty())
            }
            None => Box::new(moved.iter()),
        };
        rows.extend(
            keys.filter_map(|key| slots.get(key)?.base.as_ref())
                .filter(|row| selected(row))
                .cloned(),
        );
        rows
    }
}

/// The JSON objects of a read that returns `to_jsonb` rows.
pub(crate) fn objects(values: Vec<Value>) -> Vec<Row> {
    values
        .into_iter()
        .filter_map(|value| match value {
            Value::Object(row) => Some(row),
            _ => None,
        })
        .collect()
}

/// A text column of a row, `None` when absent or not a string.
pub(crate) fn column<'a>(row: &'a Row, column: &str) -> Option<&'a str> {
    row.get(column).and_then(Value::as_str)
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
