//! Diff complete rows of a bounded key set. Only differences enter the existing undo journal.
use super::super::{
    block::{self, BlockStats},
    input::BlockHeader,
    store,
    tables::TableSpec,
};
use crate::{ProjectError, Result};
use serde_json::{Value, json};
use sqlx::{Postgres, Transaction};
use std::collections::{BTreeMap, BTreeSet};

pub(super) async fn changed(
    transaction: &mut Transaction<'_, Postgres>,
    chain: &str,
    block: &BlockHeader,
    table: &'static TableSpec,
    before: Vec<Value>,
    after: Vec<Value>,
    stats: &mut BlockStats,
) -> Result<()> {
    let indexed = |rows: Vec<Value>| -> Result<BTreeMap<String, Value>> {
        rows.into_iter()
            .map(|row| {
                let object = row
                    .as_object()
                    .ok_or_else(|| ProjectError::data_integrity("lookup row is not an object"))?;
                Ok((store::key_text(table, object), row))
            })
            .collect()
    };
    let before = indexed(before)?;
    let after = indexed(after)?;
    let all: BTreeSet<_> = before.keys().chain(after.keys()).collect();
    let mut keys = Vec::new();
    let mut rows = Vec::new();
    let mut journal = Vec::new();
    for key in all {
        if before.get(key) == after.get(key) {
            continue;
        }
        keys.push(Value::Object(store::key_object(table, key)?));
        rows.extend(after.get(key).cloned());
        journal.push(json!({"family":table.name, "key":key, "before_image":before.get(key)}));
    }
    if keys.is_empty() {
        return Ok(());
    }
    stats.undo_rows += block::insert_journal(transaction, chain, block, journal).await?;
    let written = store::replace(transaction, table, keys, rows).await?;
    *stats.rows.entry(table.name).or_default() += written;
    Ok(())
}

pub(super) async fn rows_for_names(
    transaction: &mut Transaction<'_, Postgres>,
    chain: &str,
    table: &'static TableSpec,
    names: &[String],
) -> Result<Vec<Value>> {
    sqlx::query_scalar(&format!("/* project:families.lookup.stored_names */ SELECT to_jsonb(stored) FROM {} stored WHERE chain_id=$1 AND logical_name_id=ANY($2)", table.name))
        .bind(chain).bind(names).fetch_all(&mut **transaction).await
        .map_err(|e| ProjectError::database("failed to read stored lookup names", e))
}

pub(super) async fn rows_for_resources(
    transaction: &mut Transaction<'_, Postgres>,
    chain: &str,
    table: &'static TableSpec,
    resources: &[uuid::Uuid],
) -> Result<Vec<Value>> {
    sqlx::query_scalar(&format!("/* project:families.lookup.stored_resources */ SELECT to_jsonb(stored) FROM {} stored WHERE chain_id=$1 AND resource_id=ANY($2)", table.name))
        .bind(chain).bind(resources).fetch_all(&mut **transaction).await
        .map_err(|e| ProjectError::database("failed to read stored lookup resources", e))
}
