//! Diff complete rows of a bounded key set. Only differences enter the existing undo journal.
use super::super::{
    block::{self, BlockStats},
    input::BlockHeader,
    store,
    tables::TableSpec,
};
use crate::{ProjectError, Result};
use bigname_storage::families::records::seams::{lookup_work_timer, note_lookup_work};
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
    let started = lookup_work_timer();
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
    let compared = all.len();
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
    note_lookup_work(|| {
        json!({"stage":"comparison", "table":table.name,
        "before_rows":before.len(), "after_rows":after.len(), "compared_keys":compared,
        "changed_keys":keys.len(), "elapsed_ms":started.map(|t| t.elapsed().as_secs_f64()*1000.0)})
    });
    if keys.is_empty() {
        return Ok(());
    }
    let started = lookup_work_timer();
    let journal_rows = block::insert_journal(transaction, chain, block, journal).await?;
    stats.undo_rows += journal_rows;
    let journal_ms = started.map(|t| t.elapsed().as_secs_f64() * 1000.0);
    let started = lookup_work_timer();
    let written = store::replace(transaction, table, keys, rows).await?;
    *stats.rows.entry(table.name).or_default() += written;
    note_lookup_work(|| {
        json!({"stage":"journal_and_write", "table":table.name,
        "journal_rows":journal_rows, "written_rows":written, "journal_ms":journal_ms,
        "write_ms":started.map(|t| t.elapsed().as_secs_f64()*1000.0)})
    });
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

/// Read only the old components that exact-key selection may replace or delete.
pub(super) async fn rows_for_record_keys(
    transaction: &mut Transaction<'_, Postgres>,
    chain: &str,
    keys: &[(uuid::Uuid, String)],
) -> Result<Vec<Value>> {
    let started = lookup_work_timer();
    let resources: Vec<_> = keys.iter().map(|(resource, _)| *resource).collect();
    let records: Vec<_> = keys.iter().map(|(_, key)| key.as_str()).collect();
    let rows: Vec<Value> = sqlx::query_scalar(
        "/* project:families.lookup.stored_record_keys */
        SELECT to_jsonb(stored) FROM project_lookup_record stored
        JOIN unnest($2::uuid[], $3::text[]) requested (resource_id, record_key)
          ON stored.resource_id=requested.resource_id AND stored.record_key=requested.record_key
        WHERE stored.chain_id=$1",
    )
    .bind(chain)
    .bind(&resources)
    .bind(&records)
    .fetch_all(&mut **transaction)
    .await
    .map_err(|e| ProjectError::database("failed to read stored lookup record keys", e))?;
    note_lookup_work(|| {
        json!({"stage":"old_record_keys", "requested_keys":keys.len(),
        "rows_loaded":rows.len(), "elapsed_ms":started.map(|t| t.elapsed().as_secs_f64()*1000.0)})
    });
    Ok(rows)
}
