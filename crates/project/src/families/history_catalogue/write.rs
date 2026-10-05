//! Journal only changed chain-owned rows, preserving the first before-image of a generation.
use std::collections::BTreeMap;

use serde_json::{Value, json};
use sqlx::{Postgres, Transaction};

use super::super::{input::BlockHeader, store, tables::TableSpec};
use crate::{ProjectError, Result};

/// Diff rows already loaded on the locked chain transaction. The caller includes every row
/// in the affected key set, including obsolete rows absent from `fresh`.
pub(super) async fn replace(
    transaction: &mut Transaction<'_, Postgres>,
    chain: &str,
    block: &BlockHeader,
    table: &'static TableSpec,
    before: Vec<Value>,
    fresh: Vec<Value>,
) -> Result<(u64, u64)> {
    let keyed = |rows: Vec<Value>| -> Result<BTreeMap<String, Value>> {
        rows.into_iter()
            .map(|row| {
                let object = row.as_object().ok_or_else(|| {
                    ProjectError::data_integrity("history catalogue row is not an object")
                })?;
                Ok((store::key_text(table, object), row))
            })
            .collect()
    };
    let mut before = keyed(before)?;
    let mut fresh = keyed(fresh)?;
    let all: std::collections::BTreeSet<_> = before.keys().chain(fresh.keys()).cloned().collect();
    let (mut keys, mut rows, mut journal) = (Vec::new(), Vec::new(), Vec::new());
    for key in all {
        let prior = before.remove(&key);
        let next = fresh.remove(&key);
        if prior == next {
            continue;
        }
        journal.push(json!({"family": table.name, "key": key, "before_image": prior}));
        keys.push(Value::Object(store::key_object(table, &key)?));
        rows.extend(next);
    }
    if keys.is_empty() {
        return Ok((0, 0));
    }
    // Membership and its final source envelope can change the same anchor within one
    // publication. Keep the image from before the first change, never the intermediate row.
    let count = sqlx::query(
        "/* project:history.insert_journal */ INSERT INTO project_family_undo
           (chain_id,block_number,block_hash,family,key,before_image)
         SELECT $1,$2,$3,entry.family,entry.key,NULLIF(entry.before_image,'null'::jsonb)
         FROM jsonb_to_recordset($4) entry(family text,key text,before_image jsonb)
         ON CONFLICT (chain_id,block_number,family,key) DO NOTHING",
    )
    .bind(chain)
    .bind(block.number)
    .bind(&block.hash)
    .bind(Value::Array(journal))
    .execute(&mut **transaction)
    .await
    .map_err(|e| ProjectError::database("failed to journal history catalogue before-images", e))?
    .rows_affected();
    let written = store::replace(transaction, table, keys, rows).await?;
    Ok((written, count))
}
