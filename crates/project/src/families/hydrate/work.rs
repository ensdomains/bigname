//! Derived hydration work indexes. Read dependent keys before replacing source rows, then
//! derive their entries from the final stored rows in the same publication/undo transaction.
use std::collections::BTreeMap;

use serde_json::{Value, json};
use sqlx::{Postgres, Transaction};

use super::super::{
    store::{Change, RowSet},
    tables,
};
use crate::{ProjectError, Result};

pub(crate) type Images = BTreeMap<&'static str, Vec<Value>>;

pub(crate) fn images(changes: &[Change<'_>]) -> Images {
    let mut images = Images::new();
    for change in changes {
        if !matches!(
            change.table.name,
            "project_node_record_value"
                | "project_node_record_partition"
                | "project_resolver_classification"
                | "project_reverse_tuple"
                | "project_registry_pointer"
                | "project_resource_pointer"
                | "project_reverse_node_claim"
                | "project_claim_normalization"
        ) {
            continue;
        }
        let entries = images.entry(change.table.name).or_default();
        entries.extend(
            [change.before, change.after]
                .into_iter()
                .flatten()
                .cloned()
                .map(Value::Object),
        );
        if change.after.is_none() {
            let before = change.before.expect("a deletion has its before-image");
            entries.push(Value::Object(
                change
                    .table
                    .key
                    .iter()
                    .map(|column| ((*column).to_owned(), before[*column].clone()))
                    .collect(),
            ));
        }
    }
    images
}

fn values(images: &Images, table: &'static tables::TableSpec) -> Value {
    json!(images.get(table.name).cloned().unwrap_or_default())
}

pub(super) async fn text_keys(
    transaction: &mut Transaction<'_, Postgres>,
    chain: &str,
    images: &Images,
) -> Result<Vec<Value>> {
    if [
        tables::NODE_RECORD_VALUE.name,
        tables::NODE_RECORD_PARTITION.name,
        tables::RESOLVER_CLASSIFICATION.name,
    ]
    .iter()
    .all(|name| !images.contains_key(name))
    {
        return Ok(vec![]);
    }
    sqlx::query_scalar(include_str!("text_keys.sql"))
        .bind(chain)
        .bind(values(images, &tables::NODE_RECORD_VALUE))
        .bind(values(images, &tables::NODE_RECORD_PARTITION))
        .bind(values(images, &tables::RESOLVER_CLASSIFICATION))
        .fetch_all(&mut **transaction)
        .await
        .map_err(|e| ProjectError::database("failed to find changed text hydration keys", e))
}

pub(super) async fn reverse_keys(
    transaction: &mut Transaction<'_, Postgres>,
    chain: &str,
    images: &Images,
) -> Result<Vec<Value>> {
    if [
        tables::REVERSE_TUPLE.name,
        tables::REGISTRY_POINTER.name,
        tables::RESOURCE_POINTER.name,
        tables::REVERSE_NODE_CLAIM.name,
        tables::CLAIM_NORMALIZATION.name,
    ]
    .iter()
    .all(|name| !images.contains_key(name))
    {
        return Ok(vec![]);
    }
    sqlx::query_scalar(include_str!("reverse_keys.sql"))
        .bind(chain)
        .bind(values(images, &tables::REVERSE_TUPLE))
        .bind(values(images, &tables::REGISTRY_POINTER))
        .bind(values(images, &tables::RESOURCE_POINTER))
        .bind(values(images, &tables::REVERSE_NODE_CLAIM))
        .bind(values(images, &tables::CLAIM_NORMALIZATION))
        .fetch_all(&mut **transaction)
        .await
        .map_err(|e| ProjectError::database("failed to find changed reverse hydration keys", e))
}

pub(crate) struct Targets {
    text: Vec<Value>,
    reverse: Vec<Value>,
}

impl Targets {
    pub(crate) async fn read(
        transaction: &mut Transaction<'_, Postgres>,
        chain: &str,
        images: &Images,
    ) -> Result<Self> {
        Ok(Self {
            text: if chain == super::ETHEREUM {
                text_keys(transaction, chain, images).await?
            } else {
                vec![]
            },
            // Reverse tuple/node keys are shared across chains. Any writer can displace or
            // restore the current mainnet row; only that row contributes hydration work.
            reverse: reverse_keys(transaction, super::ETHEREUM, images).await?,
        })
    }

    pub(crate) async fn refresh(
        self,
        transaction: &mut Transaction<'_, Postgres>,
        chain: &str,
        height: i64,
    ) -> Result<()> {
        super::text::refresh(transaction, chain, height, self.text).await?;
        super::reverse::refresh(transaction, super::ETHEREUM, height, self.reverse).await
    }
}

/// Replace just the affected entries, including removing source rows that undo deleted.
pub(super) async fn replace(
    transaction: &mut Transaction<'_, Postgres>,
    table: &'static str,
    key: &[&str],
    targets: Vec<Value>,
    work: Vec<Value>,
) -> Result<()> {
    let columns = key.join(", ");
    sqlx::query(&format!(
        "/* project:families.hydrate.work.delete */ DELETE FROM {table}
        WHERE ({columns}) IN (SELECT {columns} FROM jsonb_populate_recordset(NULL::{table}, $1))"
    ))
    .bind(json!(targets))
    .execute(&mut **transaction)
    .await
    .map_err(|e| ProjectError::database("failed to remove changed hydration work", e))?;
    if !work.is_empty() {
        sqlx::query(&format!(
            "/* project:families.hydrate.work.insert */ INSERT INTO {table}
            SELECT * FROM jsonb_populate_recordset(NULL::{table}, $1)"
        ))
        .bind(json!(work))
        .execute(&mut **transaction)
        .await
        .map_err(|e| ProjectError::database("failed to insert changed hydration work", e))?;
    }
    Ok(())
}

pub(super) fn changed_images(rows: &RowSet) -> Images {
    images(&rows.written())
}
