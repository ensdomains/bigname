use std::collections::{BTreeMap, BTreeSet};

use anyhow::{Context, Result};
use serde_json::Value;
use sqlx::{PgPool, Row};
use uuid::Uuid;

pub type PhaseGraphqlRecordInventoryKey = (Uuid, Option<Value>);

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PhaseGraphqlRecordInventoryRow {
    pub selectors: Value,
    pub entries: Value,
    pub chain_positions: Value,
}

pub async fn load_phase_graphql_record_inventory_batch(
    pool: &PgPool,
    keys: &[PhaseGraphqlRecordInventoryKey],
) -> Result<Vec<Option<PhaseGraphqlRecordInventoryRow>>> {
    let resource_ids = keys
        .iter()
        .map(|(resource_id, _)| *resource_id)
        .collect::<BTreeSet<_>>();
    if resource_ids.is_empty() {
        return Ok(Vec::new());
    }

    let rows = sqlx::query(
        r#"
        SELECT resource_id, record_version_boundary, selectors, entries,
               chain_positions
        FROM record_inventory_current
        WHERE resource_id = ANY($1::UUID[])
          AND support_status = 'supported'
        ORDER BY resource_id, record_version_boundary_key
        "#,
    )
    .bind(resource_ids.into_iter().collect::<Vec<_>>())
    .fetch_all(pool)
    .await
    .context("failed to load schema-v2 GraphQL record inventories")?;

    let mut inventories = BTreeMap::<Uuid, Vec<(Value, PhaseGraphqlRecordInventoryRow)>>::new();
    for row in rows {
        let resource_id: Uuid = row.try_get("resource_id")?;
        inventories.entry(resource_id).or_default().push((
            row.try_get("record_version_boundary")?,
            PhaseGraphqlRecordInventoryRow {
                selectors: row.try_get("selectors")?,
                entries: row.try_get("entries")?,
                chain_positions: row.try_get("chain_positions")?,
            },
        ));
    }

    Ok(keys
        .iter()
        .map(|(resource_id, boundary)| {
            let candidates = inventories.get(resource_id)?;
            let boundary = boundary.as_ref()?;
            if let Some((_, row)) = candidates
                .iter()
                .find(|(candidate, _)| candidate == boundary)
            {
                return Some(row.clone());
            }
            if !boundary_is_pointerless(boundary) {
                return None;
            }
            let mut anchored = candidates
                .iter()
                .filter(|(candidate, _)| boundaries_share_anchor(boundary, candidate));
            let (_, row) = anchored.next()?;
            if anchored.next().is_some() {
                return None;
            }
            Some(row.clone())
        })
        .collect())
}

fn boundary_is_pointerless(boundary: &Value) -> bool {
    ["normalized_event_id", "event_kind"]
        .into_iter()
        .all(|field| boundary.get(field).is_none_or(Value::is_null))
}

fn boundaries_share_anchor(requested: &Value, candidate: &Value) -> bool {
    [
        "/logical_name_id",
        "/chain_position/chain_id",
        "/chain_position/block_number",
        "/chain_position/block_hash",
        "/chain_position/timestamp",
    ]
    .into_iter()
    .all(|path| {
        requested
            .pointer(path)
            .is_some_and(|value| candidate.pointer(path) == Some(value))
    })
}
