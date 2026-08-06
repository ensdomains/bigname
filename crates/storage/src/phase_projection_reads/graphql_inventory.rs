use std::collections::{BTreeMap, BTreeSet};

use anyhow::{Context, Result, bail};
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

struct InventoryCandidate {
    boundary: Value,
    supported: bool,
    row: PhaseGraphqlRecordInventoryRow,
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
               chain_positions, support_status
        FROM record_inventory_current
        WHERE resource_id = ANY($1::UUID[])
        ORDER BY resource_id, record_version_boundary_key
        "#,
    )
    .bind(resource_ids.into_iter().collect::<Vec<_>>())
    .fetch_all(pool)
    .await
    .context("failed to load schema-v2 GraphQL record inventories")?;

    let mut inventories = BTreeMap::<Uuid, Vec<InventoryCandidate>>::new();
    for row in rows {
        let resource_id: Uuid = row.try_get("resource_id")?;
        inventories
            .entry(resource_id)
            .or_default()
            .push(InventoryCandidate {
                boundary: row.try_get("record_version_boundary")?,
                supported: row.try_get::<String, _>("support_status")? == "supported",
                row: PhaseGraphqlRecordInventoryRow {
                    selectors: row.try_get("selectors")?,
                    entries: row.try_get("entries")?,
                    chain_positions: row.try_get("chain_positions")?,
                },
            });
    }

    keys.iter()
        .map(|(resource_id, boundary)| {
            let Some(candidates) = inventories.get(resource_id) else {
                return Ok(None);
            };
            if candidates.len() == 1 {
                return Ok(candidates[0].supported.then(|| candidates[0].row.clone()));
            }
            if let Some(boundary) = boundary {
                let mut exact = candidates
                    .iter()
                    .filter(|candidate| candidate.boundary == *boundary);
                if let Some(candidate) = exact.next()
                    && exact.next().is_none()
                {
                    return Ok(candidate.supported.then(|| candidate.row.clone()));
                }
            }
            bail!("schema-v2 GraphQL record inventory is ambiguous for resource {resource_id}")
        })
        .collect()
}
