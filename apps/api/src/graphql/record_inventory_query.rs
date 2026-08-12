use std::collections::{BTreeMap, BTreeSet};

use anyhow::{Context, Result, bail};
use bigname_storage::{
    RECORD_INVENTORY_CANONICALITY_SUMMARY_FILTER, RECORD_INVENTORY_PROJECTION_LINEAGE_FILTER,
    RECORD_INVENTORY_RESOURCE_CANONICALITY_FILTER, RECORD_INVENTORY_RESOURCE_LINEAGE_FILTER,
    RESOURCE_CANONICALITY_JOINS,
};
use serde_json::Value;
use sqlx::{PgPool, Row, types::Uuid};

pub type PhaseGraphqlRecordInventoryKey = (Uuid, Option<Value>);

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PhaseGraphqlRecordInventoryRow {
    pub selectors: Value,
    pub entries: Value,
    pub chain_positions: Value,
    pub chain_id: Option<String>,
}

struct InventoryCandidate {
    boundary: Value,
    supported: bool,
    row: PhaseGraphqlRecordInventoryRow,
}

fn phase_graphql_record_inventory_query() -> String {
    format!(
        r#"
        SELECT ric.resource_id, ric.record_version_boundary, ric.selectors, ric.entries,
               ric.chain_positions, ric.support_status
        FROM bigname_phase.record_inventory_current ric
        {RESOURCE_CANONICALITY_JOINS}
        WHERE ric.resource_id = ANY($1::UUID[])
          {RECORD_INVENTORY_CANONICALITY_SUMMARY_FILTER}
          {RECORD_INVENTORY_RESOURCE_CANONICALITY_FILTER}
          {RECORD_INVENTORY_RESOURCE_LINEAGE_FILTER}
          {RECORD_INVENTORY_PROJECTION_LINEAGE_FILTER}
        ORDER BY ric.resource_id, ric.record_version_boundary_key
        "#,
    )
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

    let query = phase_graphql_record_inventory_query();
    let rows = sqlx::query(&query)
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
                    chain_id: row
                        .try_get::<Value, _>("record_version_boundary")?
                        .pointer("/chain_position/chain_id")
                        .and_then(Value::as_str)
                        .map(str::to_owned),
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn query_composes_storage_canonicality_fragments_in_route_order() {
        let query = phase_graphql_record_inventory_query();
        let fragments = [
            RESOURCE_CANONICALITY_JOINS,
            RECORD_INVENTORY_CANONICALITY_SUMMARY_FILTER,
            RECORD_INVENTORY_RESOURCE_CANONICALITY_FILTER,
            RECORD_INVENTORY_RESOURCE_LINEAGE_FILTER,
            RECORD_INVENTORY_PROJECTION_LINEAGE_FILTER,
        ];

        let mut previous_end = 0;
        for fragment in fragments {
            assert_eq!(
                query.match_indices(fragment).count(),
                1,
                "storage-owned SQL fragment must appear exactly once: {fragment}"
            );
            let offset = query
                .find(fragment)
                .expect("storage-owned SQL fragment must be composed into the API query");
            assert!(
                offset >= previous_end,
                "storage-owned SQL fragments must retain the GraphQL route order"
            );
            previous_end = offset + fragment.len();
        }
    }
}
