use std::collections::{BTreeMap, BTreeSet};

use anyhow::{Context, Result, bail};
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

    // Keep this route SQL intact: its predicate order differs from the reusable storage fragment,
    // so sharing that fragment would change the query text.
    let rows = sqlx::query(
        r#"
        SELECT ric.resource_id, ric.record_version_boundary, ric.selectors, ric.entries,
               ric.chain_positions, ric.support_status
        FROM bigname_phase.record_inventory_current ric
        JOIN bigname_phase.resources resource
          ON resource.resource_id = ric.resource_id
        JOIN bigname_phase.chain_lineage resource_lineage
          ON resource_lineage.chain_id = resource.chain_id
         AND resource_lineage.block_hash = resource.block_hash
        WHERE ric.resource_id = ANY($1::UUID[])
          AND ric.canonicality_summary ->> 'state' = 'canonical_lineage'
          AND resource.canonicality_state IN (
              'canonical'::bigname_phase.canonicality_state,
              'safe'::bigname_phase.canonicality_state,
              'finalized'::bigname_phase.canonicality_state
          )
          AND resource_lineage.canonicality_state IN (
              'canonical'::bigname_phase.canonicality_state,
              'safe'::bigname_phase.canonicality_state,
              'finalized'::bigname_phase.canonicality_state
          )
          AND EXISTS (
              SELECT 1
              FROM bigname_phase.chain_lineage projection_lineage
              WHERE projection_lineage.chain_id = ric.provenance ->> 'chain_id'
                AND projection_lineage.block_hash =
                    ric.chain_positions ->> 'target_block_hash'
                AND projection_lineage.canonicality_state IN (
                    'canonical'::bigname_phase.canonicality_state,
                    'safe'::bigname_phase.canonicality_state,
                    'finalized'::bigname_phase.canonicality_state
                )
          )
        ORDER BY ric.resource_id, ric.record_version_boundary_key
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
