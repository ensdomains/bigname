use serde_json::{Map, Value};
use sqlx::{Postgres, Transaction};

use crate::{LookupError, Result, error::database};

use super::HeadRow;

#[derive(Clone, Debug)]
pub(super) struct ProjectedPosition {
    pub chain_id: String,
    pub block_hash: String,
    pub block_number: i64,
    pub timestamp: String,
}

pub(super) fn position_for_chain(positions: &Value, chain_id: &str) -> Result<ProjectedPosition> {
    let slot = chain_slot(chain_id)?;
    let value = positions
        .get(slot)
        .filter(|value| value.is_object())
        .ok_or_else(|| {
            LookupError::stale(format!(
                "projected chain positions are missing the {slot} position"
            ))
        })?;
    let position = ProjectedPosition {
        chain_id: string(value, "chain_id")?.to_owned(),
        block_hash: string(value, "block_hash")?.to_owned(),
        block_number: value
            .get("block_number")
            .and_then(Value::as_i64)
            .ok_or_else(|| LookupError::stale(format!("{slot} position has no block number")))?,
        timestamp: string(value, "timestamp")?.to_owned(),
    };
    if position.chain_id != chain_id {
        return Err(LookupError::stale(format!(
            "{slot} position identifies chain {} instead of {chain_id}",
            position.chain_id
        )));
    }
    Ok(position)
}

pub(super) fn ensure_at_head(
    table: &str,
    position: &ProjectedPosition,
    head: &HeadRow,
) -> Result<()> {
    if position.block_number != head.block_number || position.block_hash != head.block_hash {
        return Err(LookupError::stale(format!(
            "{table} is not published at the newest processed {} block",
            head.chain_id
        )));
    }
    Ok(())
}

pub(super) fn ensure_inventory_at_head(
    table: &str,
    positions: &Value,
    head: &HeadRow,
) -> Result<()> {
    let number = positions.get("target_block_number").and_then(Value::as_i64);
    let hash = positions.get("target_block_hash").and_then(Value::as_str);
    if number != Some(head.block_number) || hash != Some(head.block_hash.as_str()) {
        return Err(LookupError::stale(format!(
            "{table} is not published at the newest processed {} block",
            head.chain_id
        )));
    }
    Ok(())
}

pub(super) async fn ensure_canonical(
    transaction: &mut Transaction<'_, Postgres>,
    position: &ProjectedPosition,
) -> Result<()> {
    let exists: bool = sqlx::query_scalar(
        r#"
        SELECT EXISTS (
            SELECT 1
            FROM chain_lineage
            WHERE chain_id = $1
              AND block_hash = $2
              AND block_number = $3
              AND block_timestamp = $4::timestamptz
              AND canonicality_state IN ('canonical', 'safe', 'finalized')
        )
        "#,
    )
    .bind(&position.chain_id)
    .bind(&position.block_hash)
    .bind(position.block_number)
    .bind(&position.timestamp)
    .fetch_one(&mut **transaction)
    .await
    .map_err(database("validate projected execution position"))?;
    if !exists {
        return Err(LookupError::stale(format!(
            "projected {} execution position is not readable canonical lineage",
            position.chain_id
        )));
    }
    Ok(())
}

pub(super) fn observed_positions(
    resolver: &ProjectedPosition,
    execution: &ProjectedPosition,
) -> Result<Value> {
    let mut positions = Map::new();
    positions.insert(chain_slot(&resolver.chain_id)?.to_owned(), resolver.value());
    if resolver.chain_id != execution.chain_id {
        positions.insert(
            chain_slot(&execution.chain_id)?.to_owned(),
            execution.value(),
        );
    }
    Ok(Value::Object(positions))
}

impl ProjectedPosition {
    fn value(&self) -> Value {
        serde_json::json!({
            "chain_id": self.chain_id,
            "block_hash": self.block_hash,
            "block_number": self.block_number,
            "timestamp": self.timestamp,
        })
    }
}

fn chain_slot(chain_id: &str) -> Result<&'static str> {
    match chain_id {
        crate::ETHEREUM_MAINNET_CHAIN_ID => Ok("ethereum"),
        crate::BASE_MAINNET_CHAIN_ID => Ok("base"),
        _ => Err(LookupError::unsupported(format!(
            "lookup chain {chain_id} has no declared position slot"
        ))),
    }
}

fn string<'a>(value: &'a Value, field: &str) -> Result<&'a str> {
    value
        .get(field)
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| LookupError::stale(format!("projected position has no {field}")))
}
