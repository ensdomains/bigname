use serde_json::{Map, Value};
use sqlx::{Postgres, Transaction};

use crate::{LookupError, LookupPosition, Result, error::database};

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

pub(super) async fn ensure_project_at_head(
    transaction: &mut Transaction<'_, Postgres>,
    head: &HeadRow,
) -> Result<String> {
    let project_row_xmin: Option<String> = sqlx::query_scalar(
        r#"
        SELECT xmin::text
        FROM chain_phase_state
        WHERE chain_id = $1
          AND phase_name = 'project'
          AND phase_status = 'completed'
          AND current_block_number = $2
          AND current_block_hash = $3
          AND input_content_hash = $4
        "#,
    )
    .bind(&head.chain_id)
    .bind(head.block_number)
    .bind(&head.block_hash)
    .bind(bigname_content_hash::INTERPRETER_CONTENT_HASH)
    .fetch_optional(&mut **transaction)
    .await
    .map_err(database("validate project publication head"))?;
    project_row_xmin.ok_or_else(|| {
        LookupError::stale(format!(
            "projected state has not reached the newest processed {} block",
            head.chain_id
        ))
    })
}

pub(super) async fn inventory_position(
    transaction: &mut Transaction<'_, Postgres>,
    table: &str,
    positions: &Value,
    chain_id: &str,
) -> Result<ProjectedPosition> {
    let block_number = positions
        .get("target_block_number")
        .and_then(Value::as_i64)
        .ok_or_else(|| LookupError::stale(format!("{table} has no target block number")))?;
    let block_hash = positions
        .get("target_block_hash")
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| LookupError::stale(format!("{table} has no target block hash")))?;
    let timestamp: Option<String> = sqlx::query_scalar(
        r#"
        SELECT to_char(
            block_timestamp AT TIME ZONE 'UTC',
            'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'
        )
        FROM chain_lineage
        WHERE chain_id = $1
          AND block_hash = $2
          AND block_number = $3
          AND canonicality_state IN ('canonical', 'safe', 'finalized')
        "#,
    )
    .bind(chain_id)
    .bind(block_hash)
    .bind(block_number)
    .fetch_optional(&mut **transaction)
    .await
    .map_err(database("validate indexed comparison position"))?;
    let timestamp = timestamp.ok_or_else(|| {
        LookupError::stale(format!(
            "{table} target is not readable canonical {chain_id} lineage"
        ))
    })?;
    Ok(ProjectedPosition {
        chain_id: chain_id.to_owned(),
        block_hash: block_hash.to_owned(),
        block_number,
        timestamp,
    })
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

pub(super) fn comparison_and_live_positions(
    comparison: &ProjectedPosition,
    live: &LookupPosition,
) -> Result<Value> {
    if comparison.chain_id != live.chain_id {
        let mut positions = Map::new();
        positions.insert(
            chain_slot(&comparison.chain_id)?.to_owned(),
            comparison.value(),
        );
        positions.insert(
            chain_slot(&live.chain_id)?.to_owned(),
            serde_json::to_value(live).map_err(|error| {
                LookupError::database(format!("failed to encode live lookup position: {error}"))
            })?,
        );
        return Ok(Value::Object(positions));
    }
    if comparison.block_number == live.block_number
        && comparison.block_hash.eq_ignore_ascii_case(&live.block_hash)
    {
        return observed_positions(comparison, comparison);
    }
    Ok(serde_json::json!({
        "indexed": comparison.value(),
        "live": live,
    }))
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

impl From<ProjectedPosition> for LookupPosition {
    fn from(position: ProjectedPosition) -> Self {
        Self {
            chain_id: position.chain_id,
            block_hash: position.block_hash,
            block_number: position.block_number,
            timestamp: position.timestamp,
        }
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
