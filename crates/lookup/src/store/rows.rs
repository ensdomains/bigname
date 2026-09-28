use serde_json::Value;
use sqlx::{Postgres, Transaction};

use crate::{LookupError, Result, error::database};

use super::{HeadRow, InventoryRow, NameRow};

pub(super) async fn load_name(
    transaction: &mut Transaction<'_, Postgres>,
    logical_name_id: &str,
) -> Result<NameRow> {
    super::family_rows::load_name(transaction, logical_name_id).await
}

pub(super) async fn load_inventory(
    transaction: &mut Transaction<'_, Postgres>,
    resource_id: &str,
    boundary: &Value,
) -> Result<InventoryRow> {
    super::family_rows::load_inventory(transaction, resource_id, boundary).await
}

pub(super) async fn load_head(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
) -> Result<HeadRow> {
    sqlx::query_as::<_, HeadRow>(
        r#"
        SELECT head.chain_id, head.latest_block_hash AS block_hash,
               head.latest_block_number AS block_number,
               to_char(
                   lineage.block_timestamp AT TIME ZONE 'UTC',
                   'YYYY-MM-DD"T"HH24:MI:SS"Z"'
               ) AS timestamp
        FROM chain_heads head
        JOIN chain_lineage lineage
          ON lineage.chain_id = head.chain_id
         AND lineage.block_hash = head.latest_block_hash
         AND lineage.block_number = head.latest_block_number
        WHERE head.chain_id = $1
          AND lineage.canonicality_state IN ('canonical', 'safe', 'finalized')
        "#,
    )
    .bind(chain_id)
    .fetch_optional(&mut **transaction)
    .await
    .map_err(database("load lookup chain head"))?
    .ok_or_else(|| LookupError::stale(format!("chain {chain_id} has no readable latest head")))
}
