use sqlx::{Postgres, Transaction};

use crate::{error::RunnerResult, heads::head_write_error};

pub(crate) async fn orphan_displaced(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    hashes: &[&str],
    path_floor: i64,
    path_ceiling: i64,
) -> RunnerResult<()> {
    sqlx::query(
        "UPDATE chain_lineage
         SET canonicality_state = 'orphaned'
         WHERE chain_id = $1
           AND block_number BETWEEN $2 AND $3
           AND canonicality_state = 'observed'
           AND NOT (block_hash = ANY($4))",
    )
    .bind(chain_id)
    .bind(path_floor)
    .bind(path_ceiling)
    .bind(hashes)
    .execute(&mut **transaction)
    .await
    .map_err(|error| head_write_error("orphan displaced observed path", chain_id, error))?;
    Ok(())
}
