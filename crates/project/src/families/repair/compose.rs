//! The repair record transitions of a rebuild's composition pass (families/compose.rs). Each runs
//! inside the transaction that does the work it records, after the caller fenced the record on
//! its attempt, state and cursor.
use sqlx::{Postgres, Transaction, postgres::PgQueryResult};

use super::fence_error;
use crate::{ProjectError, Result};

/// Open the pass: a rebuild that has not composed moves to composing at its first stream.
pub(crate) async fn begin_compose(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
) -> Result<()> {
    let result = sqlx::query(
        "/* project:families.repair.compose.begin */ UPDATE project_repair_record
         SET state = 'composing', compose_stream = 0, compose_cursor = NULL, compose_chunks = 0,
             updated_at = now()
         WHERE chain_id = $1 AND state = 'rebuilding' AND composed_block IS NULL",
    )
    .bind(chain_id)
    .execute(&mut **transaction)
    .await;
    one(chain_id, result, "rebuilding before its composition pass")
}

/// Record the stream and the last key a committed chunk of the pass composed, or the move to
/// the next stream of a stream with no keys left (`chunk` false). Returns the chunks the pass has
/// committed, this one included.
pub(crate) async fn compose_step(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    stream: i16,
    cursor: Option<&str>,
    chunk: bool,
) -> Result<u64> {
    let chunks: Option<i64> = sqlx::query_scalar(
        "/* project:families.repair.compose.step */ UPDATE project_repair_record
         SET compose_stream = $2, compose_cursor = $3, compose_chunks = compose_chunks + $4,
             updated_at = now()
         WHERE chain_id = $1 AND state = 'composing'
         RETURNING compose_chunks",
    )
    .bind(chain_id)
    .bind(stream)
    .bind(cursor)
    .bind(i64::from(chunk))
    .fetch_optional(&mut **transaction)
    .await
    .map_err(|error| ProjectError::database("failed to record the composition pass", error))?;
    chunks
        .map(|chunks| u64::try_from(chunks).unwrap_or(0))
        .ok_or_else(|| fence_error(chain_id, "composing", None))
}

/// End the pass: the rebuild continues, every generation above `composed_block` composed. A pass
/// with nothing to compose ends straight from rebuilding.
pub(crate) async fn end_compose(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    composed_block: i64,
) -> Result<()> {
    let result = sqlx::query(
        "/* project:families.repair.compose.end */ UPDATE project_repair_record
         SET state = 'rebuilding', composed_block = $2, compose_stream = NULL,
             compose_cursor = NULL, updated_at = now()
         WHERE chain_id = $1 AND state IN ('rebuilding', 'composing') AND composed_block IS NULL",
    )
    .bind(chain_id)
    .bind(composed_block)
    .execute(&mut **transaction)
    .await;
    one(chain_id, result, "composing")
}

fn one(
    chain_id: &str,
    result: std::result::Result<PgQueryResult, sqlx::Error>,
    what: &str,
) -> Result<()> {
    let result = result
        .map_err(|error| ProjectError::database("failed to record the composition pass", error))?;
    if result.rows_affected() != 1 {
        return Err(fence_error(chain_id, what, None));
    }
    Ok(())
}
