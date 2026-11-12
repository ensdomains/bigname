//! The repair record transitions of a rebuild's composition pass (families/compose.rs). Each runs
//! inside the transaction that does the work it records, after the caller fenced the record on
//! its attempt, state and cursor.
use sqlx::{Postgres, Transaction, postgres::PgQueryResult};

use super::{Record, State, fence_error};
use crate::{ProjectError, Result};

/// The fence of a composition pass worker, which reads the record without locking it: the record
/// must be composing, and every field must be as the run planned it. While workers run, the
/// coordinator writes only its acknowledgement of each committed chunk (`compose_step`
/// with the names stream and no cursor), which moves `compose_chunks` and `updated_at`. Neither is
/// a field of `Record`, so no field is excluded from the compare. Each field is named so that a
/// field added later is a decision here.
pub(crate) fn require_composing(
    chain_id: &str,
    read: Option<&Record>,
    planned: &Record,
) -> Result<()> {
    let Some(read) = read else {
        return Err(fence_error(chain_id, "composing", None));
    };
    let Record {
        attempt,
        reason,
        trusted_base,
        replay_target,
        state,
        prefix_revision,
        invalidation_from,
        pending_undo_target,
        completed_sequence,
        completed_marker,
        completed_input_hash,
        reset_sequence,
        composed_block,
        compose_stream,
        compose_cursor,
    } = planned;
    let unchanged = *state == State::Composing
        && read.attempt == *attempt
        && read.reason == *reason
        && read.trusted_base == *trusted_base
        && read.replay_target == *replay_target
        && read.state == *state
        && read.prefix_revision == *prefix_revision
        && read.invalidation_from == *invalidation_from
        && read.pending_undo_target == *pending_undo_target
        && read.completed_sequence == *completed_sequence
        && read.completed_marker == *completed_marker
        && read.completed_input_hash == *completed_input_hash
        && read.reset_sequence == *reset_sequence
        && read.composed_block == *composed_block
        && read.compose_stream == *compose_stream
        && read.compose_cursor == *compose_cursor;
    if !unchanged {
        return Err(fence_error(
            chain_id,
            "the composing one the worker planned from",
            Some(read),
        ));
    }
    Ok(())
}

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
