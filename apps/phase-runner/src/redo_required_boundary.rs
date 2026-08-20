use sqlx::PgConnection;

use crate::{
    error::{RunnerError, RunnerResult},
    phase::{BlockRange, PhaseProgress},
};

pub(crate) async fn require_readable(
    connection: &mut PgConnection,
    chain_id: &str,
    range: BlockRange,
    progress: &PhaseProgress,
) -> RunnerResult<()> {
    let loaded = progress.current.as_ref().ok_or_else(|| {
        RunnerError::data_integrity("required Ingest redo completed without a loaded boundary")
    })?;
    let readable: Option<String> = sqlx::query_scalar(
        "SELECT block_hash FROM chain_lineage
         WHERE chain_id = $1 AND block_number = $2
           AND canonicality_state IN ('canonical', 'safe', 'finalized')",
    )
    .bind(chain_id)
    .bind(range.to)
    .fetch_optional(&mut *connection)
    .await
    .map_err(|error| {
        RunnerError::database(
            format!("failed to load readable required-redo boundary for chain {chain_id}"),
            error,
        )
    })?;
    if loaded.number == range.to && readable.as_deref() == Some(loaded.hash.as_str()) {
        return Ok(());
    }
    Err(RunnerError::data_integrity(format!(
        "{} for chain {chain_id} at block {}: loaded boundary hash {}, readable boundary hash {}; \
         rerun the required Ingest redo against the readable fork under the current watch plan",
        bigname_ingest::REDO_BOUNDARY_DIVERGENCE_PREFIX,
        range.to,
        loaded.hash,
        readable.as_deref().unwrap_or("missing")
    )))
}
