use sqlx::PgPool;

use crate::{IngestError, Marker, REDO_BOUNDARY_DIVERGENCE_PREFIX, Result};

pub(super) async fn reject_lineage_backed_boundary_change(
    pool: &PgPool,
    chain_id: &str,
    resume_current: Option<&Marker>,
    fresh_target: &Marker,
) -> Result<()> {
    let Some(durable) = resume_current else {
        return Ok(());
    };
    if durable.number != fresh_target.number || durable.hash == fresh_target.hash {
        return Ok(());
    }
    let has_lineage: bool = sqlx::query_scalar(
        "SELECT EXISTS (
             SELECT 1 FROM chain_lineage
             WHERE chain_id = $1 AND block_number = $2 AND block_hash = $3
         )",
    )
    .bind(chain_id)
    .bind(fresh_target.number)
    .bind(&fresh_target.hash)
    .fetch_one(pool)
    .await
    .map_err(|error| {
        IngestError::database(
            format!(
                "failed to check boundary lineage while resuming Ingest redo for chain {chain_id}"
            ),
            error,
        )
    })?;
    if !has_lineage {
        // Cursor reconciliation independently refuses hashes absent from retained lineage.
        return Ok(());
    }
    Err(IngestError::data_integrity(format!(
        "{REDO_BOUNDARY_DIVERGENCE_PREFIX} for chain {chain_id} at block {}: durable redo hash {}, freshly observed hash {}; rerun the Ingest redo so it starts fresh and reloads this boundary under the current watch plan",
        durable.number, durable.hash, fresh_target.hash
    )))
}
