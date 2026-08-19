use bigname_project::GenerationFailureEvidence;
use sqlx::PgPool;

use crate::error::{RunnerError, RunnerResult};

/// Append the evidence for a projection-blocking generation failure.
///
/// The generation transaction has already rolled back, so this runs in its own
/// transaction. A retried generation recomputes the same fingerprint and adds no
/// second row; a later success or reorg never deletes one.
pub(crate) async fn persist(
    pool: &PgPool,
    chain_id: &str,
    interpreter_content_hash: &str,
    evidence: &GenerationFailureEvidence,
) -> RunnerResult<()> {
    sqlx::query(
        "INSERT INTO project_generation_failures (
             chain_id, target_block_number, target_block_hash,
             interpreter_content_hash, failure_kind, failure_fingerprint,
             logical_name_id, evidence
         ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
         ON CONFLICT DO NOTHING",
    )
    .bind(chain_id)
    .bind(evidence.target_block_number)
    .bind(&evidence.target_block_hash)
    .bind(interpreter_content_hash)
    .bind(&evidence.failure_kind)
    .bind(&evidence.failure_fingerprint)
    .bind(&evidence.logical_name_id)
    .bind(&evidence.payload)
    .execute(pool)
    .await
    .map_err(|error| {
        RunnerError::database(
            format!(
                "failed to record projection generation failure for chain {chain_id} at block {}",
                evidence.target_block_number
            ),
            error,
        )
    })?;
    Ok(())
}
