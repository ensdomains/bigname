use sqlx::PgConnection;

use crate::{
    error::{RunnerError, RunnerResult},
    phase::PhaseName,
};

pub(crate) async fn record(
    lock_connection: &mut PgConnection,
    chain_id: &str,
    phase: PhaseName,
    error: &RunnerError,
) -> RunnerResult<()> {
    // A partial redo stays marked in progress and resumable. Boundary divergence is the exception:
    // the redo stays in progress, but its resumable progress clears so the next command reloads.
    let error = error.to_string();
    let restart_from_boundary = error.starts_with(bigname_ingest::REDO_BOUNDARY_DIVERGENCE_PREFIX);
    let result = sqlx::query(
        "
        UPDATE chain_phase_state
        SET last_error = CASE
                WHEN last_error LIKE $4
                    THEN $5 || substring(last_error FROM char_length($6) + 1)
                         || '; last attempt failed: ' || $3
                WHEN last_error LIKE $7
                    THEN last_error || '; last attempt failed: ' || $3
                ELSE $3
            END,
            redo_current_block_number = CASE WHEN $8 THEN NULL ELSE redo_current_block_number END,
            redo_current_block_hash = CASE WHEN $8 THEN NULL ELSE redo_current_block_hash END,
            redo_target_block_number = CASE WHEN $8 THEN NULL ELSE redo_target_block_number END,
            redo_target_block_hash = CASE WHEN $8 THEN NULL ELSE redo_target_block_hash END,
            redo_source_boundary_markers = CASE
                WHEN $8 THEN NULL ELSE redo_source_boundary_markers
            END,
            updated_at = now()
        WHERE chain_id = $1 AND phase_name = $2 AND redo_in_progress
        ",
    )
    .bind(chain_id)
    .bind(phase.as_str())
    .bind(error)
    .bind(format!(
        "{}%",
        crate::redo_stamp::REQUIRED_REDO_ACTIVE_PREFIX
    ))
    .bind(crate::redo_stamp::REQUIRED_REDO_PREFIX)
    .bind(crate::redo_stamp::REQUIRED_REDO_ACTIVE_PREFIX)
    .bind(crate::redo_stamp::required_redo_owner_pattern())
    .bind(restart_from_boundary)
    .execute(&mut *lock_connection)
    .await
    .map_err(|database_error| {
        RunnerError::database(
            format!("failed to record redo failure for chain {chain_id} phase {phase}"),
            database_error,
        )
    })?;
    if result.rows_affected() != 1 {
        return Err(RunnerError::data_integrity(format!(
            "redo failure requires an active redo for chain {chain_id} phase {phase}"
        )));
    }
    Ok(())
}
