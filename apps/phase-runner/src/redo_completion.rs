use sqlx::{Postgres, Transaction};

use crate::{
    error::{RunnerError, RunnerResult},
    phase::{BlockRange, PhaseName},
    transitions::PhaseStateRow,
};

pub(crate) fn restore_previous_lifecycle(previous: &mut PhaseStateRow) -> RunnerResult<()> {
    if !previous.redo_in_progress {
        return Ok(());
    }
    previous.phase_status = previous.redo_previous_phase_status.take().ok_or_else(|| {
        RunnerError::data_integrity("active redo is missing its previous phase status")
    })?;
    previous.last_error = previous.redo_previous_last_error.take();
    previous.started_at = previous.redo_previous_started_at.take();
    previous.finished_at = previous.redo_previous_finished_at.take();
    Ok(())
}

pub(crate) enum CompletionCoverage {
    Exact,
    Widened(BlockRange),
}

type ActiveMarkerRow = (bool, Option<String>, Option<i64>, Option<i64>);

pub(crate) async fn lock_completion_coverage(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    phase: PhaseName,
    expected: BlockRange,
    recompute_flags: bool,
) -> RunnerResult<CompletionCoverage> {
    let marker: Option<ActiveMarkerRow> = sqlx::query_as(
        "SELECT redo_in_progress, redo_mode,
                redo_from_block_number, redo_to_block_number
         FROM chain_phase_state
         WHERE chain_id = $1 AND phase_name = $2
         FOR UPDATE",
    )
    .bind(chain_id)
    .bind(phase.as_str())
    .fetch_optional(&mut **transaction)
    .await
    .map_err(|error| {
        RunnerError::database(
            format!("failed to lock redo completion for chain {chain_id} phase {phase}"),
            error,
        )
    })?;
    let Some((true, Some(mode), Some(from), Some(to))) = marker else {
        return Err(RunnerError::data_integrity(format!(
            "redo completion requires an active marker for chain {chain_id} phase {phase}"
        )));
    };
    let persisted = BlockRange::new(from, to)?;
    let expected_mode = if recompute_flags {
        "recompute_flags"
    } else {
        "redo"
    };
    if mode == expected_mode && persisted == expected {
        return Ok(CompletionCoverage::Exact);
    }
    if mode != expected_mode || persisted.from > expected.from || persisted.to < expected.to {
        return Err(RunnerError::data_integrity(format!(
            "redo marker changed incompatibly while chain {chain_id} phase {phase} was running: \
             expected {expected_mode} {}..={}, found {mode} {}..={}",
            expected.from, expected.to, persisted.from, persisted.to
        )));
    }

    let result = sqlx::query(
        "UPDATE chain_phase_state
         SET redo_current_block_number = NULL,
             redo_current_block_hash = NULL,
             redo_target_block_number = NULL,
             redo_target_block_hash = NULL,
             last_error = CASE
                 WHEN last_error LIKE $3 THEN $4
                      || substring(last_error FROM char_length($5) + 1)
                      || '; range widened; rerun the full persisted range'
                 ELSE last_error
             END,
             updated_at = now()
         WHERE chain_id = $1 AND phase_name = $2 AND redo_in_progress",
    )
    .bind(chain_id)
    .bind(phase.as_str())
    .bind(format!(
        "{}%",
        crate::redo_stamp::REQUIRED_REDO_ACTIVE_PREFIX
    ))
    .bind(crate::redo_stamp::REQUIRED_REDO_PREFIX)
    .bind(crate::redo_stamp::REQUIRED_REDO_ACTIVE_PREFIX)
    .execute(&mut **transaction)
    .await
    .map_err(|error| {
        RunnerError::database(
            format!("failed to preserve widened redo coverage for chain {chain_id} phase {phase}"),
            error,
        )
    })?;
    if result.rows_affected() != 1 {
        return Err(RunnerError::data_integrity(format!(
            "widened redo completion lost its active marker for chain {chain_id} phase {phase}"
        )));
    }
    Ok(CompletionCoverage::Widened(persisted))
}
