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
    /// A required-redo stamp landed on the redo after its last progress write.
    Overtaken(RunnerError),
}

type ActiveMarkerRow = (
    bool,
    Option<String>,
    Option<i64>,
    Option<i64>,
    i64,
    Option<i64>,
    Option<i64>,
);

pub(crate) fn replacement_hash<'a>(
    recorded_number: Option<i64>,
    recorded_hash: Option<&'a str>,
    progress: Option<&'a crate::heads::BlockMarker>,
) -> Option<&'a str> {
    progress
        .filter(|marker| Some(marker.number) == recorded_number)
        .map_or(recorded_hash, |marker| Some(marker.hash.as_str()))
}

pub(crate) async fn lock_completion_coverage(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    phase: PhaseName,
    expected: BlockRange,
    expected_generation: i64,
    recompute_flags: bool,
) -> RunnerResult<CompletionCoverage> {
    let marker: Option<ActiveMarkerRow> = sqlx::query_as(
        "SELECT redo_in_progress, redo_mode,
                redo_from_block_number, redo_to_block_number,
                redo_attempt_generation,
                COALESCE(redo_requested_from_block_number, redo_from_block_number),
                COALESCE(redo_requested_to_block_number, redo_to_block_number)
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
    let Some((
        true,
        Some(mode),
        Some(from),
        Some(to),
        generation,
        Some(requested_from),
        Some(requested_to),
    )) = marker
    else {
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
        if generation != expected_generation
            && !(phase == PhaseName::Project
                && interpret_repair_pending(transaction, chain_id).await?)
        {
            // A required-redo stamp landed on this redo after its last progress
            // write and left the range as it was, so only the generation shows it.
            // The stamp cleared the progress this completion would certify.
            // The rerun names the requested range, which for Project can be
            // narrower than the execution range compared above.
            return Ok(CompletionCoverage::Overtaken(overtaken_error(
                chain_id,
                phase,
                &mode,
                BlockRange::new(requested_from, requested_to)?,
            )));
        }
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
             redo_source_boundary_markers = NULL,
             redo_manifest_authority_fingerprint = NULL,
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

/// Whether Interpret carries a required redo, read while the Project row is locked.
///
/// A Project redo overtaken by a reorg may still complete when this holds: the
/// Interpret repair ends by stamping Project, so the next supervisor start redoes
/// Project on the new chain. Without it nothing would, so the caller refuses.
///
/// A plain read is enough. Only a head publication stamps a running Project redo,
/// and it stamps Interpret before Project in one transaction. The changed Project
/// generation was read from a committed publication, so this later statement sees
/// that publication's Interpret stamp as well. The stamp cannot have been used up
/// since: an Interpret redo cannot start while this Project redo is running. A
/// publication still waiting on the Project row left the generation unchanged and
/// does not reach this read.
async fn interpret_repair_pending(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
) -> RunnerResult<bool> {
    sqlx::query_scalar(
        "SELECT redo_in_progress AND COALESCE(last_error LIKE $2, false)
         FROM chain_phase_state
         WHERE chain_id = $1 AND phase_name = 'interpret'",
    )
    .bind(chain_id)
    .bind(crate::redo_stamp::required_redo_owner_pattern())
    .fetch_optional(&mut **transaction)
    .await
    .map(|pending: Option<bool>| pending.unwrap_or(false))
    .map_err(|error| {
        RunnerError::database(
            format!("failed to read the Interpret repair for chain {chain_id}"),
            error,
        )
    })
}

fn overtaken_error(chain_id: &str, phase: PhaseName, mode: &str, range: BlockRange) -> RunnerError {
    let instruction =
        crate::transitions::redo_rerun_instruction(chain_id, phase, Some(mode), Some(range));
    // Verify waits for the repairs the same reorg left on Interpret and Project.
    let after = if phase == PhaseName::Verify {
        "once the Interpret and Project repairs have completed, "
    } else {
        ""
    };
    RunnerError::data_integrity(format!(
        "redo for chain {chain_id} phase {phase} was overtaken before it completed: a reorg \
         recovery or another required redo was recorded on it after its last progress write; \
         the redo stays in progress and nothing was restored; {after}{instruction}"
    ))
}
