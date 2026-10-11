//! The refusal a start gets when a phase row it needs records another content hash than this
//! binary expects of it. A stale Interpret row needs the full-range Interpret redo that starts an
//! interpreter content-hash rotation. A stale Project row under a current Interpret row needs
//! one full-range Project redo and nothing else: the runner never starts it by itself.
use sqlx::{Postgres, Transaction};

use crate::{
    error::{ErrorKind, RunnerError, RunnerResult},
    phase::PhaseName,
    transitions::{
        PhaseStateRow, RedoRerun, StaleHash, expected_content_hash, redo_rerun_instruction,
        redo_rerun_options, row_for,
    },
};

pub(crate) async fn refusal(
    transaction: &mut Transaction<'_, Postgres>,
    rows: &[PhaseStateRow],
    chain_id: &str,
    phase: PhaseName,
    stale: StaleHash,
) -> RunnerResult<RunnerError> {
    let recorded_phase = stale.recorded_phase;
    let Some(recorded) = stale.recorded else {
        return Ok(RunnerError::new(
            ErrorKind::ContentHashMismatch,
            format!(
                "refusing derived writes for chain {chain_id} phase {phase}: phase \
                 {recorded_phase} has no recorded content hash"
            ),
        ));
    };
    if recorded_phase != PhaseName::Project {
        return Ok(RunnerError::new(
            ErrorKind::ContentHashMismatch,
            format!(
                "refusing derived writes for chain {chain_id} phase {phase}: binary \
                 interpretation-input hash {} differs from recorded {recorded} on phase \
                 {recorded_phase}; start the interpreter content-hash rotation with a full-range \
                 Interpret redo",
                expected_content_hash(recorded_phase)
            ),
        ));
    }
    let project = row_for(rows, PhaseName::Project)?;
    let full = crate::redo_hash_adoption::full_hash_range(
        transaction,
        chain_id,
        PhaseName::Project,
        project.current_block_number,
        project.redo_in_progress,
    )
    .await?;
    let instruction = match full {
        // Release tooling reads "redo the full range A..=B" to rerun with the printed range.
        Some(full) => format!(
            "redo the full range {}..={} of phase project alone: rerun `phase-runner redo --chain \
             {chain_id} --phase project --from-block {} --to-block {}`{}",
            full.from,
            full.to,
            full.from,
            full.to,
            redo_rerun_options(RedoRerun::Phase(PhaseName::Project))
        ),
        None => {
            "redo the full range of phase project alone once Ingest has completed bounds".to_owned()
        }
    };
    Ok(RunnerError::new(
        ErrorKind::ContentHashMismatch,
        format!(
            "refusing derived writes for chain {chain_id} phase {phase}: binary projection content \
             hash {} differs from recorded {recorded} on phase project while phase interpret \
             records this binary's interpreter content hash; {instruction}",
            expected_content_hash(PhaseName::Project)
        ),
    ))
}

/// What a start blocked by an active Project redo must do first when that redo began under
/// another projection content hash while Interpret records this binary's interpreter content
/// hash: no Interpret redo supersedes that Project redo then, so it has to finish first. While
/// Interpret records another hash, the Interpret redo that starts the rotation supersedes it, and
/// there is nothing to add.
pub(crate) fn stale_project_redo_instruction(
    chain_id: &str,
    rows: &[PhaseStateRow],
    row: &PhaseStateRow,
) -> String {
    let interpret_current = row_for(rows, PhaseName::Interpret).is_ok_and(|interpret| {
        interpret.input_content_hash.as_deref() == Some(expected_content_hash(PhaseName::Interpret))
    });
    let stale = interpret_current
        && row.phase_name == PhaseName::Project.as_str()
        && row.redo_in_progress
        && row.redo_mode.as_deref() == Some("redo")
        && row
            .input_content_hash
            .as_deref()
            .is_some_and(|hash| hash != expected_content_hash(PhaseName::Project));
    if !stale {
        return String::new();
    }
    format!(
        "; its redo began under projection content hash {}, so finish that Project redo first: \
         {}; a range short of the full range is refused with the full range to redo",
        row.input_content_hash.as_deref().unwrap_or_default(),
        redo_rerun_instruction(
            chain_id,
            PhaseName::Project,
            row.redo_mode.as_deref(),
            row.requested_redo_range()
        )
    )
}
