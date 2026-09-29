//! A Project redo left in progress by a binary with another interpreter content hash cannot finish
//! under this binary: Project refuses to write while Interpret records the other hash, and the
//! other binary may be gone (a schema-migration can drop the tables it needs). Its progress is
//! invalid under this hash anyway. The Interpret redo that starts this binary's hash epoch
//! therefore supersedes it: in the transaction that begins the Interpret redo, the Project row is
//! restored the way a finished redo restores it, and the Interpret redo's completion stamps the
//! Project redo again under this hash. A Project redo from this binary's own hash is live work and
//! still blocks Interpret.
use sqlx::{Postgres, Transaction};

use crate::{
    error::{ErrorKind, RunnerError, RunnerResult},
    phase::{PhaseName, RunMode},
    state::PhaseStatus,
    transitions::{PhaseStateRow, row_for},
};

/// The Project row an Interpret redo starting a new hash epoch supersedes, if any: a Project redo
/// in progress under another hash, while Interpret itself still records another hash.
pub(crate) fn superseded_project_redo<'a>(
    rows: &'a [PhaseStateRow],
    phase: PhaseName,
    mode: &RunMode,
) -> RunnerResult<Option<&'a PhaseStateRow>> {
    if phase != PhaseName::Interpret || !matches!(mode, RunMode::Redo(_)) {
        return Ok(None);
    }
    if !other_hash(row_for(rows, PhaseName::Interpret)?) {
        return Ok(None);
    }
    let project = row_for(rows, PhaseName::Project)?;
    let running = matches!(
        project.status()?,
        PhaseStatus::Running | PhaseStatus::Paused
    );
    Ok((running && project.redo_in_progress && other_hash(project)).then_some(project))
}

fn other_hash(row: &PhaseStateRow) -> bool {
    row.input_content_hash
        .as_deref()
        .is_some_and(|hash| hash != bigname_content_hash::INTERPRETER_CONTENT_HASH)
}

/// Restore the superseded Project row as `redo_state::finish` restores a finished redo: its
/// lifecycle from before the redo (a phase that was running then is failed, as an interrupted one
/// is), every redo column cleared, its recorded extent and hash kept. The advisory lock proves no
/// Project writer is still running the redo; the update is guarded on the attempt generation and
/// hash the caller locked.
pub(crate) async fn supersede(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    project: &PhaseStateRow,
) -> RunnerResult<()> {
    let free: bool = sqlx::query_scalar(
        "SELECT pg_try_advisory_xact_lock(hashtextextended($1::text, 0::bigint))",
    )
    .bind(crate::phase_lock::lock_name(chain_id, PhaseName::Project))
    .fetch_one(&mut **transaction)
    .await
    .map_err(|error| {
        RunnerError::database(
            format!("failed to probe the Project lock of chain {chain_id}"),
            error,
        )
    })?;
    if !free {
        return Err(RunnerError::new(
            ErrorKind::LockHeld,
            format!(
                "chain {chain_id} phase project is still running a redo under interpreter content \
                 hash {}; stop that runner before starting the new hash epoch",
                project.input_content_hash.as_deref().unwrap_or("none")
            ),
        ));
    }
    let previous_status = project.redo_previous_phase_status.as_deref().ok_or_else(|| {
        RunnerError::data_integrity(format!(
            "chain {chain_id} phase project has an active redo without its previous phase status"
        ))
    })?;
    let interrupted = matches!(
        previous_status.parse::<PhaseStatus>()?,
        PhaseStatus::Running | PhaseStatus::Paused
    );
    let result = sqlx::query(
        "
        UPDATE chain_phase_state
        SET phase_status = CASE WHEN $4 THEN 'failed' ELSE redo_previous_phase_status END,
            last_error = CASE
                WHEN $4 THEN 'phase was interrupted before redo; resume the normal phase'
                ELSE redo_previous_last_error
            END,
            started_at = redo_previous_started_at,
            finished_at = CASE WHEN $4 THEN now() ELSE redo_previous_finished_at END,
            redo_in_progress = false,
            redo_mode = NULL,
            redo_previous_phase_status = NULL,
            redo_previous_last_error = NULL,
            redo_previous_started_at = NULL,
            redo_previous_finished_at = NULL,
            redo_from_block_number = NULL,
            redo_to_block_number = NULL,
            redo_current_block_number = NULL,
            redo_current_block_hash = NULL,
            redo_target_block_number = NULL,
            redo_target_block_hash = NULL,
            redo_source_boundary_markers = NULL,
            redo_manifest_authority_fingerprint = NULL,
            updated_at = now()
        WHERE chain_id = $1
          AND phase_name = 'project'
          AND redo_in_progress
          AND redo_attempt_generation = $2
          AND input_content_hash = $3
        ",
    )
    .bind(chain_id)
    .bind(project.redo_attempt_generation)
    .bind(project.input_content_hash.as_deref())
    .bind(interrupted)
    .execute(&mut **transaction)
    .await
    .map_err(|error| {
        RunnerError::database(
            format!("failed to supersede the Project redo of chain {chain_id}"),
            error,
        )
    })?;
    if result.rows_affected() != 1 {
        return Err(RunnerError::transient(format!(
            "the Project redo of chain {chain_id} changed after it was locked; retry the redo"
        )));
    }
    tracing::warn!(
        chain_id,
        superseded_hash = project.input_content_hash.as_deref(),
        interpreter_content_hash = bigname_content_hash::INTERPRETER_CONTENT_HASH,
        redo_attempt_generation = project.redo_attempt_generation,
        redo_from_block = project.redo_from_block_number,
        redo_to_block = project.redo_to_block_number,
        "superseded a Project redo left by another interpreter content hash; the Interpret redo \
         that starts this hash epoch stamps it again"
    );
    Ok(())
}
