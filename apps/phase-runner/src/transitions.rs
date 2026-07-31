use sqlx::{FromRow, Postgres, Transaction};

use crate::{
    error::{ErrorKind, RunnerError, RunnerResult},
    phase::{BlockRange, PhaseName, RunMode},
    state::PhaseStatus,
};

#[derive(Clone, Debug, FromRow)]
pub(crate) struct PhaseStateRow {
    pub phase_name: String,
    pub phase_status: String,
    pub verification_level: Option<String>,
    pub current_block_number: Option<i64>,
    pub current_block_hash: Option<String>,
    pub target_block_number: Option<i64>,
    pub target_block_hash: Option<String>,
    pub input_content_hash: Option<String>,
    pub redo_in_progress: bool,
    pub redo_mode: Option<String>,
    pub redo_previous_phase_status: Option<String>,
    pub redo_previous_last_error: Option<String>,
    pub redo_previous_started_at: Option<String>,
    pub redo_previous_finished_at: Option<String>,
    pub redo_from_block_number: Option<i64>,
    pub redo_to_block_number: Option<i64>,
    pub live_handoff_block_number: Option<i64>,
    pub live_handoff_block_hash: Option<String>,
    pub last_error: Option<String>,
    pub started_at: Option<String>,
    pub finished_at: Option<String>,
}

impl PhaseStateRow {
    pub(crate) fn status(&self) -> RunnerResult<PhaseStatus> {
        self.phase_status.parse()
    }
}

pub(crate) async fn lock_chain_phase_state(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
) -> RunnerResult<Vec<PhaseStateRow>> {
    let rows = sqlx::query_as::<_, PhaseStateRow>(
        "
        SELECT phase_name,
               phase_status,
               verification_level,
               current_block_number,
               current_block_hash,
               target_block_number,
               target_block_hash,
               input_content_hash,
               redo_in_progress,
               redo_mode,
               redo_previous_phase_status,
               redo_previous_last_error,
               redo_previous_started_at::text AS redo_previous_started_at,
               redo_previous_finished_at::text AS redo_previous_finished_at,
               redo_from_block_number,
               redo_to_block_number,
               live_handoff_block_number,
               live_handoff_block_hash,
               last_error,
               started_at::text AS started_at,
               finished_at::text AS finished_at
        FROM chain_phase_state
        WHERE chain_id = $1
        ORDER BY phase_name
        FOR UPDATE
        ",
    )
    .bind(chain_id)
    .fetch_all(&mut **transaction)
    .await
    .map_err(|error| {
        RunnerError::transient(format!(
            "failed to lock phase state for chain {chain_id}: {error}"
        ))
    })?;
    if rows.len() != PhaseName::ALL.len()
        || PhaseName::ALL
            .iter()
            .any(|phase| row_for(&rows, *phase).is_err())
    {
        return Err(RunnerError::data_integrity(format!(
            "phase state is incomplete for chain {chain_id}"
        )));
    }
    Ok(rows)
}

pub(crate) fn row_for(rows: &[PhaseStateRow], phase: PhaseName) -> RunnerResult<&PhaseStateRow> {
    rows.iter()
        .find(|row| row.phase_name == phase.as_str())
        .ok_or_else(|| {
            RunnerError::data_integrity(format!("phase state is missing for phase {phase}"))
        })
}

pub(crate) fn require_start(
    rows: &[PhaseStateRow],
    chain_id: &str,
    phase: PhaseName,
    mode: &RunMode,
) -> RunnerResult<()> {
    require_no_interrupted_redo(rows, chain_id, phase, mode)?;
    require_compatible_active_phase(rows, chain_id, phase)?;
    require_prerequisite(rows, chain_id, phase)?;
    if phase.writes_derived_data() {
        require_content_hash(rows, chain_id, phase, mode)?;
    }
    Ok(())
}

fn require_no_interrupted_redo(
    rows: &[PhaseStateRow],
    chain_id: &str,
    phase: PhaseName,
    mode: &RunMode,
) -> RunnerResult<()> {
    if !matches!(mode, RunMode::Normal) {
        return Ok(());
    }
    let row = row_for(rows, phase)?;
    if !row.redo_in_progress {
        return Ok(());
    }
    let instruction = redo_rerun_instruction(
        chain_id,
        phase,
        row.redo_mode.as_deref(),
        row.redo_from_block_number
            .zip(row.redo_to_block_number)
            .map(|(from, to)| BlockRange { from, to }),
    );
    Err(RunnerError::new(
        ErrorKind::InvalidTransition,
        format!(
            "cannot resume normal phase {phase} for chain {chain_id}: an explicit redo was \
             interrupted; {instruction}"
        ),
    ))
}

pub(crate) fn redo_rerun_instruction(
    chain_id: &str,
    phase: PhaseName,
    redo_mode: Option<&str>,
    range: Option<BlockRange>,
) -> String {
    let phase_argument = if redo_mode == Some("recompute_flags") {
        "recompute-flags"
    } else {
        phase.as_str()
    };
    let range_argument = range
        .map(|range| format!(" --from-block {} --to-block {}", range.from, range.to))
        .unwrap_or_default();
    format!("rerun `phase-runner redo --chain {chain_id} --phase {phase_argument}{range_argument}`")
}

fn require_compatible_active_phase(
    rows: &[PhaseStateRow],
    chain_id: &str,
    phase: PhaseName,
) -> RunnerResult<()> {
    for row in rows {
        let other: PhaseName = row.phase_name.parse()?;
        if other == phase {
            continue;
        }
        let status = row.status()?;
        if matches!(status, PhaseStatus::Running | PhaseStatus::Paused)
            && !verify_live_pair(phase, other)
        {
            return Err(RunnerError::new(
                ErrorKind::InvalidTransition,
                format!(
                    "cannot start phase {phase} for chain {chain_id} while phase {other} is {status}"
                ),
            ));
        }
    }
    Ok(())
}

fn verify_live_pair(left: PhaseName, right: PhaseName) -> bool {
    matches!(
        (left, right),
        (PhaseName::Verify, PhaseName::Live) | (PhaseName::Live, PhaseName::Verify)
    )
}

fn require_prerequisite(
    rows: &[PhaseStateRow],
    chain_id: &str,
    phase: PhaseName,
) -> RunnerResult<()> {
    let Some(prerequisite) = phase.prerequisite() else {
        return Ok(());
    };
    let status = row_for(rows, prerequisite)?.status()?;
    if status != PhaseStatus::Completed {
        return Err(RunnerError::new(
            ErrorKind::InvalidTransition,
            format!(
                "cannot start phase {phase} for chain {chain_id}: prerequisite {prerequisite} is \
                 not completed"
            ),
        ));
    }
    Ok(())
}

fn require_content_hash(
    rows: &[PhaseStateRow],
    chain_id: &str,
    phase: PhaseName,
    mode: &RunMode,
) -> RunnerResult<()> {
    match mode {
        RunMode::Redo(_) if phase == PhaseName::Interpret => Ok(()),
        RunMode::Redo(_) if phase == PhaseName::Project => {
            require_current_hash(rows, chain_id, phase, PhaseName::Interpret)
        }
        RunMode::Redo(_) if phase == PhaseName::Live => {
            require_current_interpret_and_project(rows, chain_id, phase)
        }
        RunMode::RecomputeFlags(_) => require_current_interpret_and_project(rows, chain_id, phase),
        RunMode::Normal if phase == PhaseName::Interpret => {
            reject_different_hash(rows, chain_id, phase, PhaseName::Interpret)
        }
        RunMode::Normal if phase == PhaseName::Project => {
            require_current_hash(rows, chain_id, phase, PhaseName::Interpret)?;
            reject_different_hash(rows, chain_id, phase, PhaseName::Project)
        }
        RunMode::Normal if phase == PhaseName::Live => {
            require_current_interpret_and_project(rows, chain_id, phase)
        }
        _ => Ok(()),
    }
}

fn require_current_interpret_and_project(
    rows: &[PhaseStateRow],
    chain_id: &str,
    phase: PhaseName,
) -> RunnerResult<()> {
    require_current_hash(rows, chain_id, phase, PhaseName::Interpret)?;
    require_current_hash(rows, chain_id, phase, PhaseName::Project)
}

fn require_current_hash(
    rows: &[PhaseStateRow],
    chain_id: &str,
    phase: PhaseName,
    recorded_phase: PhaseName,
) -> RunnerResult<()> {
    let row = row_for(rows, recorded_phase)?;
    match row.input_content_hash.as_deref() {
        Some(hash) if hash == bigname_content_hash::INTERPRETER_CONTENT_HASH => Ok(()),
        Some(hash) => Err(content_hash_mismatch(chain_id, phase, recorded_phase, hash)),
        None => Err(RunnerError::new(
            ErrorKind::ContentHashMismatch,
            format!(
                "refusing derived writes for chain {chain_id} phase {phase}: phase \
                 {recorded_phase} has no recorded interpretation-input hash"
            ),
        )),
    }
}

fn reject_different_hash(
    rows: &[PhaseStateRow],
    chain_id: &str,
    phase: PhaseName,
    recorded_phase: PhaseName,
) -> RunnerResult<()> {
    if let Some(hash) = row_for(rows, recorded_phase)?
        .input_content_hash
        .as_deref()
        .filter(|hash| *hash != bigname_content_hash::INTERPRETER_CONTENT_HASH)
    {
        return Err(content_hash_mismatch(chain_id, phase, recorded_phase, hash));
    }
    Ok(())
}

fn content_hash_mismatch(
    chain_id: &str,
    phase: PhaseName,
    recorded_phase: PhaseName,
    recorded_hash: &str,
) -> RunnerError {
    RunnerError::new(
        ErrorKind::ContentHashMismatch,
        format!(
            "refusing derived writes for chain {chain_id} phase {phase}: binary \
             interpretation-input hash {} differs from recorded {recorded_hash} on phase \
             {recorded_phase}; start a new hash epoch with redo interpret",
            bigname_content_hash::INTERPRETER_CONTENT_HASH
        ),
    )
}

pub(crate) fn invalid_transition(
    chain_id: &str,
    phase: PhaseName,
    current: PhaseStatus,
    next: PhaseStatus,
) -> RunnerError {
    RunnerError::new(
        ErrorKind::InvalidTransition,
        format!("illegal phase transition for chain {chain_id} phase {phase}: {current} -> {next}"),
    )
}
