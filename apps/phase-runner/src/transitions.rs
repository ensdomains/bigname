use sqlx::{FromRow, Postgres, Transaction};

use crate::{
    error::{ErrorKind, RunnerError, RunnerResult},
    phase::{BlockRange, PhaseName, RunMode},
    state::PhaseStatus,
};

/// The content hash a phase row records: Project and Live write projected rows and record the
/// projection content hash, every other phase the interpreter content hash.
pub fn expected_content_hash(phase: PhaseName) -> &'static str {
    match phase {
        PhaseName::Project | PhaseName::Live => bigname_content_hash::PROJECTION_CONTENT_HASH,
        PhaseName::Ingest | PhaseName::Verify | PhaseName::Interpret => {
            bigname_content_hash::INTERPRETER_CONTENT_HASH
        }
    }
}

#[derive(Clone, Debug, FromRow)]
pub(crate) struct PhaseStateRow {
    pub phase_name: String,
    pub phase_status: String,
    pub verification_level: Option<String>,
    pub settled_while_unconfigured: Option<bool>,
    pub current_block_number: Option<i64>,
    pub current_block_hash: Option<String>,
    pub target_block_number: Option<i64>,
    pub target_block_hash: Option<String>,
    pub input_content_hash: Option<String>,
    pub redo_in_progress: bool,
    pub redo_attempt_generation: i64,
    pub redo_mode: Option<String>,
    pub redo_previous_phase_status: Option<String>,
    pub redo_previous_last_error: Option<String>,
    pub redo_previous_started_at: Option<String>,
    pub redo_previous_finished_at: Option<String>,
    pub redo_from_block_number: Option<i64>,
    pub redo_to_block_number: Option<i64>,
    pub redo_requested_from_block_number: Option<i64>,
    pub redo_requested_to_block_number: Option<i64>,
    pub redo_manifest_authority_fingerprint: Option<String>,
    pub live_handoff_block_number: Option<i64>,
    pub live_handoff_block_hash: Option<String>,
    pub last_error: Option<String>,
    pub started_at: Option<String>,
    pub finished_at: Option<String>,
}

impl PhaseStateRow {
    pub(crate) fn requested_redo_range(&self) -> Option<BlockRange> {
        self.redo_requested_from_block_number
            .or(self.redo_from_block_number)
            .zip(
                self.redo_requested_to_block_number
                    .or(self.redo_to_block_number),
            )
            .map(|(from, to)| BlockRange { from, to })
    }

    pub(crate) fn status(&self) -> RunnerResult<PhaseStatus> {
        self.phase_status.parse()
    }

    pub(crate) fn ingest_completion_is_incomplete(&self) -> bool {
        self.live_handoff_block_number.is_none()
            || self.live_handoff_block_number != self.target_block_number
            || self.live_handoff_block_hash != self.target_block_hash
            || self.current_block_number != self.target_block_number
            || self.current_block_hash != self.target_block_hash
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
               settled_while_unconfigured,
               current_block_number,
               current_block_hash,
               target_block_number,
               target_block_hash,
               input_content_hash,
               redo_in_progress,
               redo_attempt_generation,
               redo_mode,
               redo_previous_phase_status,
               redo_previous_last_error,
               redo_previous_started_at::text AS redo_previous_started_at,
               redo_previous_finished_at::text AS redo_previous_finished_at,
               redo_from_block_number,
               redo_to_block_number,
               redo_requested_from_block_number,
               redo_requested_to_block_number,
               redo_manifest_authority_fingerprint,
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

pub(crate) async fn require_start(
    transaction: &mut Transaction<'_, Postgres>,
    rows: &[PhaseStateRow],
    chain_id: &str,
    phase: PhaseName,
    mode: &RunMode,
) -> RunnerResult<()> {
    require_no_interrupted_redo(rows, chain_id, phase, mode)?;
    require_completed_ingest_redo(rows, chain_id, phase, mode)?;
    require_compatible_active_phase(rows, chain_id, phase, mode)?;
    require_prerequisite(rows, chain_id, phase)?;
    if phase.writes_derived_data()
        && let Some(stale) = stale_content_hash(rows, phase, mode)?
    {
        return Err(crate::content_hash_refusal::refusal(
            transaction,
            rows,
            chain_id,
            phase,
            stale,
        )
        .await?);
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
        row.requested_redo_range(),
    );
    Err(RunnerError::new(
        ErrorKind::InvalidTransition,
        format!(
            "cannot resume normal phase {phase} for chain {chain_id}: an explicit redo was \
             interrupted; {instruction}"
        ),
    ))
}

fn require_completed_ingest_redo(
    rows: &[PhaseStateRow],
    chain_id: &str,
    phase: PhaseName,
    mode: &RunMode,
) -> RunnerResult<()> {
    if !matches!(mode, RunMode::Redo(_) | RunMode::RecomputeFlags(_))
        || !matches!(phase, PhaseName::Interpret | PhaseName::Project)
    {
        return Ok(());
    }
    let ingest = row_for(rows, PhaseName::Ingest)?;
    if !is_required_downstream_redo(ingest) {
        return Ok(());
    }
    let range = ingest
        .redo_from_block_number
        .zip(ingest.redo_to_block_number)
        .ok_or_else(|| {
            RunnerError::data_integrity(format!(
                "required Ingest redo for chain {chain_id} is missing its persisted range"
            ))
        })
        .and_then(|(from, to)| BlockRange::new(from, to))?;
    Err(required_ingest_redo_error(chain_id, range))
}

pub(crate) fn required_ingest_redo_error(chain_id: &str, range: BlockRange) -> RunnerError {
    RunnerError::data_integrity(format!(
        "manifest watch plan widened over already-ingested blocks for chain {chain_id}; \
         automatic re-ingest is disabled because historical fetch cost is an operator decision; \
         rerun `phase-runner redo --chain {chain_id} --phase ingest --from-block {} \
         --to-block {}` with the configured sources before derivation",
        range.from, range.to
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
    let options = redo_rerun_options(if redo_mode == Some("recompute_flags") {
        RedoRerun::RecomputeFlags
    } else {
        RedoRerun::Phase(phase)
    });
    format!(
        "rerun `phase-runner redo --chain {chain_id} --phase {phase_argument}{range_argument}`\
         {options}"
    )
}

pub(crate) enum RedoRerun {
    Phase(PhaseName),
    All,
    RecomputeFlags,
}

/// The stamp records neither the sources nor the verifier URL, so rerun
/// instructions name what they cannot fill. Bounded replay needs no hydration RPC.
pub(crate) fn redo_rerun_options(rerun: RedoRerun) -> &'static str {
    match rerun {
        RedoRerun::RecomputeFlags => "",
        RedoRerun::All | RedoRerun::Phase(PhaseName::Verify) => {
            " with the chain's configured sources as --source options and \
             --verification-database-url"
        }
        RedoRerun::Phase(_) => " with the chain's configured sources as --source options",
    }
}

fn require_compatible_active_phase(
    rows: &[PhaseStateRow],
    chain_id: &str,
    phase: PhaseName,
    mode: &RunMode,
) -> RunnerResult<()> {
    let required_ingest_recovery = matches!(phase, PhaseName::Ingest | PhaseName::Live)
        && is_required_downstream_redo(row_for(rows, PhaseName::Ingest)?);
    // An Interpret redo starting an interpreter content-hash rotation supersedes a Project redo of
    // another projection content hash.
    let superseded = crate::redo_supersede::superseded_project_redo(rows, phase, mode)?;
    for row in rows {
        let other: PhaseName = row.phase_name.parse()?;
        if other == phase {
            continue;
        }
        let status = row.status()?;
        if matches!(status, PhaseStatus::Running | PhaseStatus::Paused)
            && !verify_live_work_pair(phase, other)
            && !is_pending_required_downstream_redo(row)
            && !required_ingest_recovery_can_precede_interrupted_derived(
                required_ingest_recovery,
                other,
                row,
            )
            && !recompute_can_queue_behind_project_redo(phase, other, mode, row)
            && superseded.is_none_or(|superseded| superseded.phase_name != row.phase_name)
        {
            return Err(RunnerError::new(
                ErrorKind::InvalidTransition,
                format!(
                    "cannot start phase {phase} for chain {chain_id} while phase {other} is \
                     {status}{}",
                    crate::content_hash_refusal::stale_project_redo_instruction(
                        chain_id, rows, row
                    )
                ),
            ));
        }
    }
    Ok(())
}

fn required_ingest_recovery_can_precede_interrupted_derived(
    required_ingest_recovery: bool,
    other: PhaseName,
    row: &PhaseStateRow,
) -> bool {
    // Manifest sync may widen Ingest while preserving an interrupted downstream redo. The
    // prerequisite repair, including raw Live catch-up after a rewind, must run first; the
    // advisory locks proved neither retained redo still has a writer.
    required_ingest_recovery
        && row.redo_in_progress
        && matches!(other, PhaseName::Interpret | PhaseName::Project)
}

fn recompute_can_queue_behind_project_redo(
    phase: PhaseName,
    other: PhaseName,
    mode: &RunMode,
    row: &PhaseStateRow,
) -> bool {
    phase == PhaseName::Interpret
        && other == PhaseName::Project
        && matches!(mode, RunMode::RecomputeFlags(_))
        && row.redo_in_progress
        && row.redo_mode.as_deref() == Some("redo")
}

fn verify_live_work_pair(left: PhaseName, right: PhaseName) -> bool {
    matches!(
        (left, right),
        (
            PhaseName::Verify,
            PhaseName::Live | PhaseName::Interpret | PhaseName::Project
        ) | (
            PhaseName::Live | PhaseName::Interpret | PhaseName::Project,
            PhaseName::Verify
        )
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
    if phase == PhaseName::Live
        && ((status == PhaseStatus::Running
            && is_pending_required_downstream_redo(row_for(rows, prerequisite)?))
            || is_required_downstream_redo(row_for(rows, PhaseName::Ingest)?))
    {
        return Ok(());
    }
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

pub(crate) fn is_pending_required_downstream_redo(row: &PhaseStateRow) -> bool {
    row.redo_in_progress
        && row
            .last_error
            .as_deref()
            .is_some_and(|reason| reason.starts_with(crate::redo_stamp::REQUIRED_REDO_PREFIX))
}

fn is_required_downstream_redo(row: &PhaseStateRow) -> bool {
    row.redo_in_progress
        && row
            .last_error
            .as_deref()
            .is_some_and(crate::redo_stamp::owns_required_redo)
}

/// The first phase row this start needs at its expected content hash that records another, if
/// any. An Interpret row is checked before a Project row, so a stale Project row is reported only
/// while Interpret is current.
fn stale_content_hash(
    rows: &[PhaseStateRow],
    phase: PhaseName,
    mode: &RunMode,
) -> RunnerResult<Option<StaleHash>> {
    let both = || Ok(current(rows, PhaseName::Interpret)?.or(current(rows, PhaseName::Project)?));
    match mode {
        RunMode::Redo(_) if phase == PhaseName::Interpret => Ok(None),
        RunMode::Redo(_) if phase == PhaseName::Project => current(rows, PhaseName::Interpret),
        RunMode::Redo(_) if phase == PhaseName::Live => both(),
        RunMode::RecomputeFlags(_) => both(),
        RunMode::Normal if phase == PhaseName::Interpret => different(rows, PhaseName::Interpret),
        RunMode::Normal if phase == PhaseName::Project => {
            Ok(current(rows, PhaseName::Interpret)?.or(different(rows, PhaseName::Project)?))
        }
        RunMode::Normal if phase == PhaseName::Live && live_can_fill_required_redo(rows)? => {
            Ok(None)
        }
        RunMode::Normal if phase == PhaseName::Live => both(),
        _ => Ok(None),
    }
}

fn live_can_fill_required_redo(rows: &[PhaseStateRow]) -> RunnerResult<bool> {
    // A rewind can leave a required raw or interpreted range above the published head. Live must
    // be able to fetch and publish that raw suffix before the required Ingest command or Interpret
    // replay can run. Required work fences existing derived state; a chain that has not derived
    // anything yet has no serving state to expose.
    Ok(
        is_required_downstream_redo(row_for(rows, PhaseName::Interpret)?)
            || is_required_downstream_redo(row_for(rows, PhaseName::Ingest)?),
    )
}

/// A phase row that records another content hash than this binary expects of it, or none.
pub(crate) struct StaleHash {
    pub(crate) recorded_phase: PhaseName,
    pub(crate) recorded: Option<String>,
}

/// The phase's row unless it records exactly its expected content hash.
fn current(rows: &[PhaseStateRow], recorded_phase: PhaseName) -> RunnerResult<Option<StaleHash>> {
    let recorded = row_for(rows, recorded_phase)?.input_content_hash.as_deref();
    Ok(
        (recorded != Some(expected_content_hash(recorded_phase))).then(|| StaleHash {
            recorded_phase,
            recorded: recorded.map(str::to_owned),
        }),
    )
}

/// The phase's row when it records a content hash other than its expected one.
fn different(rows: &[PhaseStateRow], recorded_phase: PhaseName) -> RunnerResult<Option<StaleHash>> {
    Ok(current(rows, recorded_phase)?.filter(|stale| stale.recorded.is_some()))
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
