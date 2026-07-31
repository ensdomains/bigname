use sqlx::PgPool;

use crate::{
    error::{RunnerError, RunnerResult},
    phase::{BlockRange, PhaseName, RunMode},
    state::PhaseStatus,
    transitions::{
        PhaseStateRow, invalid_transition, lock_chain_phase_state, require_start, row_for,
    },
};

pub(crate) struct RedoSession {
    previous: PhaseStateRow,
    interrupted_before_redo: bool,
}

pub(crate) async fn begin(
    pool: &PgPool,
    chain_id: &str,
    phase: PhaseName,
    mode: &RunMode,
) -> RunnerResult<RedoSession> {
    let mut transaction = pool.begin().await.map_err(|error| {
        RunnerError::transient(format!(
            "failed to begin redo transition for chain {chain_id} phase {phase}: {error}"
        ))
    })?;
    let rows = lock_chain_phase_state(&mut transaction, chain_id).await?;
    require_start(&rows, chain_id, phase, mode)?;
    let mut previous = row_for(&rows, phase)?.clone();
    restore_previous_lifecycle(&mut previous)?;
    let status = previous.status()?;
    let recorded_hash = previous.input_content_hash.as_deref();
    let hash_requires_full_redo = recorded_hash
        .is_some_and(|hash| hash != bigname_content_hash::INTERPRETER_CONTENT_HASH)
        || (status != PhaseStatus::Idle && recorded_hash.is_none());
    if matches!(phase, PhaseName::Interpret | PhaseName::Project) && hash_requires_full_redo {
        require_full_hash_redo(&mut transaction, chain_id, phase, mode).await?;
    }
    let range = mode.range().ok_or_else(|| {
        RunnerError::data_integrity("explicit redo transition is missing its block range")
    })?;
    require_recorded_extent(&mut transaction, chain_id, phase, &previous, range).await?;
    require_interrupted_redo_coverage(chain_id, phase, mode, &previous, range)?;
    if !status.can_transition_to(PhaseStatus::Running, true) {
        return Err(invalid_transition(
            chain_id,
            phase,
            status,
            PhaseStatus::Running,
        ));
    }
    let redo_mode = redo_mode(mode)?;
    let resume_same_redo = previous.redo_in_progress
        && previous.redo_mode.as_deref() == Some(redo_mode)
        && previous.redo_from_block_number == Some(range.from)
        && previous.redo_to_block_number == Some(range.to);
    sqlx::query(
        "
        UPDATE chain_phase_state
        SET phase_status = 'running',
            redo_in_progress = true,
            redo_mode = $3,
            redo_previous_phase_status = CASE
                WHEN redo_in_progress THEN redo_previous_phase_status
                ELSE phase_status
            END,
            redo_previous_last_error = CASE
                WHEN redo_in_progress THEN redo_previous_last_error
                ELSE last_error
            END,
            redo_previous_started_at = CASE
                WHEN redo_in_progress THEN redo_previous_started_at
                ELSE started_at
            END,
            redo_previous_finished_at = CASE
                WHEN redo_in_progress THEN redo_previous_finished_at
                ELSE finished_at
            END,
            redo_from_block_number = $4,
            redo_to_block_number = $5,
            redo_current_block_number = CASE WHEN $6 THEN redo_current_block_number END,
            redo_current_block_hash = CASE WHEN $6 THEN redo_current_block_hash END,
            redo_target_block_number = CASE WHEN $6 THEN redo_target_block_number END,
            redo_target_block_hash = CASE WHEN $6 THEN redo_target_block_hash END,
            last_error = NULL,
            started_at = now(),
            finished_at = NULL,
            updated_at = now()
        WHERE chain_id = $1
          AND phase_name = $2
        ",
    )
    .bind(chain_id)
    .bind(phase.as_str())
    .bind(redo_mode)
    .bind(range.from)
    .bind(range.to)
    .bind(resume_same_redo)
    .execute(&mut *transaction)
    .await
    .map_err(|error| {
        RunnerError::transient(format!(
            "failed to start redo for chain {chain_id} phase {phase}: {error}"
        ))
    })?;
    transaction.commit().await.map_err(|error| {
        RunnerError::transient(format!(
            "failed to commit redo start for chain {chain_id} phase {phase}: {error}"
        ))
    })?;
    Ok(RedoSession {
        interrupted_before_redo: matches!(status, PhaseStatus::Running | PhaseStatus::Paused),
        previous,
    })
}

fn restore_previous_lifecycle(previous: &mut PhaseStateRow) -> RunnerResult<()> {
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

fn redo_mode(mode: &RunMode) -> RunnerResult<&'static str> {
    match mode {
        RunMode::Redo(_) => Ok("redo"),
        RunMode::RecomputeFlags(_) => Ok("recompute_flags"),
        RunMode::Normal => Err(RunnerError::data_integrity(
            "normal mode cannot begin an explicit redo",
        )),
    }
}

async fn require_recorded_extent(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    chain_id: &str,
    phase: PhaseName,
    previous: &PhaseStateRow,
    range: BlockRange,
) -> RunnerResult<()> {
    let to = match (previous.current_block_number, previous.target_block_number) {
        (Some(current), Some(target)) => current.max(target),
        (Some(current), None) => current,
        (None, Some(target)) => target,
        (None, None) => {
            return Err(RunnerError::data_integrity(format!(
                "cannot redo chain {chain_id} phase {phase}: the phase has no recorded processed \
                 extent"
            )));
        }
    };
    let from: Option<i64> = sqlx::query_scalar(
        "
        SELECT min(start_block_number)
        FROM ingest_cursors
        WHERE chain_id = $1
        ",
    )
    .bind(chain_id)
    .fetch_one(&mut **transaction)
    .await
    .map_err(|error| {
        RunnerError::transient(format!(
            "failed to load recorded redo extent for chain {chain_id} phase {phase}: {error}"
        ))
    })?;
    if range.to > to || from.is_some_and(|from| range.from < from) {
        let from = from.unwrap_or(0);
        return Err(RunnerError::data_integrity(format!(
            "redo range {}..={} is outside the recorded extent {from}..={to} for chain \
             {chain_id} phase {phase}",
            range.from, range.to
        )));
    }
    Ok(())
}

fn require_interrupted_redo_coverage(
    chain_id: &str,
    phase: PhaseName,
    mode: &RunMode,
    previous: &PhaseStateRow,
    range: BlockRange,
) -> RunnerResult<()> {
    if !previous.redo_in_progress {
        return Ok(());
    }
    let requested_mode = redo_mode(mode)?;
    let interrupted_range = previous
        .redo_from_block_number
        .zip(previous.redo_to_block_number);
    let covers_interrupted_range =
        interrupted_range.is_some_and(|(from, to)| range.from <= from && range.to >= to);
    if previous.redo_mode.as_deref() == Some(requested_mode) && covers_interrupted_range {
        return Ok(());
    }
    let phase_argument = if previous.redo_mode.as_deref() == Some("recompute_flags") {
        "recompute-flags"
    } else {
        phase.as_str()
    };
    let range_argument = interrupted_range
        .map(|(from, to)| format!(" --from-block {from} --to-block {to}"))
        .unwrap_or_default();
    Err(RunnerError::data_integrity(format!(
        "chain {chain_id} phase {phase} has an interrupted redo; rerun \
         `phase-runner redo --chain {chain_id} --phase {phase_argument}{range_argument}` before \
         starting a different redo"
    )))
}

async fn require_full_hash_redo(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    chain_id: &str,
    phase: PhaseName,
    mode: &RunMode,
) -> RunnerResult<()> {
    let bounds: (Option<i64>, Option<i64>) = sqlx::query_as(
        "
        SELECT
            (SELECT min(start_block_number)
             FROM ingest_cursors
             WHERE chain_id = $1),
            (SELECT live_handoff_block_number
             FROM chain_phase_state
             WHERE chain_id = $1
               AND phase_name = 'ingest'
               AND phase_status = 'completed')
        ",
    )
    .bind(chain_id)
    .fetch_one(&mut **transaction)
    .await
    .map_err(|error| {
        RunnerError::transient(format!(
            "failed to load full redo bounds for chain {chain_id}: {error}"
        ))
    })?;
    let (Some(from), Some(to)) = bounds else {
        return Err(RunnerError::new(
            crate::error::ErrorKind::ContentHashMismatch,
            format!(
                "cannot adopt a new interpretation-input hash for chain {chain_id} phase {phase}: \
                 completed ingest bounds are missing"
            ),
        ));
    };
    let Some(range) = mode.range() else {
        return Err(RunnerError::data_integrity(
            "hash adoption requires an explicit redo range",
        ));
    };
    if range.from != from || range.to != to {
        return Err(RunnerError::new(
            crate::error::ErrorKind::ContentHashMismatch,
            format!(
                "cannot adopt a new interpretation-input hash for chain {chain_id} phase {phase} \
                 with range {}..={}; redo the full range {from}..={to}",
                range.from, range.to
            ),
        ));
    }
    Ok(())
}

pub(crate) async fn finish(
    pool: &PgPool,
    chain_id: &str,
    phase: PhaseName,
    session: RedoSession,
    completed: bool,
) -> RunnerResult<()> {
    // A partial redo may already have committed derived writes. Keep its marker and redo cursor
    // durable so normal execution cannot cross that mixed epoch.
    if !completed {
        return Ok(());
    }
    let RedoSession {
        previous,
        interrupted_before_redo,
    } = session;
    let content_hash = if phase.writes_derived_data() {
        Some(bigname_content_hash::INTERPRETER_CONTENT_HASH)
    } else {
        previous.input_content_hash.as_deref()
    };
    sqlx::query(
        "
        UPDATE chain_phase_state
        SET phase_status = CASE WHEN $15 THEN 'failed' ELSE $3 END,
            verification_level = $4,
            current_block_number = $5,
            current_block_hash = $6,
            target_block_number = $7,
            target_block_hash = $8,
            input_content_hash = $9,
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
            live_handoff_block_number = $10,
            live_handoff_block_hash = $11,
            last_error = CASE
                WHEN $15 THEN 'phase was interrupted before redo; resume the normal phase'
                ELSE $12
            END,
            started_at = $13::timestamptz,
            finished_at = CASE WHEN $15 THEN now() ELSE $14::timestamptz END,
            updated_at = now()
        WHERE chain_id = $1
          AND phase_name = $2
        ",
    )
    .bind(chain_id)
    .bind(phase.as_str())
    .bind(previous.phase_status)
    .bind(previous.verification_level)
    .bind(previous.current_block_number)
    .bind(previous.current_block_hash)
    .bind(previous.target_block_number)
    .bind(previous.target_block_hash)
    .bind(content_hash)
    .bind(previous.live_handoff_block_number)
    .bind(previous.live_handoff_block_hash)
    .bind(previous.last_error)
    .bind(previous.started_at)
    .bind(previous.finished_at)
    .bind(interrupted_before_redo)
    .execute(pool)
    .await
    .map_err(|error| {
        RunnerError::transient(format!(
            "failed to restore phase state after redo for chain {chain_id} phase {phase}: {error}"
        ))
    })?;
    Ok(())
}
