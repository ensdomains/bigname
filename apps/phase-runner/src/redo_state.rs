use sqlx::PgPool;

use crate::{
    error::{RunnerError, RunnerResult},
    phase::{PhaseName, RunMode},
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
    let previous = row_for(&rows, phase)?.clone();
    let status = previous.status()?;
    let recorded_hash = previous.input_content_hash.as_deref();
    let hash_requires_full_redo = recorded_hash
        .is_some_and(|hash| hash != bigname_content_hash::INTERPRETER_CONTENT_HASH)
        || (status == PhaseStatus::Completed && recorded_hash.is_none());
    if matches!(phase, PhaseName::Interpret | PhaseName::Project) && hash_requires_full_redo {
        require_full_hash_redo(&mut transaction, chain_id, phase, mode).await?;
    }
    if !status.can_transition_to(PhaseStatus::Running, true) {
        return Err(invalid_transition(
            chain_id,
            phase,
            status,
            PhaseStatus::Running,
        ));
    }
    let active_hash = if phase.writes_derived_data() {
        Some(bigname_content_hash::INTERPRETER_CONTENT_HASH)
    } else {
        previous.input_content_hash.as_deref()
    };
    sqlx::query(
        "
        UPDATE chain_phase_state
        SET phase_status = 'running',
            input_content_hash = $3,
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
    .bind(active_hash)
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
    let RedoSession {
        previous,
        interrupted_before_redo,
    } = session;
    let content_hash = if completed && phase.writes_derived_data() {
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
