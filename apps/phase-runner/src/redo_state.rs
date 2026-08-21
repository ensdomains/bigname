use sqlx::{Connection, PgConnection, PgPool};

use crate::{
    config::SourceConfig,
    error::{RunnerError, RunnerResult},
    phase::{BlockRange, PhaseName, PhaseProgress, RedoAttemptFence, RunMode},
    redo_manifest_audit::{ManifestAuthorityAttestationAudit, matches_active_redo},
    state::PhaseStatus,
    transitions::{
        PhaseStateRow, invalid_transition, lock_chain_phase_state, redo_rerun_instruction,
        require_start, row_for,
    },
};

pub(crate) struct RedoSession {
    previous: PhaseStateRow,
    interrupted_before_redo: bool,
    range: BlockRange,
    attempt_generation: i64,
    recompute_flags: bool,
    required_ingest: bool,
    stage_project_refresh_on_completion: bool,
    pub(crate) manifest_authority_audit: Option<ManifestAuthorityAttestationAudit>,
}

impl RedoSession {
    pub(crate) const fn attempt_fence(&self) -> RedoAttemptFence {
        RedoAttemptFence {
            generation: self.attempt_generation,
            execution_range: self.range,
        }
    }
}
pub(crate) enum RedoOutcome<'a> {
    Completed(&'a PhaseProgress),
    Failed(&'a RunnerError),
}
pub(crate) async fn begin(
    pool: &PgPool,
    chain_id: &str,
    phase: PhaseName,
    mode: &RunMode,
    sources: &[SourceConfig],
    supplied_manifest_authority_generation: Option<&str>,
    attested_by: &str,
) -> RunnerResult<RedoSession> {
    let mut transaction = pool.begin().await.map_err(|error| {
        RunnerError::database(
            format!("failed to begin redo transition for chain {chain_id} phase {phase}"),
            error,
        )
    })?;
    let rows = lock_chain_phase_state(&mut transaction, chain_id).await?;
    let active = row_for(&rows, phase)?;
    crate::redo_recompute::reject_separate_project_run(chain_id, phase, active)?;
    require_start(&rows, chain_id, phase, mode)?;
    let mut previous = row_for(&rows, phase)?.clone();
    let stage_project_refresh_on_completion = phase == PhaseName::Project
        && matches!(mode, RunMode::Redo(_))
        && previous.redo_in_progress
        && previous
            .last_error
            .as_deref()
            .is_some_and(crate::redo_recompute::owns_project_refresh);
    let pending_required_ingest = phase == PhaseName::Ingest
        && previous
            .last_error
            .as_deref()
            .is_some_and(|message| message.starts_with(crate::redo_stamp::REQUIRED_REDO_PREFIX));
    let required_ingest = phase == PhaseName::Ingest
        && previous.redo_in_progress
        && previous
            .last_error
            .as_deref()
            .is_some_and(crate::redo_stamp::owns_required_redo);
    crate::redo_completion::restore_previous_lifecycle(&mut previous)?;
    let status = previous.status()?;
    let current_interpreter_hash = bigname_content_hash::INTERPRETER_CONTENT_HASH;
    let recorded_hash = previous.input_content_hash.as_deref();
    let hash_requires_full_redo = recorded_hash
        .is_some_and(|hash| hash != current_interpreter_hash)
        || (status != PhaseStatus::Idle && recorded_hash.is_none());
    let adopts_new_hash =
        matches!(phase, PhaseName::Interpret | PhaseName::Project) && hash_requires_full_redo;
    if adopts_new_hash {
        require_full_hash_redo(
            &mut transaction,
            chain_id,
            phase,
            mode,
            previous.current_block_number,
            previous.redo_in_progress,
        )
        .await?;
    }
    let range = mode.range().ok_or_else(|| {
        RunnerError::data_integrity("explicit redo transition is missing its block range")
    })?;
    crate::redo_extent::require_recorded_extent(
        &mut transaction,
        chain_id,
        phase,
        &previous,
        range,
        adopts_new_hash,
    )
    .await?;
    let execution_range = if phase == PhaseName::Interpret && matches!(mode, RunMode::Redo(_)) {
        crate::redo_presence::interpret_replay_range(&previous, range)?
    } else {
        range
    };
    let manifest_attestation = if phase == PhaseName::Interpret && matches!(mode, RunMode::Redo(_))
    {
        crate::redo_presence::require_interpret_raw_data(
            &mut transaction,
            chain_id,
            sources,
            execution_range,
            previous.input_content_hash.as_deref(),
            supplied_manifest_authority_generation,
        )
        .await?
    } else {
        None
    };
    require_interrupted_redo_coverage(chain_id, phase, mode, &previous, execution_range)?;
    if !status.can_transition_to(PhaseStatus::Running, true) {
        return Err(invalid_transition(
            chain_id,
            phase,
            status,
            PhaseStatus::Running,
        ));
    }
    let redo_mode = redo_mode(mode)?;
    // The work stamped by manifest synchronization is not yet a resumable
    // operator attempt. Its first explicit execution binds the checkpoint to
    // the exact event/emitter set being loaded; a later crash retry resumes it.
    let same_active_redo =
        matches_active_redo(&previous, redo_mode, execution_range) && !pending_required_ingest;
    let attestation_audit = crate::redo_manifest_audit::record_or_resume(
        &mut transaction,
        chain_id,
        manifest_attestation,
        execution_range,
        same_active_redo,
        supplied_manifest_authority_generation,
        attested_by,
    )
    .await?;
    let same_active_audit = attestation_audit
        .as_ref()
        .is_some_and(ManifestAuthorityAttestationAudit::replayed);
    let (current_ingest_authority, ingest_authority_changed) =
        crate::redo_manifest_authority::for_redo_begin(
            &mut transaction,
            chain_id,
            phase,
            same_active_redo,
            previous.redo_manifest_authority_fingerprint.as_deref(),
        )
        .await?;
    let resume_same_epoch = same_active_redo
        && if phase == PhaseName::Ingest {
            !ingest_authority_changed
        } else {
            !phase.writes_derived_data() || recorded_hash == Some(current_interpreter_hash)
        };
    let preserve_started_at = resume_same_epoch || same_active_audit;
    let attempt_generation = sqlx::query_scalar::<_, i64>(
        "
        UPDATE chain_phase_state
        SET phase_status = 'running',
            redo_in_progress = true,
            redo_attempt_generation = redo_attempt_generation + 1,
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
            redo_source_boundary_markers = CASE
                WHEN $6 THEN redo_source_boundary_markers
            END,
            redo_manifest_authority_fingerprint = CASE
                WHEN phase_name = 'ingest' THEN $13
            END,
            input_content_hash = CASE WHEN $8 THEN $9 ELSE input_content_hash END,
            last_error = CASE
                WHEN last_error LIKE $10
                    THEN $11 || substring(last_error FROM char_length($12) + 1)
                WHEN redo_in_progress THEN last_error
            END,
            started_at = CASE WHEN $7 THEN started_at ELSE now() END,
            finished_at = NULL,
            updated_at = now()
        WHERE chain_id = $1
          AND phase_name = $2
        RETURNING redo_attempt_generation
        ",
    )
    .bind(chain_id)
    .bind(phase.as_str())
    .bind(redo_mode)
    .bind(execution_range.from)
    .bind(execution_range.to)
    .bind(resume_same_epoch)
    .bind(preserve_started_at)
    .bind(phase.writes_derived_data())
    .bind(current_interpreter_hash)
    .bind(format!("{}%", crate::redo_stamp::REQUIRED_REDO_PREFIX))
    .bind(crate::redo_stamp::REQUIRED_REDO_ACTIVE_PREFIX)
    .bind(crate::redo_stamp::REQUIRED_REDO_PREFIX)
    .bind(current_ingest_authority.as_deref())
    .fetch_one(&mut *transaction)
    .await
    .map_err(|error| {
        RunnerError::database(
            format!("failed to start redo for chain {chain_id} phase {phase}"),
            error,
        )
    })?;
    transaction.commit().await.map_err(|error| {
        RunnerError::database(
            format!("failed to commit redo start for chain {chain_id} phase {phase}"),
            error,
        )
    })?;
    crate::redo_manifest_authority::reject_changed(
        ingest_authority_changed,
        chain_id,
        execution_range,
    )?;
    Ok(RedoSession {
        interrupted_before_redo: matches!(status, PhaseStatus::Running | PhaseStatus::Paused),
        previous,
        range: execution_range,
        attempt_generation,
        recompute_flags: matches!(mode, RunMode::RecomputeFlags(_)),
        required_ingest,
        stage_project_refresh_on_completion,
        manifest_authority_audit: attestation_audit,
    })
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
    let instruction = redo_rerun_instruction(
        chain_id,
        phase,
        previous.redo_mode.as_deref(),
        interrupted_range.map(|(from, to)| BlockRange { from, to }),
    );
    Err(RunnerError::data_integrity(format!(
        "chain {chain_id} phase {phase} has an interrupted redo; {instruction} before starting a \
         different redo"
    )))
}

async fn require_full_hash_redo(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    chain_id: &str,
    phase: PhaseName,
    mode: &RunMode,
    recorded_head: Option<i64>,
    interrupted_redo: bool,
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
        RunnerError::database(
            format!("failed to load full redo bounds for chain {chain_id}"),
            error,
        )
    })?;
    let (Some(from), Some(mut to)) = bounds else {
        return Err(RunnerError::new(
            crate::error::ErrorKind::ContentHashMismatch,
            format!(
                "cannot adopt a new interpretation-input hash for chain {chain_id} phase {phase}: \
                 completed ingest bounds are missing"
            ),
        ));
    };
    if phase == PhaseName::Project || interrupted_redo {
        to = to.max(recorded_head.unwrap_or(to));
    }
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
    lock_connection: &mut PgConnection,
    chain_id: &str,
    phase: PhaseName,
    session: RedoSession,
    outcome: RedoOutcome<'_>,
) -> RunnerResult<()> {
    let progress = match outcome {
        RedoOutcome::Completed(progress) => progress,
        RedoOutcome::Failed(error) => {
            crate::redo_failure::record(lock_connection, chain_id, phase, error).await?;
            return Ok(());
        }
    };
    crate::state_persistence::validate_progress(phase, progress, true)?;
    let verification_level = if phase == PhaseName::Verify {
        progress
            .verification_level
            .map(|level| level.as_str().to_owned())
    } else {
        session.previous.verification_level.clone()
    };
    let RedoSession {
        previous,
        interrupted_before_redo,
        range,
        recompute_flags,
        required_ingest,
        stage_project_refresh_on_completion,
        ..
    } = session;
    if required_ingest
        && let Err(error) = crate::redo_required_boundary::require_readable(
            lock_connection,
            chain_id,
            range,
            progress,
        )
        .await
    {
        crate::redo_failure::record(lock_connection, chain_id, phase, &error).await?;
        return Err(error);
    }
    let content_hash = if phase.writes_derived_data() {
        Some(bigname_content_hash::INTERPRETER_CONTENT_HASH)
    } else {
        previous.input_content_hash.as_deref()
    };
    let restored_current_hash = crate::redo_completion::replacement_hash(
        previous.current_block_number,
        previous.current_block_hash.as_deref(),
        progress.current.as_ref(),
    );
    let restored_target_hash = crate::redo_completion::replacement_hash(
        previous.target_block_number,
        previous.target_block_hash.as_deref(),
        progress.target.as_ref(),
    );
    let mut transaction = lock_connection.begin().await.map_err(|error| {
        RunnerError::database(
            format!("failed to begin redo completion for chain {chain_id} phase {phase}"),
            error,
        )
    })?;
    if let crate::redo_completion::CompletionCoverage::Widened(persisted) =
        crate::redo_completion::lock_completion_coverage(
            &mut transaction,
            chain_id,
            phase,
            range,
            recompute_flags,
        )
        .await?
    {
        transaction.commit().await.map_err(|error| {
            RunnerError::database(
                format!(
                    "failed to preserve widened redo completion for chain {chain_id} phase {phase}"
                ),
                error,
            )
        })?;
        tracing::warn!(
            chain_id,
            phase = %phase,
            from_block = persisted.from,
            to_block = persisted.to,
            "redo range widened while the phase was running; preserved the full marker"
        );
        return Ok(());
    }
    if stage_project_refresh_on_completion {
        crate::redo_recompute::stage_project_refresh(
            &mut transaction,
            chain_id,
            crate::redo_recompute::ProjectRefreshCompletion {
                previous: &previous,
                verification_level: verification_level.as_deref(),
                current_hash: restored_current_hash,
                target_hash: restored_target_hash,
                content_hash,
            },
        )
        .await?;
        transaction.commit().await.map_err(|error| {
            RunnerError::database(
                format!(
                    "failed to commit staged recompute-flags project refresh for chain {chain_id}"
                ),
                error,
            )
        })?;
        return Ok(());
    }
    let recompute_summary = if recompute_flags && phase == PhaseName::Interpret {
        Some(crate::redo_recompute::finalize_metadata(&mut transaction, chain_id, range).await?)
    } else {
        None
    };
    let result = sqlx::query(
        "
        UPDATE chain_phase_state
        SET phase_status = CASE WHEN $15 THEN 'failed' ELSE $3 END,
            verification_level = $4,
            settled_while_unconfigured = CASE WHEN NOT $15 AND $5 IS NOT NULL AND $7 IS NOT NULL AND $5 = $7 AND $6 IS NOT NULL AND $8 IS NOT NULL AND $6 = $8 AND (phase_name != 'verify' OR $4 IS NOT NULL) AND (phase_name != 'ingest' OR ($10 IS NOT NULL AND $11 IS NOT NULL AND $10 = $5 AND $11 = $6)) THEN NULL ELSE settled_while_unconfigured END,
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
            redo_source_boundary_markers = NULL,
            redo_manifest_authority_fingerprint = NULL,
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
    .bind(verification_level)
    .bind(previous.current_block_number)
    .bind(restored_current_hash)
    .bind(previous.target_block_number)
    .bind(restored_target_hash)
    .bind(content_hash)
    .bind(previous.live_handoff_block_number)
    .bind(previous.live_handoff_block_hash)
    .bind(previous.last_error)
    .bind(previous.started_at)
    .bind(previous.finished_at)
    .bind(interrupted_before_redo)
    .execute(&mut *transaction)
    .await
    .map_err(|error| {
        RunnerError::database(
            format!("failed to restore phase state after redo for chain {chain_id} phase {phase}"),
            error,
        )
    })?;
    if result.rows_affected() != 1 {
        return Err(RunnerError::data_integrity(format!(
            "redo completion requires phase state for chain {chain_id} phase {phase}"
        )));
    }
    if phase == PhaseName::Ingest {
        crate::state_ingest_progress::reconcile_redo_boundary_cursors(
            &mut transaction,
            chain_id,
            range,
            progress,
        )
        .await?;
    }
    if phase == PhaseName::Interpret && !recompute_flags {
        crate::redo_stamp::stamp_required_in_transaction(
            &mut transaction,
            chain_id,
            PhaseName::Project,
            range,
            "interpret redo completed",
        )
        .await?;
    }
    if phase == PhaseName::Interpret && recompute_flags {
        crate::redo_recompute::clear_staged_project_refresh(&mut transaction, chain_id).await?;
    }
    let stamped_ranges = crate::redo_recompute::stamp_transitions_and_load_ranges(
        &mut transaction,
        chain_id,
        recompute_summary,
    )
    .await?;
    transaction.commit().await.map_err(|error| {
        RunnerError::database(
            format!("failed to commit redo completion for chain {chain_id} phase {phase}"),
            error,
        )
    })?;
    crate::redo_recompute::report(chain_id, recompute_summary, &stamped_ranges);
    Ok(())
}
