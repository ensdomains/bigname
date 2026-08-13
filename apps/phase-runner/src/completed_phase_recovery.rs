use crate::{
    error::{COMPLETED_VALIDATION_FAILURE_PREFIX, RunnerError, RunnerResult},
    phase::PhaseName,
    state::{PhaseStatus, PhaseStore},
    transitions::{PhaseStateRow, lock_chain_phase_state, row_for},
};

#[derive(sqlx::FromRow)]
struct CompletedRecoveryState {
    phase_status: String,
    verification_level: Option<String>,
    current_block_number: Option<i64>,
    current_block_hash: Option<String>,
    target_block_number: Option<i64>,
    target_block_hash: Option<String>,
    live_handoff_block_number: Option<i64>,
    live_handoff_block_hash: Option<String>,
    last_error: Option<String>,
}

impl CompletedRecoveryState {
    fn from_locked(row: &PhaseStateRow) -> Self {
        Self {
            phase_status: row.phase_status.clone(),
            verification_level: row.verification_level.clone(),
            current_block_number: row.current_block_number,
            current_block_hash: row.current_block_hash.clone(),
            target_block_number: row.target_block_number,
            target_block_hash: row.target_block_hash.clone(),
            live_handoff_block_number: row.live_handoff_block_number,
            live_handoff_block_hash: row.live_handoff_block_hash.clone(),
            last_error: row.last_error.clone(),
        }
    }

    fn is_retained_completed_validation(&self, phase: PhaseName) -> bool {
        if self.phase_status != PhaseStatus::Failed.as_str()
            || !self
                .last_error
                .as_deref()
                .is_some_and(|error| error.starts_with(COMPLETED_VALIDATION_FAILURE_PREFIX))
        {
            return false;
        }
        let completed_extent = self.current_block_number.is_some()
            && self.current_block_number == self.target_block_number
            && self.current_block_hash.is_some()
            && self.current_block_hash == self.target_block_hash;
        match phase {
            PhaseName::Ingest => {
                completed_extent
                    && self.live_handoff_block_number == self.target_block_number
                    && self.live_handoff_block_hash == self.target_block_hash
            }
            PhaseName::Verify => completed_extent && self.verification_level.is_some(),
            PhaseName::Interpret | PhaseName::Project | PhaseName::Live => false,
        }
    }
}

impl PhaseStore {
    pub(crate) async fn fail_completed_validation(
        &self,
        chain_id: &str,
        phase: PhaseName,
        message: &str,
    ) -> RunnerResult<()> {
        if !matches!(phase, PhaseName::Ingest | PhaseName::Verify)
            || !message.starts_with(COMPLETED_VALIDATION_FAILURE_PREFIX)
        {
            return Err(RunnerError::data_integrity(format!(
                "completed-validation failure is not valid for chain {chain_id} phase {phase}"
            )));
        }
        let mut transaction = self.pool().begin().await.map_err(|error| {
            RunnerError::database(
                format!(
                    "failed to begin completed-validation failure for chain {chain_id} phase \
                     {phase}"
                ),
                error,
            )
        })?;
        let rows = lock_chain_phase_state(&mut transaction, chain_id).await?;
        let row = row_for(&rows, phase)?;
        if row.status()? != PhaseStatus::Completed {
            return Err(RunnerError::data_integrity(format!(
                "cannot record completed-validation failure for chain {chain_id} phase {phase} \
                 unless it is completed"
            )));
        }
        sqlx::query(
            "UPDATE chain_phase_state
             SET phase_status = 'failed', last_error = $3,
                 finished_at = now(), updated_at = now()
             WHERE chain_id = $1 AND phase_name = $2",
        )
        .bind(chain_id)
        .bind(phase.as_str())
        .bind(message)
        .execute(&mut *transaction)
        .await
        .map_err(|error| {
            RunnerError::database(
                format!(
                    "failed to record completed-validation failure for chain {chain_id} phase \
                     {phase}"
                ),
                error,
            )
        })?;
        transaction.commit().await.map_err(|error| {
            RunnerError::database(
                format!(
                    "failed to commit completed-validation failure for chain {chain_id} phase \
                     {phase}"
                ),
                error,
            )
        })?;
        Ok(())
    }

    pub(crate) async fn complete_revalidated_phase(
        &self,
        chain_id: &str,
        phase: PhaseName,
    ) -> RunnerResult<()> {
        let mut transaction = self.pool().begin().await.map_err(|error| {
            RunnerError::database(
                format!(
                    "failed to begin revalidated phase completion for chain {chain_id} phase \
                     {phase}"
                ),
                error,
            )
        })?;
        let rows = lock_chain_phase_state(&mut transaction, chain_id).await?;
        let row = row_for(&rows, phase)?;
        let current = row.status()?;
        let retained_completion = current == PhaseStatus::Failed
            && CompletedRecoveryState::from_locked(row).is_retained_completed_validation(phase);
        if !retained_completion {
            return Err(RunnerError::data_integrity(format!(
                "cannot complete revalidated phase {phase} for chain {chain_id} without its \
                 retained completed-validation failure"
            )));
        }
        sqlx::query(
            "UPDATE chain_phase_state
             SET phase_status = 'running', last_error = NULL,
                 started_at = now(), finished_at = NULL, updated_at = now()
             WHERE chain_id = $1 AND phase_name = $2",
        )
        .bind(chain_id)
        .bind(phase.as_str())
        .execute(&mut *transaction)
        .await
        .map_err(|error| {
            RunnerError::database(
                format!("failed to restart revalidated phase {phase} for chain {chain_id}"),
                error,
            )
        })?;
        sqlx::query(
            "UPDATE chain_phase_state
             SET phase_status = 'completed', finished_at = now(), updated_at = now()
             WHERE chain_id = $1 AND phase_name = $2",
        )
        .bind(chain_id)
        .bind(phase.as_str())
        .execute(&mut *transaction)
        .await
        .map_err(|error| {
            RunnerError::database(
                format!("failed to complete revalidated phase {phase} for chain {chain_id}"),
                error,
            )
        })?;
        transaction.commit().await.map_err(|error| {
            RunnerError::database(
                format!(
                    "failed to commit revalidated phase completion for chain {chain_id} phase \
                     {phase}"
                ),
                error,
            )
        })?;
        Ok(())
    }

    pub(crate) async fn pending_completed_validation(
        &self,
        chain_id: &str,
        phase: PhaseName,
    ) -> RunnerResult<bool> {
        let row: Option<CompletedRecoveryState> = sqlx::query_as(
            "SELECT phase_status, verification_level,
                    current_block_number, current_block_hash,
                    target_block_number, target_block_hash,
                    live_handoff_block_number, live_handoff_block_hash,
                    last_error
             FROM chain_phase_state
             WHERE chain_id = $1 AND phase_name = $2",
        )
        .bind(chain_id)
        .bind(phase.as_str())
        .fetch_optional(self.pool())
        .await
        .map_err(|error| {
            RunnerError::database(
                format!(
                    "failed to load completed-validation recovery state for chain {chain_id} \
                     phase {phase}"
                ),
                error,
            )
        })?;
        Ok(row.is_some_and(|row| row.is_retained_completed_validation(phase)))
    }
}

pub(crate) fn locked_completed_validation_recovery(row: &PhaseStateRow, phase: PhaseName) -> bool {
    CompletedRecoveryState::from_locked(row).is_retained_completed_validation(phase)
}
