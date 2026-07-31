use std::{fmt, str::FromStr};

use sqlx::PgPool;

use crate::{
    config::SourceConfig,
    error::{ErrorKind, RunnerError, RunnerResult},
    heads::BlockMarker,
    phase::{PhaseName, PhaseProgress, PhaseResume, RunMode},
    redo_state::{self, RedoSession},
    state_persistence::{
        load_phase_resume, load_redo_resume, update_ingest_cursors, update_progress,
        update_redo_progress,
    },
    transitions::{invalid_transition, lock_chain_phase_state, require_start, row_for},
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PhaseStatus {
    Idle,
    Running,
    Paused,
    Completed,
    Failed,
}

impl PhaseStatus {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Idle => "idle",
            Self::Running => "running",
            Self::Paused => "paused",
            Self::Completed => "completed",
            Self::Failed => "failed",
        }
    }

    pub const fn can_transition_to(self, next: Self, explicit_redo: bool) -> bool {
        matches!(
            (self, next),
            (Self::Idle, Self::Running)
                | (Self::Running, Self::Running)
                | (Self::Running, Self::Paused)
                | (Self::Running, Self::Completed)
                | (Self::Running, Self::Failed)
                | (Self::Paused, Self::Running)
                | (Self::Paused, Self::Completed)
                | (Self::Paused, Self::Failed)
                | (Self::Failed, Self::Running)
        ) || (explicit_redo && matches!((self, next), (Self::Completed, Self::Running)))
    }
}

impl fmt::Display for PhaseStatus {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl FromStr for PhaseStatus {
    type Err = RunnerError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "idle" => Ok(Self::Idle),
            "running" => Ok(Self::Running),
            "paused" => Ok(Self::Paused),
            "completed" => Ok(Self::Completed),
            "failed" => Ok(Self::Failed),
            _ => Err(RunnerError::data_integrity(format!(
                "database contains unknown phase status {value:?}"
            ))),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StartDisposition {
    Started,
    AlreadyCompleted,
}

#[derive(Clone)]
pub struct PhaseStore {
    pool: PgPool,
}

impl PhaseStore {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    pub async fn initialize_chain(&self, chain_id: &str) -> RunnerResult<()> {
        for phase in PhaseName::ALL {
            sqlx::query(
                "
                INSERT INTO chain_phase_state (chain_id, phase_name)
                VALUES ($1, $2)
                ON CONFLICT (chain_id, phase_name) DO NOTHING
                ",
            )
            .bind(chain_id)
            .bind(phase.as_str())
            .execute(&self.pool)
            .await
            .map_err(|error| {
                RunnerError::transient(format!(
                    "failed to initialize phase {phase} for chain {chain_id}: {error}"
                ))
            })?;
        }
        Ok(())
    }

    pub async fn start_phase(
        &self,
        chain_id: &str,
        phase: PhaseName,
        mode: &RunMode,
    ) -> RunnerResult<StartDisposition> {
        if mode.is_redo() {
            return Err(RunnerError::new(
                ErrorKind::Configuration,
                "redo transitions must be driven through PhaseRunner::redo",
            ));
        }
        let mut transaction = self.pool.begin().await.map_err(|error| {
            RunnerError::transient(format!(
                "failed to begin phase transition for chain {chain_id} phase {phase}: {error}"
            ))
        })?;
        let rows = lock_chain_phase_state(&mut transaction, chain_id).await?;
        let status = row_for(&rows, phase)?.status()?;
        let restarts_completed = phase == PhaseName::Live;
        if status == PhaseStatus::Completed && !restarts_completed {
            transaction.commit().await.map_err(|error| {
                RunnerError::transient(format!(
                    "failed to finish completed-phase check for chain {chain_id} phase {phase}: \
                     {error}"
                ))
            })?;
            return Ok(StartDisposition::AlreadyCompleted);
        }
        require_start(&rows, chain_id, phase, mode)?;
        if !status.can_transition_to(PhaseStatus::Running, restarts_completed) {
            return Err(invalid_transition(
                chain_id,
                phase,
                status,
                PhaseStatus::Running,
            ));
        }
        let resume_position = status != PhaseStatus::Idle;
        sqlx::query(
            "
            UPDATE chain_phase_state
            SET phase_status = 'running',
                verification_level = CASE
                    WHEN phase_name = 'verify' AND $4 THEN verification_level
                    ELSE NULL
                END,
                current_block_number = CASE WHEN $4 THEN current_block_number ELSE NULL END,
                current_block_hash = CASE WHEN $4 THEN current_block_hash ELSE NULL END,
                target_block_number = CASE WHEN $4 THEN target_block_number ELSE NULL END,
                target_block_hash = CASE WHEN $4 THEN target_block_hash ELSE NULL END,
                input_content_hash = $3,
                live_handoff_block_number = CASE
                    WHEN phase_name = 'ingest' AND NOT $4 THEN NULL
                    ELSE live_handoff_block_number
                END,
                live_handoff_block_hash = CASE
                    WHEN phase_name = 'ingest' AND NOT $4 THEN NULL
                    ELSE live_handoff_block_hash
                END,
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
        .bind(bigname_content_hash::INTERPRETER_CONTENT_HASH)
        .bind(resume_position)
        .execute(&mut *transaction)
        .await
        .map_err(|error| {
            RunnerError::transient(format!(
                "failed to start phase {phase} for chain {chain_id}: {error}"
            ))
        })?;
        transaction.commit().await.map_err(|error| {
            RunnerError::transient(format!(
                "failed to commit phase start for chain {chain_id} phase {phase}: {error}"
            ))
        })?;
        Ok(StartDisposition::Started)
    }

    pub(crate) async fn begin_redo(
        &self,
        chain_id: &str,
        phase: PhaseName,
        mode: &RunMode,
    ) -> RunnerResult<RedoSession> {
        redo_state::begin(&self.pool, chain_id, phase, mode).await
    }

    pub(crate) async fn finish_redo(
        &self,
        chain_id: &str,
        phase: PhaseName,
        session: RedoSession,
        completed: bool,
    ) -> RunnerResult<()> {
        redo_state::finish(&self.pool, chain_id, phase, session, completed).await
    }

    pub async fn pause_phase(&self, chain_id: &str, phase: PhaseName) -> RunnerResult<()> {
        self.change_active_status(chain_id, phase, PhaseStatus::Paused)
            .await
    }

    pub async fn resume_phase(&self, chain_id: &str, phase: PhaseName) -> RunnerResult<()> {
        self.change_active_status(chain_id, phase, PhaseStatus::Running)
            .await
    }

    async fn change_active_status(
        &self,
        chain_id: &str,
        phase: PhaseName,
        next: PhaseStatus,
    ) -> RunnerResult<()> {
        let current = self.status(chain_id, phase).await?;
        if current == next {
            return Ok(());
        }
        if !current.can_transition_to(next, false) {
            return Err(invalid_transition(chain_id, phase, current, next));
        }
        sqlx::query(
            "
            UPDATE chain_phase_state
            SET phase_status = $3,
                updated_at = now()
            WHERE chain_id = $1
              AND phase_name = $2
            ",
        )
        .bind(chain_id)
        .bind(phase.as_str())
        .bind(next.as_str())
        .execute(&self.pool)
        .await
        .map_err(|error| {
            RunnerError::transient(format!(
                "failed to change phase {phase} to {next} for chain {chain_id}: {error}"
            ))
        })?;
        Ok(())
    }

    pub async fn record_progress(
        &self,
        chain_id: &str,
        phase: PhaseName,
        mode: &RunMode,
        progress: &PhaseProgress,
    ) -> RunnerResult<()> {
        if mode.is_redo() {
            update_redo_progress(&self.pool, chain_id, phase, progress).await
        } else {
            update_progress(&self.pool, chain_id, phase, progress, "updated_at = now()").await
        }
    }

    pub async fn complete_phase(
        &self,
        chain_id: &str,
        phase: PhaseName,
        progress: &PhaseProgress,
    ) -> RunnerResult<()> {
        let current = self.status(chain_id, phase).await?;
        if !current.can_transition_to(PhaseStatus::Completed, false) {
            return Err(invalid_transition(
                chain_id,
                phase,
                current,
                PhaseStatus::Completed,
            ));
        }
        crate::state_persistence::validate_progress(phase, progress, true)?;
        update_progress(
            &self.pool,
            chain_id,
            phase,
            progress,
            "phase_status = 'completed', finished_at = now(), updated_at = now()",
        )
        .await
    }

    pub async fn fail_phase(
        &self,
        chain_id: &str,
        phase: PhaseName,
        message: &str,
    ) -> RunnerResult<()> {
        let current = self.status(chain_id, phase).await?;
        if !current.can_transition_to(PhaseStatus::Failed, false) {
            return Err(invalid_transition(
                chain_id,
                phase,
                current,
                PhaseStatus::Failed,
            ));
        }
        sqlx::query(
            "
            UPDATE chain_phase_state
            SET phase_status = 'failed',
                last_error = $3,
                finished_at = now(),
                updated_at = now()
            WHERE chain_id = $1
              AND phase_name = $2
            ",
        )
        .bind(chain_id)
        .bind(phase.as_str())
        .bind(message)
        .execute(&self.pool)
        .await
        .map_err(|error| {
            RunnerError::transient(format!(
                "failed to record phase {phase} failure for chain {chain_id}: {error}"
            ))
        })?;
        Ok(())
    }

    pub async fn status(&self, chain_id: &str, phase: PhaseName) -> RunnerResult<PhaseStatus> {
        let status: Option<String> = sqlx::query_scalar(
            "
            SELECT phase_status
            FROM chain_phase_state
            WHERE chain_id = $1
              AND phase_name = $2
            ",
        )
        .bind(chain_id)
        .bind(phase.as_str())
        .fetch_optional(&self.pool)
        .await
        .map_err(|error| {
            RunnerError::transient(format!(
                "failed to load phase {phase} status for chain {chain_id}: {error}"
            ))
        })?;
        status
            .ok_or_else(|| {
                RunnerError::data_integrity(format!(
                    "phase state is missing for chain {chain_id} phase {phase}"
                ))
            })?
            .parse()
    }

    pub async fn start_heartbeat(
        &self,
        instance_id: &str,
        chain_id: &str,
        phase: PhaseName,
    ) -> RunnerResult<()> {
        let result = sqlx::query(
            "
            INSERT INTO service_heartbeats (
                service_name,
                instance_id,
                chain_id,
                phase_name,
                started_at,
                heartbeat_at
            )
            SELECT 'phase-runner', $1, chain_id, phase_name, started_at, now()
            FROM chain_phase_state
            WHERE chain_id = $2
              AND phase_name = $3
              AND phase_status IN ('running', 'paused')
            ON CONFLICT (service_name, instance_id, chain_id, phase_name)
            DO UPDATE SET started_at = EXCLUDED.started_at,
                          heartbeat_at = EXCLUDED.heartbeat_at
            ",
        )
        .bind(instance_id)
        .bind(chain_id)
        .bind(phase.as_str())
        .execute(&self.pool)
        .await
        .map_err(|error| {
            RunnerError::transient(format!(
                "failed to write heartbeat for chain {chain_id} phase {phase}: {error}"
            ))
        })?;
        if result.rows_affected() != 1 {
            return Err(RunnerError::data_integrity(format!(
                "heartbeat requires an active phase for chain {chain_id} phase {phase}"
            )));
        }
        Ok(())
    }

    pub async fn record_heartbeat(
        &self,
        instance_id: &str,
        chain_id: &str,
        phase: PhaseName,
    ) -> RunnerResult<()> {
        let result = sqlx::query(
            "
            UPDATE service_heartbeats
            SET heartbeat_at = now()
            WHERE service_name = 'phase-runner'
              AND instance_id = $1
              AND chain_id = $2
              AND phase_name = $3
            ",
        )
        .bind(instance_id)
        .bind(chain_id)
        .bind(phase.as_str())
        .execute(&self.pool)
        .await
        .map_err(|error| {
            RunnerError::transient(format!(
                "failed to update heartbeat for chain {chain_id} phase {phase}: {error}"
            ))
        })?;
        if result.rows_affected() != 1 {
            return Err(RunnerError::data_integrity(format!(
                "heartbeat has not been started for chain {chain_id} phase {phase}"
            )));
        }
        Ok(())
    }

    pub async fn ingest_handoff(&self, chain_id: &str) -> RunnerResult<Option<BlockMarker>> {
        sqlx::query_as::<_, (i64, String)>(
            "
            SELECT live_handoff_block_number, live_handoff_block_hash
            FROM chain_phase_state
            WHERE chain_id = $1
              AND phase_name = 'ingest'
              AND live_handoff_block_number IS NOT NULL
            ",
        )
        .bind(chain_id)
        .fetch_optional(&self.pool)
        .await
        .map(|marker| marker.map(|(number, hash)| BlockMarker { number, hash }))
        .map_err(|error| {
            RunnerError::transient(format!(
                "failed to load live handoff for chain {chain_id}: {error}"
            ))
        })
    }

    pub async fn phase_resume(
        &self,
        chain_id: &str,
        phase: PhaseName,
        mode: &RunMode,
    ) -> RunnerResult<PhaseResume> {
        if mode.is_redo() {
            load_redo_resume(&self.pool, chain_id, phase).await
        } else {
            load_phase_resume(&self.pool, chain_id, phase).await
        }
    }

    pub async fn update_ingest_cursors(
        &self,
        sources: &[SourceConfig],
        progress: &PhaseProgress,
    ) -> RunnerResult<()> {
        update_ingest_cursors(&self.pool, sources, progress).await
    }
}
