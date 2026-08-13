use std::{fmt, str::FromStr};

use sqlx::PgPool;

use crate::{
    config::SourceConfig,
    error::{ErrorKind, RunnerError, RunnerResult},
    heads::BlockMarker,
    phase::{PhaseName, PhaseProgress, PhaseResume, RunMode},
    redo_state::{self, RedoOutcome, RedoSession},
    state_ingest_progress::{update_ingest_cursors, update_ingest_progress},
    state_persistence::{
        load_phase_resume, load_redo_resume, update_progress, update_redo_progress,
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
    RecoveringCompleted,
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
        let row = row_for(&rows, phase)?;
        let status = row.status()?;
        let recovering_completed = status == PhaseStatus::Failed
            && crate::completed_phase_recovery::locked_completed_validation_recovery(row, phase);
        let restarts_completed = match (status, phase) {
            (_, PhaseName::Live) => true,
            (PhaseStatus::Completed, PhaseName::Ingest) => {
                row.live_handoff_block_number.is_none()
                    || row.live_handoff_block_number != row.target_block_number
                    || row.live_handoff_block_hash != row.target_block_hash
                    || row.current_block_number != row.target_block_number
                    || row.current_block_hash != row.target_block_hash
            }
            (PhaseStatus::Completed, PhaseName::Interpret | PhaseName::Project) => {
                completed_phase_is_behind(&mut transaction, chain_id, phase, row).await?
            }
            (PhaseStatus::Completed, PhaseName::Verify) => {
                row.verification_level.is_none()
                    || row.current_block_number.is_none()
                    || row.current_block_number != row.target_block_number
                    || row.current_block_hash != row.target_block_hash
            }
            _ => false,
        };
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
        if recovering_completed {
            transaction.commit().await.map_err(|error| {
                RunnerError::transient(format!(
                    "failed to finish revalidation recovery check for chain {chain_id} phase \
                     {phase}: {error}"
                ))
            })?;
            return Ok(StartDisposition::RecoveringCompleted);
        }
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
        sources: &[SourceConfig],
        supplied_manifest_authority_generation: Option<&str>,
        attested_by: &str,
    ) -> RunnerResult<RedoSession> {
        redo_state::begin(
            &self.pool,
            chain_id,
            phase,
            mode,
            sources,
            supplied_manifest_authority_generation,
            attested_by,
        )
        .await
    }

    pub(crate) async fn finish_redo(
        &self,
        chain_id: &str,
        phase: PhaseName,
        session: RedoSession,
        outcome: RedoOutcome<'_>,
    ) -> RunnerResult<()> {
        redo_state::finish(&self.pool, chain_id, phase, session, outcome).await
    }

    pub(crate) async fn required_redo_range(
        &self,
        chain_id: &str,
        phase: PhaseName,
    ) -> RunnerResult<Option<crate::phase::BlockRange>> {
        crate::redo_stamp::required_range(&self.pool, chain_id, phase).await
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

    pub(crate) async fn active_normal_phases(&self) -> RunnerResult<Vec<(String, PhaseName)>> {
        let rows: Vec<(String, String)> = sqlx::query_as(
            "SELECT chain_id, phase_name
             FROM chain_phase_state
             WHERE phase_status IN ('running', 'paused')
               AND NOT redo_in_progress
             ORDER BY chain_id, phase_name",
        )
        .fetch_all(&self.pool)
        .await
        .map_err(|error| {
            RunnerError::database("failed to load active normal phase state", error)
        })?;
        rows.into_iter()
            .map(|(chain_id, phase)| Ok((chain_id, phase.parse()?)))
            .collect()
    }

    pub(crate) async fn complete_stopped_phase(
        &self,
        chain_id: &str,
        phase: PhaseName,
    ) -> RunnerResult<bool> {
        sqlx::query(
            "UPDATE chain_phase_state
             SET phase_status = 'completed', last_error = NULL,
                 live_handoff_block_number = CASE
                     WHEN phase_name = 'ingest' THEN NULL
                     ELSE live_handoff_block_number
                 END,
                 live_handoff_block_hash = CASE
                     WHEN phase_name = 'ingest' THEN NULL
                     ELSE live_handoff_block_hash
                 END,
                 finished_at = now(), updated_at = now()
             WHERE chain_id = $1 AND phase_name = $2
               AND phase_status IN ('running', 'paused')
               AND NOT redo_in_progress",
        )
        .bind(chain_id)
        .bind(phase.as_str())
        .execute(&self.pool)
        .await
        .map(|result| result.rows_affected() == 1)
        .map_err(|error| {
            RunnerError::database(
                format!("failed to complete stopped phase {phase} for chain {chain_id}"),
                error,
            )
        })
    }

    pub(crate) async fn complete_stopped_live(&self, chain_id: &str) -> RunnerResult<()> {
        self.complete_stopped_phase(chain_id, PhaseName::Live)
            .await
            .map(|_| ())
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
            RunnerError::database(
                format!("failed to record phase {phase} failure for chain {chain_id}"),
                error,
            )
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

    pub async fn record_ingest_progress(
        &self,
        chain_id: &str,
        sources: &[SourceConfig],
        progress: &PhaseProgress,
    ) -> RunnerResult<()> {
        update_ingest_progress(&self.pool, chain_id, sources, progress).await
    }

    pub async fn ensure_ingest_sources(
        &self,
        chain_id: &str,
        sources: &[SourceConfig],
    ) -> RunnerResult<()> {
        crate::ingest_cursor_config::ensure_all(&self.pool, chain_id, sources).await
    }

    pub async fn validate_existing_ingest_sources(
        &self,
        chain_id: &str,
        sources: &[SourceConfig],
    ) -> RunnerResult<()> {
        crate::ingest_cursor_config::validate_existing(&self.pool, chain_id, sources).await
    }

    pub async fn validate_completed_ingest_sources(
        &self,
        chain_id: &str,
        sources: &[SourceConfig],
    ) -> RunnerResult<()> {
        crate::ingest_cursor_config::validate_completed(&self.pool, chain_id, sources).await
    }
}

async fn completed_phase_is_behind(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    chain_id: &str,
    phase: PhaseName,
    row: &crate::transitions::PhaseStateRow,
) -> RunnerResult<bool> {
    let head: Option<(i64, String)> = sqlx::query_as(
        "SELECT latest_block_number, latest_block_hash
         FROM chain_heads WHERE chain_id = $1",
    )
    .bind(chain_id)
    .fetch_optional(&mut **transaction)
    .await
    .map_err(|error| {
        RunnerError::database(
            format!("failed to compare phase {phase} with the live head for chain {chain_id}"),
            error,
        )
    })?;
    let Some((head_number, head_hash)) = head else {
        return Ok(false);
    };
    let Some(current_number) = row.current_block_number else {
        return Ok(true);
    };
    if current_number > head_number {
        return Err(RunnerError::data_integrity(format!(
            "phase {phase} cursor {current_number} is above canonical head {head_number} for chain \
             {chain_id} without required redo state"
        )));
    }
    if current_number == head_number
        && row.current_block_hash.as_deref() != Some(head_hash.as_str())
    {
        return Err(RunnerError::data_integrity(format!(
            "phase {phase} cursor hash differs from canonical head {head_hash} at block \
             {head_number} for chain {chain_id} without required redo state"
        )));
    }
    Ok(current_number < head_number)
}
