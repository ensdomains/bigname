use sqlx::{PgConnection, types::time::OffsetDateTime};

use crate::{
    error::{RunnerError, RunnerResult},
    phase::PhaseName,
    state::PhaseStore,
};

impl PhaseStore {
    pub(crate) async fn active_normal_phases(
        &self,
    ) -> RunnerResult<Vec<(String, PhaseName, OffsetDateTime)>> {
        let rows: Vec<(String, String, OffsetDateTime)> = sqlx::query_as(
            "SELECT chain_id, phase_name, updated_at
             FROM chain_phase_state
             WHERE phase_status IN ('running', 'paused')
               AND NOT redo_in_progress
             ORDER BY chain_id, phase_name",
        )
        .fetch_all(self.pool())
        .await
        .map_err(|error| {
            RunnerError::database("failed to load active normal phase state", error)
        })?;
        rows.into_iter()
            .map(|(chain_id, phase, updated_at)| Ok((chain_id, phase.parse()?, updated_at)))
            .collect()
    }

    pub(crate) async fn complete_stopped_phase(
        &self,
        lock_connection: &mut PgConnection,
        chain_id: &str,
        phase: PhaseName,
    ) -> RunnerResult<bool> {
        self.settle_stopped_phase(lock_connection, chain_id, phase, false, None)
            .await
    }

    pub(crate) async fn complete_unconfigured_phase(
        &self,
        lock_connection: &mut PgConnection,
        chain_id: &str,
        phase: PhaseName,
        observed_updated_at: OffsetDateTime,
    ) -> RunnerResult<bool> {
        self.settle_stopped_phase(
            lock_connection,
            chain_id,
            phase,
            true,
            Some(observed_updated_at),
        )
        .await
    }

    async fn settle_stopped_phase(
        &self,
        lock_connection: &mut PgConnection,
        chain_id: &str,
        phase: PhaseName,
        unconfigured: bool,
        observed_updated_at: Option<OffsetDateTime>,
    ) -> RunnerResult<bool> {
        sqlx::query(
            "UPDATE chain_phase_state
             SET phase_status = 'completed', last_error = NULL,
                 settled_while_unconfigured = CASE
                     WHEN $3 AND phase_name = 'verify' THEN TRUE
                     ELSE NULL
                 END,
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
               AND NOT redo_in_progress
               AND ($4::timestamptz IS NULL OR updated_at = $4)",
        )
        .bind(chain_id)
        .bind(phase.as_str())
        .bind(unconfigured)
        .bind(observed_updated_at)
        .execute(&mut *lock_connection)
        .await
        .map(|result| result.rows_affected() == 1)
        .map_err(|error| {
            RunnerError::database(
                format!("failed to settle stopped phase {phase} for chain {chain_id}"),
                error,
            )
        })
    }

    pub(crate) async fn clear_unconfigured_settlement(
        &self,
        lock_connection: &mut PgConnection,
        chain_id: &str,
        phase: PhaseName,
    ) -> RunnerResult<()> {
        if phase != PhaseName::Verify {
            return Ok(());
        }
        sqlx::query(
            "UPDATE chain_phase_state
             SET settled_while_unconfigured = NULL, updated_at = now()
             WHERE chain_id = $1 AND phase_name = 'verify'
               AND phase_status = 'completed'
               AND settled_while_unconfigured",
        )
        .bind(chain_id)
        .execute(&mut *lock_connection)
        .await
        .map(|_| ())
        .map_err(|error| {
            RunnerError::database(
                format!("failed to clear unconfigured Verify settlement for chain {chain_id}"),
                error,
            )
        })
    }

    pub(crate) async fn complete_stopped_live(
        &self,
        lock_connection: &mut PgConnection,
        chain_id: &str,
    ) -> RunnerResult<()> {
        self.complete_stopped_phase(lock_connection, chain_id, PhaseName::Live)
            .await
            .map(|_| ())
    }
}
