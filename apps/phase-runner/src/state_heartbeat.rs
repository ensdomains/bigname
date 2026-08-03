use crate::{
    error::{RunnerError, RunnerResult},
    phase::PhaseName,
    state::PhaseStore,
};

impl PhaseStore {
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
        .execute(self.pool())
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
        .execute(self.pool())
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
}
