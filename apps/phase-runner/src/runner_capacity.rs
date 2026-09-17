use tokio_util::sync::CancellationToken;
use tracing::warn;

use crate::{
    config::ChainConfig, error::RunnerResult, phase::PhaseName, phase_lock::PhaseLock,
    runner_support::HeartbeatThrottle,
};

use super::PhaseRunner;

impl PhaseRunner {
    pub(super) async fn wait_for_capacity(
        &self,
        chain: &ChainConfig,
        phase: PhaseName,
        reserved_write_bytes: u64,
        cancellation: &CancellationToken,
        heartbeat: &mut HeartbeatThrottle,
        phase_lock: &mut PhaseLock,
    ) -> RunnerResult<bool> {
        let mut paused = false;
        loop {
            phase_lock.check_alive().await?;
            let status = self
                .capacity
                .check(self.store.pool(), reserved_write_bytes)
                .await;
            phase_lock.check_alive().await?;
            let status = status?;
            if status.is_available() {
                if paused {
                    phase_lock.check_alive().await?;
                    self.store.resume_phase(&chain.chain_id, phase).await?;
                }
                return Ok(false);
            }
            if !paused {
                phase_lock.check_alive().await?;
                self.store.pause_phase(&chain.chain_id, phase).await?;
                self.phase_progress.clear_phase(&chain.chain_id, phase);
                paused = true;
            }
            warn!(
                chain_id = chain.chain_id,
                phase = %phase,
                breach_reasons = ?status.breach_reasons,
                database_size_bytes = status.measurement.database_size_bytes,
                free_disk_bytes = status.measurement.free_disk_bytes,
                reserved_write_bytes,
                "phase paused until storage capacity recovers"
            );
            phase_lock.check_alive().await?;
            heartbeat
                .record_if_due(&self.store, &self.instance_id, &chain.chain_id, phase)
                .await?;
            self.record_loop_progress(&chain.chain_id);
            tokio::select! {
                () = cancellation.cancelled() => return Ok(true),
                () = tokio::time::sleep(self.capacity.poll_interval()) => {}
            }
        }
    }
}
