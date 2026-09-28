use std::{
    collections::BTreeMap,
    sync::{
        Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};

use tokio_util::sync::CancellationToken;
use tracing::warn;

use crate::{
    capacity::CapacityMeasurement, config::ChainConfig, error::RunnerResult, phase::PhaseName,
    phase_lock::PhaseLock, runner_support::HeartbeatThrottle,
};

use super::PhaseRunner;

/// Each chain's last capacity measurement and when it was taken. A family run that follows a
/// batch within the capacity poll interval reuses the batch prelude's measurement when it showed
/// room, instead of a second `pg_database_size` and probe-file write moments later.
#[derive(Debug, Default)]
pub(super) struct CapacityMemo {
    taken: Mutex<BTreeMap<String, (Instant, CapacityMeasurement)>>,
    disabled: AtomicBool,
}

impl CapacityMemo {
    /// The chain's last measurement when it is younger than `window`.
    fn fresh(&self, chain_id: &str, window: Duration) -> Option<CapacityMeasurement> {
        if self.disabled.load(Ordering::Relaxed) {
            return None;
        }
        let taken = self
            .taken
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let (at, measurement) = taken.get(chain_id)?;
        (at.elapsed() < window).then(|| measurement.clone())
    }

    fn record(&self, chain_id: &str, measurement: CapacityMeasurement) {
        self.taken
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .insert(chain_id.to_owned(), (Instant::now(), measurement));
    }
}

impl PhaseRunner {
    /// Probe capacity at every check, never reusing a fresh measurement: for tests whose probe
    /// must be asked before each family run.
    #[doc(hidden)]
    pub fn without_capacity_reuse(self) -> Self {
        self.capacity_memo.disabled.store(true, Ordering::Relaxed);
        self
    }

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
            // A fresh measurement is reused only while it shows room; a breach, or one older
            // than the poll interval, is measured again.
            let reused = self
                .capacity_memo
                .fresh(&chain.chain_id, self.capacity.poll_interval())
                .map(|measurement| self.capacity.evaluate(measurement, reserved_write_bytes))
                .filter(|status| !paused && status.is_available());
            let status = match reused {
                Some(status) => status,
                None => {
                    let status = self
                        .capacity
                        .check(self.store.pool(), reserved_write_bytes)
                        .await;
                    phase_lock.check_alive().await?;
                    let status = status?;
                    self.capacity_memo
                        .record(&chain.chain_id, status.measurement.clone());
                    status
                }
            };
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

#[cfg(test)]
mod tests {
    use super::*;

    fn measurement(database_size_bytes: u64) -> CapacityMeasurement {
        CapacityMeasurement {
            database_size_bytes,
            free_disk_bytes: u64::MAX,
        }
    }

    #[test]
    fn a_measurement_is_fresh_only_inside_its_window_and_per_chain() {
        let memo = CapacityMemo::default();
        assert_eq!(memo.fresh("a", Duration::from_secs(60)), None);
        memo.record("a", measurement(7));
        assert_eq!(
            memo.fresh("a", Duration::from_secs(60)),
            Some(measurement(7))
        );
        assert_eq!(memo.fresh("b", Duration::from_secs(60)), None);
        assert_eq!(
            memo.fresh("a", Duration::ZERO),
            None,
            "an aged measurement is not reused"
        );
        memo.disabled.store(true, Ordering::Relaxed);
        assert_eq!(memo.fresh("a", Duration::from_secs(60)), None);
    }
}
