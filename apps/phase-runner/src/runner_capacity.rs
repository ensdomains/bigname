use std::{
    collections::BTreeMap,
    sync::{
        Arc, Mutex,
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

/// Each chain's last capacity measurement and when its probe started. A family run that follows a
/// batch within the capacity poll interval reuses the batch prelude's measurement when it showed
/// room, instead of a second `pg_database_size` and probe-file write moments later. Writes made
/// since that measurement are not in it, which is why batch preludes never reuse one.
#[derive(Default)]
pub(super) struct CapacityMemo {
    taken: Mutex<BTreeMap<String, (Instant, CapacityMeasurement)>>,
    disabled: AtomicBool,
    clock: Mutex<Option<Clock>>,
}

/// A monotonic `now` a test can hold still; the runner uses `Instant::now` otherwise.
type Clock = Arc<dyn Fn() -> Instant + Send + Sync>;

impl CapacityMemo {
    /// The instant freshness is measured by: the injected clock's, or `Instant::now()`.
    fn now(&self) -> Instant {
        let clock = self
            .clock
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone();
        clock.map_or_else(Instant::now, |clock| clock())
    }

    /// The chain's last measurement when, at `now`, less than `window` has passed since its probe
    /// started. A lookup never extends that.
    fn fresh(&self, chain_id: &str, window: Duration, now: Instant) -> Option<CapacityMeasurement> {
        if self.disabled.load(Ordering::Relaxed) {
            return None;
        }
        let taken = self
            .taken
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let (started, measurement) = taken.get(chain_id)?;
        (now.saturating_duration_since(*started) < window).then(|| measurement.clone())
    }

    /// Records a measurement whose probe started at `started`.
    fn record(&self, chain_id: &str, measurement: CapacityMeasurement, started: Instant) {
        self.taken
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .insert(chain_id.to_owned(), (started, measurement));
    }
}

/// Whether a capacity check may reuse a fresh measurement.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum Measure {
    /// Probe every time: a batch prelude, whose last reading does not count the last batch's
    /// writes.
    Fresh,
    /// Reuse a measurement younger than the poll interval that showed room: the check before a
    /// family run, which moments earlier followed the batch prelude's probe.
    ReuseFresh,
}

impl PhaseRunner {
    /// Public test-support hook: probe capacity at every check, never reusing a fresh
    /// measurement, for tests whose probe must be asked before each family run. `doc(hidden)`
    /// hides it from the docs; the method is still public.
    #[doc(hidden)]
    pub fn without_capacity_reuse(self) -> Self {
        self.capacity_memo.disabled.store(true, Ordering::Relaxed);
        self
    }

    /// Public test-support hook: the monotonic clock capacity freshness is measured by, both when
    /// a probe starts and when a reading is looked up, in place of `Instant::now`, so a test can
    /// hold a reading fresh. Pause polling still sleeps in real time. `doc(hidden)` hides it from
    /// the docs; the method is still public.
    #[doc(hidden)]
    pub fn with_capacity_clock(self, clock: impl Fn() -> Instant + Send + Sync + 'static) -> Self {
        *self
            .capacity_memo
            .clock
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(Arc::new(clock));
        self
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) async fn wait_for_capacity(
        &self,
        chain: &ChainConfig,
        phase: PhaseName,
        reserved_write_bytes: u64,
        measure: Measure,
        cancellation: &CancellationToken,
        heartbeat: &mut HeartbeatThrottle,
        phase_lock: &mut PhaseLock,
    ) -> RunnerResult<bool> {
        let mut paused = false;
        loop {
            phase_lock.check_alive().await?;
            // Only a family run's check reuses, and only a fresh measurement that shows room;
            // while paused every check probes.
            let reused = (measure == Measure::ReuseFresh && !paused)
                .then(|| {
                    self.capacity_memo.fresh(
                        &chain.chain_id,
                        self.capacity.poll_interval(),
                        self.capacity_memo.now(),
                    )
                })
                .flatten()
                .map(|measurement| self.capacity.evaluate(measurement, reserved_write_bytes))
                .filter(|status| status.is_available());
            let status = match reused {
                Some(status) => status,
                None => {
                    let started = self.capacity_memo.now();
                    let status = self
                        .capacity
                        .check(self.store.pool(), reserved_write_bytes)
                        .await;
                    phase_lock.check_alive().await?;
                    let status = status?;
                    self.capacity_memo
                        .record(&chain.chain_id, status.measurement.clone(), started);
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

    const WINDOW: Duration = Duration::from_secs(5);

    #[test]
    fn a_measurement_expires_a_window_after_its_probe_started() {
        let memo = CapacityMemo::default();
        let started = Instant::now();
        memo.record("a", measurement(7), started);
        let just_inside = started + WINDOW - Duration::from_millis(1);
        assert_eq!(memo.fresh("a", WINDOW, just_inside), Some(measurement(7)));
        assert_eq!(memo.fresh("a", WINDOW, started + WINDOW), None);
        assert_eq!(memo.fresh("b", WINDOW, started), None, "per chain");
    }

    #[test]
    fn a_probe_that_took_the_whole_window_is_already_stale() {
        let memo = CapacityMemo::default();
        let started = Instant::now();
        memo.record("a", measurement(7), started);
        assert_eq!(memo.fresh("a", WINDOW, started + WINDOW), None);
    }

    #[test]
    fn lookups_do_not_extend_freshness() {
        let memo = CapacityMemo::default();
        let started = Instant::now();
        memo.record("a", measurement(7), started);
        for step in 1..5 {
            let at = started + WINDOW * step / 5;
            assert_eq!(memo.fresh("a", WINDOW, at), Some(measurement(7)));
        }
        assert_eq!(memo.fresh("a", WINDOW, started + WINDOW), None);
    }

    #[test]
    fn a_disabled_memo_reuses_nothing() {
        let memo = CapacityMemo::default();
        let started = Instant::now();
        memo.record("a", measurement(7), started);
        memo.disabled.store(true, Ordering::Relaxed);
        assert_eq!(memo.fresh("a", WINDOW, started), None);
    }
}
