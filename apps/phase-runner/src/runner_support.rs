use std::time::{Duration, Instant};

use crate::{
    config::TimingConfig,
    error::RunnerResult,
    phase::{PhaseName, PhaseProgress},
    state::PhaseStore,
};

const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(5);

pub(crate) enum PhaseLoopResult {
    Completed(Box<PhaseProgress>),
    Cancelled,
}

pub(crate) struct Backoff {
    current: Duration,
    maximum: Duration,
}

impl Backoff {
    pub(crate) fn new(config: &TimingConfig) -> Self {
        Self {
            current: config.initial_backoff,
            maximum: config.maximum_backoff,
        }
    }

    pub(crate) fn next_delay(&mut self) -> Duration {
        let delay = self.current;
        self.current = self.current.saturating_mul(2).min(self.maximum);
        delay
    }
}

pub(crate) struct HeartbeatThrottle {
    last_recorded: Instant,
}

impl HeartbeatThrottle {
    pub(crate) fn new() -> Self {
        Self {
            last_recorded: Instant::now(),
        }
    }

    pub(crate) async fn record_if_due(
        &mut self,
        store: &PhaseStore,
        instance_id: &str,
        chain_id: &str,
        phase: PhaseName,
    ) -> RunnerResult<()> {
        if self.last_recorded.elapsed() < HEARTBEAT_INTERVAL {
            return Ok(());
        }
        store.record_heartbeat(instance_id, chain_id, phase).await?;
        self.last_recorded = Instant::now();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn restart_backoff_doubles_and_stays_capped() {
        let timing = TimingConfig {
            initial_backoff: Duration::from_millis(3),
            maximum_backoff: Duration::from_millis(10),
            live_poll_interval: Duration::from_millis(1),
        };
        let mut backoff = Backoff::new(&timing);

        assert_eq!(backoff.next_delay(), Duration::from_millis(3));
        assert_eq!(backoff.next_delay(), Duration::from_millis(6));
        assert_eq!(backoff.next_delay(), Duration::from_millis(10));
        assert_eq!(backoff.next_delay(), Duration::from_millis(10));
    }
}
