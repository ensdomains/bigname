use std::time::{Duration, Instant};

use sqlx::PgConnection;

use crate::{
    config::TimingConfig,
    database::RunnerDatabase,
    error::{ErrorKind, RunnerError, RunnerResult},
    phase::{BlockRange, PhaseName, PhaseProgress},
    phase_lock::PhaseLock,
    redo_state::{RedoOutcome, RedoSession},
    state::PhaseStore,
    state_persistence::{load_redo_marker, record_live_verification_mismatch},
    transitions::redo_rerun_instruction,
};

const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(5);

pub(crate) enum PhaseLoopResult {
    Completed(Box<PhaseProgress>),
    Cancelled,
}

pub(crate) async fn cancelled_redo_error(
    store: &PhaseStore,
    chain_id: &str,
    phase: PhaseName,
) -> RunnerResult<RunnerError> {
    let Some((redo_mode, from, to)) = load_redo_marker(store.pool(), chain_id, phase).await? else {
        return Ok(RunnerError::new(
            ErrorKind::InvalidTransition,
            format!(
                "redo for chain {chain_id} phase {phase} was cancelled before it started; no \
                 unfinished redo was recorded"
            ),
        ));
    };
    let instruction = redo_rerun_instruction(
        chain_id,
        phase,
        Some(&redo_mode),
        Some(BlockRange { from, to }),
    );
    Ok(RunnerError::new(
        ErrorKind::InvalidTransition,
        format!(
            "redo for chain {chain_id} phase {phase} is incomplete; the phase remains blocked \
             from normal restart; {instruction}"
        ),
    ))
}

pub(crate) fn redo_outcome(result: &RunnerResult<PhaseLoopResult>) -> RedoOutcome<'_> {
    match result {
        Ok(PhaseLoopResult::Completed(progress)) => RedoOutcome::Completed(progress),
        Err(error) => RedoOutcome::Failed(error),
        Ok(PhaseLoopResult::Cancelled) => unreachable!("redo cancellation became an error"),
    }
}

pub(crate) async fn record_live_mismatch_with_lock(
    database: &RunnerDatabase,
    store: &PhaseStore,
    chain_id: &str,
    reason: &str,
) -> RunnerResult<()> {
    let mut phase_lock =
        PhaseLock::acquire(database.connect_options(), chain_id, PhaseName::Live).await?;
    phase_lock.check_alive().await?;
    let result = record_live_verification_mismatch(store.pool(), chain_id, reason).await;
    let release = phase_lock.release().await;
    match (result, release) {
        (Ok(true), Ok(())) => Ok(()),
        (Ok(false), Ok(())) => Err(RunnerError::data_integrity(format!(
            "verification mismatch could not mark live failed for chain {chain_id}"
        ))),
        (Err(error), Ok(())) => Err(error),
        (Ok(_), Err(error)) => Err(error),
        (Err(error), Err(release_error)) => {
            Err(error.with_secondary("release the live phase lock", release_error))
        }
    }
}

pub(crate) async fn finish_failed_redo_start(
    store: &PhaseStore,
    lock_connection: &mut PgConnection,
    chain_id: &str,
    phase: PhaseName,
    session: RedoSession,
    error: RunnerError,
) -> RunnerResult<()> {
    match store
        .finish_redo(
            lock_connection,
            chain_id,
            phase,
            session,
            RedoOutcome::Failed(&error),
        )
        .await
    {
        Ok(()) => Err(error),
        Err(record_error) => {
            Err(error.with_secondary("record the failed redo attempt", record_error))
        }
    }
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
