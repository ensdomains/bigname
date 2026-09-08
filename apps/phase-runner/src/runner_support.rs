use std::time::{Duration, Instant};

use sqlx::PgConnection;
use tokio_util::sync::CancellationToken;

use crate::{
    config::{ChainConfig, TimingConfig},
    database::RunnerDatabase,
    error::{ErrorKind, RunnerError, RunnerResult},
    phase::{BlockRange, PhaseName, PhaseProgress, RunMode},
    phase_lock::PhaseLock,
    redo_state::{RedoOutcome, RedoSession},
    runner::SupervisorReport,
    state::PhaseStore,
    state_persistence::{load_redo_marker, record_live_verification_mismatch},
    transitions::redo_rerun_instruction,
};

const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(5);

pub(crate) enum PhaseLoopResult {
    Completed(Box<PhaseProgress>),
    Cancelled,
}

/// How long a stop already accepted may wait on the database to learn what to
/// report. The read runs after the race is lost, often because that same
/// database stalled, so it cannot be allowed to hold the stop itself.
#[cfg(not(test))]
pub(crate) const STOPPED_MARKER_LOOKUP: Duration = Duration::from_secs(5);
#[cfg(test)]
pub(crate) const STOPPED_MARKER_LOOKUP: Duration = Duration::from_millis(50);

/// Touch the database after a stop has been accepted, giving up within
/// [`STOPPED_MARKER_LOOKUP`] with an error that says what was left undone.
pub(crate) async fn read_after_stop<T>(
    what: &str,
    read: impl std::future::Future<Output = RunnerResult<T>>,
) -> RunnerResult<T> {
    match tokio::time::timeout(STOPPED_MARKER_LOOKUP, read).await {
        Ok(result) => result,
        Err(_) => Err(RunnerError::new(
            ErrorKind::InvalidTransition,
            format!(
                "stopped, and the database did not answer within {}s while {what}; inspect \
                 chain_phase_state once it responds",
                STOPPED_MARKER_LOOKUP.as_secs_f64()
            ),
        )),
    }
}

/// Release a phase lock, racing the release against a stop. The release is an
/// unlock and a close on a connection that may be the stall a stop is waiting
/// out, so once a stop is pending -- already, or arriving mid-release -- the
/// rest of it is bounded; a lock dropped on expiry closes its session, which
/// releases it. Without a stop the release waits as long as it needs to.
pub(crate) async fn release_lock_racing_stop(
    phase_lock: PhaseLock,
    chain_id: &str,
    phase: PhaseName,
    cancellation: &CancellationToken,
) -> RunnerResult<()> {
    let release = phase_lock.release();
    tokio::pin!(release);
    tokio::select! {
        biased;
        released = &mut release => return released,
        () = cancellation.cancelled() => {}
    }
    match tokio::time::timeout(STOPPED_MARKER_LOOKUP, release).await {
        Ok(released) => released,
        Err(_elapsed) => {
            tracing::warn!(
                chain_id,
                phase = %phase,
                "phase lock release did not answer after a stop; the connection is dropped"
            );
            Ok(())
        }
    }
}

/// A redo whose start lost the stop race still records the failed attempt, but
/// through the lock's connection, which may be the stall that lost the race.
/// The recording is bounded; the marker already says the redo is incomplete,
/// so the error to report is the same either way.
pub(crate) async fn finish_stopped_redo_start(
    store: &PhaseStore,
    phase_lock: &mut PhaseLock,
    chain_id: &str,
    phase: PhaseName,
    session: RedoSession,
    error: RunnerError,
) -> RunnerError {
    let recorded = read_after_stop(
        &format!("recording the stopped redo start for chain {chain_id} phase {phase}"),
        async {
            phase_lock.check_alive().await?;
            store
                .finish_redo(
                    phase_lock.connection(),
                    chain_id,
                    phase,
                    session,
                    RedoOutcome::Failed(&error),
                )
                .await
        },
    )
    .await;
    match recorded {
        Ok(()) => error,
        Err(record_error) => error.with_secondary("record the stopped redo start", record_error),
    }
}

pub(crate) async fn cancelled_redo_error(
    store: &PhaseStore,
    chain_id: &str,
    phase: PhaseName,
) -> RunnerResult<RunnerError> {
    let marker = read_after_stop(
        &format!("reading the redo for chain {chain_id} phase {phase}"),
        load_redo_marker(store.pool(), chain_id, phase),
    )
    .await?;
    let Some((redo_mode, from, to)) = marker else {
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

/// A stop accepted between chains leaves the rest of an explicit redo unstarted.
/// Each one is reported, so the command cannot exit clean with a prefix redone.
pub(crate) fn report_undispatched_redo(report: &mut SupervisorReport, chains: &[ChainConfig]) {
    for chain in chains {
        report.stopped_chains.push((
            chain.chain_id.clone(),
            RunnerError::new(
                ErrorKind::InvalidTransition,
                format!(
                    "redo for chain {} was not started: the stop was accepted before its \
                     dispatch and no redo stamp was recorded; rerun the command for this chain",
                    chain.chain_id
                ),
            ),
        ));
    }
}

/// True when the Interpret marker is this recompute's own interrupted run, which
/// resumes; an ordinary redo or a different range is an error.
pub(crate) async fn resumable_recompute_marker(
    store: &PhaseStore,
    chain_id: &str,
    range: BlockRange,
) -> RunnerResult<bool> {
    let Some((redo_mode, from, to)) =
        load_redo_marker(store.pool(), chain_id, PhaseName::Interpret).await?
    else {
        return Ok(false);
    };
    if redo_mode != "recompute_flags" {
        return Err(RunnerError::data_integrity(format!(
            "interpret phase for chain {chain_id} already has an ordinary redo; complete it \
             before starting recompute-flags"
        )));
    }
    let persisted = BlockRange::new(from, to)?;
    if persisted != range {
        return Err(RunnerError::data_integrity(format!(
            "recompute-flags for chain {chain_id} is interrupted; rerun the exact persisted \
             range {}..={}",
            persisted.from, persisted.to
        )));
    }
    Ok(true)
}

pub(crate) async fn require_all_phase_range_within_verify(
    store: &PhaseStore,
    chain_id: &str,
    range: BlockRange,
) -> RunnerResult<()> {
    let verify_to = store
        .phase_resume(chain_id, PhaseName::Verify, &RunMode::Normal)
        .await?
        .current
        .ok_or_else(|| RunnerError::data_integrity("Verify has no recorded extent"))?
        .number;
    if range.to > verify_to {
        return Err(RunnerError::data_integrity(format!(
            "all-phase redo range ends at {}, beyond Verify's recorded extent {verify_to}",
            range.to
        )));
    }
    Ok(())
}

pub(crate) fn redo_outcome(result: &RunnerResult<PhaseLoopResult>) -> RedoOutcome<'_> {
    match result {
        Ok(PhaseLoopResult::Completed(progress)) => RedoOutcome::Completed(progress),
        Err(error) => RedoOutcome::Failed(error),
        Ok(PhaseLoopResult::Cancelled) => unreachable!("redo cancellation became an error"),
    }
}

/// Record a Live verification mismatch once a stop has been accepted. The
/// recording opens, probes, writes through, and releases a lock connection, any
/// of which can be the stall that let the stop win, so it is bounded; the error
/// on expiry says the failure state is not persisted.
pub(crate) async fn record_live_mismatch_after_stop(
    database: &RunnerDatabase,
    store: &PhaseStore,
    chain_id: &str,
    reason: &str,
) -> RunnerResult<()> {
    read_after_stop(
        &format!("recording the live verification mismatch for chain {chain_id}"),
        record_live_mismatch_with_lock(database, store, chain_id, reason),
    )
    .await
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

#[cfg(test)]
mod read_after_stop_tests {
    use crate::error::{ErrorKind, RunnerResult};

    #[tokio::test]
    async fn a_read_that_never_answers_reports_the_state_as_unread() {
        let error =
            super::read_after_stop("the redo for chain some-chain phase interpret", async {
                std::future::pending::<RunnerResult<()>>().await
            })
            .await
            .expect_err("a read that never answers must not hold the stop");
        assert_eq!(error.kind(), ErrorKind::InvalidTransition);
        let message = error.to_string();
        assert!(message.contains("did not answer within"), "{message}");
        assert!(message.contains("some-chain phase interpret"), "{message}");
    }

    #[tokio::test]
    async fn a_read_that_answers_in_time_is_passed_through() {
        let value =
            super::read_after_stop("nothing", async { Ok::<_, crate::error::RunnerError>(7) })
                .await
                .expect("an answered read is returned");
        assert_eq!(value, 7);
    }
}
