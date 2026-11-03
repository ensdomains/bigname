//! The rebuild's wait for storage capacity before each build, with the batch loop's check.
use tracing::warn;

use super::{Restore, Session, raced};
use crate::{
    error::{RunnerError, RunnerResult},
    phase::PhaseName,
    shutdown::until_cancelled,
    state::PhaseStatus,
};

impl Session {
    /// Waits until storage capacity allows the next build, with the batch loop's probe and
    /// thresholds. While it waits the chain's gauge reads 1 and the session is pinged every
    /// poll, so an idle cutoff cannot end it, and the rebuild lock with it, unnoticed. A running
    /// Project row is paused, as the batch loop pauses it, and resumed after. A running or
    /// paused row records loop progress. An idle or completed row is left as it is and records
    /// none, so the stale-loop alert names a long wait. `false` when a stop came first.
    pub(super) async fn await_capacity(&mut self, run: &Restore<'_>) -> RunnerResult<bool> {
        let mut waiting: Option<Waiting<'_, '_>> = None;
        loop {
            let Some(status) =
                until_cancelled(run.cancellation, run.capacity.check(&self.pool, 0)).await?
            else {
                return Ok(false);
            };
            if status.is_available() {
                if waiting.as_ref().is_some_and(|waiting| waiting.resume) {
                    let resumed = until_cancelled(
                        run.cancellation,
                        run.store.resume_phase(run.chain_id, PhaseName::Project),
                    )
                    .await?;
                    return Ok(resumed.is_some());
                }
                return Ok(true);
            }
            if waiting.is_none() {
                let mut started = Waiting::start(run);
                let Some(row) = until_cancelled(run.cancellation, pause_running(run)).await? else {
                    return Ok(false);
                };
                started.resume = row == PhaseStatus::Running;
                started.records_progress =
                    matches!(row, PhaseStatus::Running | PhaseStatus::Paused);
                waiting = Some(started);
            }
            warn!(
                chain_id = run.chain_id,
                breach_reasons = ?status.breach_reasons,
                database_size_bytes = status.measurement.database_size_bytes,
                free_disk_bytes = status.measurement.free_disk_bytes,
                "the rebuild of the normalized_events indexes the walk index set scripts drop \
                 waits until storage capacity recovers"
            );
            if waiting
                .as_ref()
                .is_some_and(|waiting| waiting.records_progress)
            {
                run.progress();
            }
            tokio::select! {
                () = run.cancellation.cancelled() => return Ok(false),
                () = tokio::time::sleep(run.capacity.poll_interval()) => {}
            }
            if !self.ping(run).await? {
                return Ok(false);
            }
        }
    }

    /// A round trip on the session. A connection that has gone fails as a retryable error, so
    /// the restart loop opens a new one, rather than as a failed build. `false` when a stop came
    /// first.
    async fn ping(&mut self, run: &Restore<'_>) -> RunnerResult<bool> {
        let (connection, pool, pid) = (&mut self.connection, &self.pool, self.pid);
        let pinged = raced(pool, pid, run, async move {
            sqlx::query("SELECT 1")
                .execute(connection)
                .await
                .map_err(|error| {
                    RunnerError::database(
                        "the index rebuild connection failed while it waited for storage capacity",
                        error,
                    )
                })
        })
        .await
        .transpose()?;
        Ok(pinged.is_some())
    }
}

/// A wait for storage capacity in progress: the chain's gauge reads 1 until it is dropped, on
/// every way out of the wait.
struct Waiting<'r, 'a> {
    run: &'r Restore<'a>,
    /// Whether the wait paused a running Project row, which it resumes.
    resume: bool,
    /// Whether the row is running or paused, so the wait records loop progress as the batch
    /// loop's does.
    records_progress: bool,
}

impl<'r, 'a> Waiting<'r, 'a> {
    fn start(run: &'r Restore<'a>) -> Self {
        run.set_capacity_blocked(true);
        Self {
            run,
            resume: false,
            records_progress: false,
        }
    }
}

impl Drop for Waiting<'_, '_> {
    fn drop(&mut self) {
        self.run.set_capacity_blocked(false);
    }
}

/// The chain's Project row, paused first when it is running. The batch loop pauses a running
/// row it holds for capacity, and an idle or completed one has no pause.
async fn pause_running(run: &Restore<'_>) -> RunnerResult<PhaseStatus> {
    let row = run.store.status(run.chain_id, PhaseName::Project).await?;
    if row == PhaseStatus::Running {
        run.store
            .pause_phase(run.chain_id, PhaseName::Project)
            .await?;
    }
    Ok(row)
}
