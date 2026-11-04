//! Before Project runs, rebuilds the `normalized_events` indexes `ops/walk-index-set/drop.sql`
//! drops for a from-zero walk or a full-history Interpret redo (`docs/storage.md` § Walk index
//! set). Each build runs `install.sql`'s statement, concurrently and one at a time, on a
//! connection of its own outside any transaction, under a session advisory lock that one
//! restore holds at a time across every process on the database. A valid index is kept
//! whatever its definition, and a name another relation holds stops Project instead of being
//! dropped.
use std::{collections::BTreeSet, future::Future, pin::Pin, sync::Mutex, time::Duration};

use bigname_storage::walk_index_set::{IndexState, IndexStatus, load_drop_list_state};
use sqlx::{Connection, PgConnection, PgPool};
use tokio_util::sync::CancellationToken;
use tracing::warn;

use crate::{
    capacity::CapacityGuard,
    database::RunnerDatabase,
    error::{ErrorKind, RunnerError, RunnerResult},
    metrics::RunnerLoopHeartbeat,
    runner_support::StopClock,
    shutdown::until_cancelled,
    state::PhaseStore,
};

mod session;

use session::Session;

const LOCK: &str = "bigname.walk_index_set";
const POLL: Duration = Duration::from_secs(5);
const CLOSE: Duration = Duration::from_secs(10);
/// How often a running build records loop progress, well inside the runner's stale-loop alert.
const PROGRESS: Duration = Duration::from_secs(30);
/// `install.sql`'s bound. A concurrent build waits for every transaction already running, such
/// as another chain's Interpret batch.
const BUILD_TIMEOUT: &str = "6h";
const RECOVERY: &str = "Project does not start until every one is in place; confirm in \
                        pg_stat_activity that no CREATE INDEX on normalized_events is still \
                        running, then restart the runner, or after a one-shot Interpret redo \
                        run the matching Project redo, which drops an invalid leftover and \
                        builds it again";

/// Session settings for the rebuild. Unset keeps the server's value.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct IndexBuildSettings {
    pub maintenance_work_mem: Option<String>,
    pub max_parallel_maintenance_workers: Option<u16>,
}

/// What a restore reports to the runner while it waits or builds.
pub(crate) struct Restore<'a> {
    pub(crate) chain_id: &'a str,
    pub(crate) cancellation: &'a CancellationToken,
    /// The chain's stop clock, which bounds everything after a stop.
    pub(crate) stop_clock: &'a StopClock,
    /// Where loop progress and a wait for storage capacity are recorded, so a long build does
    /// not read as a stalled runner and a wait for capacity shows on its gauge.
    pub(crate) heartbeat: Option<&'a RunnerLoopHeartbeat>,
    /// The batch loop's storage capacity check, which each build waits on.
    pub(crate) capacity: &'a CapacityGuard,
    /// The chain's phase rows, for pausing a running Project row while a build waits.
    pub(crate) store: &'a PhaseStore,
}

impl Restore<'_> {
    fn progress(&self) {
        if let Some(heartbeat) = self.heartbeat {
            heartbeat.record_progress(self.chain_id);
        }
    }

    fn set_capacity_blocked(&self, blocked: bool) {
        if let Some(heartbeat) = self.heartbeat {
            heartbeat.set_walk_index_capacity_blocked(self.chain_id, blocked);
        }
    }
}

/// The restore as a boxed `Send` future, so the restart loop's future stays `Send` without
/// rustc proving it for every lifetime of the references the restore holds.
pub(crate) type RestoreFuture<'a> = Pin<Box<dyn Future<Output = RunnerResult<()>> + Send + 'a>>;

/// Returns once every index is in place, or as soon as `cancellation` fires. A failed build is
/// not retried in this process: the next start drops its invalid leftover and builds it again.
pub(crate) fn restore<'a>(database: &'a RunnerDatabase, run: &'a Restore<'a>) -> RestoreFuture<'a> {
    Box::pin(restore_all(database, run))
}

async fn restore_all(database: &RunnerDatabase, run: &Restore<'_>) -> RunnerResult<()> {
    let Restore {
        chain_id,
        cancellation,
        ..
    } = *run;
    let Some(states) = until_cancelled(cancellation, load_from_pool(database.pool())).await? else {
        return Ok(());
    };
    if states.iter().all(present) {
        warn_other_definitions(chain_id, &states);
        return Ok(());
    }
    failed_before(database)?;
    let Some(mut session) = until_cancelled(cancellation, Session::open(database)).await? else {
        return Ok(());
    };
    let result = session.restore(database, run).await;
    // Only a failed build, drop or final check is a not-retried transient error here.
    if let Err(error) = &result
        && error.kind() == ErrorKind::Transient
        && !error.is_retryable()
    {
        let _ = database.index_build_failure().set(error.to_string());
    }
    // After a stop a cancelled statement may still be owed its reply, so the session is dropped
    // rather than closed. Either way the advisory lock goes with it.
    if !cancellation.is_cancelled() {
        let _ = tokio::time::timeout(CLOSE, session.connection.close()).await;
    }
    result
}

async fn load_from_pool(pool: &PgPool) -> RunnerResult<Vec<IndexStatus>> {
    let mut connection = pool.acquire().await.map_err(|error| {
        RunnerError::database(
            "failed to acquire a connection to read the normalized_events indexes",
            error,
        )
    })?;
    load(&mut connection).await
}

async fn load(connection: &mut PgConnection) -> RunnerResult<Vec<IndexStatus>> {
    load_drop_list_state(connection)
        .await
        .map(Option::unwrap_or_default)
        .map_err(|error| {
            RunnerError::database(
                "failed to read the state of the normalized_events indexes the walk index set \
                 scripts drop",
                error,
            )
        })
}

/// A rebuild that failed in this process is not run again by another chain: each stops at its
/// Project entry with the same error, so the process exits and a restart drops the leftover.
fn failed_before(database: &RunnerDatabase) -> RunnerResult<()> {
    match database.index_build_failure().get() {
        Some(failure) => Err(RunnerError::transient(format!(
            "a rebuild of the normalized_events indexes the walk index set scripts drop already \
             failed in this process, and it is not retried: {failure}"
        ))
        .not_retried()),
        None => Ok(()),
    }
}

fn present(status: &IndexStatus) -> bool {
    matches!(status.state, IndexState::Present { .. })
}

fn describe(state: &IndexState) -> &'static str {
    match state {
        IndexState::Present { .. } => "in place",
        IndexState::Missing => "missing",
        IndexState::Invalid => "not valid and ready",
        IndexState::Foreign { .. } => "held by another relation",
    }
}

/// Each index and definition already warned about in this process. Project is entered again
/// after every post-Live pass, and one warning per definition is enough.
static WARNED: Mutex<BTreeSet<(&str, String)>> = Mutex::new(BTreeSet::new());

fn warn_other_definitions(chain_id: &str, states: &[IndexStatus]) {
    for status in states {
        let Some(found) = status.other_definition() else {
            continue;
        };
        let first = WARNED
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert((status.index.name, found.to_owned()));
        if first {
            warn!(
                chain_id,
                index = status.index.name,
                found,
                expected = status.index.definition,
                "a normalized_events index the walk index set scripts drop has another \
                 definition than this build expects; it is kept and read as found, because a \
                 changed definition takes a new name (docs/storage.md, Walk index set)"
            );
        }
    }
}
