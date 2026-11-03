//! The rebuild's own connection. Every statement on it is raced against the stop and, when the
//! stop wins, cancelled on the server, so a statement waiting on a lock never holds the stop or
//! the rebuild lock. Until the builds start, its statements run under short lock and statement
//! timeouts.
use std::{future::Future, pin::Pin, time::Instant};

use bigname_storage::walk_index_set::{IndexState, IndexStatus};
use sqlx::{Connection, PgConnection, PgPool};
use tracing::{info, warn};

use super::{
    BUILD_TIMEOUT, IndexBuildSettings, LOCK, POLL, PROGRESS, RECOVERY, Restore, describe,
    failed_before, load, present, warn_other_definitions,
};
use crate::{
    database::RunnerDatabase,
    error::{ErrorKind, RunnerError, RunnerResult},
    runner_support::StopClock,
};

/// The bounds on the session's reads before the builds. A read that waits longer on a lock,
/// such as one behind an `ACCESS EXCLUSIVE` lock on `chain_phase_state`, fails as a transient
/// error and the restart loop retries it.
const PREPARE_TIMEOUTS: &str = "SELECT set_config('lock_timeout', '10s', false),
                                       set_config('statement_timeout', '1min', false)";

mod capacity;

pub(super) struct Session {
    pub(super) connection: PgConnection,
    pid: i32,
    pool: PgPool,
}

impl Session {
    pub(super) async fn open(database: &RunnerDatabase) -> RunnerResult<Self> {
        let mut connection = PgConnection::connect_with(&database.connect_options())
            .await
            .map_err(|error| {
                RunnerError::database("failed to open the index rebuild connection", error)
            })?;
        let pid = sqlx::query_scalar("SELECT pg_backend_pid()")
            .fetch_one(&mut connection)
            .await
            .map_err(|error| {
                RunnerError::database("failed to read the index rebuild backend", error)
            })?;
        sqlx::query(PREPARE_TIMEOUTS)
            .execute(&mut connection)
            .await
            .map_err(|error| {
                RunnerError::database("failed to bound the index rebuild's reads", error)
            })?;
        Ok(Self {
            connection,
            pid,
            pool: database.pool().clone(),
        })
    }

    pub(super) async fn restore(
        &mut self,
        database: &RunnerDatabase,
        run: &Restore<'_>,
    ) -> RunnerResult<()> {
        let chain_id = run.chain_id;
        if !self.lock(run).await? {
            return Ok(());
        }
        // Another chain or process may have finished while this one waited.
        let Some(states) = self.load(run).await? else {
            return Ok(());
        };
        if states.iter().all(present) {
            warn_other_definitions(chain_id, &states);
            return Ok(());
        }
        failed_before(database)?;
        if !self.await_other_builds(run).await? {
            return Ok(());
        }
        let Some(states) = self.load(run).await? else {
            return Ok(());
        };
        if let Some((name, held_by)) = states.iter().find_map(|status| match &status.state {
            IndexState::Foreign { held_by } => Some((status.index.name, held_by)),
            _ => None,
        }) {
            return Err(RunnerError::new(
                ErrorKind::Configuration,
                format!(
                    "bigname_phase.{name} is {held_by}, not an index on \
                     bigname_phase.normalized_events, so the index the walk index set scripts \
                     drop under that name cannot be built; remove or rename that relation, then \
                     restart the runner"
                ),
            ));
        }
        let pending: Vec<&IndexStatus> = states.iter().filter(|status| !present(status)).collect();
        if pending.is_empty() {
            warn_other_definitions(chain_id, &states);
            return Ok(());
        }
        if !self.warn_unfinished_interprets(run).await? {
            return Ok(());
        }
        let Some((maintenance_work_mem, max_parallel_maintenance_workers)) =
            self.configure(database.index_build_settings(), run).await?
        else {
            return Ok(());
        };
        let mut announced = false;
        for status in &pending {
            let name = status.index.name;
            // An invalid leftover is dropped before the wait: it holds space that a build cut
            // off part way already wrote, which the wait may need back.
            if status.state == IndexState::Invalid {
                let statement = format!("DROP INDEX CONCURRENTLY IF EXISTS bigname_phase.{name}");
                if !self.run(&statement, run).await? {
                    return Ok(());
                }
            }
            if !self.await_capacity(run).await? {
                return Ok(());
            }
            if !announced {
                info!(
                    chain_id,
                    indexes = pending.len(),
                    maintenance_work_mem = %maintenance_work_mem,
                    max_parallel_maintenance_workers = %max_parallel_maintenance_workers,
                    "building the normalized_events indexes the walk index set scripts drop \
                     before Project"
                );
                announced = true;
            }
            let started = Instant::now();
            if !self.run(status.index.create, run).await? {
                return Ok(());
            }
            info!(
                chain_id,
                index = name,
                elapsed_ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX),
                "built a normalized_events index the walk index set scripts drop"
            );
        }
        if !self.analyze(run).await? {
            return Ok(());
        }
        let Some(states) = self.load(run).await? else {
            return Ok(());
        };
        if let Some(status) = states.iter().find(|status| !present(status)) {
            return Err(RunnerError::transient(format!(
                "failed to build the normalized_events indexes the walk index set scripts drop: \
                 bigname_phase.{} is {} after its build; {RECOVERY}",
                status.index.name,
                describe(&status.state)
            ))
            .not_retried());
        }
        warn_other_definitions(chain_id, &states);
        info!(
            chain_id,
            built = pending.len(),
            "every normalized_events index the walk index set scripts drop is in place"
        );
        Ok(())
    }

    /// The drop list's state, read on the session. `None` when a stop came first.
    async fn load(&mut self, run: &Restore<'_>) -> RunnerResult<Option<Vec<IndexStatus>>> {
        let (connection, pool, pid) = (&mut self.connection, &self.pool, self.pid);
        raced(pool, pid, run, load(connection)).await.transpose()
    }

    /// Waits for the restore lock. `false` when a stop came first.
    async fn lock(&mut self, run: &Restore<'_>) -> RunnerResult<bool> {
        let mut logged = false;
        loop {
            let (connection, pool, pid) = (&mut self.connection, &self.pool, self.pid);
            let acquired = raced(pool, pid, run, async move {
                sqlx::query_scalar::<_, bool>(
                    "SELECT pg_try_advisory_lock(hashtextextended($1::text, 0::bigint))",
                )
                .bind(LOCK)
                .fetch_one(connection)
                .await
                .map_err(|error| {
                    RunnerError::database("failed to take the index rebuild lock", error)
                })
            })
            .await
            .transpose()?;
            match acquired {
                None => return Ok(false),
                Some(true) => return Ok(true),
                Some(false) => {}
            }
            if !logged {
                info!(
                    chain_id = run.chain_id,
                    "waiting for another restore of the normalized_events indexes to finish"
                );
                logged = true;
            }
            if !pause(run).await {
                return Ok(false);
            }
        }
    }

    /// Waits until no other backend holds the lock an index build, a `VACUUM` or an `ANALYZE`
    /// holds on the table, so an invalid index is never dropped while a build of it, say one a
    /// killed runner or another login left on the server, can still complete. `pg_locks` shows
    /// every login's locks, where `pg_stat_progress_create_index` shows only this login's
    /// builds. Autovacuum is not waited for: it gives way to a build, apart from an
    /// anti-wraparound vacuum, which the build itself then waits behind. `false` when a stop
    /// came first.
    async fn await_other_builds(&mut self, run: &Restore<'_>) -> RunnerResult<bool> {
        let mut logged = false;
        loop {
            let (connection, pool, pid) = (&mut self.connection, &self.pool, self.pid);
            let building = raced(pool, pid, run, async move {
                sqlx::query_scalar::<_, Option<String>>(
                    "SELECT string_agg(DISTINCT held.pid::text, ', ')
                     FROM pg_locks held
                     LEFT JOIN pg_stat_activity activity ON activity.pid = held.pid
                     WHERE held.locktype = 'relation'
                       AND held.database = (SELECT oid FROM pg_database
                                            WHERE datname = current_database())
                       AND held.relation = to_regclass('bigname_phase.normalized_events')
                       AND held.mode = 'ShareUpdateExclusiveLock'
                       AND held.granted
                       AND held.pid <> pg_backend_pid()
                       AND activity.backend_type IS DISTINCT FROM 'autovacuum worker'",
                )
                .fetch_one(connection)
                .await
                .map_err(|error| {
                    RunnerError::database("failed to read the index builds in progress", error)
                })
            })
            .await
            .transpose()?;
            let building = match building {
                None => return Ok(false),
                Some(None) => return Ok(true),
                Some(Some(building)) => building,
            };
            if !logged {
                info!(
                    chain_id = run.chain_id,
                    backends = %building,
                    "waiting for another index build or maintenance on normalized_events to \
                     finish"
                );
                logged = true;
            }
            if !pause(run).await {
                return Ok(false);
            }
        }
    }

    /// The indexes serve every chain on the database, so a build while another chain's Interpret
    /// has not completed, in a redo or on its first walk, makes that chain's remaining batches
    /// maintain them. `false` when a stop came first.
    async fn warn_unfinished_interprets(&mut self, run: &Restore<'_>) -> RunnerResult<bool> {
        let chain_id = run.chain_id;
        let (connection, pool, pid) = (&mut self.connection, &self.pool, self.pid);
        let walking = raced(pool, pid, run, async move {
            sqlx::query_scalar::<_, Option<String>>(
                "SELECT string_agg(
                            chain_id || ' (' || phase_status::text
                                || CASE WHEN redo_in_progress THEN ', redo' ELSE '' END || ')',
                            ', ' ORDER BY chain_id)
                 FROM chain_phase_state
                 WHERE phase_name = 'interpret'
                   AND chain_id <> $1
                   AND (redo_in_progress OR phase_status::text <> 'completed')",
            )
            .bind(chain_id)
            .fetch_one(connection)
            .await
            .map_err(|error| {
                RunnerError::database("failed to read the other chains' Interpret state", error)
            })
        })
        .await
        .transpose()?;
        let Some(walking) = walking else {
            return Ok(false);
        };
        if let Some(walking) = walking {
            warn!(
                chain_id,
                interpret_unfinished_chains = %walking,
                "building the normalized_events indexes the walk index set scripts drop while \
                 these chains' Interpret has not completed; their remaining batches maintain the \
                 rebuilt indexes"
            );
        }
        Ok(true)
    }

    /// Sets the session up for the builds and returns the maintenance settings in force. `None`
    /// when a stop came first.
    async fn configure(
        &mut self,
        settings: &IndexBuildSettings,
        run: &Restore<'_>,
    ) -> RunnerResult<Option<(String, String)>> {
        let (connection, pool, pid) = (&mut self.connection, &self.pool, self.pid);
        let configured = raced(pool, pid, run, async move {
            sqlx::query(
                "SELECT set_config('lock_timeout', '0', false),
                        set_config('statement_timeout', $1, false)",
            )
            .bind(BUILD_TIMEOUT)
            .execute(&mut *connection)
            .await
            .map_err(|error| {
                RunnerError::database("failed to set the index rebuild timeouts", error)
            })?;
            for (setting, environment, value) in [
                (
                    "maintenance_work_mem",
                    "BIGNAME_PHASE_RUNNER_RESTORE_MAINTENANCE_WORK_MEM",
                    settings.maintenance_work_mem.clone(),
                ),
                (
                    "max_parallel_maintenance_workers",
                    "BIGNAME_PHASE_RUNNER_RESTORE_PARALLEL_MAINTENANCE_WORKERS",
                    settings
                        .max_parallel_maintenance_workers
                        .map(|workers| workers.to_string()),
                ),
            ] {
                let Some(value) = value else {
                    continue;
                };
                sqlx::query("SELECT set_config($1, $2, false)")
                    .bind(setting)
                    .bind(&value)
                    .execute(&mut *connection)
                    .await
                    .map_err(|error| refused_setting(environment, &value, setting, error))?;
            }
            sqlx::query_as::<_, (String, String)>(
                "SELECT current_setting('maintenance_work_mem'),
                        current_setting('max_parallel_maintenance_workers')",
            )
            .fetch_one(&mut *connection)
            .await
            .map_err(|error| {
                RunnerError::database("failed to read the index rebuild settings", error)
            })
        })
        .await;
        configured.transpose()
    }

    /// Refreshes the planner statistics, which expression and partial indexes lack until the
    /// table is analyzed. A failure or a stop is a warning that asks for an `ANALYZE` by hand: a
    /// later restore finds every index in place and does not analyze.
    async fn analyze(&mut self, run: &Restore<'_>) -> RunnerResult<bool> {
        let (connection, pool, pid) = (&mut self.connection, &self.pool, self.pid);
        let bounded = raced(pool, pid, run, async move {
            sqlx::query(
                "SELECT set_config('lock_timeout', '10s', false),
                        set_config('statement_timeout', '1h', false)",
            )
            .execute(connection)
            .await
        })
        .await;
        let analyzed = match bounded {
            None => Ok(false),
            Some(Ok(_)) => {
                self.statement("ANALYZE bigname_phase.normalized_events", run)
                    .await
            }
            Some(Err(error)) => Err(error),
        };
        let (done, error) = match analyzed {
            Ok(done) => (done, None),
            Err(error) => (true, Some(error.to_string())),
        };
        if !done || error.is_some() {
            warn!(
                chain_id = run.chain_id,
                error = error.as_deref().unwrap_or("stopped"),
                "could not refresh normalized_events statistics after building its indexes, and \
                 no later restore does; run ANALYZE bigname_phase.normalized_events by hand"
            );
        }
        Ok(done)
    }

    /// Runs one DDL statement. `false` when a stop came first.
    async fn run(&mut self, statement: &str, run: &Restore<'_>) -> RunnerResult<bool> {
        self.statement(statement, run).await.map_err(|error| {
            let what = statement.lines().next().unwrap_or(statement);
            RunnerError::transient(format!(
                "failed to build the normalized_events indexes the walk index set scripts \
                 drop: {what} failed: {error}; {RECOVERY}"
            ))
            .not_retried()
        })
    }

    /// Runs a statement that can take hours, recording loop progress while the server runs it.
    /// Under Verify and Live a Verify failure cancels the same token as a stop and so starts the
    /// stop budget, which then bounds only the chain's exit.
    async fn statement(&mut self, statement: &str, run: &Restore<'_>) -> Result<bool, sqlx::Error> {
        if run.cancellation.is_cancelled() {
            return Ok(false);
        }
        let (pool, pid) = (&self.pool, self.pid);
        // `Executor::execute` returns a boxed `Send` future, and a `&str` goes out as a simple
        // query, outside any transaction, as a concurrent build needs.
        let mut running = sqlx::Executor::execute(&mut self.connection, statement);
        let mut progress =
            tokio::time::interval_at(tokio::time::Instant::now() + PROGRESS, PROGRESS);
        progress.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                biased;
                result = &mut running => return result.map(|_| true),
                () = run.cancellation.cancelled() => {
                    cancel_on_server(pool, pid, run.stop_clock, Pin::new(&mut running)).await;
                    return Ok(false);
                }
                _ = progress.tick() => {
                    if building_on_server(pool, pid).await {
                        run.progress();
                    }
                }
            }
        }
    }
}

fn refused_setting(
    environment: &str,
    value: &str,
    setting: &str,
    error: sqlx::Error,
) -> RunnerError {
    match error {
        sqlx::Error::Database(ref refused)
            if refused
                .code()
                .is_some_and(|code| matches!(&*code, "22023" | "22003")) =>
        {
            RunnerError::new(
                ErrorKind::Configuration,
                format!("PostgreSQL refused {environment}={value} as {setting}: {error}"),
            )
        }
        error => RunnerError::database("failed to set the index rebuild settings", error),
    }
}

/// Runs `query` on the session's connection unless a stop comes first. Then the query is
/// cancelled on the server, because a dropped future alone would leave it waiting there, say on
/// a lock, holding the rebuild lock until it ends. `None` when a stop came first.
async fn raced<F>(pool: &PgPool, pid: i32, run: &Restore<'_>, query: F) -> Option<F::Output>
where
    F: Future + Send,
    F::Output: Send,
{
    if run.cancellation.is_cancelled() {
        return None;
    }
    let mut query = std::pin::pin!(query);
    tokio::select! {
        biased;
        output = &mut query => Some(output),
        () = run.cancellation.cancelled() => {
            cancel_on_server(pool, pid, run.stop_clock, query).await;
            None
        }
    }
}

/// Cancels the session's statement on the server and waits for its reply, within what is left
/// of the chain's stop budget.
async fn cancel_on_server<F>(pool: &PgPool, pid: i32, stop_clock: &StopClock, query: Pin<&mut F>)
where
    F: Future + Send,
{
    let _ = tokio::time::timeout(stop_clock.remaining(), async {
        let _ = sqlx::query("SELECT pg_cancel_backend($1)")
            .bind(pid)
            .execute(pool)
            .await;
        let _ = query.await;
    })
    .await;
}

/// Whether the rebuild's backend is still running its statement, read through the pool. Loop
/// progress is recorded only then, so a connection that died without a reply reads as a
/// stalled loop rather than a build.
async fn building_on_server(pool: &PgPool, pid: i32) -> bool {
    let active = sqlx::query_scalar::<_, bool>(
        "SELECT coalesce(state = 'active', false) FROM pg_stat_activity WHERE pid = $1",
    )
    .bind(pid)
    .fetch_optional(pool);
    matches!(tokio::time::timeout(POLL, active).await, Ok(Ok(Some(true))))
}

/// Waits one poll interval, recording loop progress. `false` when a stop came first.
async fn pause(run: &Restore<'_>) -> bool {
    run.progress();
    tokio::select! {
        () = run.cancellation.cancelled() => false,
        () = tokio::time::sleep(POLL) => true,
    }
}
