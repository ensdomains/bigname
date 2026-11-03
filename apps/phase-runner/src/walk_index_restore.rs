//! Before Project runs, rebuilds the `normalized_events` indexes `ops/walk-index-set/drop.sql`
//! drops for a from-zero walk or a full-history Interpret redo (`docs/storage.md` § Walk index
//! set). Each build runs `install.sql`'s statement, concurrently and one at a time, on a
//! connection of its own outside any transaction, under a session advisory lock that one
//! restore holds at a time across every process on the database. A valid index is kept
//! whatever its definition, and a name another relation holds stops Project instead of being
//! dropped.
use std::{
    collections::BTreeSet,
    sync::Mutex,
    time::{Duration, Instant},
};

use bigname_storage::walk_index_set::{IndexState, IndexStatus, load_drop_list_state};
use sqlx::{Connection, PgConnection, PgPool};
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use crate::{
    database::RunnerDatabase,
    error::{ErrorKind, RunnerError, RunnerResult},
    runner_support::StopClock,
    shutdown::until_cancelled,
};

const LOCK: &str = "bigname.walk_index_set";
const POLL: Duration = Duration::from_secs(5);
const CLOSE: Duration = Duration::from_secs(10);
/// How often a running build records loop progress, well inside the runner's stale-loop alert.
const PROGRESS: Duration = Duration::from_secs(30);
/// `install.sql`'s bound. A concurrent build waits for every transaction already running, such
/// as another chain's Interpret batch.
const BUILD_TIMEOUT: &str = "6h";
const RECOVERY: &str = "Project does not start until every one is in place; confirm in \
                        pg_stat_progress_create_index that no build is still running, then \
                        restart the runner, which drops an invalid leftover and builds it again";

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
    /// Records loop progress, so a long build does not read as a stalled runner.
    pub(crate) progress: &'a (dyn Fn() + Sync),
}

/// Returns once every index is in place, or as soon as `cancellation` fires. A failed build is
/// not retried in this process: the next start drops its invalid leftover and builds it again.
pub(crate) async fn restore(database: &RunnerDatabase, run: &Restore<'_>) -> RunnerResult<()> {
    let Restore {
        chain_id,
        cancellation,
        ..
    } = *run;
    let Some(states) = until_cancelled(cancellation, load(database.pool())).await? else {
        return Ok(());
    };
    if states.iter().all(present) {
        warn_other_definitions(chain_id, &states);
        return Ok(());
    }
    let Some(mut session) = until_cancelled(cancellation, Session::open(database)).await? else {
        return Ok(());
    };
    let result = session.restore(database, run).await;
    // After a stop a cancelled statement may still be owed its reply, so the session is dropped
    // rather than closed. Either way the advisory lock goes with it.
    if !cancellation.is_cancelled() {
        let _ = tokio::time::timeout(CLOSE, session.connection.close()).await;
    }
    result
}

async fn load<'a, A>(connection: A) -> RunnerResult<Vec<IndexStatus>>
where
    A: sqlx::Acquire<'a, Database = sqlx::Postgres>,
{
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

struct Session {
    connection: PgConnection,
    pid: i32,
}

impl Session {
    async fn open(database: &RunnerDatabase) -> RunnerResult<Self> {
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
        Ok(Self { connection, pid })
    }

    async fn restore(&mut self, database: &RunnerDatabase, run: &Restore<'_>) -> RunnerResult<()> {
        let chain_id = run.chain_id;
        if !self.lock(run).await? || !self.await_other_builds(run).await? {
            return Ok(());
        }
        // Another process may have finished while this one waited.
        let states = load(&mut self.connection).await?;
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
        self.warn_interpret_redos(chain_id).await?;
        let (maintenance_work_mem, max_parallel_maintenance_workers) =
            self.configure(database.index_build_settings()).await?;
        info!(
            chain_id,
            indexes = pending.len(),
            maintenance_work_mem = %maintenance_work_mem,
            max_parallel_maintenance_workers = %max_parallel_maintenance_workers,
            "building the normalized_events indexes the walk index set scripts drop before Project"
        );
        let pool = database.pool();
        for status in &pending {
            let name = status.index.name;
            if status.state == IndexState::Invalid {
                let statement = format!("DROP INDEX CONCURRENTLY IF EXISTS bigname_phase.{name}");
                if !self.run(pool, &statement, run).await? {
                    return Ok(());
                }
            }
            let started = Instant::now();
            if !self.run(pool, status.index.create, run).await? {
                return Ok(());
            }
            info!(
                chain_id,
                index = name,
                elapsed_ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX),
                "built a normalized_events index the walk index set scripts drop"
            );
        }
        if !self.analyze(pool, run).await? {
            return Ok(());
        }
        let states = load(&mut self.connection).await?;
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

    /// Waits for the restore lock. `false` when a stop came first.
    async fn lock(&mut self, run: &Restore<'_>) -> RunnerResult<bool> {
        let mut logged = false;
        loop {
            let acquired: bool = sqlx::query_scalar(
                "SELECT pg_try_advisory_lock(hashtextextended($1::text, 0::bigint))",
            )
            .bind(LOCK)
            .fetch_one(&mut self.connection)
            .await
            .map_err(|error| {
                RunnerError::database("failed to take the index rebuild lock", error)
            })?;
            if acquired {
                return Ok(true);
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
    /// builds. Autovacuum is not waited for: it gives way to a build. `false` when a stop came
    /// first.
    async fn await_other_builds(&mut self, run: &Restore<'_>) -> RunnerResult<bool> {
        let mut logged = false;
        loop {
            let building: Option<String> = sqlx::query_scalar(
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
            .fetch_one(&mut self.connection)
            .await
            .map_err(|error| {
                RunnerError::database("failed to read the index builds in progress", error)
            })?;
            let Some(building) = building else {
                return Ok(true);
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

    /// The indexes serve every chain on the database, so a build while another chain is still
    /// in an Interpret redo makes that redo's remaining batches maintain them.
    async fn warn_interpret_redos(&mut self, chain_id: &str) -> RunnerResult<()> {
        let walking: Option<String> = sqlx::query_scalar(
            "SELECT string_agg(chain_id, ', ' ORDER BY chain_id) FROM chain_phase_state
             WHERE phase_name = 'interpret' AND redo_in_progress",
        )
        .fetch_one(&mut self.connection)
        .await
        .map_err(|error| RunnerError::database("failed to read the Interpret redos", error))?;
        if let Some(walking) = walking {
            warn!(
                chain_id,
                interpret_redo_chains = %walking,
                "building the normalized_events indexes the walk index set scripts drop while \
                 these chains are still in an Interpret redo; their remaining batches maintain \
                 the rebuilt indexes"
            );
        }
        Ok(())
    }

    /// Sets the session up for the builds and returns the maintenance settings in force.
    async fn configure(&mut self, settings: &IndexBuildSettings) -> RunnerResult<(String, String)> {
        sqlx::query(
            "SELECT set_config('lock_timeout', '0', false),
                    set_config('statement_timeout', $1, false)",
        )
        .bind(BUILD_TIMEOUT)
        .execute(&mut self.connection)
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
                .execute(&mut self.connection)
                .await
                .map_err(|error| {
                    RunnerError::new(
                        ErrorKind::Configuration,
                        format!("PostgreSQL refused {environment}={value} as {setting}: {error}"),
                    )
                })?;
        }
        sqlx::query_as::<_, (String, String)>(
            "SELECT current_setting('maintenance_work_mem'),
                    current_setting('max_parallel_maintenance_workers')",
        )
        .fetch_one(&mut self.connection)
        .await
        .map_err(|error| RunnerError::database("failed to read the index rebuild settings", error))
    }

    /// Refreshes the planner statistics, which expression and partial indexes lack until the
    /// table is analyzed. A failure is a warning: Interpret and the next restore analyze again.
    async fn analyze(&mut self, pool: &PgPool, run: &Restore<'_>) -> RunnerResult<bool> {
        let analyzed = match sqlx::query(
            "SELECT set_config('lock_timeout', '10s', false),
                    set_config('statement_timeout', '1h', false)",
        )
        .execute(&mut self.connection)
        .await
        {
            Ok(_) => {
                self.statement(pool, "ANALYZE bigname_phase.normalized_events", run)
                    .await
            }
            Err(error) => Err(error),
        };
        match analyzed {
            Ok(done) => Ok(done),
            Err(error) => {
                warn!(
                    chain_id = run.chain_id,
                    error = %error,
                    "could not refresh normalized_events statistics after building its indexes"
                );
                Ok(true)
            }
        }
    }

    /// Runs one DDL statement. `false` when a stop came first.
    async fn run(
        &mut self,
        pool: &PgPool,
        statement: &str,
        run: &Restore<'_>,
    ) -> RunnerResult<bool> {
        self.statement(pool, statement, run).await.map_err(|error| {
            let what = statement.lines().next().unwrap_or(statement);
            RunnerError::transient(format!(
                "failed to build the normalized_events indexes the walk index set scripts \
                 drop: {what} failed: {error}; {RECOVERY}"
            ))
            .not_retried()
        })
    }

    /// A stop cancels the statement on the server: a dropped connection alone would leave a
    /// build running there until it next writes to this client. The cancel and its reply share
    /// what is left of the chain's stop budget.
    async fn statement(
        &mut self,
        pool: &PgPool,
        statement: &str,
        run: &Restore<'_>,
    ) -> Result<bool, sqlx::Error> {
        if run.cancellation.is_cancelled() {
            return Ok(false);
        }
        let pid = self.pid;
        let mut running = std::pin::pin!(sqlx::raw_sql(statement).execute(&mut self.connection));
        let mut progress = tokio::time::interval(PROGRESS);
        progress.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                biased;
                result = &mut running => return result.map(|_| true),
                () = run.cancellation.cancelled() => {
                    let _ = tokio::time::timeout(run.stop_clock.remaining(), async {
                        let _ = sqlx::query("SELECT pg_cancel_backend($1)")
                            .bind(pid)
                            .execute(pool)
                            .await;
                        let _ = (&mut running).await;
                    })
                    .await;
                    return Ok(false);
                }
                _ = progress.tick() => (run.progress)(),
            }
        }
    }
}

/// Waits one poll interval, recording loop progress. `false` when a stop came first.
async fn pause(run: &Restore<'_>) -> bool {
    (run.progress)();
    tokio::select! {
        () = run.cancellation.cancelled() => false,
        () = tokio::time::sleep(POLL) => true,
    }
}
