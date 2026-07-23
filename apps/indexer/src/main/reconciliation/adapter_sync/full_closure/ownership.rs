use std::{
    env,
    fmt::{self, Write as _},
    future::Future,
    time::{Duration, Instant},
};

use anyhow::{Context, Result, bail};
use sqlx::{
    Connection, Either, PgConnection, PgPool,
    postgres::{PgAdvisoryLock, PgAdvisoryLockKey},
};
use tracing::{error, info, warn};

use crate::reconciliation::guard_release::prioritize_operation_error;

const FULL_CLOSURE_REPLAY_LOCK_POLL_INTERVAL: Duration = Duration::from_millis(50);
const DEFAULT_FULL_CLOSURE_REPLAY_LOCK_WAIT_LOG_INTERVAL: Duration = Duration::from_secs(60);
const FULL_CLOSURE_REPLAY_LOCK_WAIT_LOG_INTERVAL_ENV: &str =
    "BIGNAME_INDEXER_FULL_CLOSURE_REPLAY_LOCK_WAIT_LOG_INTERVAL_SECS";
const FULL_CLOSURE_REPLAY_LOCK_WAIT_DEADLINE_ENV: &str =
    "BIGNAME_INDEXER_FULL_CLOSURE_REPLAY_LOCK_WAIT_DEADLINE_SECS";

pub(crate) trait FullClosureReplayLockWaitHeartbeat: Send {
    fn begin_wait<'a>(
        &'a mut self,
        pool: &'a PgPool,
        deployment_profile: &'a str,
        chain: &'a str,
    ) -> bigname_adapters::StartupAdapterProgressFuture<'a>;

    fn finish_wait<'a>(
        &'a mut self,
        pool: &'a PgPool,
        deployment_profile: &'a str,
        chain: &'a str,
    ) -> bigname_adapters::StartupAdapterProgressFuture<'a>;
}

#[derive(Clone, Copy, Debug)]
struct FullClosureReplayLockWaitConfig {
    log_interval: Duration,
    deadline: Option<Duration>,
}

impl Default for FullClosureReplayLockWaitConfig {
    fn default() -> Self {
        Self {
            log_interval: DEFAULT_FULL_CLOSURE_REPLAY_LOCK_WAIT_LOG_INTERVAL,
            deadline: None,
        }
    }
}

impl FullClosureReplayLockWaitConfig {
    fn from_env() -> Result<Self> {
        let log_interval = match seconds_from_env(FULL_CLOSURE_REPLAY_LOCK_WAIT_LOG_INTERVAL_ENV)? {
            Some(0) => {
                bail!("{FULL_CLOSURE_REPLAY_LOCK_WAIT_LOG_INTERVAL_ENV} must be greater than zero")
            }
            Some(seconds) => Duration::from_secs(seconds),
            None => DEFAULT_FULL_CLOSURE_REPLAY_LOCK_WAIT_LOG_INTERVAL,
        };
        let deadline = seconds_from_env(FULL_CLOSURE_REPLAY_LOCK_WAIT_DEADLINE_ENV)?
            .filter(|seconds| *seconds > 0)
            .map(Duration::from_secs);
        Ok(Self {
            log_interval,
            deadline,
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct FullClosureReplayLockHolder {
    pid: i32,
    application_name: String,
}

#[derive(Debug)]
pub(crate) struct FullClosureReplayLockWaitDeadlineExceeded {
    deployment_profile: String,
    chain: String,
    waited: Duration,
    deadline: Duration,
    holder: Option<FullClosureReplayLockHolder>,
}

impl fmt::Display for FullClosureReplayLockWaitDeadlineExceeded {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "full-closure replay ownership wait deadline exceeded for {}/{} after {} ms \
             (configured deadline {} ms",
            self.deployment_profile,
            self.chain,
            duration_millis(self.waited),
            duration_millis(self.deadline),
        )?;
        if let Some(holder) = &self.holder {
            write!(
                formatter,
                "; holder pid {} application_name {:?}",
                holder.pid, holder.application_name
            )?;
        } else {
            formatter.write_str("; holder unavailable")?;
        }
        formatter.write_char(')')
    }
}

impl std::error::Error for FullClosureReplayLockWaitDeadlineExceeded {}

pub(super) async fn with_full_closure_replay_lock<T, Operation, OperationFuture>(
    pool: &PgPool,
    deployment_profile: &str,
    chain: &str,
    wait_heartbeat: &mut Option<&mut dyn FullClosureReplayLockWaitHeartbeat>,
    operation: Operation,
) -> Result<T>
where
    Operation: FnOnce() -> OperationFuture,
    OperationFuture: Future<Output = Result<T>>,
{
    let config = FullClosureReplayLockWaitConfig::from_env()?;
    with_full_closure_replay_lock_config(
        pool,
        deployment_profile,
        chain,
        wait_heartbeat,
        config,
        operation,
    )
    .await
}

async fn with_full_closure_replay_lock_config<T, Operation, OperationFuture>(
    pool: &PgPool,
    deployment_profile: &str,
    chain: &str,
    wait_heartbeat: &mut Option<&mut dyn FullClosureReplayLockWaitHeartbeat>,
    config: FullClosureReplayLockWaitConfig,
    operation: Operation,
) -> Result<T>
where
    Operation: FnOnce() -> OperationFuture,
    OperationFuture: Future<Output = Result<T>>,
{
    // Use a dedicated connection rather than occupying a pool slot. Runtime
    // processes already retain one pool connection for the Base correction
    // writer guard, and full-closure adapters need the pool while this
    // cross-process ownership fence is held.
    let mut connection = PgConnection::connect_with(pool.connect_options().as_ref())
        .await
        .context("failed to connect the full-closure replay ownership fence")?;
    let lock_identity = format!("bigname:indexer:full-closure-replay:{deployment_profile}:{chain}");
    let lock = PgAdvisoryLock::new(lock_identity);
    let started_at = Instant::now();
    let mut waiting = false;
    let mut next_log_at = started_at;
    let mut last_holder = None;
    let guard = loop {
        if waiting {
            let waited = started_at.elapsed();
            if let Some(deadline) = config.deadline
                && waited >= deadline
            {
                return Err(full_closure_replay_lock_deadline_exceeded(
                    deployment_profile,
                    chain,
                    waited,
                    deadline,
                    last_holder,
                )
                .into());
            }
        }

        // A backend waiting inside pg_advisory_lock can retain a snapshot for
        // the entire competing replay. Polling leaves no long-lived statement
        // behind to hold back CREATE INDEX CONCURRENTLY or vacuum horizons.
        match lock.try_acquire(connection).await.with_context(|| {
            format!("failed to try full-closure replay ownership for {deployment_profile}/{chain}")
        })? {
            Either::Left(guard) => break guard,
            Either::Right(unlocked) => {
                connection = unlocked;
                let wait_started = !waiting;
                waiting = true;

                let now = Instant::now();
                if full_closure_replay_lock_wait_log_due(
                    &mut next_log_at,
                    now,
                    config.log_interval,
                )? {
                    match load_full_closure_replay_lock_holder(&mut connection, lock.key()).await {
                        Ok(holder) => last_holder = holder,
                        Err(holder_error) => {
                            last_holder = None;
                            warn!(
                                service = "indexer",
                                deployment_profile,
                                chain,
                                error = %format!("{holder_error:#}"),
                                "failed to inspect the full-closure replay ownership holder"
                            );
                        }
                    }
                    warn_full_closure_replay_lock_wait(
                        deployment_profile,
                        chain,
                        started_at.elapsed(),
                        config.deadline,
                        last_holder.as_ref(),
                    );
                }
                if wait_started && let Some(wait_heartbeat) = wait_heartbeat.as_deref_mut() {
                    wait_heartbeat
                        .begin_wait(pool, deployment_profile, chain)
                        .await
                        .with_context(|| {
                            format!(
                                "failed to expose the full-closure replay ownership wait phase for {deployment_profile}/{chain}"
                            )
                        })?;
                }

                let waited = started_at.elapsed();
                if let Some(deadline) = config.deadline
                    && waited >= deadline
                {
                    return Err(full_closure_replay_lock_deadline_exceeded(
                        deployment_profile,
                        chain,
                        waited,
                        deadline,
                        last_holder,
                    )
                    .into());
                }

                let sleep_duration =
                    config
                        .deadline
                        .map_or(FULL_CLOSURE_REPLAY_LOCK_POLL_INTERVAL, |deadline| {
                            FULL_CLOSURE_REPLAY_LOCK_POLL_INTERVAL
                                .min(deadline.saturating_sub(waited))
                        });
                if !sleep_duration.is_zero() {
                    tokio::time::sleep(sleep_duration).await;
                }
            }
        }
    };

    let wait_duration = started_at.elapsed();
    if let Some(wait_heartbeat) = wait_heartbeat.as_deref_mut()
        && let Err(heartbeat_error) = wait_heartbeat
            .finish_wait(pool, deployment_profile, chain)
            .await
    {
        warn!(
            service = "indexer",
            deployment_profile,
            chain,
            error = %format!("{heartbeat_error:#}"),
            "failed to finish the full-closure replay ownership wait phase; continuing with degraded liveness evidence"
        );
    }
    info!(
        service = "indexer",
        deployment_profile,
        chain,
        contended = waiting,
        wait_duration_ms = duration_millis(wait_duration),
        "full-closure replay ownership acquired"
    );

    let operation_result = operation().await;
    #[cfg(test)]
    test_hook::pause_before_release(pool, deployment_profile, chain).await;
    let release_result = guard
        .release_now()
        .await
        .with_context(|| {
            format!(
                "failed to release full-closure replay ownership for {deployment_profile}/{chain}"
            )
        })
        .map(|_| ());
    prioritize_operation_error(operation_result, release_result)
}

fn seconds_from_env(name: &str) -> Result<Option<u64>> {
    match env::var(name) {
        Ok(value) => value
            .trim()
            .parse::<u64>()
            .with_context(|| format!("{name} must be a non-negative integer number of seconds"))
            .map(Some),
        Err(env::VarError::NotPresent) => Ok(None),
        Err(env::VarError::NotUnicode(_)) => {
            bail!("{name} must contain valid Unicode")
        }
    }
}

fn full_closure_replay_lock_wait_log_due(
    next_log_at: &mut Instant,
    now: Instant,
    interval: Duration,
) -> Result<bool> {
    if now < *next_log_at {
        return Ok(false);
    }
    *next_log_at = now.checked_add(interval).with_context(|| {
        format!("{FULL_CLOSURE_REPLAY_LOCK_WAIT_LOG_INTERVAL_ENV} is too large")
    })?;
    Ok(true)
}

async fn load_full_closure_replay_lock_holder(
    connection: &mut PgConnection,
    lock_key: &PgAdvisoryLockKey,
) -> Result<Option<FullClosureReplayLockHolder>> {
    let key = lock_key
        .as_bigint()
        .context("full-closure replay ownership must use one 64-bit advisory lock key")?;
    let holder = sqlx::query_as::<_, (i32, String)>(
        r#"
        SELECT locks.pid, COALESCE(NULLIF(activity.application_name, ''), '<unset>')
        FROM pg_locks AS locks
        LEFT JOIN pg_stat_activity AS activity ON activity.pid = locks.pid
        WHERE locks.locktype = 'advisory'
          AND locks.database = (
              SELECT oid
              FROM pg_database
              WHERE datname = current_database()
          )
          AND locks.classid = (($1::BIGINT >> 32) & 4294967295)::OID
          AND locks.objid = ($1::BIGINT & 4294967295)::OID
          AND locks.objsubid = 1
          AND locks.granted
          AND locks.pid <> pg_backend_pid()
        ORDER BY locks.pid
        LIMIT 1
        "#,
    )
    .bind(key)
    .fetch_optional(connection)
    .await
    .context("failed to query PostgreSQL advisory-lock holder activity")?;
    Ok(
        holder.map(|(pid, application_name)| FullClosureReplayLockHolder {
            pid,
            application_name,
        }),
    )
}

fn warn_full_closure_replay_lock_wait(
    deployment_profile: &str,
    chain: &str,
    waited: Duration,
    deadline: Option<Duration>,
    holder: Option<&FullClosureReplayLockHolder>,
) {
    warn!(
        service = "indexer",
        deployment_profile,
        chain,
        wait_duration_ms = duration_millis(waited),
        wait_deadline_ms = deadline.map(duration_millis),
        holder_pid = holder.map(|holder| holder.pid),
        holder_application_name = holder
            .map(|holder| holder.application_name.as_str())
            .unwrap_or("<unavailable>"),
        "waiting for full-closure replay ownership"
    );
}

fn full_closure_replay_lock_deadline_exceeded(
    deployment_profile: &str,
    chain: &str,
    waited: Duration,
    deadline: Duration,
    holder: Option<FullClosureReplayLockHolder>,
) -> FullClosureReplayLockWaitDeadlineExceeded {
    error!(
        service = "indexer",
        deployment_profile,
        chain,
        wait_duration_ms = duration_millis(waited),
        wait_deadline_ms = duration_millis(deadline),
        holder_pid = holder.as_ref().map(|holder| holder.pid),
        holder_application_name = holder
            .as_ref()
            .map(|holder| holder.application_name.as_str())
            .unwrap_or("<unavailable>"),
        "full-closure replay ownership wait deadline exceeded"
    );
    FullClosureReplayLockWaitDeadlineExceeded {
        deployment_profile: deployment_profile.to_owned(),
        chain: chain.to_owned(),
        waited,
        deadline,
        holder,
    }
}

fn duration_millis(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

#[cfg(test)]
pub(crate) use test_hook::install as install_ownership_release_test_hook;

#[cfg(test)]
#[path = "ownership/tests.rs"]
mod tests;

#[cfg(test)]
mod test_hook {
    use std::sync::Arc;

    use bigname_test_support::{
        ScopedTestHookGuard, ScopedTestHookRegistry, current_test_database,
    };
    use sqlx::PgPool;
    use tokio::sync::Notify;

    pub(crate) struct FullClosureOwnershipReleaseTestHook {
        state: FullClosureOwnershipReleaseTestHookState,
        _registration: ScopedTestHookGuard<HookKey, FullClosureOwnershipReleaseTestHookState>,
    }

    #[derive(Clone)]
    struct FullClosureOwnershipReleaseTestHookState {
        before_release: Arc<Notify>,
        resume: Arc<Notify>,
    }

    impl FullClosureOwnershipReleaseTestHook {
        pub(crate) async fn wait_until_before_release(&self) {
            self.state.before_release.notified().await;
        }

        pub(crate) fn resume(&self) {
            self.state.resume.notify_one();
        }
    }

    impl Drop for FullClosureOwnershipReleaseTestHook {
        fn drop(&mut self) {
            self.state.resume.notify_one();
        }
    }

    type HookKey = (String, String, String);

    static HOOKS: ScopedTestHookRegistry<HookKey, FullClosureOwnershipReleaseTestHookState> =
        ScopedTestHookRegistry::new();

    pub(crate) async fn install(
        pool: &PgPool,
        deployment_profile: &str,
        chain: &str,
    ) -> FullClosureOwnershipReleaseTestHook {
        let database = current_test_database(pool)
            .await
            .expect("full-closure ownership test hook must identify its database");
        let state = FullClosureOwnershipReleaseTestHookState {
            before_release: Arc::new(Notify::new()),
            resume: Arc::new(Notify::new()),
        };
        let registration = HOOKS.install(
            (database, deployment_profile.to_owned(), chain.to_owned()),
            state.clone(),
        );
        FullClosureOwnershipReleaseTestHook {
            state,
            _registration: registration,
        }
    }

    pub(super) async fn pause_before_release(pool: &PgPool, deployment_profile: &str, chain: &str) {
        let database = current_test_database(pool)
            .await
            .expect("full-closure ownership test hook must identify its database");
        let hook = HOOKS.take(&(database, deployment_profile.to_owned(), chain.to_owned()));
        if let Some(hook) = hook {
            hook.before_release.notify_one();
            hook.resume.notified().await;
        }
    }
}
