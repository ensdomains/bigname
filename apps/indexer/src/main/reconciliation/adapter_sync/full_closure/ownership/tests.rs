use std::{
    io,
    sync::{
        Arc, Mutex, OnceLock,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};

use anyhow::{Context, Result};
use bigname_test_support::{TestDatabase, TestDatabaseConfig};
use sqlx::{
    Connection, PgConnection,
    postgres::{PgAdvisoryLock, PgConnectOptions, PgPoolOptions},
    types::time::OffsetDateTime,
};
use tokio::time::{sleep, timeout};
use tracing_subscriber::fmt::MakeWriter;

use super::*;
use crate::run::startup_heartbeat::NormalizedReplayHeartbeat;

// The full indexer suite runs many database-heavy tests in parallel, so allow
// enough wall time for PostgreSQL to schedule both holder-inspection queries.
const TEST_TIMEOUT: Duration = Duration::from_secs(15);
const TEST_CONNECTION_TIMEOUT: Duration = Duration::from_secs(60);

#[derive(Clone, Default)]
struct CapturedLogs {
    bytes: Arc<Mutex<Vec<u8>>>,
}

struct CapturedLogWriter {
    bytes: Arc<Mutex<Vec<u8>>>,
}

impl CapturedLogs {
    fn contents(&self) -> String {
        String::from_utf8(
            self.bytes
                .lock()
                .expect("captured log mutex must not be poisoned")
                .clone(),
        )
        .expect("captured logs must be UTF-8")
    }
}

impl io::Write for CapturedLogWriter {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        self.bytes
            .lock()
            .expect("captured log mutex must not be poisoned")
            .extend_from_slice(buffer);
        Ok(buffer.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl<'writer> MakeWriter<'writer> for CapturedLogs {
    type Writer = CapturedLogWriter;

    fn make_writer(&'writer self) -> Self::Writer {
        CapturedLogWriter {
            bytes: Arc::clone(&self.bytes),
        }
    }
}

fn captured_warn_logs() -> CapturedLogs {
    static CAPTURED_WARN_LOGS: OnceLock<CapturedLogs> = OnceLock::new();
    CAPTURED_WARN_LOGS
        .get_or_init(|| {
            let captured_logs = CapturedLogs::default();
            let subscriber = tracing_subscriber::fmt()
                .without_time()
                .with_ansi(false)
                .with_target(false)
                .with_max_level(tracing::Level::WARN)
                .with_writer(captured_logs.clone())
                .finish();
            tracing::subscriber::set_global_default(subscriber)
                .expect("ownership tests must install the process-wide tracing subscriber");
            captured_logs
        })
        .clone()
}

#[tokio::test]
async fn contended_wait_logs_the_holder_within_the_configured_interval() -> Result<()> {
    let database = test_database("full_closure_lock_log").await?;
    let deployment_profile = "log-profile";
    let chain = "log-chain";
    let holder_application_name = "full-closure-holder-log-test";
    let waiter_application_name = "full-closure-waiter-log-test";
    let ownership = replay_lock(deployment_profile, chain);
    let mut holder_connection = holder_connection(database.pool(), holder_application_name).await?;
    let holder_pid = sqlx::query_scalar::<_, i32>("SELECT pg_backend_pid()")
        .fetch_one(&mut holder_connection)
        .await?;
    let ownership_guard = ownership.acquire(holder_connection).await?;

    let captured_logs = captured_warn_logs();
    let waiter_options: PgConnectOptions = database
        .pool()
        .connect_options()
        .as_ref()
        .clone()
        .application_name(waiter_application_name);
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect_lazy_with(waiter_options);
    let mut waiter = tokio::spawn(async move {
        let mut wait_heartbeat: Option<&mut dyn FullClosureReplayLockWaitHeartbeat> = None;
        with_full_closure_replay_lock_config(
            &pool,
            deployment_profile,
            chain,
            &mut wait_heartbeat,
            FullClosureReplayLockWaitConfig {
                log_interval: Duration::from_millis(75),
                deadline: None,
            },
            || async { Ok(()) },
        )
        .await
    });

    timeout(TEST_CONNECTION_TIMEOUT, async {
        loop {
            let waiter_connected = sqlx::query_scalar::<_, bool>(
                r#"
                SELECT EXISTS (
                    SELECT 1
                    FROM pg_stat_activity
                    WHERE datname = current_database()
                      AND application_name = $1
                )
                "#,
            )
            .bind(waiter_application_name)
            .fetch_one(database.pool())
            .await?;
            if waiter_connected {
                return Ok::<_, anyhow::Error>(());
            }
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .context("full-closure lock waiter did not establish its dedicated connection")??;

    let log_result = timeout(TEST_TIMEOUT, async {
        loop {
            let logs = captured_logs.contents();
            if logs.contains("waiting for full-closure replay ownership")
                && logs.contains(holder_application_name)
                && logs.contains(&holder_pid.to_string())
            {
                return Ok::<_, anyhow::Error>(());
            }
            tokio::select! {
                waiter_result = &mut waiter => {
                    anyhow::bail!(
                        "full-closure lock waiter exited before holder logging: \
                         {waiter_result:?}; captured_logs={logs:?}"
                    );
                }
                () = sleep(Duration::from_millis(10)) => {}
            }
        }
    })
    .await;
    match log_result {
        Ok(result) => result?,
        Err(timeout_error) => {
            let waiter_activity =
                sqlx::query_as::<_, (String, Option<String>, Option<String>, String)>(
                    r#"
                    SELECT state, wait_event_type, wait_event, query
                    FROM pg_stat_activity
                    WHERE datname = current_database()
                      AND application_name = $1
                    "#,
                )
                .bind(waiter_application_name)
                .fetch_all(database.pool())
                .await?;
            return Err(timeout_error).with_context(|| {
                format!(
                    "waiter did not log the PostgreSQL lock holder within the configured interval; \
                     waiter_finished={}; waiter_activity={waiter_activity:?}; captured_logs={:?}",
                    waiter.is_finished(),
                    captured_logs.contents()
                )
            });
        }
    }

    ownership_guard.release_now().await?;
    timeout(TEST_TIMEOUT, waiter)
        .await
        .context("logged full-closure lock waiter did not finish after release")?
        .context("logged full-closure lock waiter task panicked")??;
    database.cleanup().await
}

#[test]
fn wait_log_schedule_repeats_at_the_configured_interval() -> Result<()> {
    let started_at = Instant::now();
    let interval = Duration::from_millis(75);
    let mut next_log_at = started_at;

    assert!(full_closure_replay_lock_wait_log_due(
        &mut next_log_at,
        started_at,
        interval
    )?);
    assert!(!full_closure_replay_lock_wait_log_due(
        &mut next_log_at,
        started_at + Duration::from_millis(74),
        interval
    )?);
    assert!(full_closure_replay_lock_wait_log_due(
        &mut next_log_at,
        started_at + interval,
        interval
    )?);
    Ok(())
}

#[tokio::test]
async fn configured_deadline_returns_a_typed_loud_failure() -> Result<()> {
    let database = test_database("full_closure_lock_deadline").await?;
    let deployment_profile = "deadline-profile";
    let chain = "deadline-chain";
    let holder_application_name = "full-closure-holder-deadline-test";
    let ownership = replay_lock(deployment_profile, chain);
    let mut holder_connection = holder_connection(database.pool(), holder_application_name).await?;
    let holder_pid = sqlx::query_scalar::<_, i32>("SELECT pg_backend_pid()")
        .fetch_one(&mut holder_connection)
        .await?;
    let ownership_guard = ownership.acquire(holder_connection).await?;
    let operation_ran = Arc::new(AtomicBool::new(false));
    let operation_ran_in_waiter = Arc::clone(&operation_ran);
    let mut wait_heartbeat: Option<&mut dyn FullClosureReplayLockWaitHeartbeat> = None;
    let started_at = Instant::now();

    let error = with_full_closure_replay_lock_config(
        database.pool(),
        deployment_profile,
        chain,
        &mut wait_heartbeat,
        FullClosureReplayLockWaitConfig {
            log_interval: Duration::from_secs(1),
            deadline: Some(Duration::from_millis(125)),
        },
        || async move {
            operation_ran_in_waiter.store(true, Ordering::Release);
            Ok(())
        },
    )
    .await
    .expect_err("configured full-closure lock deadline must fail while the holder remains");

    let elapsed = started_at.elapsed();
    assert!(
        elapsed >= Duration::from_millis(100) && elapsed < TEST_TIMEOUT,
        "deadline failure arrived outside the expected bounded interval: {elapsed:?}"
    );
    assert!(
        error
            .downcast_ref::<FullClosureReplayLockWaitDeadlineExceeded>()
            .is_some(),
        "deadline failure must retain its typed cause: {error:#}"
    );
    let rendered = format!("{error:#}");
    assert!(
        rendered.contains(holder_application_name)
            && rendered.contains(&holder_pid.to_string())
            && rendered.contains(deployment_profile)
            && rendered.contains(chain),
        "deadline failure must identify the lock and holder: {rendered}"
    );
    assert!(
        !operation_ran.load(Ordering::Acquire),
        "deadline failure must not run the fenced operation"
    );

    ownership_guard.release_now().await?;
    database.cleanup().await
}

#[tokio::test]
async fn repeated_deadlines_preserve_the_aging_phase_until_ownership_is_acquired() -> Result<()> {
    let database = test_migrated_database("full_closure_lock_deadline_phase").await?;
    let deployment_profile = "deadline-phase-profile";
    let chain = "deadline-phase-chain";
    let other_chain = "uncontended-chain";
    let instance_id = "full-closure-deadline-phase-waiter";
    bigname_storage::register_service_loop(
        database.pool(),
        bigname_storage::INDEXER_SERVICE_NAME,
        instance_id,
    )
    .await?;

    let ownership = replay_lock(deployment_profile, chain);
    let holder_connection =
        holder_connection(database.pool(), "full-closure-holder-deadline-phase-test").await?;
    let ownership_guard = ownership.acquire(holder_connection).await?;
    let mut heartbeat = NormalizedReplayHeartbeat::new(
        instance_id.to_owned(),
        Duration::from_millis(1),
        vec![chain.to_owned()],
    );
    let mut wait_heartbeat: Option<&mut dyn FullClosureReplayLockWaitHeartbeat> =
        Some(&mut heartbeat);
    let config = FullClosureReplayLockWaitConfig {
        log_interval: Duration::from_secs(1),
        deadline: Some(Duration::from_millis(125)),
    };
    let mut aging_phase_heartbeat_at = None;

    for attempt in 1..=2 {
        let error = with_full_closure_replay_lock_config(
            database.pool(),
            deployment_profile,
            chain,
            &mut wait_heartbeat,
            config,
            || async { Ok(()) },
        )
        .await
        .expect_err("held ownership must exceed each configured deadline");
        assert!(
            error
                .downcast_ref::<FullClosureReplayLockWaitDeadlineExceeded>()
                .is_some(),
            "deadline attempt {attempt} must retain its typed cause: {error:#}"
        );

        let mut observed_at = lock_wait_phase_heartbeat_at(database.pool(), instance_id)
            .await?
            .context("deadline attempt must leave the lock-wait phase visible")?;
        if attempt == 1 {
            sqlx::query(
                r#"
                UPDATE service_loop_heartbeats
                SET started_at = clock_timestamp() - INTERVAL '3 minutes',
                    heartbeat_at = clock_timestamp() - INTERVAL '2 minutes'
                WHERE service_name = 'indexer'
                  AND instance_id = $1
                  AND scope_kind = 'phase'
                  AND scope_id = 'full_closure_replay_lock.wait'
                "#,
            )
            .bind(instance_id)
            .execute(database.pool())
            .await?;
            observed_at = lock_wait_phase_heartbeat_at(database.pool(), instance_id)
                .await?
                .context("aged lock-wait phase disappeared before parent progress")?;
            bigname_storage::record_service_loop_heartbeat(
                database.pool(),
                bigname_storage::INDEXER_SERVICE_NAME,
                instance_id,
                &[chain.to_owned()],
            )
            .await?;
            let after_parent_progress = lock_wait_phase_heartbeat_at(database.pool(), instance_id)
                .await?
                .context("parent progress must not hide a required child's lock wait")?;
            assert_eq!(
                after_parent_progress, observed_at,
                "parent progress must preserve the original lock-wait phase timestamp"
            );
            with_full_closure_replay_lock_config(
                database.pool(),
                deployment_profile,
                other_chain,
                &mut wait_heartbeat,
                config,
                || async { Ok(()) },
            )
            .await?;
            let after_other_chain = lock_wait_phase_heartbeat_at(database.pool(), instance_id)
                .await?
                .context("uncontended work on another chain must not hide the lock wait")?;
            assert_eq!(
                after_other_chain, observed_at,
                "another chain must not finish or refresh the unresolved lock-wait phase"
            );
            aging_phase_heartbeat_at = Some(observed_at);
        } else if let Some(first_observed_at) = aging_phase_heartbeat_at {
            assert_eq!(
                observed_at, first_observed_at,
                "a deadline retry is not forward progress and must not refresh the wait phase"
            );
        }
    }
    let stale_error = bigname_storage::ensure_service_loop_heartbeat_recent(
        database.pool(),
        bigname_storage::INDEXER_SERVICE_NAME,
        instance_id,
        20,
    )
    .await
    .expect_err("the preserved lock-wait phase must age indexer health stale");
    assert!(
        stale_error
            .to_string()
            .contains("phase full_closure_replay_lock.wait"),
        "unexpected stale lock-wait health error: {stale_error:#}"
    );

    ownership_guard.release_now().await?;
    with_full_closure_replay_lock_config(
        database.pool(),
        deployment_profile,
        chain,
        &mut wait_heartbeat,
        config,
        || async { Ok(()) },
    )
    .await?;
    assert!(
        lock_wait_phase_heartbeat_at(database.pool(), instance_id)
            .await?
            .is_none(),
        "acquiring ownership after deadline retries must finish the aging wait phase"
    );

    database.cleanup().await
}

#[tokio::test]
async fn default_wait_remains_unlimited_and_exposes_an_aging_heartbeat_phase() -> Result<()> {
    let database = test_migrated_database("full_closure_lock_default").await?;
    let deployment_profile = "default-profile";
    let chain = "default-chain";
    let instance_id = "full-closure-default-waiter";
    bigname_storage::register_service_loop(
        database.pool(),
        bigname_storage::INDEXER_SERVICE_NAME,
        instance_id,
    )
    .await?;

    let ownership = replay_lock(deployment_profile, chain);
    let holder_connection =
        holder_connection(database.pool(), "full-closure-holder-default-test").await?;
    let ownership_guard = ownership.acquire(holder_connection).await?;
    let pool = database.pool().clone();
    let operation_ran = Arc::new(AtomicBool::new(false));
    let operation_ran_in_waiter = Arc::clone(&operation_ran);
    let mut waiter = tokio::spawn(async move {
        let mut heartbeat = NormalizedReplayHeartbeat::new(
            instance_id.to_owned(),
            Duration::from_millis(1),
            vec![chain.to_owned()],
        );
        let mut wait_heartbeat: Option<&mut dyn FullClosureReplayLockWaitHeartbeat> =
            Some(&mut heartbeat);
        with_full_closure_replay_lock_config(
            &pool,
            deployment_profile,
            chain,
            &mut wait_heartbeat,
            FullClosureReplayLockWaitConfig::default(),
            || async move {
                operation_ran_in_waiter.store(true, Ordering::Release);
                Ok(())
            },
        )
        .await
    });

    let phase_heartbeat_at = timeout(TEST_TIMEOUT, async {
        loop {
            if let Some(heartbeat_at) = sqlx::query_scalar::<_, OffsetDateTime>(
                r#"
                SELECT heartbeat_at
                FROM service_loop_heartbeats
                WHERE service_name = 'indexer'
                  AND instance_id = $1
                  AND scope_kind = 'phase'
                  AND scope_id = 'full_closure_replay_lock.wait'
                "#,
            )
            .bind(instance_id)
            .fetch_optional(database.pool())
            .await?
            {
                return Ok::<_, anyhow::Error>(heartbeat_at);
            }
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    let phase_heartbeat_at = match phase_heartbeat_at {
        Ok(phase_heartbeat_at) => phase_heartbeat_at?,
        Err(timeout_error) => {
            let waiter_finished = waiter.is_finished();
            let heartbeat_rows = sqlx::query_as::<_, (String, String)>(
                r#"
                SELECT scope_kind, scope_id
                FROM service_loop_heartbeats
                WHERE service_name = 'indexer'
                  AND instance_id = $1
                ORDER BY scope_kind, scope_id
                "#,
            )
            .bind(instance_id)
            .fetch_all(database.pool())
            .await?;
            ownership_guard.release_now().await?;
            let waiter_result = timeout(TEST_TIMEOUT, &mut waiter)
                .await
                .context("diagnostic waiter did not finish after ownership release")?;
            return Err(timeout_error).with_context(|| {
                format!(
                    "default waiter did not expose its heartbeat phase; \
                     waiter_finished={waiter_finished}, heartbeat_rows={heartbeat_rows:?}, \
                     waiter_result={waiter_result:?}"
                )
            });
        }
    };

    assert!(
        timeout(Duration::from_millis(250), &mut waiter)
            .await
            .is_err(),
        "default lock wait must remain unlimited while the holder is live"
    );
    let unchanged_phase_heartbeat_at = sqlx::query_scalar::<_, OffsetDateTime>(
        r#"
        SELECT heartbeat_at
        FROM service_loop_heartbeats
        WHERE service_name = 'indexer'
          AND instance_id = $1
          AND scope_kind = 'phase'
          AND scope_id = 'full_closure_replay_lock.wait'
        "#,
    )
    .bind(instance_id)
    .fetch_one(database.pool())
    .await?;
    assert_eq!(
        unchanged_phase_heartbeat_at, phase_heartbeat_at,
        "lock poll ticks must not refresh the wait phase"
    );
    assert!(
        !operation_ran.load(Ordering::Acquire),
        "the fenced operation must not run while ownership is held"
    );

    ownership_guard.release_now().await?;
    timeout(TEST_TIMEOUT, waiter)
        .await
        .context("default full-closure lock waiter did not finish after release")?
        .context("default full-closure lock waiter task panicked")??;
    assert!(operation_ran.load(Ordering::Acquire));
    let heartbeat = bigname_storage::load_service_loop_heartbeat(
        database.pool(),
        bigname_storage::INDEXER_SERVICE_NAME,
        instance_id,
    )
    .await?
    .context("registered indexer heartbeat disappeared")?;
    assert!(
        heartbeat.active_phase.is_none(),
        "acquisition must finish the lock-wait heartbeat phase"
    );

    database.cleanup().await
}

async fn test_database(prefix: &str) -> Result<TestDatabase> {
    TestDatabase::create(TestDatabaseConfig::new(prefix))
        .await
        .context("failed to create full-closure ownership test database")
}

async fn test_migrated_database(prefix: &str) -> Result<TestDatabase> {
    TestDatabase::create_migrated(
        TestDatabaseConfig::new(prefix),
        &bigname_storage::MIGRATOR,
        "failed to migrate full-closure ownership test database",
    )
    .await
}

fn replay_lock(deployment_profile: &str, chain: &str) -> PgAdvisoryLock {
    PgAdvisoryLock::new(format!(
        "bigname:indexer:full-closure-replay:{deployment_profile}:{chain}"
    ))
}

async fn holder_connection(pool: &PgPool, application_name: &str) -> Result<PgConnection> {
    let options: PgConnectOptions = pool
        .connect_options()
        .as_ref()
        .clone()
        .application_name(application_name);
    PgConnection::connect_with(&options)
        .await
        .context("failed to connect the test lock holder")
}

async fn lock_wait_phase_heartbeat_at(
    pool: &PgPool,
    instance_id: &str,
) -> Result<Option<OffsetDateTime>> {
    sqlx::query_scalar(
        r#"
        SELECT heartbeat_at
        FROM service_loop_heartbeats
        WHERE service_name = 'indexer'
          AND instance_id = $1
          AND scope_kind = 'phase'
          AND scope_id = 'full_closure_replay_lock.wait'
        "#,
    )
    .bind(instance_id)
    .fetch_optional(pool)
    .await
    .context("failed to load the full-closure lock-wait heartbeat phase")
}
