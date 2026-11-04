use std::{
    io,
    sync::{Arc, Mutex},
    time::Duration,
};

use anyhow::{Context, Result, bail, ensure};
use bigname_test_support::{StatementCounter, TestDatabase, TestDatabaseConfig};
use sqlx::{
    Connection, PgConnection, PgPool, Postgres, pool::PoolConnection, postgres::PgPoolOptions,
};

use super::{
    IN_TRANSACTION_BLOCK, MAX_REQUEST_DEADLINE_MS, ROLLED_BACK_LEFT_OPEN,
    idle_in_transaction_setting, serving_pool_options,
};

/// A statement whose reply arrives long after [`DROP_AFTER`], so a stalled test host cannot let
/// it answer first.
const HELD_REPLY: &str = "SELECT pg_sleep(2)";
const DROP_AFTER: Duration = Duration::from_millis(50);
const WAIT_STEP: Duration = Duration::from_millis(20);
const WAIT_STEPS: usize = 500;

async fn database(name: &str) -> Result<TestDatabase> {
    TestDatabase::create(TestDatabaseConfig::new(name)).await
}

/// A pool built as the API builds its request pool, on the test database.
async fn serving_pool(db: &TestDatabase, max_connections: u32) -> Result<PgPool> {
    Ok(serving_pool_options(max_connections)
        .connect_with(db.pool().connect_options().as_ref().clone())
        .await?)
}

async fn acquire_all(pool: &PgPool, size: u32) -> Result<Vec<PoolConnection<Postgres>>> {
    let mut held = Vec::new();
    for _ in 0..size {
        held.push(pool.acquire().await?);
    }
    Ok(held)
}

/// Leaves `conn` inside a server transaction that sqlx does not count, the way a request whose
/// future is dropped while it awaits the reply to its `BEGIN` does. The statement sent ahead of
/// the `BEGIN` holds that reply back until after the future is dropped.
async fn drop_begin_in_flight(conn: &mut PgConnection) -> Result<()> {
    let held = tokio::time::timeout(DROP_AFTER, sqlx::raw_sql(HELD_REPLY).execute(&mut *conn))
        .await
        .is_err();
    ensure!(
        held,
        "the statement ahead of BEGIN answered before its future was dropped"
    );
    let begun = tokio::time::timeout(DROP_AFTER, Connection::begin(&mut *conn))
        .await
        .is_ok();
    ensure!(!begun, "BEGIN answered before its future was dropped");
    // Reads the replies still owed, the BEGIN's among them.
    Connection::ping(&mut *conn).await?;
    Ok(())
}

async fn backend_pid(conn: &mut PgConnection) -> Result<i32> {
    Ok(sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&mut *conn)
        .await?)
}

/// The backend of `conn` and the session state the server reports for it, read from `observer`,
/// another pool.
async fn server_state(observer: &PgPool, conn: &mut PgConnection) -> Result<(i32, String)> {
    let pid = backend_pid(conn).await?;
    let state: String = sqlx::query_scalar("SELECT state FROM pg_stat_activity WHERE pid = $1")
        .bind(pid)
        .fetch_one(observer)
        .await
        .context("failed to read the session state")?;
    Ok((pid, state))
}

fn idle(pid: i32) -> (i32, String) {
    (pid, "idle".to_owned())
}

async fn wait_until_idle(pool: &PgPool, count: usize) -> Result<()> {
    for _ in 0..WAIT_STEPS {
        if pool.num_idle() == count {
            return Ok(());
        }
        tokio::time::sleep(WAIT_STEP).await;
    }
    bail!("the pool never held {count} idle connections");
}

async fn wait_until_ended(observer: &PgPool, pid: i32) -> Result<()> {
    for _ in 0..WAIT_STEPS {
        let sessions: i64 =
            sqlx::query_scalar("SELECT count(*) FROM pg_stat_activity WHERE pid = $1")
                .bind(pid)
                .fetch_one(observer)
                .await?;
        if sessions == 0 {
            return Ok(());
        }
        tokio::time::sleep(WAIT_STEP).await;
    }
    bail!("backend {pid} was never ended");
}

/// A read as the API composes one: a pinned snapshot, a statement in it, a commit.
async fn composed_read(pool: &PgPool) -> Result<()> {
    let mut snapshot = crate::begin_read_snapshot(pool).await?;
    let isolation: String = sqlx::query_scalar("SHOW transaction_isolation")
        .fetch_one(&mut *snapshot)
        .await?;
    ensure!(
        isolation == "repeatable read",
        "the read ran at {isolation}"
    );
    snapshot.commit().await?;
    Ok(())
}

/// What this thread logs while a capture is held.
#[derive(Clone, Default)]
struct Logs(Arc<Mutex<Vec<u8>>>);

impl io::Write for Logs {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.0
            .lock()
            .expect("log buffer poisoned")
            .extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl Logs {
    fn capture(&self) -> tracing::subscriber::DefaultGuard {
        let logs = self.clone();
        tracing::subscriber::set_default(
            tracing_subscriber::fmt()
                .with_ansi(false)
                .with_writer(move || logs.clone())
                .finish(),
        )
    }

    fn text(&self) -> String {
        String::from_utf8_lossy(&self.0.lock().expect("log buffer poisoned")).into_owned()
    }
}

#[tokio::test]
async fn begin_dropped_in_flight_leaves_no_transaction_on_the_next_acquire() -> Result<()> {
    let db = database("serving_pool_begin_dropped").await?;
    let pool = serving_pool(&db, 1).await?;
    let mut conn = pool.acquire().await?;
    let pid = backend_pid(&mut conn).await?;
    drop_begin_in_flight(&mut conn).await?;
    drop(conn);

    let mut conn = pool.acquire().await?;
    assert_eq!(server_state(db.pool(), &mut conn).await?, idle(pid));
    drop(conn);
    composed_read(&pool).await?;

    pool.close().await;
    db.cleanup().await
}

#[tokio::test]
async fn statement_dropped_mid_flight_in_a_transaction_left_open_does_not_fail_the_next_read()
-> Result<()> {
    let db = database("serving_pool_statement_dropped").await?;
    let pool = serving_pool(&db, 1).await?;
    let mut conn = pool.acquire().await?;
    let pid = backend_pid(&mut conn).await?;
    drop_begin_in_flight(&mut conn).await?;
    let dropped = tokio::time::timeout(DROP_AFTER, sqlx::raw_sql(HELD_REPLY).execute(&mut *conn))
        .await
        .is_err();
    ensure!(
        dropped,
        "the statement answered before its future was dropped"
    );
    drop(conn);

    let mut conn = pool.acquire().await?;
    assert_eq!(server_state(db.pool(), &mut conn).await?, idle(pid));
    drop(conn);
    composed_read(&pool).await?;

    pool.close().await;
    db.cleanup().await
}

#[tokio::test]
async fn completed_statement_in_a_transaction_left_open_does_not_fail_the_next_read() -> Result<()>
{
    let db = database("serving_pool_statement_completed").await?;
    let pool = serving_pool(&db, 1).await?;
    let mut conn = pool.acquire().await?;
    let pid = backend_pid(&mut conn).await?;
    drop_begin_in_flight(&mut conn).await?;
    sqlx::query("SELECT 1").execute(&mut *conn).await?;
    drop(conn);

    let mut conn = pool.acquire().await?;
    assert_eq!(server_state(db.pool(), &mut conn).await?, idle(pid));
    drop(conn);
    composed_read(&pool).await?;

    pool.close().await;
    db.cleanup().await
}

#[tokio::test]
async fn healthy_idle_connection_is_handed_out_unchanged_after_one_statement() -> Result<()> {
    let db = database("serving_pool_healthy").await?;
    let counter =
        StatementCounter::connect_with_pool_options(db.pool(), serving_pool_options(1)).await?;
    let pool = counter.pool();
    let mut conn = pool.acquire().await?;
    sqlx::raw_sql("SET application_name = 'serving-pool-healthy'")
        .execute(&mut *conn)
        .await?;
    let pid = backend_pid(&mut conn).await?;
    drop(conn);
    // The release sends its own ping before the count starts.
    wait_until_idle(pool, 1).await?;

    let (conn, sent) = counter.count(pool.acquire()).await;
    let mut conn = conn?;
    assert_eq!(sent.statements(), [IN_TRANSACTION_BLOCK]);
    assert_eq!(sent.syncs(), 0, "an empty ping was sent as well");
    let (same_pid, application_name): (i32, String) =
        sqlx::query_as("SELECT pg_backend_pid(), current_setting('application_name')")
            .fetch_one(&mut *conn)
            .await?;
    assert_eq!(same_pid, pid);
    assert_eq!(application_name, "serving-pool-healthy");
    drop(conn);
    composed_read(pool).await?;

    counter.close().await;
    db.cleanup().await
}

#[tokio::test]
async fn connection_the_server_terminated_is_replaced_quietly_on_acquire() -> Result<()> {
    let db = database("serving_pool_server_terminated").await?;
    let pool = serving_pool(&db, 1).await?;
    let pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&pool)
        .await?;
    wait_until_idle(&pool, 1).await?;
    let terminated: bool = sqlx::query_scalar("SELECT pg_terminate_backend($1)")
        .bind(pid)
        .fetch_one(db.pool())
        .await?;
    ensure!(terminated, "backend {pid} was not signalled");
    wait_until_ended(db.pool(), pid).await?;

    let logs = Logs::default();
    {
        let _capture = logs.capture();
        composed_read(&pool).await?;
    }
    let replacement: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&pool)
        .await?;
    assert_ne!(replacement, pid);
    let logged = logs.text();
    assert!(!logged.contains("WARN"), "{logged}");

    pool.close().await;
    db.cleanup().await
}

#[tokio::test]
async fn connection_the_server_ended_for_idling_in_a_transaction_is_replaced_with_a_warning()
-> Result<()> {
    let db = database("serving_pool_idle_timeout").await?;
    let pool = serving_pool_options(1)
        .connect_with(
            db.pool()
                .connect_options()
                .as_ref()
                .clone()
                .options([("idle_in_transaction_session_timeout", "1s")]),
        )
        .await?;
    let mut conn = pool.acquire().await?;
    let pid = backend_pid(&mut conn).await?;
    drop_begin_in_flight(&mut conn).await?;
    drop(conn);
    wait_until_idle(&pool, 1).await?;
    wait_until_ended(db.pool(), pid).await?;

    let logs = Logs::default();
    {
        let _capture = logs.capture();
        composed_read(&pool).await?;
    }
    let replacement: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&pool)
        .await?;
    assert_ne!(replacement, pid);
    let logged = logs.text();
    assert!(
        logged.contains("WARN") && logged.contains("sqlstate=25P03"),
        "{logged}"
    );

    pool.close().await;
    db.cleanup().await
}

#[tokio::test]
async fn full_pool_with_every_connection_left_in_a_transaction_serves_every_read() -> Result<()> {
    const SIZE: u32 = 3;
    let db = database("serving_pool_all_left_open").await?;
    let pool = serving_pool(&db, SIZE).await?;
    let mut held = acquire_all(&pool, SIZE).await?;
    let mut pids = Vec::new();
    for conn in &mut held {
        pids.push(backend_pid(conn).await?);
    }
    pids.sort_unstable();
    futures_util::future::try_join_all(held.iter_mut().map(|conn| async move {
        drop_begin_in_flight(conn).await?;
        sqlx::query("SELECT 1").execute(&mut **conn).await?;
        anyhow::Ok(())
    }))
    .await?;
    drop(held);

    let mut held = acquire_all(&pool, SIZE).await?;
    let mut states = Vec::new();
    for conn in &mut held {
        states.push(server_state(db.pool(), conn).await?);
    }
    states.sort_unstable();
    assert_eq!(states, pids.iter().copied().map(idle).collect::<Vec<_>>());
    drop(held);

    futures_util::future::try_join_all((0..2 * SIZE).map(|_| composed_read(&pool))).await?;
    let mut held = acquire_all(&pool, SIZE).await?;
    let mut after = Vec::new();
    for conn in &mut held {
        after.push(backend_pid(conn).await?);
    }
    after.sort_unstable();
    assert_eq!(after, pids);
    drop(held);

    pool.close().await;
    db.cleanup().await
}

/// The second line on its own: a pool without the acquire check, as every pool but the API's is.
#[tokio::test]
async fn read_snapshot_pin_starts_again_once_after_a_transaction_left_open() -> Result<()> {
    let db = database("serving_pool_pin_retry").await?;
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect_with(db.pool().connect_options().as_ref().clone())
        .await?;
    let mut conn = pool.acquire().await?;
    let pid = backend_pid(&mut conn).await?;
    drop_begin_in_flight(&mut conn).await?;
    sqlx::query("SELECT 1").execute(&mut *conn).await?;
    drop(conn);

    let logs = Logs::default();
    {
        let _capture = logs.capture();
        composed_read(&pool).await?;
    }
    let logged = logs.text();
    assert!(
        logged.contains("WARN") && logged.contains(ROLLED_BACK_LEFT_OPEN),
        "{logged}"
    );
    let mut conn = pool.acquire().await?;
    assert_eq!(server_state(db.pool(), &mut conn).await?, idle(pid));
    drop(conn);

    pool.close().await;
    db.cleanup().await
}

#[test]
fn sessions_end_a_transaction_left_idle_past_the_request_deadline() -> Result<()> {
    assert_eq!(
        idle_in_transaction_setting(Duration::from_secs(30)),
        ("idle_in_transaction_session_timeout", "35000ms".to_owned())
    );
    assert_eq!(
        idle_in_transaction_setting(Duration::from_millis(MAX_REQUEST_DEADLINE_MS)).1,
        format!("{}ms", i32::MAX)
    );
    let config = crate::DatabaseConfig {
        database_url: Some("postgres://bigname@localhost/bigname".to_owned()),
        max_connections: 1,
    };
    let options = crate::connect_options(
        &config,
        "bigname-api",
        Duration::from_secs(25),
        Duration::from_secs(30),
    )?;
    let startup = options.get_options().unwrap_or_default();
    assert!(
        startup.contains("-c idle_in_transaction_session_timeout=35000ms"),
        "{startup}"
    );
    Ok(())
}
