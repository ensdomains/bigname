//! A read snapshot dropped before it ends cancels its statement (`crate::ReadSnapshot`). Each test
//! watches the dropped read's backend from another pool, the test database's own, through
//! `pg_stat_activity`.
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, bail, ensure};
use bigname_test_support::{StatementCounter, StatementCounts, TestDatabase};
use sqlx::{
    PgPool,
    postgres::{PgConnectOptions, PgPoolOptions},
};
use tokio::time::{Instant, timeout, timeout_at};

use super::tests::{
    Logs, acquire_all, backend_pid, composed_read, database, idle, server_state, wait_until_idle,
};
use super::{
    CANCEL_FAILED, CANCEL_REPLY_BACKSTOP, CANCEL_WAIT, ROLLED_BACK_LEFT_OPEN, cancel_pool,
    cancel_reads_on_drop, register_cancel_pool, serving_pool_options,
};

/// A statement no test lets end: each drops its read first.
const LONG: &str = "SELECT pg_sleep(20)";
/// How soon after a drop its backend must have stopped running the dropped statement, and its
/// connection must be free again.
const FREED_WITHIN: Duration = Duration::from_secs(1);
const DROP_AFTER: Duration = Duration::from_millis(200);
const STEP: Duration = Duration::from_millis(20);

/// A request pool built as the API builds its own, whose dropped reads cancel through a counted
/// pool of one connection.
async fn cancelling_pool(db: &TestDatabase, size: u32) -> Result<(PgPool, StatementCounter)> {
    cancelling_pool_with(db, size, db.pool().connect_options().as_ref().clone()).await
}

async fn cancelling_pool_with(
    db: &TestDatabase,
    size: u32,
    options: PgConnectOptions,
) -> Result<(PgPool, StatementCounter)> {
    let pool = serving_pool_options(size).connect_with(options).await?;
    let cancels = StatementCounter::connect_with_pool_options(
        db.pool(),
        PgPoolOptions::new().max_connections(1),
    )
    .await?;
    register_cancel_pool(&pool, cancels.pool().clone());
    Ok((pool, cancels))
}

fn cancels_sent(sent: &StatementCounts) -> usize {
    sent.statements()
        .iter()
        .filter(|statement| statement.contains("pg_cancel_backend"))
        .count()
}

/// Begins a read snapshot on `pool`, runs `statement` on it and drops the read `after` into the
/// statement, as a request dropped mid-statement does. Returns the read's backend.
async fn drop_mid_statement(pool: &PgPool, statement: &str, after: Duration) -> Result<i32> {
    let mut snapshot = crate::begin_read_snapshot(pool).await?;
    let pid = backend_pid(&mut snapshot).await?;
    let ended = timeout(after, sqlx::raw_sql(statement).execute(&mut *snapshot))
        .await
        .is_ok();
    ensure!(!ended, "the statement ended before its read was dropped");
    drop(snapshot);
    Ok(pid)
}

/// The state of backend `pid`, `None` once it has gone.
async fn backend_state(observer: &PgPool, pid: i32) -> Result<Option<String>> {
    Ok(
        sqlx::query_scalar("SELECT coalesce(state, '') FROM pg_stat_activity WHERE pid = $1")
            .bind(pid)
            .fetch_optional(observer)
            .await?,
    )
}

/// Waits until backend `pid` runs no statement, idle or gone, failing at `deadline`.
async fn wait_until_not_active(observer: &PgPool, pid: i32, deadline: Instant) -> Result<()> {
    loop {
        match backend_state(observer, pid).await? {
            Some(state) if state == "active" => {}
            _ => return Ok(()),
        }
        if Instant::now() >= deadline {
            bail!("backend {pid} still ran the statement of its dropped read");
        }
        tokio::time::sleep(STEP).await;
    }
}

async fn ensure_active(observer: &PgPool, pid: i32) -> Result<()> {
    let state = backend_state(observer, pid).await?;
    ensure!(
        state.as_deref() == Some("active"),
        "backend {pid} no longer runs its statement: {state:?}"
    );
    Ok(())
}

async fn acquire_by(pool: &PgPool, deadline: Instant) -> Result<()> {
    let conn = timeout_at(deadline, pool.acquire())
        .await
        .context("the dropped read's connection was not free in time")??;
    drop(conn);
    Ok(())
}

/// A client that goes away 2 s into a long statement.
#[tokio::test]
async fn read_dropped_mid_statement_ends_its_statement_and_frees_its_connection() -> Result<()> {
    let db = database("read_cancel_client_abort").await?;
    let (pool, cancels) = cancelling_pool(&db, 1).await?;
    let pid = drop_mid_statement(&pool, LONG, Duration::from_secs(2)).await?;
    let deadline = Instant::now() + FREED_WITHIN;
    wait_until_not_active(db.pool(), pid, deadline).await?;
    acquire_by(&pool, deadline).await?;
    composed_read(&pool).await?;

    pool.close().await;
    cancels.close().await;
    db.cleanup().await
}

/// The invariant: the cancel is sent before the connection can return to the pool. With the
/// cancel held back, the connection stays with the dropped read.
#[tokio::test]
async fn dropped_read_keeps_its_connection_until_its_cancel_is_sent() -> Result<()> {
    let db = database("read_cancel_order").await?;
    let (pool, cancels) = cancelling_pool(&db, 1).await?;
    let mut snapshot = crate::begin_read_snapshot(&pool).await?;
    let pid = backend_pid(&mut snapshot).await?;
    // Holding the cancel pool's only connection holds the cancel back.
    let held = cancels.pool().acquire().await?;
    drop(snapshot);
    let early = timeout(CANCEL_WAIT / 3, pool.acquire()).await;
    ensure!(
        early.is_err(),
        "the dropped read's connection returned to the pool before its cancel was sent"
    );

    let (conn, sent) = cancels
        .count(async {
            drop(held);
            timeout(CANCEL_WAIT, pool.acquire()).await
        })
        .await;
    let mut conn = conn.context("the connection never returned after its cancel")??;
    assert_eq!(cancels_sent(&sent), 1, "{:?}", sent.statements());
    assert_eq!(server_state(db.pool(), &mut conn).await?, idle(pid));
    drop(conn);

    pool.close().await;
    cancels.close().await;
    db.cleanup().await
}

/// `statement_timeout` ends the statement first and the read returns its error. Its drop still
/// cancels once, which the backend, then between statements, ignores.
#[tokio::test]
async fn read_whose_statement_reached_statement_timeout_cancels_once_and_keeps_its_connection()
-> Result<()> {
    let db = database("read_cancel_statement_timeout").await?;
    let options = db
        .pool()
        .connect_options()
        .as_ref()
        .clone()
        .options([("statement_timeout", "300ms")]);
    let (pool, cancels) = cancelling_pool_with(&db, 1, options).await?;
    let (pid, sent) = cancels
        .count(async {
            let mut snapshot = crate::begin_read_snapshot(&pool).await?;
            let pid = backend_pid(&mut snapshot).await?;
            let Err(error) = sqlx::raw_sql("SELECT pg_sleep(5)")
                .execute(&mut *snapshot)
                .await
            else {
                bail!("the statement outlived statement_timeout");
            };
            let code = error
                .as_database_error()
                .and_then(|error| error.code())
                .map(|code| code.into_owned());
            ensure!(code.as_deref() == Some("57014"), "{error}");
            drop(snapshot);
            wait_until_idle(&pool, 1).await?;
            anyhow::Ok(pid)
        })
        .await;
    let pid = pid?;
    assert_eq!(cancels_sent(&sent), 1, "{:?}", sent.statements());
    let mut conn = pool.acquire().await?;
    assert_eq!(server_state(db.pool(), &mut conn).await?, idle(pid));
    drop(conn);
    composed_read(&pool).await?;

    pool.close().await;
    cancels.close().await;
    db.cleanup().await
}

async fn one_read(pool: &PgPool) -> Result<()> {
    let mut snapshot = crate::begin_read_snapshot(pool).await?;
    sqlx::query("SELECT 1").execute(&mut *snapshot).await?;
    snapshot.commit().await?;
    Ok(())
}

/// A committed read sends no cancel. Its cost is one statement, the pid read.
#[tokio::test]
async fn committed_reads_send_no_cancel_and_one_more_statement_each() -> Result<()> {
    let db = database("read_cancel_commit").await?;
    let requests =
        StatementCounter::connect_with_pool_options(db.pool(), serving_pool_options(1)).await?;
    let plain =
        StatementCounter::connect_with_pool_options(db.pool(), serving_pool_options(1)).await?;
    let cancels = StatementCounter::connect_with_pool_options(
        db.pool(),
        PgPoolOptions::new().max_connections(1),
    )
    .await?;
    register_cancel_pool(requests.pool(), cancels.pool().clone());

    let (read, cancelling) = requests.count(one_read(requests.pool())).await;
    read?;
    let (read, without) = plain.count(one_read(plain.pool())).await;
    read?;
    assert_eq!(
        cancelling.total(),
        without.total() + 1,
        "{:?} against {:?}",
        cancelling.statements(),
        without.statements()
    );
    let pid_reads = cancelling
        .statements()
        .iter()
        .filter(|statement| statement.contains("pg_backend_pid"))
        .count();
    assert_eq!(pid_reads, 1, "{:?}", cancelling.statements());

    let (reads, sent) = cancels
        .count(async {
            for _ in 0..1_000 {
                one_read(requests.pool()).await?;
            }
            wait_until_idle(requests.pool(), 1).await
        })
        .await;
    reads?;
    assert_eq!(sent.total(), 0, "{:?}", sent.statements());

    requests.close().await;
    plain.close().await;
    cancels.close().await;
    db.cleanup().await
}

/// A read nested on the snapshot, as a route's savepoint is, keeps sqlx's transaction depth: it
/// opens a savepoint, not a second transaction.
#[tokio::test]
async fn read_nested_on_a_snapshot_opens_a_savepoint() -> Result<()> {
    let db = database("read_cancel_savepoint").await?;
    let requests =
        StatementCounter::connect_with_pool_options(db.pool(), serving_pool_options(1)).await?;
    let cancels = StatementCounter::connect_with_pool_options(
        db.pool(),
        PgPoolOptions::new().max_connections(1),
    )
    .await?;
    register_cancel_pool(requests.pool(), cancels.pool().clone());
    let (read, sent) = requests
        .count(async {
            let mut snapshot = crate::begin_read_snapshot(requests.pool()).await?;
            let mut nested = sqlx::Connection::begin(&mut *snapshot).await?;
            sqlx::query("SELECT 1").execute(&mut *nested).await?;
            nested.rollback().await?;
            snapshot.commit().await?;
            anyhow::Ok(())
        })
        .await;
    read?;
    let statements = sent.statements();
    let begins = statements.iter().filter(|s| s.trim() == "BEGIN").count();
    let savepoints = statements
        .iter()
        .filter(|s| s.trim_start().starts_with("SAVEPOINT"))
        .count();
    assert_eq!((begins, savepoints), (1, 1), "{statements:?}");

    requests.close().await;
    cancels.close().await;
    db.cleanup().await
}

#[tokio::test]
async fn full_pool_whose_reads_are_dropped_mid_statement_serves_the_next_read() -> Result<()> {
    let db = database("read_cancel_full_pool").await?;
    let (pool, cancels) = cancelling_pool(&db, 2).await?;
    let (first, second) = tokio::try_join!(
        drop_mid_statement(&pool, LONG, DROP_AFTER),
        drop_mid_statement(&pool, LONG, DROP_AFTER),
    )?;
    ensure!(first != second, "both reads ran on one connection");
    let deadline = Instant::now() + FREED_WITHIN;
    wait_until_not_active(db.pool(), first, deadline).await?;
    wait_until_not_active(db.pool(), second, deadline).await?;
    acquire_by(&pool, Instant::now() + Duration::from_secs(2)).await?;
    composed_read(&pool).await?;

    pool.close().await;
    cancels.close().await;
    db.cleanup().await
}

#[tokio::test]
async fn read_dropped_on_its_third_statement_leaves_no_transaction_open() -> Result<()> {
    let db = database("read_cancel_third_statement").await?;
    let (pool, cancels) = cancelling_pool(&db, 1).await?;
    let mut snapshot = crate::begin_read_snapshot(&pool).await?;
    let pid = backend_pid(&mut snapshot).await?;
    sqlx::query("SELECT 1").execute(&mut *snapshot).await?;
    sqlx::query("SELECT 2").execute(&mut *snapshot).await?;
    let ended = timeout(DROP_AFTER, sqlx::raw_sql(LONG).execute(&mut *snapshot))
        .await
        .is_ok();
    ensure!(!ended, "the statement ended before its read was dropped");

    let logs = Logs::default();
    let _capture = logs.capture();
    drop(snapshot);
    let deadline = Instant::now() + FREED_WITHIN;
    wait_until_not_active(db.pool(), pid, deadline).await?;
    let mut conn = timeout_at(deadline, pool.acquire())
        .await
        .context("the dropped read's connection was not free in time")??;
    let (_, state) = server_state(db.pool(), &mut conn).await?;
    assert_eq!(state, "idle");
    drop(conn);
    composed_read(&pool).await?;
    let logged = logs.text();
    assert!(!logged.contains(ROLLED_BACK_LEFT_OPEN), "{logged}");

    pool.close().await;
    cancels.close().await;
    db.cleanup().await
}

/// Drops on either side of the statement's end, so some cancels arrive after it, or as the
/// rollback starts. A cancel may cost its own connection, never the next read.
#[tokio::test]
async fn cancel_racing_the_end_of_its_statement_never_reaches_the_next_read() -> Result<()> {
    let db = database("read_cancel_race").await?;
    let (pool, cancels) = cancelling_pool(&db, 1).await?;
    let logs = Logs::default();
    let _capture = logs.capture();
    for iteration in 0..50_u64 {
        let mut snapshot = crate::begin_read_snapshot(&pool).await?;
        let _ = timeout(
            Duration::from_millis(40 + iteration % 21),
            sqlx::raw_sql("SELECT pg_sleep(0.05)").execute(&mut *snapshot),
        )
        .await;
        drop(snapshot);
        composed_read(&pool)
            .await
            .with_context(|| format!("the read after drop {iteration}"))?;
    }
    let logged = logs.text();
    assert!(!logged.contains(ROLLED_BACK_LEFT_OPEN), "{logged}");
    assert!(!logged.contains("error from `before_acquire`"), "{logged}");
    assert!(!logged.contains(CANCEL_FAILED), "{logged}");

    pool.close().await;
    cancels.close().await;
    db.cleanup().await
}

/// A read dropped between statements, as a not-found answer that returns before its commit is.
#[tokio::test]
async fn read_dropped_between_statements_keeps_its_connection() -> Result<()> {
    let db = database("read_cancel_idle").await?;
    let (pool, cancels) = cancelling_pool(&db, 1).await?;
    let (pid, sent) = cancels
        .count(async {
            let mut snapshot = crate::begin_read_snapshot(&pool).await?;
            let pid = backend_pid(&mut snapshot).await?;
            drop(snapshot);
            wait_until_idle(&pool, 1).await?;
            anyhow::Ok(pid)
        })
        .await;
    let pid = pid?;
    assert_eq!(cancels_sent(&sent), 1, "{:?}", sent.statements());
    let mut conn = pool.acquire().await?;
    assert_eq!(server_state(db.pool(), &mut conn).await?, idle(pid));
    drop(conn);

    pool.close().await;
    cancels.close().await;
    db.cleanup().await
}

/// Without a cancel connection the drop ends as it would with no cancel at all: the statement
/// runs to its end, and its connection then returns.
#[tokio::test]
async fn read_dropped_while_the_cancel_pool_is_closed_warns_once_and_ends_its_statement_as_before()
-> Result<()> {
    let db = database("read_cancel_closed").await?;
    let (pool, cancels) = cancelling_pool(&db, 1).await?;
    cancels.pool().close().await;
    let logs = Logs::default();
    let _capture = logs.capture();
    let pid = drop_mid_statement(&pool, "SELECT pg_sleep(2)", DROP_AFTER).await?;
    tokio::time::sleep(Duration::from_millis(500)).await;
    ensure_active(db.pool(), pid).await?;
    acquire_by(&pool, Instant::now() + Duration::from_secs(5)).await?;
    let logged = logs.text();
    assert_eq!(logged.matches(CANCEL_FAILED).count(), 1, "{logged}");

    pool.close().await;
    db.cleanup().await
}

/// The cancel waits at most `CANCEL_WAIT` for its connection, then lets the drop end as before.
/// Nothing was sent, so the connection goes back to the pool.
#[tokio::test]
async fn read_dropped_while_the_cancel_connection_is_busy_gives_up_after_the_wait() -> Result<()> {
    let db = database("read_cancel_busy").await?;
    let (pool, cancels) = cancelling_pool(&db, 1).await?;
    let held = cancels.pool().acquire().await?;
    let logs = Logs::default();
    let _capture = logs.capture();
    let pid = drop_mid_statement(&pool, "SELECT pg_sleep(3)", DROP_AFTER).await?;
    tokio::time::sleep(CANCEL_WAIT + Duration::from_millis(300)).await;
    ensure_active(db.pool(), pid).await?;
    let logged = logs.text();
    assert_eq!(logged.matches(CANCEL_FAILED).count(), 1, "{logged}");
    assert!(
        logged.contains("timed out waiting for the cancel connection"),
        "{logged}"
    );
    let mut conn = timeout(Duration::from_secs(5), pool.acquire())
        .await
        .context("the dropped read's connection never came back")??;
    let (returned, _) = server_state(db.pool(), &mut conn).await?;
    assert_eq!(returned, pid, "the connection was closed, not returned");
    drop(conn);
    drop(held);

    pool.close().await;
    cancels.close().await;
    db.cleanup().await
}

/// A cancel pool whose first `pg_cancel_backend` takes `delay_secs` before it runs, as on a
/// loaded server. Its sessions find the function in `late` ahead of `pg_catalog`, and run under
/// `statement_timeout` when it is given.
async fn late_cancel_pool(
    db: &TestDatabase,
    delay_secs: u32,
    statement_timeout: Option<&str>,
) -> Result<PgPool> {
    sqlx::raw_sql(&format!(
        "CREATE SCHEMA late;
         CREATE SEQUENCE late.calls;
         CREATE FUNCTION late.pg_cancel_backend(pid integer) RETURNS boolean
         LANGUAGE plpgsql AS $$
         BEGIN
             IF nextval('late.calls') = 1 THEN
                 PERFORM pg_catalog.pg_sleep({delay_secs});
             END IF;
             RETURN pg_catalog.pg_cancel_backend(pid);
         END
         $$;"
    ))
    .execute(db.pool())
    .await?;
    let mut options = db
        .pool()
        .connect_options()
        .as_ref()
        .clone()
        // No space: the startup options are split on whitespace.
        .options([("search_path", "late,pg_catalog")]);
    if let Some(statement_timeout) = statement_timeout {
        options = options.options([("statement_timeout", statement_timeout)]);
    }
    Ok(PgPoolOptions::new()
        .max_connections(1)
        .test_before_acquire(false)
        .connect_with(options)
        .await?)
}

async fn late_cancel_setup(
    name: &str,
    delay_secs: u32,
    statement_timeout: Option<&str>,
) -> Result<(TestDatabase, PgPool, PgPool)> {
    let db = database(name).await?;
    let pool = serving_pool_options(1)
        .connect_with(db.pool().connect_options().as_ref().clone())
        .await?;
    let cancels = late_cancel_pool(&db, delay_secs, statement_timeout).await?;
    register_cancel_pool(&pool, cancels.clone());
    Ok((db, pool, cancels))
}

/// The cancel was sent and its reply is late. The read's connection stays out of the pool until
/// the reply, so the cancel cannot reach the next read on it.
#[tokio::test]
async fn read_whose_cancel_reply_is_late_keeps_its_connection_until_the_reply() -> Result<()> {
    let (db, pool, cancels) = late_cancel_setup("read_cancel_late_reply", 3, None).await?;
    let mut snapshot = crate::begin_read_snapshot(&pool).await?;
    let pid = backend_pid(&mut snapshot).await?;
    let logs = Logs::default();
    let _capture = logs.capture();
    drop(snapshot);

    let early = timeout(Duration::from_secs(2), pool.acquire()).await;
    ensure!(
        early.is_err(),
        "the connection went back to the pool before its cancel was answered"
    );
    let mut next = timeout(Duration::from_secs(5), pool.acquire())
        .await
        .context("the connection never came back after the reply")??;
    let (next_pid, _) = server_state(db.pool(), &mut next).await?;
    assert_eq!(next_pid, pid);
    sqlx::raw_sql("SELECT pg_sleep(1)")
        .execute(&mut *next)
        .await
        .context("the read after the late cancel failed")?;
    drop(next);
    let logged = logs.text();
    assert!(!logged.contains(CANCEL_FAILED), "{logged}");

    pool.close().await;
    cancels.close().await;
    db.cleanup().await
}

/// The cancel connection's `statement_timeout` ends a cancel the server is slow to run. The drop
/// warns once, and its connection returns once the server has answered.
#[tokio::test]
async fn cancel_that_reaches_its_own_statement_timeout_warns_and_returns_the_connection()
-> Result<()> {
    let (db, pool, cancels) =
        late_cancel_setup("read_cancel_own_timeout", 3, Some("1500ms")).await?;
    let mut snapshot = crate::begin_read_snapshot(&pool).await?;
    let pid = backend_pid(&mut snapshot).await?;
    let logs = Logs::default();
    let _capture = logs.capture();
    drop(snapshot);

    let early = timeout(Duration::from_millis(500), pool.acquire()).await;
    ensure!(
        early.is_err(),
        "the connection went back to the pool before the server answered its cancel"
    );
    let mut next = timeout(Duration::from_secs(3), pool.acquire())
        .await
        .context("the connection never came back after the cancel timed out")??;
    let (next_pid, _) = server_state(db.pool(), &mut next).await?;
    assert_eq!(next_pid, pid);
    drop(next);
    let logged = logs.text();
    assert_eq!(logged.matches(CANCEL_FAILED).count(), 1, "{logged}");
    assert!(logged.contains("statement timeout"), "{logged}");

    pool.close().await;
    cancels.close().await;
    db.cleanup().await
}

async fn wait_until_logged(logs: &Logs, text: &str, within: Duration) -> Result<()> {
    let deadline = Instant::now() + within;
    while !logs.text().contains(text) {
        if Instant::now() >= deadline {
            bail!("nothing logged {text:?}");
        }
        tokio::time::sleep(STEP).await;
    }
    Ok(())
}

/// A cancel whose reply does not come within `CANCEL_REPLY_BACKSTOP` gives up, and its
/// connection, with that reply still due, is closed. The next cancel goes out at once on a new
/// connection rather than waiting behind it.
#[tokio::test]
async fn cancel_past_the_reply_backstop_frees_the_cancel_connection_for_the_next_cancel()
-> Result<()> {
    let delay = CANCEL_REPLY_BACKSTOP + Duration::from_secs(5);
    let (db, pool, cancels) = late_cancel_setup(
        "read_cancel_backstop",
        u32::try_from(delay.as_secs())?,
        None,
    )
    .await?;
    let logs = Logs::default();
    let _capture = logs.capture();
    drop(crate::begin_read_snapshot(&pool).await?);
    wait_until_logged(
        &logs,
        "no reply from the server",
        CANCEL_REPLY_BACKSTOP + Duration::from_secs(3),
    )
    .await?;

    let pid = drop_mid_statement(&pool, LONG, DROP_AFTER).await?;
    wait_until_not_active(db.pool(), pid, Instant::now() + FREED_WITHIN).await?;
    let logged = logs.text();
    assert_eq!(logged.matches(CANCEL_FAILED).count(), 1, "{logged}");

    pool.close().await;
    cancels.close().await;
    db.cleanup().await
}

/// Fifty reads dropped at once, each mid-statement: one cancel connection serves every cancel
/// within its wait, and every connection is free again.
#[tokio::test]
async fn fifty_reads_dropped_at_once_are_all_cancelled_promptly() -> Result<()> {
    const READS: u32 = 50;
    let db = database("read_cancel_burst").await?;
    let (pool, cancels) = cancelling_pool(&db, READS).await?;
    let mut snapshots = Vec::new();
    let mut pids = Vec::new();
    for _ in 0..READS {
        let mut snapshot = crate::begin_read_snapshot(&pool).await?;
        pids.push(backend_pid(&mut snapshot).await?);
        snapshots.push(snapshot);
    }
    let logs = Logs::default();
    let _capture = logs.capture();
    let (freed, sent) = cancels
        .count(async {
            let running = futures_util::future::join_all(
                snapshots
                    .iter_mut()
                    .map(|snapshot| sqlx::raw_sql(LONG).execute(&mut **snapshot)),
            );
            ensure!(
                timeout(Duration::from_millis(500), running).await.is_err(),
                "a statement ended before its read was dropped"
            );
            drop(snapshots);
            let deadline = Instant::now() + 2 * FREED_WITHIN;
            for pid in &pids {
                wait_until_not_active(db.pool(), *pid, deadline).await?;
            }
            timeout(Duration::from_secs(5), acquire_all(&pool, READS))
                .await
                .context("the pool did not hand out every connection again")?
        })
        .await;
    drop(freed?);
    assert_eq!(
        cancels_sent(&sent),
        READS as usize,
        "{:?}",
        sent.statements()
    );
    let logged = logs.text();
    assert!(!logged.contains(CANCEL_FAILED), "{logged}");

    pool.close().await;
    cancels.close().await;
    db.cleanup().await
}

/// The API connects as a role without superuser (`docs/deployment.md`). The cancel pool built for
/// it connects as that role, and its cancel reaches the role's own backend.
#[tokio::test]
async fn cancel_from_a_role_without_superuser_ends_that_roles_own_statement() -> Result<()> {
    const PASSWORD: &str = "read-cancel";
    let db = database("read_cancel_role").await?;
    let role = format!(
        "read_cancel_{:x}",
        SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos()
    );
    sqlx::raw_sql(&format!(
        "CREATE ROLE {role} LOGIN NOSUPERUSER PASSWORD '{PASSWORD}'"
    ))
    .execute(db.pool())
    .await?;
    let options = db
        .pool()
        .connect_options()
        .as_ref()
        .clone()
        .username(&role)
        .password(PASSWORD);
    let pool = serving_pool_options(1).connect_with(options).await?;
    cancel_reads_on_drop(&pool, "read-cancel-role");
    let cancels = cancel_pool(&pool).context("the request pool has no cancel pool")?;
    let superuser: bool =
        sqlx::query_scalar("SELECT rolsuper FROM pg_roles WHERE rolname = current_user")
            .fetch_one(&pool)
            .await?;
    ensure!(!superuser, "the request pool connected as a superuser");

    let pid = drop_mid_statement(&pool, LONG, DROP_AFTER).await?;
    let deadline = Instant::now() + FREED_WITHIN;
    wait_until_not_active(db.pool(), pid, deadline).await?;
    acquire_by(&pool, deadline).await?;
    let cancel_sessions: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM pg_stat_activity
         WHERE usename = $1 AND application_name = 'read-cancel-role'",
    )
    .bind(&role)
    .fetch_one(db.pool())
    .await?;
    assert_eq!(cancel_sessions, 1);
    let cancel_timeout: String = sqlx::query_scalar("SHOW statement_timeout")
        .fetch_one(&cancels)
        .await?;
    assert_eq!(cancel_timeout, "5s");
    composed_read(&pool).await?;

    pool.close().await;
    cancels.close().await;
    sqlx::raw_sql(&format!("DROP ROLE {role}"))
        .execute(db.pool())
        .await?;
    db.cleanup().await
}
