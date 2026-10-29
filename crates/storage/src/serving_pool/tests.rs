use std::time::Duration;

use anyhow::{Context, Result, ensure};
use bigname_test_support::{StatementCounter, TestDatabase, TestDatabaseConfig};
use sqlx::{Connection, PgConnection, PgPool, postgres::PgPoolOptions};

use super::{IN_TRANSACTION_BLOCK, serving_pool_options};

/// A statement whose reply arrives well after [`DROP_AFTER`].
const HELD_REPLY: &str = "SELECT pg_sleep(0.5)";
const DROP_AFTER: Duration = Duration::from_millis(50);

async fn database(name: &str) -> Result<TestDatabase> {
    TestDatabase::create(TestDatabaseConfig::new(name)).await
}

/// A pool built as the API builds its request pool, on the test database.
async fn serving_pool(db: &TestDatabase, max_connections: u32) -> Result<PgPool> {
    Ok(serving_pool_options(max_connections)
        .connect_with(db.pool().connect_options().as_ref().clone())
        .await?)
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

/// The state the server reports for `conn`'s session, read from `observer`, another pool.
async fn server_state(observer: &PgPool, conn: &mut PgConnection) -> Result<String> {
    let pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&mut *conn)
        .await?;
    sqlx::query_scalar("SELECT state FROM pg_stat_activity WHERE pid = $1")
        .bind(pid)
        .fetch_one(observer)
        .await
        .context("failed to read the session state")
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

#[tokio::test]
async fn begin_dropped_in_flight_leaves_no_transaction_on_the_next_acquire() -> Result<()> {
    let db = database("serving_pool_begin_dropped").await?;
    let pool = serving_pool(&db, 1).await?;
    drop_begin_in_flight(&mut pool.acquire().await?).await?;

    let mut conn = pool.acquire().await?;
    assert_eq!(server_state(db.pool(), &mut conn).await?, "idle");
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
    {
        let mut conn = pool.acquire().await?;
        drop_begin_in_flight(&mut conn).await?;
        let dropped =
            tokio::time::timeout(DROP_AFTER, sqlx::raw_sql(HELD_REPLY).execute(&mut *conn))
                .await
                .is_err();
        ensure!(
            dropped,
            "the statement answered before its future was dropped"
        );
    }

    composed_read(&pool).await?;
    let mut conn = pool.acquire().await?;
    assert_eq!(server_state(db.pool(), &mut conn).await?, "idle");
    drop(conn);

    pool.close().await;
    db.cleanup().await
}

#[tokio::test]
async fn completed_statement_in_a_transaction_left_open_does_not_fail_the_next_read() -> Result<()>
{
    let db = database("serving_pool_statement_completed").await?;
    let pool = serving_pool(&db, 1).await?;
    {
        let mut conn = pool.acquire().await?;
        drop_begin_in_flight(&mut conn).await?;
        sqlx::query("SELECT 1").execute(&mut *conn).await?;
    }

    composed_read(&pool).await?;
    let mut conn = pool.acquire().await?;
    assert_eq!(server_state(db.pool(), &mut conn).await?, "idle");
    drop(conn);

    pool.close().await;
    db.cleanup().await
}

#[tokio::test]
async fn healthy_idle_connection_is_handed_out_unchanged_after_one_statement() -> Result<()> {
    let db = database("serving_pool_healthy").await?;
    let counter =
        StatementCounter::connect_with_pool_options(db.pool(), serving_pool_options(1)).await?;
    let pool = counter.pool();
    let pid: i32 = {
        let mut conn = pool.acquire().await?;
        sqlx::raw_sql("SET application_name = 'serving-pool-healthy'")
            .execute(&mut *conn)
            .await?;
        sqlx::query_scalar("SELECT pg_backend_pid()")
            .fetch_one(&mut *conn)
            .await?
    };

    let (conn, sent) = counter.count(pool.acquire()).await;
    let mut conn = conn?;
    assert_eq!(sent.statements(), [IN_TRANSACTION_BLOCK]);
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
async fn connection_the_server_closed_is_replaced_on_acquire() -> Result<()> {
    let db = database("serving_pool_server_closed").await?;
    let pool = serving_pool(&db, 1).await?;
    let pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&pool)
        .await?;
    // The connection is back in the pool before the server closes it.
    for _ in 0..100 {
        if pool.num_idle() == 1 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    ensure!(
        pool.num_idle() == 1,
        "the connection did not return to the pool"
    );
    let terminated: bool = sqlx::query_scalar("SELECT pg_terminate_backend($1)")
        .bind(pid)
        .fetch_one(db.pool())
        .await?;
    ensure!(terminated, "backend {pid} was not signalled");
    let mut gone = false;
    for _ in 0..100 {
        let sessions: i64 =
            sqlx::query_scalar("SELECT count(*) FROM pg_stat_activity WHERE pid = $1")
                .bind(pid)
                .fetch_one(db.pool())
                .await?;
        if sessions == 0 {
            gone = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    ensure!(gone, "backend {pid} outlived its termination");

    composed_read(&pool).await?;
    let replacement: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&pool)
        .await?;
    assert_ne!(replacement, pid);

    pool.close().await;
    db.cleanup().await
}

#[tokio::test]
async fn full_pool_with_every_connection_left_in_a_transaction_serves_every_read() -> Result<()> {
    const SIZE: u32 = 3;
    let db = database("serving_pool_all_left_open").await?;
    let pool = serving_pool(&db, SIZE).await?;
    let mut held = Vec::new();
    for _ in 0..SIZE {
        held.push(pool.acquire().await?);
    }
    futures_util::future::try_join_all(held.iter_mut().map(|conn| async move {
        drop_begin_in_flight(conn).await?;
        sqlx::query("SELECT 1").execute(&mut **conn).await?;
        anyhow::Ok(())
    }))
    .await?;
    drop(held);

    futures_util::future::try_join_all((0..2 * SIZE).map(|_| composed_read(&pool))).await?;
    let mut held = Vec::new();
    for _ in 0..SIZE {
        held.push(pool.acquire().await?);
    }
    for conn in &mut held {
        assert_eq!(server_state(db.pool(), conn).await?, "idle");
    }
    assert_eq!(pool.size(), SIZE);
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
    {
        let mut conn = pool.acquire().await?;
        drop_begin_in_flight(&mut conn).await?;
        sqlx::query("SELECT 1").execute(&mut *conn).await?;
    }

    composed_read(&pool).await?;
    let mut conn = pool.acquire().await?;
    assert_eq!(server_state(db.pool(), &mut conn).await?, "idle");
    drop(conn);

    pool.close().await;
    db.cleanup().await
}
