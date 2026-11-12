//! A request dropped mid-statement, by the request timeout or by its client going away, has its
//! statement cancelled (`bigname_storage::ReadSnapshot`). The request pool is the API's own
//! (`connect_phase_with_application_name_and_statement_timeout`). Each test watches the request's
//! backend from the test database's pool, through `pg_stat_activity`.
use std::{net::SocketAddr, str::FromStr, time::Duration};

use anyhow::{Context, Result, bail, ensure};
use axum::{
    Router,
    body::{Body, to_bytes},
    http::{Request, StatusCode},
    routing::get,
};
use bigname_test_support::{TestDatabase, TestDatabaseConfig, database_url_from_env};
use serde_json::{Value, json};
use sqlx::{PgPool, postgres::PgConnectOptions};
use tokio::{
    io::AsyncWriteExt,
    net::{TcpListener, TcpStream},
    sync::mpsc,
    time::{Instant, timeout, timeout_at},
};
use tower::ServiceExt;

use crate::bounds::{ApiBoundsConfig, apply_request_bounds};

/// How soon after the drop the statement must have ended and its connection be free again.
const FREED_WITHIN: Duration = Duration::from_secs(1);

/// The API's request pool on the test database, with one connection.
async fn request_pool(db: &TestDatabase) -> Result<PgPool> {
    let url = PgConnectOptions::from_str(&database_url_from_env())?
        .database(db.database_name())
        .to_url_lossy()
        .to_string();
    let config = bigname_storage::DatabaseConfig {
        database_url: Some(url),
        max_connections: 1,
    };
    bigname_storage::connect_phase_with_application_name_and_statement_timeout(
        &config,
        "bigname-api",
        Duration::from_secs(25),
        Duration::from_secs(30),
    )
    .await
}

/// A route that reads on one snapshot, reports the snapshot's backend, then runs a statement that
/// outlives every test.
fn slow_read_router(pool: PgPool, pids: mpsc::UnboundedSender<i32>) -> Router {
    Router::new().route(
        "/slow",
        get(move || {
            let (pool, pids) = (pool.clone(), pids.clone());
            async move {
                let mut snapshot = bigname_storage::begin_read_snapshot(&pool)
                    .await
                    .expect("the read snapshot begins");
                let pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
                    .fetch_one(&mut *snapshot)
                    .await
                    .expect("the backend pid reads");
                let _ = pids.send(pid);
                let _ = sqlx::raw_sql("SELECT pg_sleep(20)")
                    .execute(&mut *snapshot)
                    .await;
                "late"
            }
        }),
    )
}

async fn backend_state(observer: &PgPool, pid: i32) -> Result<Option<String>> {
    Ok(
        sqlx::query_scalar("SELECT coalesce(state, '') FROM pg_stat_activity WHERE pid = $1")
            .bind(pid)
            .fetch_optional(observer)
            .await?,
    )
}

async fn wait_until_not_active(observer: &PgPool, pid: i32, deadline: Instant) -> Result<()> {
    loop {
        match backend_state(observer, pid).await? {
            Some(state) if state == "active" => {}
            _ => return Ok(()),
        }
        if Instant::now() >= deadline {
            bail!("backend {pid} still ran the statement of its dropped request");
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

async fn acquire_by(pool: &PgPool, deadline: Instant) -> Result<()> {
    let conn = timeout_at(deadline, pool.acquire())
        .await
        .context("the dropped request's connection was not free in time")??;
    drop(conn);
    Ok(())
}

#[tokio::test]
async fn request_timeout_cancels_the_statement_its_dropped_read_was_running() -> Result<()> {
    let db = TestDatabase::create(TestDatabaseConfig::new("api_read_cancel_timeout")).await?;
    let pool = request_pool(&db).await?;
    let (pids, mut reported) = mpsc::unbounded_channel();
    let config = ApiBoundsConfig {
        request_timeout_ms: 500,
        ..ApiBoundsConfig::default()
    };
    let app = apply_request_bounds(slow_read_router(pool.clone(), pids), Router::new(), &config);

    let response = app
        .oneshot(Request::get("/slow").body(Body::empty())?)
        .await
        .expect("a bounded request completes");
    let deadline = Instant::now() + FREED_WITHIN;
    assert_eq!(response.status(), StatusCode::REQUEST_TIMEOUT);
    let body = to_bytes(response.into_body(), usize::MAX).await?;
    let payload: Value = serde_json::from_slice(&body)?;
    assert_eq!(
        payload.pointer("/error/code"),
        Some(&json!("request_timeout"))
    );
    let pid = reported
        .recv()
        .await
        .context("the route never reported its backend")?;
    wait_until_not_active(db.pool(), pid, deadline).await?;
    acquire_by(&pool, deadline).await?;

    pool.close().await;
    db.cleanup().await
}

#[tokio::test]
async fn client_that_goes_away_mid_statement_has_the_statement_cancelled() -> Result<()> {
    let db = TestDatabase::create(TestDatabaseConfig::new("api_read_cancel_client")).await?;
    let pool = request_pool(&db).await?;
    let (pids, mut reported) = mpsc::unbounded_channel();
    let app = apply_request_bounds(
        slow_read_router(pool.clone(), pids),
        Router::new(),
        &ApiBoundsConfig::default(),
    );
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let server = tokio::spawn(async move {
        axum::serve(
            listener,
            app.into_make_service_with_connect_info::<SocketAddr>(),
        )
        .await
    });

    let mut client = TcpStream::connect(address).await?;
    client
        .write_all(b"GET /slow HTTP/1.1\r\nhost: localhost\r\n\r\n")
        .await?;
    let pid = timeout(Duration::from_secs(5), reported.recv())
        .await
        .context("the route never began its read")?
        .context("the route never reported its backend")?;
    tokio::time::sleep(Duration::from_secs(2)).await;
    let state = backend_state(db.pool(), pid).await?;
    ensure!(
        state.as_deref() == Some("active"),
        "the statement was not running when the client went away: {state:?}"
    );
    drop(client);
    let deadline = Instant::now() + FREED_WITHIN;
    wait_until_not_active(db.pool(), pid, deadline).await?;
    acquire_by(&pool, deadline).await?;

    server.abort();
    pool.close().await;
    db.cleanup().await
}
