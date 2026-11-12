//! The options of the pool the API serves reads from.
//!
//! sqlx counts the transactions it began on a connection, and raises that count only when the
//! server's reply to `BEGIN` arrives. A request whose future is dropped while that reply is in
//! flight, as when a client disconnects, leaves the server inside a transaction that sqlx does not
//! count. Its rollback guard does nothing at a count of zero, so the pool takes the connection back
//! as clean. The next reader's `BEGIN` then only warns, and once any statement has run in the
//! leftover transaction, that reader's `SET TRANSACTION ISOLATION LEVEL` fails with SQLSTATE 25001.
//!
//! Three lines keep that from reaching a response:
//!
//! - The acquire check below ends such a transaction before the connection is handed out.
//! - A request's read snapshot that still meets one at its pin rolls it back and starts again once
//!   (`crate::begin_read_snapshot`).
//! - Every session sets `idle_in_transaction_session_timeout` a little past the request deadline,
//!   so the server ends a transaction left open even when no request takes its connection again.
//!   A live request holds no transaction past its own deadline, so this never ends one in use.
//!   It matters on a hot standby. A transaction left open holds its locks and, after a snapshot
//!   pin, its snapshot. Either can hold up WAL replay there: an exclusive-lock record waits on the
//!   locks, and a conflicting cleanup record on the snapshot. Each waits up to
//!   `max_standby_streaming_delay` before the standby cancels the session, and the published head
//!   falls behind meanwhile. See the PostgreSQL documentation, "Client Connection Defaults"
//!   (`idle_in_transaction_session_timeout`) and "Hot Standby", "Handling Query Conflicts".
//!
//! A request whose future is dropped mid-statement leaves that statement running, and the driver
//! returns its connection only once the statement ends. The request pool therefore has a cancel
//! pool of one connection (`cancel_reads_on_drop`). A read snapshot dropped before it ends sends
//! `pg_cancel_backend` for its backend through it, and only then gives its connection back
//! (`crate::ReadSnapshot`). The cancel waits at most `CANCEL_WAIT` for that connection and the
//! reply together. Past that, or on an error, it logs a warning and the statement runs on as it
//! would without the cancel. PostgreSQL ignores a cancel that reaches a backend between
//! statements. The cancel needs no privilege beyond the role the request pool connects as, and it
//! works on a hot standby. See the PostgreSQL documentation, "Server Signaling Functions" and
//! "Hot Standby", "Administrator's Overview".

use std::{
    borrow::Cow,
    sync::{Mutex, PoisonError},
    time::Duration,
};

use futures_util::future::BoxFuture;
use sqlx::{Executor, PgConnection, PgPool, Row, postgres::PgPoolOptions};

/// True when an earlier statement opened the transaction the session is in. Sent as a simple
/// query, which is one protocol message: PostgreSQL gives the first statement of a transaction the
/// transaction's start time as its own, so the two times differ only inside a transaction block.
const IN_TRANSACTION_BLOCK: &str = "SELECT statement_timestamp() <> transaction_timestamp()";

/// What is logged when a transaction left open is rolled back, by the acquire check or the pin.
pub(crate) const ROLLED_BACK_LEFT_OPEN: &str =
    "rolled back a transaction left open on a pooled read connection";

/// How long past the request deadline a session may sit idle inside a transaction.
pub const IDLE_IN_TRANSACTION_MARGIN_MS: u64 = 5_000;
const IDLE_IN_TRANSACTION_MARGIN: Duration = Duration::from_millis(IDLE_IN_TRANSACTION_MARGIN_MS);

/// The longest request deadline in milliseconds whose idle-in-transaction timeout PostgreSQL
/// accepts. The setting is an `int` of milliseconds.
pub const MAX_REQUEST_DEADLINE_MS: u64 = i32::MAX as u64 - IDLE_IN_TRANSACTION_MARGIN_MS;

/// The SQLSTATE of a session the server ended under `idle_in_transaction_session_timeout`.
const IDLE_IN_TRANSACTION_SESSION_TIMEOUT: &str = "25P03";

pub(crate) fn serving_pool_options(max_connections: u32) -> PgPoolOptions {
    PgPoolOptions::new()
        .max_connections(max_connections)
        // The check is one round trip that a dead connection fails as well, so it takes the place
        // of the empty ping sqlx would otherwise send first. A failed check closes the connection
        // and the acquire opens a new one.
        .test_before_acquire(false)
        .before_acquire(|conn, _| end_transaction_left_open(conn))
}

/// The startup setting that ends a session left idle in a transaction, derived from the request
/// deadline so the two move together.
pub(crate) fn idle_in_transaction_setting(request_deadline: Duration) -> (&'static str, String) {
    (
        "idle_in_transaction_session_timeout",
        format!(
            "{}ms",
            (request_deadline + IDLE_IN_TRANSACTION_MARGIN).as_millis()
        ),
    )
}

/// Ends a transaction an earlier request left open, before the connection is handed out. sqlx
/// exposes the server's transaction status only through its own count, which is what the leak
/// leaves wrong, so the server is asked. A `&str` statement goes out as a simple query.
///
/// The statements go through `Executor` directly. Its boxed futures are `Send` for any connection
/// lifetime, as the hook's higher-ranked bound needs. The generic `async fn` wrappers, such as
/// `raw_sql(..).fetch_one`, cannot be proved so.
fn end_transaction_left_open(conn: &mut PgConnection) -> BoxFuture<'_, Result<bool, sqlx::Error>> {
    Box::pin(async move {
        let left_open: bool = match Executor::fetch_one(&mut *conn, IN_TRANSACTION_BLOCK).await {
            Ok(row) => row.try_get(0)?,
            Err(error) => return ended_by_server(error),
        };
        if left_open {
            tracing::warn!("{ROLLED_BACK_LEFT_OPEN}");
            Executor::execute(&mut *conn, "ROLLBACK").await?;
        }
        Ok(true)
    })
}

/// A connection the server has ended is closed and replaced (`Ok(false)`). After a server restart
/// every pooled connection fails this way, so a shutdown or a lost socket logs at the level sqlx's
/// own ping did. An end under the idle-in-transaction timeout means a transaction was left open,
/// so it warns. Any other error closes the connection through sqlx, which logs it.
fn ended_by_server(error: sqlx::Error) -> Result<bool, sqlx::Error> {
    let sqlstate = error
        .as_database_error()
        .and_then(|error| error.code())
        .map(Cow::into_owned);
    match sqlstate.as_deref() {
        Some(IDLE_IN_TRANSACTION_SESSION_TIMEOUT) => {
            tracing::warn!(
                sqlstate = %IDLE_IN_TRANSACTION_SESSION_TIMEOUT,
                %error,
                "closed a pooled read connection the server ended because a transaction was \
                 left open on it"
            );
            Ok(false)
        }
        // admin_shutdown and crash_shutdown.
        Some("57P01" | "57P02") => {
            tracing::info!(%error, "closed a pooled read connection the server ended");
            Ok(false)
        }
        None if matches!(error, sqlx::Error::Io(_)) => {
            tracing::info!(%error, "closed a pooled read connection the server ended");
            Ok(false)
        }
        _ => Err(error),
    }
}

/// How long a dropped read waits for its cancel: for the cancel connection and the
/// `pg_cancel_backend` reply together.
pub(crate) const CANCEL_WAIT: Duration = Duration::from_secs(1);

/// What is logged when a dropped read's statement could not be cancelled.
pub(crate) const CANCEL_FAILED: &str = "could not cancel the statement of a dropped read; it runs \
     until it ends or reaches statement_timeout, and holds its connection until then";

/// The cancel pool of each request pool that has one. A request pool is matched by the address of
/// its options, which live as long as the pool. The entry keeps the request pool alive, so the
/// address cannot pass to another pool while the entry exists.
static CANCEL_POOLS: Mutex<Vec<(PgPool, PgPool)>> = Mutex::new(Vec::new());

/// Makes every read snapshot begun on `requests` cancel its statement when it is dropped before
/// it ends (`crate::ReadSnapshot`). The cancels go through a pool of one connection with the
/// request pool's connect options and `application_name`, opened on first use.
pub fn cancel_reads_on_drop(requests: &PgPool, application_name: &str) {
    let options = requests
        .connect_options()
        .as_ref()
        .clone()
        .application_name(application_name);
    let cancels = PgPoolOptions::new()
        .max_connections(1)
        .min_connections(0)
        .connect_lazy_with(options);
    register_cancel_pool(requests, cancels);
}

pub(crate) fn register_cancel_pool(requests: &PgPool, cancels: PgPool) {
    let mut pools = CANCEL_POOLS.lock().unwrap_or_else(PoisonError::into_inner);
    pools.retain(|(pool, _)| !pool.is_closed() && !same_pool(pool, requests));
    pools.push((requests.clone(), cancels));
}

pub(crate) fn cancel_pool(requests: &PgPool) -> Option<PgPool> {
    CANCEL_POOLS
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .iter()
        .find(|(pool, _)| same_pool(pool, requests))
        .map(|(_, cancels)| cancels.clone())
}

fn same_pool(left: &PgPool, right: &PgPool) -> bool {
    std::ptr::eq(left.options(), right.options())
}

/// Asks the server to cancel the statement backend `pid` is running, if any. The caller still
/// holds that backend's connection, so the pid cannot belong to another request.
pub(crate) async fn cancel_statement(cancels: &PgPool, pid: i32) {
    let sent = tokio::time::timeout(CANCEL_WAIT, async {
        let mut conn = cancels.acquire().await?;
        sqlx::query_scalar::<_, bool>("SELECT pg_cancel_backend($1)")
            .bind(pid)
            .fetch_one(&mut *conn)
            .await
    })
    .await;
    match sent {
        Ok(Ok(true)) => {}
        Ok(Ok(false)) => tracing::warn!(pid, reason = "no such backend", "{CANCEL_FAILED}"),
        Ok(Err(error)) => tracing::warn!(pid, %error, "{CANCEL_FAILED}"),
        Err(_) => tracing::warn!(
            pid,
            reason = "timed out",
            wait_ms = CANCEL_WAIT.as_millis() as u64,
            "{CANCEL_FAILED}"
        ),
    }
}

#[cfg(test)]
mod cancel_tests;
#[cfg(test)]
mod tests;
