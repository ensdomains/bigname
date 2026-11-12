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
//! (`crate::ReadSnapshot`). The cancel waits at most `CANCEL_WAIT` for that connection. Past
//! that, or on an error, nothing was sent: it logs a warning and the statement runs on as it would
//! without the cancel. Once sent, a cancel can reach the server however late, so the read keeps its
//! connection until the reply. The cancel connection's own `statement_timeout`,
//! `CANCEL_STATEMENT_TIMEOUT`, bounds that on the server. A cancel still unanswered after
//! `CANCEL_REPLY_BACKSTOP` closes the read's connection, and the cancel connection, rather than
//! return them. The read's backend then exits, so the late cancel reaches no later request,
//! unless the OS has handed that pid to a new backend of the same role meanwhile. PostgreSQL
//! ignores a cancel that reaches a backend between statements. The cancel needs no privilege
//! beyond the role the request pool connects as, and it works on a hot standby. See the
//! PostgreSQL documentation, "Server Signaling Functions" and "Hot Standby", "Administrator's
//! Overview".

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

/// How long a dropped read waits for the cancel connection.
pub(crate) const CANCEL_WAIT: Duration = Duration::from_secs(1);

/// The `statement_timeout` of the cancel connection, which ends a cancel the server is slow to
/// run. A dropped read holds its connection no longer than this once its cancel is sent, which
/// is less than the request pool's own statement timeout it would be held for without a cancel.
pub(crate) const CANCEL_STATEMENT_TIMEOUT: Duration = Duration::from_secs(5);

/// How long a sent cancel waits for its reply when the server never answers, as on a dead
/// socket. The read's connection and the cancel connection are then closed.
pub(crate) const CANCEL_REPLY_BACKSTOP: Duration = Duration::from_secs(20);

/// What is logged when a dropped read's statement could not be cancelled.
pub(crate) const CANCEL_FAILED: &str = "could not cancel the statement of a dropped read; it runs \
     until it ends or reaches statement_timeout, and holds its connection until then";

/// The cancel pool of each request pool that has one. A request pool is matched by the address of
/// its options, which live as long as the pool. The entry keeps the request pool alive, so the
/// address cannot pass to another pool while the entry exists.
static CANCEL_POOLS: Mutex<Vec<(PgPool, PgPool)>> = Mutex::new(Vec::new());

/// Makes every read snapshot begun on `requests` cancel its statement when it is dropped before
/// it ends (`crate::ReadSnapshot`). The cancels go through a pool of one connection with the
/// request pool's connect options, `application_name` and [`CANCEL_STATEMENT_TIMEOUT`], opened
/// on first use.
pub fn cancel_reads_on_drop(requests: &PgPool, application_name: &str) {
    // The later `statement_timeout` in the startup options is the one the session takes.
    let options = requests
        .connect_options()
        .as_ref()
        .clone()
        .application_name(application_name)
        .options([(
            "statement_timeout",
            format!("{}ms", CANCEL_STATEMENT_TIMEOUT.as_millis()),
        )]);
    // A dead cancel connection fails the cancel itself, so no ping goes ahead of it.
    let cancels = PgPoolOptions::new()
        .max_connections(1)
        .min_connections(0)
        .test_before_acquire(false)
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

/// Whether the server is done with a dropped read's cancel.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum CancelOutcome {
    /// Nothing was sent, or the server answered it, with a result or an error. The read's
    /// connection may go back to the pool.
    Settled,
    /// The cancel was sent and no answer came: the server may still run it, against whatever
    /// that backend serves by then. The read's connection must be closed, not returned.
    Unsettled,
}

/// Asks the server to cancel the statement backend `pid` is running, if any. The caller still
/// holds that backend's connection, so the pid cannot belong to another request.
///
/// Only the wait for the cancel connection is bounded by `CANCEL_WAIT`. A sent cancel waits for
/// its answer up to `CANCEL_REPLY_BACKSTOP`, and the caller keeps the connection meanwhile. Past
/// the backstop, or on a transport or protocol error after the send, the outcome is
/// [`CancelOutcome::Unsettled`]. The cancel connection, with its answer still due, is closed then
/// as well: sqlx's return to the pool would wait for that answer with no limit.
pub(crate) async fn cancel_statement(cancels: &PgPool, pid: i32) -> CancelOutcome {
    let mut conn = match tokio::time::timeout(CANCEL_WAIT, cancels.acquire()).await {
        Ok(Ok(conn)) => conn,
        Ok(Err(error)) => {
            tracing::warn!(pid, %error, "{CANCEL_FAILED}");
            return CancelOutcome::Settled;
        }
        Err(_) => {
            tracing::warn!(
                pid,
                reason = "timed out waiting for the cancel connection",
                "{CANCEL_FAILED}"
            );
            return CancelOutcome::Settled;
        }
    };
    let replied = tokio::time::timeout(
        CANCEL_REPLY_BACKSTOP,
        sqlx::query_scalar::<_, bool>("SELECT pg_cancel_backend($1)")
            .bind(pid)
            .fetch_one(&mut *conn),
    )
    .await;
    match replied {
        Ok(Ok(true)) => CancelOutcome::Settled,
        Ok(Ok(false)) => {
            tracing::warn!(pid, reason = "no such backend", "{CANCEL_FAILED}");
            CancelOutcome::Settled
        }
        Ok(Err(error @ sqlx::Error::Database(_))) => {
            tracing::warn!(pid, %error, "{CANCEL_FAILED}");
            CancelOutcome::Settled
        }
        Ok(Err(error)) => {
            conn.close_on_drop();
            tracing::warn!(pid, %error, closed = true, "{CANCEL_FAILED}");
            CancelOutcome::Unsettled
        }
        Err(_) => {
            conn.close_on_drop();
            tracing::warn!(
                pid,
                reason = "no reply from the server",
                closed = true,
                "{CANCEL_FAILED}"
            );
            CancelOutcome::Unsettled
        }
    }
}

#[cfg(test)]
mod cancel_tests;
#[cfg(test)]
mod tests;
