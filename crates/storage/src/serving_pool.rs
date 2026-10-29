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

use std::{borrow::Cow, time::Duration};

use futures_util::future::BoxFuture;
use sqlx::{Executor, PgConnection, Row, postgres::PgPoolOptions};

/// True when an earlier statement opened the transaction the session is in. Sent as a simple
/// query, which is one protocol message: PostgreSQL gives the first statement of a transaction the
/// transaction's start time as its own, so the two times differ only inside a transaction block.
const IN_TRANSACTION_BLOCK: &str = "SELECT statement_timestamp() <> transaction_timestamp()";

/// What is logged when a transaction left open is rolled back, by the acquire check or the pin.
pub(crate) const ROLLED_BACK_LEFT_OPEN: &str =
    "rolled back a transaction left open on a pooled read connection";

/// How long past the request deadline a session may sit idle inside a transaction.
const IDLE_IN_TRANSACTION_MARGIN_MS: u64 = 5_000;
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

#[cfg(test)]
mod tests;
