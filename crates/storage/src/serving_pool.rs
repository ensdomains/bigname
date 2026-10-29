//! The options of the pool the API serves reads from.
//!
//! sqlx counts the transactions it began on a connection, and raises that count only when the
//! server's reply to `BEGIN` arrives. A request whose future is dropped while that reply is in
//! flight, as when a client disconnects, leaves the server inside a transaction that sqlx does not
//! count. Its rollback guard does nothing at a count of zero, so the pool takes the connection back
//! as clean. The next reader's `BEGIN` then only warns, and once any statement has run in the
//! leftover transaction, that reader's `SET TRANSACTION ISOLATION LEVEL` fails with SQLSTATE 25001.

use sqlx::{PgConnection, Row, postgres::PgPoolOptions};

/// True when an earlier statement opened the transaction the session is in. Sent as a simple
/// query, which is one protocol message: PostgreSQL gives the first statement of a transaction the
/// transaction's start time as its own, so the two times differ only inside a transaction block.
const IN_TRANSACTION_BLOCK: &str = "SELECT statement_timestamp() <> transaction_timestamp()";

pub(crate) fn serving_pool_options(max_connections: u32) -> PgPoolOptions {
    PgPoolOptions::new()
        .max_connections(max_connections)
        // The check is one round trip that a dead connection fails as well, so it takes the place
        // of the empty ping sqlx would otherwise send first. A failed check closes the connection
        // and the acquire opens a new one.
        .test_before_acquire(false)
        .before_acquire(|conn, _| Box::pin(end_transaction_left_open(conn)))
}

/// Ends a transaction an earlier request left open, before the connection is handed out. sqlx
/// exposes the server's transaction status only through its own count, which is what the leak
/// leaves wrong, so the server is asked.
async fn end_transaction_left_open(conn: &mut PgConnection) -> Result<bool, sqlx::Error> {
    let left_open: bool = sqlx::raw_sql(IN_TRANSACTION_BLOCK)
        .fetch_one(&mut *conn)
        .await?
        .try_get(0)?;
    if left_open {
        tracing::warn!("rolled back a transaction left open on a pooled read connection");
        sqlx::raw_sql("ROLLBACK").execute(&mut *conn).await?;
    }
    Ok(true)
}

#[cfg(test)]
mod tests;
