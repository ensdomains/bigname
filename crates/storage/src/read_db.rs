//! The database a serving reader runs on.
use std::ops::{Deref, DerefMut};

use anyhow::{Context, Result};
use sqlx::{Database, PgConnection, PgPool, Postgres, TransactionManager, pool::PoolConnection};

use crate::serving_pool::CancelOutcome;

/// A pool, where each reader takes its own connection and, for a multi-statement read, its own
/// REPEATABLE READ snapshot; or a caller's connection already inside one read-only REPEATABLE
/// READ transaction, which every reader shares, so a response built from several reads sees one
/// committed state.
pub enum ReadDb<'a> {
    Pool(&'a PgPool),
    Snapshot(&'a mut PgConnection),
}

impl<'a> From<&'a PgPool> for ReadDb<'a> {
    fn from(pool: &'a PgPool) -> Self {
        Self::Pool(pool)
    }
}

impl<'a> From<&'a mut PgConnection> for ReadDb<'a> {
    fn from(conn: &'a mut PgConnection) -> Self {
        Self::Snapshot(conn)
    }
}

impl<'a> ReadDb<'a> {
    pub fn reborrow(&mut self) -> ReadDb<'_> {
        match self {
            Self::Pool(pool) => ReadDb::Pool(pool),
            Self::Snapshot(conn) => ReadDb::Snapshot(conn),
        }
    }

    /// A connection for reads that need no snapshot of their own.
    pub(crate) async fn acquire(self) -> Result<ReadConn<'a>> {
        match self {
            Self::Pool(pool) => Ok(ReadConn::Pooled(
                pool.acquire()
                    .await
                    .context("failed to acquire a read connection")?,
            )),
            Self::Snapshot(conn) => Ok(ReadConn::Shared(conn)),
        }
    }

    /// A connection on which every statement sees one snapshot: a new read-only REPEATABLE READ
    /// transaction from a pool, the caller's own otherwise. [`ReadConn::close`] ends one it began.
    pub(crate) async fn snapshot(self) -> Result<ReadConn<'a>> {
        match self {
            Self::Pool(pool) => Ok(ReadConn::Owned(begin_read_snapshot(pool).await?)),
            Self::Snapshot(conn) => Ok(ReadConn::Shared(conn)),
        }
    }
}

/// Begins the shared snapshot a [`ReadDb::Snapshot`] caller holds: a read-only REPEATABLE READ
/// transaction, whose snapshot is taken at its first statement.
pub async fn begin_read_snapshot(pool: &PgPool) -> Result<ReadSnapshot> {
    crate::families::name::seams::before_snapshot().await;
    open_read_snapshot(pool).await
}

/// [`begin_read_snapshot`] without its test seam.
pub(crate) async fn open_read_snapshot(pool: &PgPool) -> Result<ReadSnapshot> {
    let cancels = crate::serving_pool::cancel_pool(pool);
    let mut snapshot = begin(pool, cancels.as_ref()).await?;
    let pinned = sqlx::query(PIN).execute(&mut *snapshot).await;
    if let Err(error) = pinned {
        if !ran_after_a_statement(&error) {
            return Err(error).context(PIN_FAILED);
        }
        // The connection came with a transaction an earlier request left open, and a statement
        // had run in it (`crate::serving_pool`). The rollback ends that transaction. The read
        // starts again once. The acquire check should have ended it first, so this warns.
        snapshot
            .rollback()
            .await
            .context("failed to end a transaction left open on a pooled connection")?;
        tracing::warn!("{}", crate::serving_pool::ROLLED_BACK_LEFT_OPEN);
        snapshot = begin(pool, cancels.as_ref()).await?;
        sqlx::query(PIN)
            .execute(&mut *snapshot)
            .await
            .context(PIN_FAILED)?;
    }
    Ok(snapshot)
}

const PIN: &str = "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY";
const PIN_FAILED: &str = "failed to pin the read to one snapshot";
const BEGIN_FAILED: &str = "failed to begin a read snapshot";

/// The backend's pid is read before `BEGIN`, so the read's snapshot is still taken at its own
/// first statement.
async fn begin(pool: &PgPool, cancels: Option<&PgPool>) -> Result<ReadSnapshot> {
    let mut conn = pool.acquire().await.context(BEGIN_FAILED)?;
    let cancel = match cancels {
        Some(cancels) => {
            let pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
                .fetch_one(&mut *conn)
                .await
                .context(BEGIN_FAILED)?;
            Some((cancels.clone(), pid))
        }
        None => None,
    };
    Transactions::begin(&mut *conn, None)
        .await
        .context(BEGIN_FAILED)?;
    Ok(ReadSnapshot {
        conn: Some(conn),
        open: true,
        cancel,
    })
}

/// sqlx's own transaction control, which its `Transaction` wraps (sqlx 0.8.6,
/// `sqlx-core/src/transaction.rs`). A read snapshot drives it on a connection it holds itself,
/// so that a drop can close that connection rather than return it to the pool: sqlx's
/// `Transaction` gives no access to its pool connection. It mirrors sqlx-core 0.8.6's
/// `Transaction`, and a sqlx upgrade must re-check it (`cancel_tests.rs` pins the version).
type Transactions = <Postgres as Database>::TransactionManager;

/// A read-only REPEATABLE READ transaction one request reads on, used as its connection.
///
/// A request whose future is dropped, because its client went away or its deadline passed,
/// drops its snapshot with any statement still running on the server. The database driver then
/// returns the connection only after that statement ends, so the statement and the connection
/// would stay taken until `statement_timeout`. On a pool that cancels dropped reads
/// (`crate::cancel_reads_on_drop`), a snapshot dropped before [`Self::commit`] or
/// [`Self::rollback`] first asks the server to cancel whatever its backend runs, then lets the
/// connection go. The cancel is sent while this snapshot still holds the connection, which goes
/// back to the pool only once the server has answered it. When no answer comes, the server may
/// still run the cancel, so the connection is closed instead. Either way the cancel can only reach
/// this read's own statement, never one of a later request.
pub struct ReadSnapshot {
    conn: Option<PoolConnection<Postgres>>,
    /// Whether the transaction has yet to end.
    open: bool,
    /// The cancel pool and the backend pid, while a drop must cancel.
    cancel: Option<(PgPool, i32)>,
}

impl ReadSnapshot {
    pub async fn commit(mut self) -> Result<(), sqlx::Error> {
        self.cancel = None;
        Transactions::commit(self.conn.as_deref_mut().expect(OPEN)).await?;
        self.open = false;
        Ok(())
    }

    pub async fn rollback(mut self) -> Result<(), sqlx::Error> {
        self.cancel = None;
        Transactions::rollback(self.conn.as_deref_mut().expect(OPEN)).await?;
        self.open = false;
        Ok(())
    }
}

const OPEN: &str = "a read snapshot holds its connection until it is dropped";

impl Drop for ReadSnapshot {
    fn drop(&mut self) {
        let Some(mut conn) = self.conn.take() else {
            return;
        };
        if self.open {
            // As sqlx's `Transaction` does: queue a `ROLLBACK`, which goes out with the
            // connection's next message.
            Transactions::start_rollback(&mut *conn);
        }
        let Some((cancels, pid)) = self.cancel.take() else {
            return;
        };
        let Ok(runtime) = tokio::runtime::Handle::try_current() else {
            return;
        };
        runtime.spawn(async move {
            if crate::serving_pool::cancel_statement(&cancels, pid).await
                == CancelOutcome::Unsettled
            {
                conn.close_on_drop();
            }
            // Only now may the connection go back to the pool.
            drop(conn);
        });
    }
}

impl Deref for ReadSnapshot {
    type Target = PgConnection;

    fn deref(&self) -> &PgConnection {
        self.conn.as_deref().expect(OPEN)
    }
}

impl DerefMut for ReadSnapshot {
    fn deref_mut(&mut self) -> &mut PgConnection {
        self.conn.as_deref_mut().expect(OPEN)
    }
}

/// SQLSTATE 25001: `SET TRANSACTION ISOLATION LEVEL` after a statement of the same transaction.
fn ran_after_a_statement(error: &sqlx::Error) -> bool {
    error
        .as_database_error()
        .and_then(|error| error.code())
        .is_some_and(|code| code == "25001")
}

pub(crate) enum ReadConn<'a> {
    Pooled(PoolConnection<Postgres>),
    Owned(ReadSnapshot),
    Shared(&'a mut PgConnection),
}

impl ReadConn<'_> {
    /// Ends a snapshot this read began; a shared one stays open for its owner.
    pub(crate) async fn close(self) -> Result<()> {
        if let Self::Owned(snapshot) = self {
            snapshot.commit().await?;
        }
        Ok(())
    }
}

impl Deref for ReadConn<'_> {
    type Target = PgConnection;

    fn deref(&self) -> &PgConnection {
        match self {
            Self::Pooled(conn) => conn,
            Self::Owned(snapshot) => snapshot,
            Self::Shared(conn) => conn,
        }
    }
}

impl DerefMut for ReadConn<'_> {
    fn deref_mut(&mut self) -> &mut PgConnection {
        match self {
            Self::Pooled(conn) => conn,
            Self::Owned(snapshot) => snapshot,
            Self::Shared(conn) => conn,
        }
    }
}
