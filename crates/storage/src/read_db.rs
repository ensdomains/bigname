//! The database a serving reader runs on.
use std::ops::{Deref, DerefMut};

use anyhow::{Context, Result};
use sqlx::{PgConnection, PgPool, Postgres, Transaction, pool::PoolConnection};

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
            Self::Pool(pool) => Ok(ReadConn::Owned(crate::families::read_snapshot(pool).await?)),
            Self::Snapshot(conn) => Ok(ReadConn::Shared(conn)),
        }
    }
}

/// Begins the shared snapshot a [`ReadDb::Snapshot`] caller holds: a read-only REPEATABLE READ
/// transaction, whose snapshot is taken at its first statement.
pub async fn begin_read_snapshot(pool: &PgPool) -> Result<Transaction<'static, Postgres>> {
    crate::families::name::seams::before_snapshot().await;
    let mut transaction = begin(pool).await?;
    if let Err(error) = pin(&mut transaction).await {
        if !ran_after_a_statement(&error) {
            return Err(error).context(PIN_FAILED);
        }
        // The connection came with a transaction an earlier request left open, and a statement
        // had run in it (`crate::serving_pool`). The rollback ends that transaction. The read
        // starts again once.
        transaction
            .rollback()
            .await
            .context("failed to end a transaction left open on a pooled connection")?;
        transaction = begin(pool).await?;
        pin(&mut transaction).await.context(PIN_FAILED)?;
    }
    Ok(transaction)
}

const PIN_FAILED: &str = "failed to pin the read to one snapshot";

async fn begin(pool: &PgPool) -> Result<Transaction<'static, Postgres>> {
    pool.begin()
        .await
        .context("failed to begin a read snapshot")
}

async fn pin(transaction: &mut Transaction<'static, Postgres>) -> sqlx::Result<()> {
    sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
        .execute(&mut **transaction)
        .await?;
    Ok(())
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
    Owned(Transaction<'static, Postgres>),
    Shared(&'a mut PgConnection),
}

impl ReadConn<'_> {
    /// Ends a snapshot this read began; a shared one stays open for its owner.
    pub(crate) async fn close(self) -> Result<()> {
        if let Self::Owned(transaction) = self {
            transaction.commit().await?;
        }
        Ok(())
    }
}

impl Deref for ReadConn<'_> {
    type Target = PgConnection;

    fn deref(&self) -> &PgConnection {
        match self {
            Self::Pooled(conn) => conn,
            Self::Owned(transaction) => transaction,
            Self::Shared(conn) => conn,
        }
    }
}

impl DerefMut for ReadConn<'_> {
    fn deref_mut(&mut self) -> &mut PgConnection {
        match self {
            Self::Pooled(conn) => conn,
            Self::Owned(transaction) => transaction,
            Self::Shared(conn) => conn,
        }
    }
}
