//! A transaction on a pool connection held here, so that a read snapshot can close that
//! connection instead of returning it to the pool (`super::ReadSnapshot`). sqlx's own
//! `Transaction` gives no access to its pool connection.
//!
//! This mirrors `sqlx::Transaction` as read in sqlx-core 0.8.6, `src/transaction.rs`: its
//! `begin`, `commit`, `rollback` and `Drop`, and its `open` flag. It calls these sqlx internals,
//! all `#[doc(hidden)]`:
//!
//! - `sqlx::TransactionManager::begin`, `commit`, `rollback` and `start_rollback`;
//! - through `<Postgres as sqlx::Database>::TransactionManager`, which is
//!   `sqlx::postgres::PgTransactionManager` (sqlx-postgres 0.8.6, `src/transaction.rs`). It keeps
//!   the connection's transaction depth, so a nested begin on the connection opens a savepoint.
//!
//! A test below fails once Cargo.lock moves sqlx-core off 0.8.6.
use std::ops::{Deref, DerefMut};

use sqlx::{Database, PgConnection, Postgres, TransactionManager, pool::PoolConnection};

type Manager = <Postgres as Database>::TransactionManager;

pub(super) struct OwnedTransaction {
    conn: PoolConnection<Postgres>,
    /// As `Transaction`'s: set once `BEGIN` succeeds, cleared once `COMMIT` or `ROLLBACK` does.
    open: bool,
}

impl OwnedTransaction {
    pub(super) async fn begin(mut conn: PoolConnection<Postgres>) -> Result<Self, sqlx::Error> {
        Manager::begin(&mut *conn, None).await?;
        Ok(Self { conn, open: true })
    }

    pub(super) async fn commit(mut self) -> Result<(), sqlx::Error> {
        Manager::commit(&mut *self.conn).await?;
        self.open = false;
        Ok(())
    }

    pub(super) async fn rollback(mut self) -> Result<(), sqlx::Error> {
        Manager::rollback(&mut *self.conn).await?;
        self.open = false;
        Ok(())
    }

    /// Closes the connection when this is dropped, instead of returning it to the pool.
    pub(super) fn close_on_drop(&mut self) {
        self.conn.close_on_drop();
    }
}

impl Drop for OwnedTransaction {
    /// As `Transaction`'s: queue a `ROLLBACK`, which goes out with the connection's next message.
    fn drop(&mut self) {
        if self.open {
            Manager::start_rollback(&mut *self.conn);
        }
    }
}

impl Deref for OwnedTransaction {
    type Target = PgConnection;

    fn deref(&self) -> &PgConnection {
        &self.conn
    }
}

impl DerefMut for OwnedTransaction {
    fn deref_mut(&mut self) -> &mut PgConnection {
        &mut self.conn
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn sqlx_core_is_the_version_this_mirror_was_read_against() {
        let lock = include_str!("../../../../Cargo.lock");
        assert!(
            lock.contains("name = \"sqlx-core\"\nversion = \"0.8.6\"\n"),
            "Cargo.lock no longer locks sqlx-core 0.8.6. Re-read OwnedTransaction against \
             sqlx's Transaction in the new version, and only then update the version pinned here."
        );
    }
}
