//! When the runner refreshes the planner statistics of the Project tables the permission reads
//! join, and the refresh.
//!
//! Project analyzes its family tables during a rebuild and leaves them to autovacuum afterwards.
//! So statistics older than the process, after a restore or a restart, stay until autovacuum's
//! threshold is crossed, and a table PostgreSQL has never analyzed is planned from a default
//! guess. The phase runner therefore runs `ANALYZE` itself between Project batches: after the
//! first batch of the process that writes a table, and again each time the writes since the last
//! refresh reach the table's row count. A change much smaller than the table waits for autovacuum.
//!
//! Statistics belong to a table, not to a chain, so the schedule is per table and every chain's
//! writes count towards it.
use std::{
    collections::BTreeMap,
    sync::{Mutex, MutexGuard, PoisonError},
    time::{Duration, Instant},
};

use sqlx::PgPool;
use tokio::sync::Semaphore;

use crate::phase::{AfterProgress, RunMode};

/// The family tables the permission reads join. `project_family_marker` is left out: it holds one
/// row per chain, and every block rewrites that row, so autovacuum already reaches it.
const TABLES: [&str; 7] = [
    "project_grant",
    "project_account_approval",
    "project_ens_v2_entry_owner",
    "project_ens_v2_registry_parent",
    "project_registry_binding_observation",
    "project_binding_candidate",
    "project_lifecycle_key_state",
];

/// The fewest writes between two refreshes that follow a first one.
const REFRESH_FLOOR: u64 = 1_000;
/// A failed refresh is not tried again sooner than this.
const RETRY_AFTER: Duration = Duration::from_secs(60);
/// How many of these `ANALYZE` statements the process runs at once. The next call on any chain
/// takes whatever is due, so one is enough, and a table is never analyzed twice at once.
const CONCURRENT_REFRESHES: usize = 1;

/// `ANALYZE` waits behind another `ANALYZE`, a `VACUUM` or an index build on the table. The lock
/// wait bounds that. The statement limit bounds the lock wait and the sample together.
#[derive(Clone, Copy)]
pub(super) struct Timeouts {
    lock: Duration,
    statement: Duration,
}

pub(super) const TIMEOUTS: Timeouts = Timeouts {
    lock: Duration::from_secs(10),
    statement: Duration::from_secs(120),
};

#[derive(Default)]
struct Table {
    /// Writes since the last refresh, or since the process started.
    written: u64,
    /// The writes that make the next refresh due. `None` until this process has refreshed the
    /// table: until then its statistics are either older than the process or missing, and the
    /// process cannot tell how far they are from the rows.
    refresh_at: Option<u64>,
    retry_from: Option<Instant>,
}

impl Table {
    fn due(&self, now: Instant) -> bool {
        if self.retry_from.is_some_and(|retry_from| now < retry_from) {
            return false;
        }
        self.written > 0 && self.written >= self.refresh_at.unwrap_or(0)
    }
}

/// Per table and in memory: a restarted process refreshes a table again after its first write.
pub(super) struct Schedule {
    tables: Mutex<BTreeMap<&'static str, Table>>,
    refreshes: Semaphore,
}

impl Default for Schedule {
    fn default() -> Self {
        Self {
            tables: Mutex::default(),
            refreshes: Semaphore::new(CONCURRENT_REFRESHES),
        }
    }
}

impl Schedule {
    fn tables(&self) -> MutexGuard<'_, BTreeMap<&'static str, Table>> {
        self.tables.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Called with each batch's committed rows. A redo's writes are not counted: Project analyzes
    /// a rebuild itself, and a stop that finds a refresh waiting after a redo's last batch would
    /// report the finished redo as incomplete.
    pub(super) fn record(&self, mode: &RunMode, rows: &BTreeMap<&'static str, u64>) {
        if !matches!(mode, RunMode::Normal) {
            return;
        }
        let mut tables = self.tables();
        for table in TABLES {
            if let Some(&written) = rows.get(table).filter(|written| **written > 0) {
                let table = tables.entry(table).or_default();
                table.written = table.written.saturating_add(written);
            }
        }
    }

    /// The first table due and its count of writes, for `settle` once it has run.
    fn next_due(&self, now: Instant) -> Option<(&'static str, u64)> {
        let tables = self.tables();
        TABLES.into_iter().find_map(|name| {
            tables
                .get(name)
                .filter(|table| table.due(now))
                .map(|table| (name, table.written))
        })
    }

    /// Whether a call of `refresh` now would run `ANALYZE`.
    pub(super) fn due(&self, now: Instant) -> bool {
        self.refreshes.available_permits() > 0 && self.next_due(now).is_some()
    }

    /// `rows` is the table's row count after a refresh, `None` when it failed. Writes committed
    /// while the refresh ran stay counted. A failed refresh stays due and waits `RETRY_AFTER`.
    fn settle(&self, name: &'static str, written: u64, rows: Option<u64>, now: Instant) {
        let mut tables = self.tables();
        let table = tables.entry(name).or_default();
        match rows {
            Some(rows) => {
                table.written = table.written.saturating_sub(written);
                table.refresh_at = Some(rows.max(REFRESH_FLOOR));
                table.retry_from = None;
            }
            None => table.retry_from = Some(now + RETRY_AFTER),
        }
    }

    /// Refreshes one due table and says whether another is due. A failure only costs plan quality,
    /// so it is logged and left for a later call. While another chain's refresh runs this returns
    /// at once and the chain goes on to its next batch: no chain's publication waits for another's
    /// `ANALYZE`. A stop that drops this call leaves the table due and releases the permit.
    pub(super) async fn refresh(
        &self,
        pool: &PgPool,
        timeouts: Timeouts,
        now: Instant,
    ) -> AfterProgress {
        let Ok(_permit) = self.refreshes.try_acquire() else {
            return AfterProgress::Done;
        };
        let Some((table, written)) = self.next_due(now) else {
            return AfterProgress::Done;
        };
        let started = Instant::now();
        let result = analyze(pool, table, timeouts).await;
        let completed = Instant::now();
        match result {
            Ok(rows) => {
                tracing::info!(
                    table,
                    writes_since_last_refresh = written,
                    rows,
                    duration_ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX),
                    "project refreshed table statistics"
                );
                self.settle(table, written, Some(rows), completed);
            }
            Err(error) => {
                tracing::warn!(table, %error, "project could not refresh table statistics");
                self.settle(table, written, None, completed);
            }
        }
        if self.next_due(completed).is_some() {
            AfterProgress::More
        } else {
            AfterProgress::Done
        }
    }
}

/// In a transaction of its own, so the limits end with it and the lock is held no longer than the
/// statement. Returns the row count the refresh recorded.
async fn analyze(pool: &PgPool, table: &str, timeouts: Timeouts) -> sqlx::Result<u64> {
    let mut transaction = pool.begin().await?;
    sqlx::query(
        "SELECT set_config('lock_timeout', $1, true), set_config('statement_timeout', $2, true)",
    )
    .bind(format!("{}ms", timeouts.lock.as_millis()))
    .bind(format!("{}ms", timeouts.statement.as_millis()))
    .execute(&mut *transaction)
    .await?;
    sqlx::query(&format!(
        "/* phase-runner:project.statistics.refresh */ ANALYZE {table}"
    ))
    .execute(&mut *transaction)
    .await?;
    let rows: i64 =
        sqlx::query_scalar("SELECT reltuples::bigint FROM pg_class WHERE oid = to_regclass($1)")
            .bind(table)
            .fetch_one(&mut *transaction)
            .await?;
    transaction.commit().await?;
    Ok(u64::try_from(rows).unwrap_or(0))
}

#[cfg(test)]
#[path = "statistics_tests.rs"]
mod tests;
