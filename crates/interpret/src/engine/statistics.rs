//! When Interpret refreshes the planner statistics of `normalized_events`, and the refresh.
//!
//! A redo deletes a chain's events and writes them again. When PostgreSQL analyzes the table
//! in between, while no ENSv2 event is live, it records the ENSv2 partial indexes as holding
//! no rows. The planner then reads the ENSv2 state keys of a batch by scanning one of those
//! indexes once per key, until the next `ANALYZE`. Interpret therefore runs `ANALYZE` itself
//! as ENSv2 events arrive.
use std::{
    collections::HashMap,
    sync::{Mutex, PoisonError},
    time::{Duration, Instant},
};

use sqlx::PgPool;

use super::{Engine, RunMode};

/// The fewest ENSv2 events between two refreshes that follow a first one.
const REFRESH_FLOOR: u64 = 1_000;
/// A failed refresh is not tried again sooner than this.
const RETRY_AFTER: Duration = Duration::from_secs(60);

/// `ANALYZE` waits behind another `ANALYZE`, a `VACUUM` or an index build on the table. The
/// lock wait bounds that; the statement limit bounds the lock wait and the sample together.
#[derive(Clone, Copy)]
pub(super) struct Timeouts {
    pub(super) lock: Duration,
    pub(super) statement: Duration,
}

pub(super) const TIMEOUTS: Timeouts = Timeouts {
    lock: Duration::from_secs(10),
    statement: Duration::from_secs(120),
};

#[derive(Default)]
struct Chain {
    /// ENSv2 events this process has written for the chain.
    written: u64,
    /// `written` at the last refresh. `None` until a refresh has run with an ENSv2 event
    /// written: an earlier one saw the ENSv2 indexes empty and does not count as the first.
    refreshed_at: Option<u64>,
    /// A redo has completed since the last refresh.
    redo_completed: bool,
    mode: Option<RunMode>,
    retry_from: Option<Instant>,
}

impl Chain {
    fn since_refresh(&self) -> u64 {
        self.written - self.refreshed_at.unwrap_or(0)
    }

    fn due(&self, now: Instant) -> bool {
        if self.retry_from.is_some_and(|retry_from| now < retry_from) {
            return false;
        }
        self.redo_completed
            || match self.refreshed_at {
                None => self.written > 0,
                Some(refreshed_at) => self.since_refresh() >= refreshed_at.max(REFRESH_FLOOR),
            }
    }
}

struct Due {
    written: u64,
    since_refresh: u64,
    mode: &'static str,
}

/// Per chain and in memory: a restarted process refreshes again after its first batch that
/// writes an ENSv2 event.
#[derive(Default)]
pub(super) struct Schedule(Mutex<HashMap<String, Chain>>);

impl Schedule {
    fn chains(&self) -> std::sync::MutexGuard<'_, HashMap<String, Chain>> {
        self.0.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Called once per committed batch. A redo's first batch deletes the redo range, and a
    /// refresh right after it would record the ENSv2 indexes as empty, so a batch that wrote
    /// no ENSv2 event never makes a refresh due unless it completes a redo.
    pub(super) fn record(&self, chain_id: &str, mode: RunMode, complete: bool, events: u64) {
        let mut chains = self.chains();
        let chain = chains.entry(chain_id.to_owned()).or_default();
        chain.written += events;
        chain.redo_completed |= complete && mode == RunMode::Redo;
        chain.mode = Some(mode);
    }

    pub(super) fn due(&self, chain_id: &str, now: Instant) -> bool {
        self.take_due(chain_id, now).is_some()
    }

    /// The chain's counters when a refresh is due, for `settle` once it has run.
    fn take_due(&self, chain_id: &str, now: Instant) -> Option<Due> {
        self.chains()
            .get(chain_id)
            .filter(|chain| chain.due(now))
            .map(|chain| Due {
                written: chain.written,
                since_refresh: chain.since_refresh(),
                mode: match chain.mode {
                    Some(RunMode::Redo) => "redo",
                    _ => "normal",
                },
            })
    }

    /// A failed refresh stays due and waits `RETRY_AFTER`.
    fn settle(&self, chain_id: &str, due: &Due, refreshed: bool, now: Instant) {
        let mut chains = self.chains();
        let chain = chains.entry(chain_id.to_owned()).or_default();
        if refreshed {
            chain.refreshed_at = Some(due.written).filter(|written| *written > 0);
            chain.redo_completed = false;
            chain.retry_from = None;
        } else {
            chain.retry_from = Some(now + RETRY_AFTER);
        }
    }

    /// Refreshes when due and returns whether the statistics were refreshed. A failure only
    /// costs plan quality, so it is logged and left for a later call.
    pub(super) async fn refresh(
        &self,
        pool: &PgPool,
        chain_id: &str,
        timeouts: Timeouts,
        now: Instant,
    ) -> bool {
        let Some(due) = self.take_due(chain_id, now) else {
            return false;
        };
        let started = Instant::now();
        let result = analyze(pool, timeouts).await;
        let completed = Instant::now();
        let refreshed = result.is_ok();
        self.settle(chain_id, &due, refreshed, completed);
        match result {
            Ok(()) => tracing::info!(
                chain_id,
                mode = due.mode,
                ensv2_events_since_last_refresh = due.since_refresh,
                duration_ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX),
                "interpret refreshed normalized_events statistics"
            ),
            Err(error) => tracing::warn!(
                chain_id,
                mode = due.mode,
                %error,
                "interpret could not refresh normalized_events statistics"
            ),
        }
        refreshed
    }
}

/// How many of a batch's events, given by source family, are ENSv2 events.
pub(super) fn ensv2_events<'a>(source_families: impl IntoIterator<Item = &'a str>) -> u64 {
    let count = source_families
        .into_iter()
        .filter(|family| family.starts_with("ens_v2_"))
        .count();
    u64::try_from(count).unwrap_or(u64::MAX)
}

/// In a transaction of its own, so the limits end with it and the lock is held no longer
/// than the statement.
async fn analyze(pool: &PgPool, timeouts: Timeouts) -> sqlx::Result<()> {
    let mut transaction = pool.begin().await?;
    sqlx::query(
        "SELECT set_config('lock_timeout', $1, true), set_config('statement_timeout', $2, true)",
    )
    .bind(format!("{}ms", timeouts.lock.as_millis()))
    .bind(format!("{}ms", timeouts.statement.as_millis()))
    .execute(&mut *transaction)
    .await?;
    sqlx::query("/* interpret:statistics.refresh */ ANALYZE normalized_events")
        .execute(&mut *transaction)
        .await?;
    transaction.commit().await
}

impl Engine {
    /// Whether `refresh_statistics` would run `ANALYZE` for the chain now.
    pub fn statistics_refresh_due(&self, chain_id: &str) -> bool {
        self.statistics.due(chain_id, Instant::now())
    }

    /// Refreshes the planner statistics of `normalized_events` when the chain's schedule says
    /// so, outside any batch transaction. Returns whether it did; it never fails the caller.
    pub async fn refresh_statistics(&self, chain_id: &str) -> bool {
        self.statistics
            .refresh(&self.pool, chain_id, TIMEOUTS, Instant::now())
            .await
    }
}

#[cfg(test)]
#[path = "statistics_tests.rs"]
mod tests;
