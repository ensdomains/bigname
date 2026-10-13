use std::{
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use bigname_test_support::{TestDatabase, TestDatabaseConfig};
use sqlx::PgPool;

use super::{Schedule, TIMEOUTS, Timeouts, ensv2_events};
use crate::{Engine, RunMode};

type Result<T = ()> = anyhow::Result<T>;
const CHAIN: &str = "statistics-test";

impl Engine {
    /// Feeds the schedule as the completing batch of a redo does.
    pub(crate) fn record_completed_redo(&self, chain_id: &str) {
        self.statistics.record(chain_id, RunMode::Redo, true, 0);
    }
}

/// Records a successful refresh without a database.
fn refreshed(schedule: &Schedule, chain_id: &str, now: Instant) {
    let due = schedule
        .take_due(chain_id, now)
        .expect("a refresh must be due");
    schedule.settle(chain_id, &due, true, now);
}

#[test]
fn redo_clear_without_ensv2_writes_is_not_due() {
    let (schedule, now) = (Schedule::default(), Instant::now());
    schedule.record(CHAIN, RunMode::Redo, false, 0);
    assert!(!schedule.due(CHAIN, now));
}

#[test]
fn first_ensv2_write_is_due() {
    for mode in [RunMode::Redo, RunMode::Normal] {
        let (schedule, now) = (Schedule::default(), Instant::now());
        schedule.record(CHAIN, mode, false, 0);
        schedule.record(CHAIN, mode, false, 1);
        assert!(schedule.due(CHAIN, now), "{mode:?}");
    }
}

#[test]
fn fresh_schedule_is_due_on_its_first_ensv2_batch() {
    // A restarted process resumes a redo inside the chain's ENSv2 range with no counters.
    let (schedule, now) = (Schedule::default(), Instant::now());
    assert!(!schedule.due(CHAIN, now));
    schedule.record(CHAIN, RunMode::Redo, false, 40_000);
    assert!(schedule.due(CHAIN, now));
}

#[test]
fn first_ensv2_write_after_a_refresh_without_ensv2_is_due() {
    // The redo of a chain with no ENSv2 event completes and refreshes while the ENSv2
    // indexes are empty. That refresh must not stand in for the one after the first event.
    let (schedule, now) = (Schedule::default(), Instant::now());
    schedule.record(CHAIN, RunMode::Redo, true, 0);
    refreshed(&schedule, CHAIN, now);
    schedule.record(CHAIN, RunMode::Normal, true, 0);
    assert!(!schedule.due(CHAIN, now));
    schedule.record(CHAIN, RunMode::Normal, true, 1);
    assert!(schedule.due(CHAIN, now));
    refreshed(&schedule, CHAIN, now);
    // From here the floor applies.
    schedule.record(CHAIN, RunMode::Normal, true, 999);
    assert!(!schedule.due(CHAIN, now));
    schedule.record(CHAIN, RunMode::Normal, true, 1);
    assert!(schedule.due(CHAIN, now));
}

#[test]
fn refresh_repeats_when_ensv2_writes_double() {
    let (schedule, now) = (Schedule::default(), Instant::now());
    schedule.record(CHAIN, RunMode::Redo, false, 50);
    refreshed(&schedule, CHAIN, now);
    // Refreshed at 50 events: the floor of 1,000 further events applies.
    schedule.record(CHAIN, RunMode::Redo, false, 999);
    assert!(!schedule.due(CHAIN, now));
    schedule.record(CHAIN, RunMode::Redo, false, 1);
    assert!(schedule.due(CHAIN, now));
    refreshed(&schedule, CHAIN, now);
    // Refreshed at 1,050 events: the next refresh waits for as many again.
    schedule.record(CHAIN, RunMode::Redo, false, 1_049);
    assert!(!schedule.due(CHAIN, now));
    schedule.record(CHAIN, RunMode::Redo, false, 1);
    assert!(schedule.due(CHAIN, now));
    refreshed(&schedule, CHAIN, now);
    schedule.record(CHAIN, RunMode::Redo, false, 2_099);
    assert!(!schedule.due(CHAIN, now));
}

#[test]
fn redo_completion_is_always_due() {
    let (schedule, now) = (Schedule::default(), Instant::now());
    schedule.record(CHAIN, RunMode::Redo, false, 5_000);
    refreshed(&schedule, CHAIN, now);
    schedule.record(CHAIN, RunMode::Redo, false, 10);
    assert!(!schedule.due(CHAIN, now));
    schedule.record(CHAIN, RunMode::Redo, true, 0);
    assert!(schedule.due(CHAIN, now));
    refreshed(&schedule, CHAIN, now);
    assert!(!schedule.due(CHAIN, now));
}

#[test]
fn chain_without_ensv2_refreshes_only_at_redo_completion() {
    let (schedule, now) = (Schedule::default(), Instant::now());
    for complete in [false, true] {
        schedule.record(CHAIN, RunMode::Normal, complete, 0);
        assert!(!schedule.due(CHAIN, now));
    }
    schedule.record(CHAIN, RunMode::Redo, false, 0);
    assert!(!schedule.due(CHAIN, now));
    schedule.record(CHAIN, RunMode::Redo, true, 0);
    assert!(schedule.due(CHAIN, now));
    refreshed(&schedule, CHAIN, now);
    schedule.record(CHAIN, RunMode::Normal, true, 0);
    assert!(!schedule.due(CHAIN, now));
}

#[test]
fn normal_mode_completion_is_not_due() {
    let (schedule, now) = (Schedule::default(), Instant::now());
    schedule.record(CHAIN, RunMode::Normal, true, 3);
    refreshed(&schedule, CHAIN, now);
    // A live runner completes a run at every catch-up to the head.
    for _ in 0..3 {
        schedule.record(CHAIN, RunMode::Normal, true, 3);
        assert!(!schedule.due(CHAIN, now));
    }
}

#[test]
fn rewritten_events_count_once_per_write() {
    let (schedule, now) = (Schedule::default(), Instant::now());
    schedule.record(CHAIN, RunMode::Normal, true, 600);
    refreshed(&schedule, CHAIN, now);
    // Reorg recovery writes the same 600 events again: 600 more writes, under the floor.
    schedule.record(CHAIN, RunMode::Normal, true, 600);
    assert!(!schedule.due(CHAIN, now));
    schedule.record(CHAIN, RunMode::Normal, true, 600);
    assert!(schedule.due(CHAIN, now));
}

#[test]
fn schedules_are_per_chain() {
    let (schedule, now) = (Schedule::default(), Instant::now());
    schedule.record("first", RunMode::Redo, false, 7);
    assert!(schedule.due("first", now));
    assert!(!schedule.due("second", now));
    refreshed(&schedule, "first", now);
    schedule.record("second", RunMode::Redo, true, 0);
    assert!(!schedule.due("first", now));
    assert!(schedule.due("second", now));
}

#[test]
fn a_failed_refresh_stays_due_and_waits_a_minute() {
    let (schedule, now) = (Schedule::default(), Instant::now());
    schedule.record(CHAIN, RunMode::Redo, false, 7);
    let due = schedule.take_due(CHAIN, now).expect("due");
    schedule.settle(CHAIN, &due, false, now);
    assert!(!schedule.due(CHAIN, now + Duration::from_secs(59)));
    assert!(schedule.due(CHAIN, now + Duration::from_secs(60)));
}

#[test]
fn only_ensv2_source_families_are_counted() {
    let families = [
        "ens_v2_registry_l1",
        "ens_v1_registrar_l1",
        "ens_v2_root_l1",
        "basenames_base_registry",
    ];
    assert_eq!(ensv2_events(families), 2);
}

async fn database() -> Result<TestDatabase> {
    let db = TestDatabase::create(TestDatabaseConfig::new("interpret_statistics")).await?;
    for sql in [
        include_str!("../../../storage/schema/baseline/01_chain.sql"),
        include_str!("../../../storage/schema/baseline/03_identity.sql"),
        include_str!("../../../storage/schema/baseline/04_manifests.sql"),
        include_str!("../../../storage/schema/baseline/05_normalized_events.sql"),
    ] {
        sqlx::raw_sql(sql).execute(db.pool()).await?;
    }
    Ok(db)
}

/// `pg_class.reltuples` of `normalized_events`: -1 until the table is first analyzed.
async fn analyzed_rows(pool: &PgPool) -> Result<f32> {
    Ok(
        sqlx::query_scalar("SELECT reltuples FROM pg_class WHERE relname = 'normalized_events'")
            .fetch_one(pool)
            .await?,
    )
}

#[derive(Clone, Default)]
struct Logs(Arc<Mutex<Vec<u8>>>);

impl Logs {
    fn count(&self, text: &str) -> usize {
        String::from_utf8_lossy(&self.0.lock().expect("log lock"))
            .matches(text)
            .count()
    }
}

impl std::io::Write for Logs {
    fn write(&mut self, buffer: &[u8]) -> std::io::Result<usize> {
        self.0.lock().expect("log lock").extend_from_slice(buffer);
        Ok(buffer.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

fn capture() -> (Logs, tracing::subscriber::DefaultGuard) {
    let logs = Logs::default();
    let subscriber = tracing_subscriber::fmt()
        .with_ansi(false)
        .with_writer({
            let logs = logs.clone();
            move || logs.clone()
        })
        .finish();
    (logs.clone(), tracing::subscriber::set_default(subscriber))
}

const REFRESHED: &str = "interpret refreshed normalized_events statistics";
const FAILED: &str = "interpret could not refresh normalized_events statistics";

#[tokio::test]
async fn refresh_on_an_empty_table_succeeds() -> Result {
    let db = database().await?;
    let engine = Engine::new(db.pool().clone());
    let (logs, _guard) = capture();
    assert!(!engine.refresh_statistics(CHAIN).await, "nothing is due");
    assert_eq!(analyzed_rows(db.pool()).await?, -1.0);
    engine.record_completed_redo(CHAIN);
    assert!(engine.statistics_refresh_due(CHAIN));
    assert!(engine.refresh_statistics(CHAIN).await);
    assert_eq!(analyzed_rows(db.pool()).await?, 0.0);
    assert!(!engine.statistics_refresh_due(CHAIN));
    assert_eq!(logs.count(REFRESHED), 1);
    assert_eq!(logs.count("mode=\"redo\""), 1);
    assert_eq!(logs.count(FAILED), 0);
    drop(engine);
    db.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn refresh_lock_timeout_is_logged_and_retried() -> Result {
    let db = database().await?;
    let (schedule, now) = (Schedule::default(), Instant::now());
    schedule.record(CHAIN, RunMode::Redo, false, 3);
    let (logs, _guard) = capture();
    // An index build or another ANALYZE holds this lock.
    let mut blocker = db.pool().begin().await?;
    sqlx::query("LOCK TABLE normalized_events IN SHARE UPDATE EXCLUSIVE MODE")
        .execute(&mut *blocker)
        .await?;
    let short_lock_wait = Timeouts {
        lock: Duration::from_millis(200),
        ..TIMEOUTS
    };
    assert!(
        !schedule
            .refresh(db.pool(), CHAIN, short_lock_wait, now)
            .await
    );
    let failed_returned = Instant::now();
    assert_eq!(logs.count(FAILED), 1);
    assert_eq!(logs.count("lock timeout"), 1);
    assert_eq!(analyzed_rows(db.pool()).await?, -1.0);
    blocker.rollback().await?;
    // The lock is free again, but the retry waits its minute.
    let early = failed_returned + Duration::from_secs(59);
    assert!(!schedule.due(CHAIN, early));
    assert!(!schedule.refresh(db.pool(), CHAIN, TIMEOUTS, early).await);
    assert_eq!(analyzed_rows(db.pool()).await?, -1.0);
    let later = failed_returned + Duration::from_secs(60);
    assert!(schedule.due(CHAIN, later));
    assert!(schedule.refresh(db.pool(), CHAIN, TIMEOUTS, later).await);
    assert_eq!(analyzed_rows(db.pool()).await?, 0.0);
    assert!(!schedule.due(CHAIN, later));
    assert_eq!((logs.count(FAILED), logs.count(REFRESHED)), (1, 1));
    db.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn refresh_failure_never_fails_the_phase() -> Result {
    let db = database().await?;
    let (logs, _guard) = capture();
    // The statement limit ends a refresh that is still waiting for its lock.
    let schedule = Schedule::default();
    schedule.record(CHAIN, RunMode::Normal, false, 3);
    let mut blocker = db.pool().begin().await?;
    sqlx::query("LOCK TABLE normalized_events IN SHARE UPDATE EXCLUSIVE MODE")
        .execute(&mut *blocker)
        .await?;
    let short_statement = Timeouts {
        statement: Duration::from_millis(200),
        ..TIMEOUTS
    };
    let call_started = Instant::now();
    assert!(
        !schedule
            .refresh(db.pool(), CHAIN, short_statement, call_started)
            .await
    );
    let failed_returned = Instant::now();
    assert_eq!(logs.count("statement timeout"), 1);
    // The failed statement spent time awaiting the database. Its quiet minute starts
    // after that await, so a start-to-start deadline would admit this call too early.
    assert!(
        !schedule.due(CHAIN, call_started + Duration::from_secs(60)),
        "the retry minute must start after the failed refresh returns"
    );
    assert!(!schedule.due(CHAIN, failed_returned + Duration::from_secs(59)));
    let eligible = failed_returned + Duration::from_secs(60);
    assert!(schedule.due(CHAIN, eligible));
    // Settlement is inside the call, before failed_returned. The external clock samples
    // bracket its anchor instead of assuming the sample after return is the exact one.
    let settled = schedule
        .chains()
        .get(CHAIN)
        .expect("the chain was recorded")
        .retry_from
        .expect("the failed refresh has a retry deadline")
        - Duration::from_secs(60);
    assert!(settled >= call_started && settled <= failed_returned);
    blocker.rollback().await?;
    assert!(schedule.refresh(db.pool(), CHAIN, TIMEOUTS, eligible).await);
    assert_eq!(analyzed_rows(db.pool()).await?, 0.0);
    assert!(!schedule.due(CHAIN, eligible));
    schedule.record(CHAIN, RunMode::Normal, false, 1_000);
    assert!(
        schedule.due(CHAIN, failed_returned),
        "a successful refresh clears the failed attempt's retry deadline"
    );

    // Any other database error: the engine's own entry point returns and stays quiet for a
    // minute.
    let engine = Engine::new(db.pool().clone());
    engine.record_completed_redo(CHAIN);
    sqlx::query("ALTER TABLE normalized_events RENAME TO normalized_events_moved")
        .execute(db.pool())
        .await?;
    assert!(!engine.refresh_statistics(CHAIN).await);
    assert!(!engine.statistics_refresh_due(CHAIN));
    assert_eq!((logs.count(FAILED), logs.count(REFRESHED)), (2, 1));
    drop(engine);
    db.cleanup().await?;
    Ok(())
}
