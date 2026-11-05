use std::{
    collections::BTreeMap,
    time::{Duration, Instant},
};

use sqlx::{PgPool, postgres::PgPoolOptions};

use super::{REFRESH_FLOOR, RETRY_AFTER, Schedule, TABLES, TIMEOUTS};
use crate::phase::{AfterProgress, BlockRange, RunMode};

const GRANT: &str = "project_grant";

fn rows(table: &'static str, written: u64) -> BTreeMap<&'static str, u64> {
    BTreeMap::from([(table, written)])
}

fn write(schedule: &Schedule, table: &'static str, written: u64) {
    schedule.record(&RunMode::Normal, &rows(table, written));
}

/// Records a successful refresh that found `rows` rows, without a database.
fn refreshed(schedule: &Schedule, rows: u64, now: Instant) {
    let (table, written) = schedule.next_due(now).expect("a refresh must be due");
    schedule.settle(table, written, Some(rows), now);
}

/// A pool whose every connection fails, soon.
fn unreachable_pool() -> PgPool {
    PgPoolOptions::new()
        .acquire_timeout(Duration::from_millis(100))
        .connect_lazy("postgres://bigname@127.0.0.1:1/bigname")
        .expect("a lazy pool never connects")
}

#[test]
fn listed_tables_are_family_tables() {
    let families: Vec<_> = bigname_project::families::family_tables().collect();
    for table in TABLES {
        assert!(families.contains(&table), "{table}");
    }
}

#[test]
fn first_write_of_a_process_is_due() {
    let (schedule, now) = (Schedule::default(), Instant::now());
    assert!(!schedule.due(now));
    write(&schedule, GRANT, 1);
    assert!(schedule.due(now));
}

#[test]
fn a_batch_that_changes_no_row_of_a_listed_table_is_not_due() {
    let (schedule, now) = (Schedule::default(), Instant::now());
    write(&schedule, GRANT, 0);
    write(&schedule, "project_wrapper_state", 5_000);
    assert!(!schedule.due(now));
}

#[test]
fn every_table_keeps_its_own_count() {
    let (schedule, now) = (Schedule::default(), Instant::now());
    write(&schedule, GRANT, 1);
    write(&schedule, "project_account_approval", 1);
    refreshed(&schedule, 0, now);
    assert_eq!(
        schedule.next_due(now),
        Some(("project_account_approval", 1))
    );
    refreshed(&schedule, 0, now);
    assert_eq!(schedule.next_due(now), None);
}

#[test]
fn a_table_below_the_floor_waits_for_the_floor() {
    for table_rows in [0, 10, REFRESH_FLOOR] {
        let (schedule, now) = (Schedule::default(), Instant::now());
        write(&schedule, GRANT, 1);
        refreshed(&schedule, table_rows, now);
        write(&schedule, GRANT, REFRESH_FLOOR - 1);
        assert!(!schedule.due(now), "{table_rows}");
        write(&schedule, GRANT, 1);
        assert!(schedule.due(now), "{table_rows}");
    }
}

#[test]
fn a_table_above_the_floor_waits_for_as_many_writes_as_it_has_rows() {
    let (schedule, now) = (Schedule::default(), Instant::now());
    write(&schedule, GRANT, 1);
    refreshed(&schedule, 5_000, now);
    write(&schedule, GRANT, 4_999);
    assert!(!schedule.due(now));
    write(&schedule, GRANT, 1);
    assert!(schedule.due(now));
    refreshed(&schedule, 10_000, now);
    write(&schedule, GRANT, 9_999);
    assert!(!schedule.due(now));
}

#[test]
fn writes_of_every_chain_count_towards_one_refresh() {
    let (schedule, now) = (Schedule::default(), Instant::now());
    write(&schedule, GRANT, 1);
    refreshed(&schedule, 0, now);
    // Two chains' batches between two calls, recorded through the same schedule.
    write(&schedule, GRANT, REFRESH_FLOOR / 2);
    write(&schedule, GRANT, REFRESH_FLOOR / 2);
    assert!(schedule.due(now));
    refreshed(&schedule, 0, now);
    assert!(!schedule.due(now));
}

#[test]
fn writes_committed_while_a_refresh_runs_stay_counted() {
    let (schedule, now) = (Schedule::default(), Instant::now());
    write(&schedule, GRANT, 5);
    let (table, written) = schedule.next_due(now).expect("due");
    write(&schedule, GRANT, 3);
    schedule.settle(table, written, Some(0), now);
    write(&schedule, GRANT, REFRESH_FLOOR - 4);
    assert!(!schedule.due(now));
    write(&schedule, GRANT, 1);
    assert!(schedule.due(now));
}

#[test]
fn a_redo_counts_no_write() {
    let (schedule, now) = (Schedule::default(), Instant::now());
    let range = BlockRange { from: 0, to: 10 };
    schedule.record(&RunMode::Redo(range), &rows(GRANT, 5_000));
    assert!(!schedule.due(now));
    write(&schedule, GRANT, 1);
    assert_eq!(schedule.next_due(now), Some((GRANT, 1)));
}

#[tokio::test]
async fn a_failed_refresh_stays_due_and_waits() {
    let schedule = Schedule::default();
    write(&schedule, GRANT, 1);
    let step = schedule
        .refresh(&unreachable_pool(), TIMEOUTS, Instant::now())
        .await;
    assert_eq!(step, AfterProgress::Done);
    let failed = Instant::now();
    assert!(!schedule.due(failed));
    assert!(schedule.due(failed + RETRY_AFTER));
    // The failure leaves every other table free to refresh.
    write(&schedule, "project_account_approval", 1);
    assert_eq!(
        schedule.next_due(failed),
        Some(("project_account_approval", 1))
    );
}

#[tokio::test]
async fn a_refresh_under_way_leaves_nothing_to_do_for_another_chain() {
    let (schedule, now) = (Schedule::default(), Instant::now());
    write(&schedule, GRANT, 1);
    let permit = schedule.refreshes.try_acquire().expect("a free permit");
    assert!(!schedule.due(now));
    let step = schedule.refresh(&unreachable_pool(), TIMEOUTS, now).await;
    assert_eq!(step, AfterProgress::Done);
    drop(permit);
    // The skipped call tried no connection, so nothing failed and the table is due at once.
    assert!(schedule.due(now));
}
