use std::{future::Future, time::Duration};

#[derive(Clone, Copy, Debug)]
pub(super) struct InterpretWalkMetrics {
    pub(super) elapsed_seconds: f64,
    pub(super) budget_peak_rss_mib: f64,
    pub(super) kernel_hwm_rss_mib: f64,
    pub(super) sampled_peak_rss_mib: f64,
}

pub(super) enum InterpretWalkOutcome {
    Completed(InterpretWalkMetrics),
    TimedOut {
        metrics: InterpretWalkMetrics,
        failure: String,
    },
}

pub(super) fn from_throughput_floor(
    walk_blocks: i64,
    minimum_blocks_per_hour: u64,
    multiplier: u64,
    maximum_seconds: u64,
) -> Duration {
    let blocks = u128::try_from(walk_blocks).unwrap_or_default();
    let floor = u128::from(minimum_blocks_per_hour.max(1));
    let seconds = blocks
        .saturating_mul(3_600)
        .saturating_mul(u128::from(multiplier.max(1)))
        .div_ceil(floor)
        .clamp(1, u128::from(maximum_seconds.max(1)));
    Duration::from_secs(u64::try_from(seconds).unwrap_or(u64::MAX))
}

pub(super) async fn complete_within<T>(
    limit: Duration,
    work: impl Future<Output = T>,
) -> Option<T> {
    tokio::time::timeout(limit, work).await.ok()
}

pub(super) fn failure(
    limit: Duration,
    minimum_blocks_per_hour: u64,
    multiplier: u64,
    maximum_seconds: u64,
) -> String {
    format!(
        "Interpret walk exceeded its throughput-derived wall-clock deadline of {:.3}s; limit is the smaller of {multiplier} times the duration implied by the {minimum_blocks_per_hour} blocks/hour floor and the configured {maximum_seconds}s cap",
        limit.as_secs_f64()
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    use bigname_test_support::{TestDatabase, TestDatabaseConfig, database_url_from_env};
    use sqlx::postgres::PgPoolOptions;

    #[test]
    fn deadline_is_twice_the_floor_implied_duration() {
        assert_eq!(
            from_throughput_floor(100_000, 500_000, 2, 21_600),
            Duration::from_secs(1_440)
        );
        assert_eq!(
            from_throughput_floor(1, 10_000_000, 2, 21_600),
            Duration::from_secs(1)
        );
    }

    #[test]
    fn smoke_walk_deadline_is_bounded() {
        assert_eq!(
            from_throughput_floor(16, 1, 2, 30),
            Duration::from_secs(30),
            "the checked-in smoke walk must not wait for its one-block/hour floor"
        );
    }

    #[test]
    fn timeout_report_stops_before_project_rebuild() {
        let source = include_str!("../indexing.rs");
        let stop = source
            .find("return Ok(walk_failure_report(")
            .expect("a timed-out walk must return its red report immediately");
        let rebuild = source
            .find("let rebuild_started")
            .expect("the indexing gate must retain its rebuild measurement");
        assert!(stop < rebuild);
    }

    #[tokio::test]
    async fn stalled_walk_becomes_a_named_red_instead_of_hanging() {
        let limit = Duration::from_millis(1);
        let completed = complete_within(limit, tokio::time::sleep(Duration::from_millis(20))).await;

        assert!(completed.is_none());
        assert_eq!(
            failure(limit, 500_000, 2, 21_600),
            "Interpret walk exceeded its throughput-derived wall-clock deadline of 0.001s; limit is the smaller of 2 times the duration implied by the 500000 blocks/hour floor and the configured 21600s cap"
        );
    }

    #[tokio::test]
    async fn timed_out_database_work_does_not_delay_pool_shutdown() {
        let database = TestDatabase::create(TestDatabaseConfig::new(
            "benchmark_interpret_deadline_cancellation",
        ))
        .await
        .unwrap();
        let options = sqlx::postgres::PgConnectOptions::from_str(&database_url_from_env())
            .unwrap()
            .database(database.database_name())
            .application_name("benchmark-interpret-deadline-test");
        let pool = PgPoolOptions::new()
            .max_connections(2)
            .connect_with(options)
            .await
            .unwrap();

        let completed = complete_within(Duration::from_millis(20), async {
            let mut transaction = pool.begin().await.unwrap();
            sqlx::query("SELECT pg_sleep(5)")
                .execute(&mut *transaction)
                .await
                .unwrap();
            transaction.commit().await.unwrap();
        })
        .await;
        assert!(completed.is_none());
        let closed = tokio::time::timeout(Duration::from_millis(250), pool.close()).await;

        drop(pool);
        database.cleanup().await.unwrap();
        assert!(
            closed.is_ok(),
            "the cancelled query kept pool shutdown blocked past the report bound"
        );
    }
}
