use anyhow::Result;
use sqlx::PgPool;
use tokio::time::{Duration, Instant};

pub(crate) struct StartupHeartbeat {
    instance_id: String,
    interval: Duration,
    last_recorded_at: Instant,
}

impl StartupHeartbeat {
    pub(crate) fn new(instance_id: String, interval: Duration) -> Self {
        Self {
            instance_id,
            interval,
            last_recorded_at: Instant::now(),
        }
    }

    pub(crate) async fn record_if_due(
        &mut self,
        pool: &PgPool,
        chain_ids: &[String],
    ) -> Result<()> {
        if self.last_recorded_at.elapsed() < self.interval {
            return Ok(());
        }
        self.record(pool, chain_ids).await
    }

    pub(crate) async fn record(&mut self, pool: &PgPool, chain_ids: &[String]) -> Result<()> {
        bigname_storage::record_service_loop_heartbeat(
            pool,
            bigname_storage::INDEXER_SERVICE_NAME,
            &self.instance_id,
            chain_ids,
        )
        .await?;
        self.last_recorded_at = Instant::now();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn bootstrap_progress_refreshes_the_registered_indexer_loop() -> Result<()> {
        let database = bigname_test_support::TestDatabase::create_migrated(
            bigname_test_support::TestDatabaseConfig::new("bigname_indexer_startup_heartbeat_test"),
            &bigname_storage::MIGRATOR,
            "failed to migrate indexer startup-heartbeat test database",
        )
        .await?;
        bigname_storage::register_service_loop(
            database.pool(),
            bigname_storage::INDEXER_SERVICE_NAME,
            "bootstrap-test",
        )
        .await?;
        sqlx::query(
            r#"
            UPDATE service_loop_heartbeats
            SET started_at = clock_timestamp() - INTERVAL '3 minutes',
                heartbeat_at = clock_timestamp() - INTERVAL '2 minutes'
            WHERE service_name = 'indexer'
              AND instance_id = 'bootstrap-test'
            "#,
        )
        .execute(database.pool())
        .await?;

        let mut heartbeat =
            StartupHeartbeat::new("bootstrap-test".to_owned(), Duration::from_secs(0));
        heartbeat
            .record_if_due(
                database.pool(),
                &["ethereum-mainnet".to_owned(), "ethereum-mainnet".to_owned()],
            )
            .await?;

        let observed = bigname_storage::load_service_loop_heartbeat(
            database.pool(),
            bigname_storage::INDEXER_SERVICE_NAME,
            "bootstrap-test",
        )
        .await?
        .expect("registered startup heartbeat must exist");
        assert!(observed.age_seconds < 5);
        let chain_row_count = sqlx::query_scalar::<_, i64>(
            r#"
            SELECT COUNT(*)
            FROM service_loop_heartbeats
            WHERE service_name = 'indexer'
              AND instance_id = 'bootstrap-test'
              AND scope_kind = 'chain'
            "#,
        )
        .fetch_one(database.pool())
        .await?;
        assert_eq!(
            chain_row_count, 1,
            "duplicate chain ids must be deduplicated"
        );

        database.cleanup().await
    }
}
