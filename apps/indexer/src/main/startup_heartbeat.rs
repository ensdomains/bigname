use std::{future::Future, sync::Arc};

use anyhow::Result;
use sqlx::PgPool;
use tokio::{
    sync::{Mutex, OwnedMutexGuard},
    time::{Duration, Instant},
};

const MAX_PROGRESS_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(5);

#[derive(Clone, Default)]
pub(crate) struct RequiredSubtaskActivity {
    exclusion: Arc<Mutex<()>>,
}

pub(crate) struct RequiredSubtaskActivityGuard {
    _exclusion: OwnedMutexGuard<()>,
}

impl RequiredSubtaskActivity {
    pub(crate) async fn begin(&self) -> RequiredSubtaskActivityGuard {
        RequiredSubtaskActivityGuard {
            _exclusion: Arc::clone(&self.exclusion).lock_owned().await,
        }
    }

    pub(crate) async fn exclude_required_subtask(&self) -> OwnedMutexGuard<()> {
        Arc::clone(&self.exclusion).lock_owned().await
    }

    pub(crate) async fn exclude_required_subtask_or_shutdown<F>(
        &self,
        shutdown: F,
    ) -> Option<OwnedMutexGuard<()>>
    where
        F: Future,
    {
        tokio::select! {
            biased;
            _ = shutdown => None,
            exclusion = self.exclude_required_subtask() => Some(exclusion),
        }
    }
}

pub(crate) struct StartupHeartbeat {
    instance_id: String,
    interval: Duration,
    last_recorded_at: Instant,
    #[cfg(test)]
    adapter_progress_count: usize,
}

pub(crate) struct StartupAdapterHeartbeat<'a> {
    heartbeat: &'a mut StartupHeartbeat,
    chain_ids: &'a [String],
}

#[derive(Clone)]
pub(crate) struct NormalizedReplayHeartbeat {
    heartbeat: Arc<Mutex<StartupHeartbeat>>,
    chain_ids: Arc<Vec<String>>,
}

impl NormalizedReplayHeartbeat {
    pub(crate) fn new(instance_id: String, interval: Duration, chain_ids: Vec<String>) -> Self {
        Self {
            heartbeat: Arc::new(Mutex::new(StartupHeartbeat::new(instance_id, interval))),
            chain_ids: Arc::new(chain_ids),
        }
    }

    #[cfg(test)]
    pub(crate) async fn adapter_progress_count(&self) -> usize {
        self.heartbeat.lock().await.adapter_progress_count()
    }
}

impl<'a> StartupAdapterHeartbeat<'a> {
    pub(crate) fn new(heartbeat: &'a mut StartupHeartbeat, chain_ids: &'a [String]) -> Self {
        Self {
            heartbeat,
            chain_ids,
        }
    }
}

impl bigname_adapters::StartupAdapterProgress for StartupAdapterHeartbeat<'_> {
    fn record<'a>(
        &'a mut self,
        pool: &'a PgPool,
    ) -> bigname_adapters::StartupAdapterProgressFuture<'a> {
        Box::pin(async move {
            #[cfg(test)]
            {
                self.heartbeat.adapter_progress_count += 1;
            }
            self.heartbeat.record_if_due(pool, self.chain_ids).await
        })
    }
}

impl bigname_manifests::ManifestRuntimeProgress for StartupAdapterHeartbeat<'_> {
    fn record<'a>(
        &'a mut self,
        pool: &'a PgPool,
    ) -> bigname_manifests::ManifestRuntimeProgressFuture<'a> {
        Box::pin(async move {
            #[cfg(test)]
            {
                self.heartbeat.adapter_progress_count += 1;
            }
            self.heartbeat.record_if_due(pool, self.chain_ids).await
        })
    }
}

impl bigname_adapters::StartupAdapterProgress for NormalizedReplayHeartbeat {
    fn record<'a>(
        &'a mut self,
        pool: &'a PgPool,
    ) -> bigname_adapters::StartupAdapterProgressFuture<'a> {
        Box::pin(async move {
            let mut heartbeat = self.heartbeat.lock().await;
            #[cfg(test)]
            {
                heartbeat.adapter_progress_count += 1;
            }
            heartbeat.record_if_due(pool, &self.chain_ids).await
        })
    }
}

impl StartupHeartbeat {
    pub(crate) fn new(instance_id: String, interval: Duration) -> Self {
        Self {
            instance_id,
            interval: interval.min(MAX_PROGRESS_HEARTBEAT_INTERVAL),
            last_recorded_at: Instant::now(),
            #[cfg(test)]
            adapter_progress_count: 0,
        }
    }

    #[cfg(test)]
    pub(crate) fn adapter_progress_count(&self) -> usize {
        self.adapter_progress_count
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

    #[test]
    fn progress_heartbeat_throttle_never_inherits_a_stale_poll_interval() {
        let heartbeat = StartupHeartbeat::new("long-poll-test".to_owned(), Duration::from_secs(60));

        assert_eq!(heartbeat.interval, Duration::from_secs(5));
    }

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
        let chain_ids = vec!["ethereum-mainnet".to_owned(), "ethereum-mainnet".to_owned()];
        let mut progress = StartupAdapterHeartbeat::new(&mut heartbeat, &chain_ids);
        bigname_adapters::StartupAdapterProgress::record(&mut progress, database.pool()).await?;

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
