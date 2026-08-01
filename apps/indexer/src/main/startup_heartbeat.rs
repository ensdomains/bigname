use std::{collections::BTreeSet, sync::Arc};

use anyhow::{Result, ensure};
use sqlx::PgPool;
use tokio::{
    sync::Mutex,
    time::{Duration, Instant},
};

use crate::reconciliation::FullClosureReplayLockWaitHeartbeat;

#[path = "startup_heartbeat/activity.rs"]
mod activity;

pub(crate) use activity::RequiredSubtaskActivity;

const MAX_PROGRESS_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(5);
const FULL_CLOSURE_REPLAY_LOCK_WAIT_PHASE: &str = "full_closure_replay_lock.wait";

pub(crate) struct StartupHeartbeat {
    instance_id: String,
    interval: Duration,
    last_recorded_at: Instant,
    full_closure_replay_lock_waits: BTreeSet<(String, String)>,
}

pub(crate) struct StartupAdapterHeartbeat<'a> {
    heartbeat: &'a mut StartupHeartbeat,
    chain_ids: &'a [String],
}

#[derive(Clone)]
pub(crate) struct NormalizedReplayHeartbeat {
    heartbeat: Arc<Mutex<StartupHeartbeat>>,
    chain_ids: Arc<Vec<String>>,
    interval: Duration,
    last_recorded_at: Arc<Mutex<Instant>>,
}

impl NormalizedReplayHeartbeat {
    pub(crate) fn new(instance_id: String, interval: Duration, chain_ids: Vec<String>) -> Self {
        let interval = interval.min(MAX_PROGRESS_HEARTBEAT_INTERVAL);
        Self {
            heartbeat: Arc::new(Mutex::new(StartupHeartbeat::new(instance_id, interval))),
            chain_ids: Arc::new(chain_ids),
            interval,
            last_recorded_at: Arc::new(Mutex::new(Instant::now())),
        }
    }

    pub(crate) fn for_chain(&self, chain_id: &str) -> Result<Self> {
        ensure!(
            self.chain_ids.iter().any(|candidate| candidate == chain_id),
            "normalized replay heartbeat chain {chain_id} is not in the configured live-chain set"
        );
        Ok(Self {
            heartbeat: Arc::clone(&self.heartbeat),
            chain_ids: Arc::new(vec![chain_id.to_owned()]),
            interval: self.interval,
            last_recorded_at: Arc::new(Mutex::new(Instant::now())),
        })
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

impl crate::StartupAdapterProgress for StartupAdapterHeartbeat<'_> {
    fn record<'a>(&'a mut self, pool: &'a PgPool) -> crate::StartupAdapterProgressFuture<'a> {
        Box::pin(async move { self.heartbeat.record_if_due(pool, self.chain_ids).await })
    }
}

impl bigname_manifests::ManifestRuntimeProgress for StartupAdapterHeartbeat<'_> {
    fn record<'a>(
        &'a mut self,
        pool: &'a PgPool,
    ) -> bigname_manifests::ManifestRuntimeProgressFuture<'a> {
        Box::pin(async move { self.heartbeat.record_if_due(pool, self.chain_ids).await })
    }
}

impl crate::StartupAdapterProgress for NormalizedReplayHeartbeat {
    fn record<'a>(&'a mut self, pool: &'a PgPool) -> crate::StartupAdapterProgressFuture<'a> {
        Box::pin(async move {
            let mut last_recorded_at = self.last_recorded_at.lock().await;
            let mut heartbeat = self.heartbeat.lock().await;
            if last_recorded_at.elapsed() < self.interval {
                return Ok(());
            }
            let [chain_id] = self.chain_ids.as_slice() else {
                anyhow::bail!(
                    "normalized replay heartbeat must be scoped to exactly one chain before recording progress"
                );
            };
            heartbeat.record_chain(pool, chain_id).await?;
            *last_recorded_at = Instant::now();
            Ok(())
        })
    }
}

impl FullClosureReplayLockWaitHeartbeat for StartupAdapterHeartbeat<'_> {
    fn begin_wait<'a>(
        &'a mut self,
        pool: &'a PgPool,
        deployment_profile: &'a str,
        chain: &'a str,
    ) -> crate::StartupAdapterProgressFuture<'a> {
        Box::pin(self.heartbeat.begin_full_closure_replay_lock_wait(
            pool,
            deployment_profile,
            chain,
        ))
    }

    fn finish_wait<'a>(
        &'a mut self,
        pool: &'a PgPool,
        deployment_profile: &'a str,
        chain: &'a str,
    ) -> crate::StartupAdapterProgressFuture<'a> {
        Box::pin(self.heartbeat.finish_full_closure_replay_lock_wait(
            pool,
            deployment_profile,
            chain,
        ))
    }
}

impl FullClosureReplayLockWaitHeartbeat for NormalizedReplayHeartbeat {
    fn begin_wait<'a>(
        &'a mut self,
        pool: &'a PgPool,
        deployment_profile: &'a str,
        chain: &'a str,
    ) -> crate::StartupAdapterProgressFuture<'a> {
        Box::pin(async move {
            self.heartbeat
                .lock()
                .await
                .begin_full_closure_replay_lock_wait(pool, deployment_profile, chain)
                .await
        })
    }

    fn finish_wait<'a>(
        &'a mut self,
        pool: &'a PgPool,
        deployment_profile: &'a str,
        chain: &'a str,
    ) -> crate::StartupAdapterProgressFuture<'a> {
        Box::pin(async move {
            self.heartbeat
                .lock()
                .await
                .finish_full_closure_replay_lock_wait(pool, deployment_profile, chain)
                .await
        })
    }
}

impl StartupHeartbeat {
    pub(crate) fn new(instance_id: String, interval: Duration) -> Self {
        Self {
            instance_id,
            interval: interval.min(MAX_PROGRESS_HEARTBEAT_INTERVAL),
            last_recorded_at: Instant::now(),
            full_closure_replay_lock_waits: BTreeSet::new(),
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

    async fn record_chain(&mut self, pool: &PgPool, chain_id: &str) -> Result<()> {
        bigname_storage::record_service_loop_chain_heartbeat(pool, &self.instance_id, chain_id)
            .await?;
        self.last_recorded_at = Instant::now();
        Ok(())
    }

    async fn begin_full_closure_replay_lock_wait(
        &mut self,
        pool: &PgPool,
        deployment_profile: &str,
        chain: &str,
    ) -> Result<()> {
        let wait_identity = (deployment_profile.to_owned(), chain.to_owned());
        if self.full_closure_replay_lock_waits.contains(&wait_identity) {
            return Ok(());
        }
        if self.full_closure_replay_lock_waits.is_empty() {
            bigname_storage::begin_service_loop_phase(
                pool,
                bigname_storage::INDEXER_SERVICE_NAME,
                &self.instance_id,
                FULL_CLOSURE_REPLAY_LOCK_WAIT_PHASE,
            )
            .await?;
            self.last_recorded_at = Instant::now();
        }
        self.full_closure_replay_lock_waits.insert(wait_identity);
        Ok(())
    }

    async fn finish_full_closure_replay_lock_wait(
        &mut self,
        pool: &PgPool,
        deployment_profile: &str,
        chain: &str,
    ) -> Result<()> {
        let wait_identity = (deployment_profile.to_owned(), chain.to_owned());
        if !self.full_closure_replay_lock_waits.contains(&wait_identity) {
            return Ok(());
        }
        if self.full_closure_replay_lock_waits.len() == 1 {
            bigname_storage::finish_service_loop_phase(
                pool,
                bigname_storage::INDEXER_SERVICE_NAME,
                &self.instance_id,
                FULL_CLOSURE_REPLAY_LOCK_WAIT_PHASE,
            )
            .await?;
            self.last_recorded_at = Instant::now();
        }
        self.full_closure_replay_lock_waits.remove(&wait_identity);
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
        crate::StartupAdapterProgress::record(&mut progress, database.pool()).await?;

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
