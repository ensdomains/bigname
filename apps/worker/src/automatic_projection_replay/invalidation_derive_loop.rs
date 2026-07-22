use std::future::Future;

use anyhow::Result;
use sqlx::PgPool;
use tokio::time::{Duration, sleep};
use tracing::{info, warn};

use super::subtask_supervision::SubtaskSpawner;
use crate::projection_apply;

const SUBTASK_NAME: &str = "projection_invalidation_derivation";

pub(super) fn spawn(
    subtasks: &SubtaskSpawner,
    pool: PgPool,
    poll_interval_secs: u64,
) -> Result<()> {
    spawn_future(
        subtasks,
        run_continuous_projection_invalidation_derivation(pool, poll_interval_secs),
    )
}

fn spawn_future<Subtask>(subtasks: &SubtaskSpawner, subtask: Subtask) -> Result<()>
where
    Subtask: Future<Output = Result<()>> + Send + 'static,
{
    subtasks.spawn(SUBTASK_NAME, subtask)
}

async fn run_continuous_projection_invalidation_derivation(
    pool: PgPool,
    poll_interval_secs: u64,
) -> Result<()> {
    let poll_interval = Duration::from_secs(poll_interval_secs.max(1));
    info!(
        service = "worker",
        projection_apply = true,
        "continuous projection invalidation derive loop started"
    );

    loop {
        let mut progressed = false;
        match projection_apply::derive_once(&pool).await {
            Ok(summary) => {
                progressed = summary.scanned_event_count > 0;
                if progressed {
                    info!(
                        service = "worker",
                        projection_apply = true,
                        scanned_event_count = summary.scanned_event_count,
                        enqueued_invalidation_count = summary.enqueued_invalidation_count,
                        "continuous projection invalidation derive iteration completed"
                    );
                }
            }
            Err(error) => {
                warn!(
                    service = "worker",
                    projection_apply = true,
                    error = %format!("{error:#}"),
                    "continuous projection invalidation derive iteration failed"
                );
            }
        }

        if !progressed {
            sleep(poll_interval).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{str::FromStr, sync::Arc};

    use anyhow::Context;
    use bigname_storage::DatabaseConfig;
    use bigname_test_support::{TestDatabase, TestDatabaseConfig};
    use sqlx::{ConnectOptions, postgres::PgConnectOptions};
    use tokio::{sync::Notify, time::timeout};

    use super::*;
    use crate::{cli::HealthcheckArgs, healthcheck};

    fn healthcheck_args(database: &TestDatabase, instance_id: &str) -> Result<HealthcheckArgs> {
        let database_url =
            PgConnectOptions::from_str(&bigname_test_support::database_url_from_env())
                .context("failed to parse worker liveness test database URL")?
                .database(database.database_name())
                .to_url_lossy()
                .to_string();
        Ok(HealthcheckArgs {
            database: DatabaseConfig {
                database_url: Some(database_url),
                max_connections: 2,
            },
            heartbeat_instance_id: Some(instance_id.to_owned()),
            heartbeat_max_age_secs: 1,
            rebuild_phase_max_age_secs: bigname_storage::DEFAULT_WORKER_REBUILD_PHASE_MAX_AGE_SECS,
        })
    }

    async fn panic_when_released(release: Arc<Notify>) -> Result<()> {
        release.notified().await;
        panic!("injected invalidation derivation panic");
    }

    #[tokio::test]
    async fn invalidation_derive_panic_stops_worker_liveness() -> Result<()> {
        let database = TestDatabase::create_migrated(
            TestDatabaseConfig::new("bigname_worker_invalidation_supervision_test"),
            &bigname_storage::MIGRATOR,
            "failed to migrate invalidation supervision test database",
        )
        .await?;
        let instance_id = "invalidation-supervision-test";
        bigname_storage::register_service_loop(
            database.pool(),
            bigname_storage::WORKER_SERVICE_NAME,
            instance_id,
        )
        .await?;
        bigname_storage::begin_service_loop_phase(
            database.pool(),
            bigname_storage::WORKER_SERVICE_NAME,
            instance_id,
            "test_invalidation_supervision",
        )
        .await?;

        let (subtasks, monitor) = super::super::subtask_supervision::channel("worker");
        let release = Arc::new(Notify::new());
        spawn_future(&subtasks, panic_when_released(Arc::clone(&release)))?;
        let parent_pool = database.pool().clone();
        let parent_instance_id = instance_id.to_owned();
        let parent = tokio::spawn(async move {
            super::super::shutdown::run_until_shutdown(
                &parent_pool,
                &parent_instance_id,
                monitor.run(std::future::pending::<Result<()>>()),
                std::future::pending::<Result<()>>(),
            )
            .await
        });

        sleep(Duration::from_millis(100)).await;
        healthcheck::healthcheck(healthcheck_args(&database, instance_id)?).await?;
        release.notify_one();
        let parent_result = timeout(Duration::from_secs(2), parent)
            .await
            .context("worker did not detect the panicked invalidation subtask")?
            .context("worker supervision task panicked")?;
        let error = parent_result.expect_err("worker parent loop must fail after subtask panic");
        assert!(
            error.to_string().contains(SUBTASK_NAME) && error.to_string().contains("panicked"),
            "unexpected supervision error: {error:#}"
        );

        sqlx::query(
            r#"
            UPDATE service_loop_heartbeats
            SET started_at = clock_timestamp() - INTERVAL '3 seconds',
                heartbeat_at = clock_timestamp() - INTERVAL '2 seconds'
            WHERE service_name = 'worker'
              AND instance_id = $1
              AND scope_kind = 'process'
            "#,
        )
        .bind(instance_id)
        .execute(database.pool())
        .await?;
        let health_error = healthcheck::healthcheck(healthcheck_args(&database, instance_id)?)
            .await
            .expect_err("stopped worker heartbeat must become unhealthy");
        assert!(
            health_error.to_string().contains("never started"),
            "unexpected worker health error: {health_error:#}"
        );

        database.cleanup().await
    }
}
