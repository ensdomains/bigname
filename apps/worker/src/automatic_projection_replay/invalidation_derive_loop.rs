use std::future::Future;

use anyhow::Result;
use sqlx::PgPool;
use tokio::time::{Duration, sleep};
use tracing::{info, warn};

use super::subtask_supervision::SubtaskSpawner;
use crate::{
    primary_name::rebuild_heartbeat::{LoopHeartbeat, RequiredSubtaskActivity},
    projection_apply,
};

const SUBTASK_NAME: &str = "projection_invalidation_derivation";

pub(super) fn spawn(
    subtasks: &SubtaskSpawner,
    pool: PgPool,
    poll_interval_secs: u64,
    loop_heartbeat: LoopHeartbeat,
    activity: RequiredSubtaskActivity,
) -> Result<()> {
    spawn_future(
        subtasks,
        run_continuous_projection_invalidation_derivation(
            pool,
            poll_interval_secs,
            loop_heartbeat,
            activity,
        ),
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
    mut loop_heartbeat: LoopHeartbeat,
    activity: RequiredSubtaskActivity,
) -> Result<()> {
    let poll_interval = Duration::from_secs(poll_interval_secs.max(1));
    info!(
        service = "worker",
        projection_apply = true,
        "continuous projection invalidation derive loop started"
    );

    loop {
        let mut progressed = false;
        match run_invalidation_derivation_iteration(&pool, &mut loop_heartbeat, &activity).await {
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
                if bigname_storage::projection_staging::is_outdated_projection_replay_version_error(
                    &error,
                ) {
                    return Err(error);
                }
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

async fn run_invalidation_derivation_iteration(
    pool: &PgPool,
    loop_heartbeat: &mut LoopHeartbeat,
    activity: &RequiredSubtaskActivity,
) -> Result<crate::projection_apply::ProjectionInvalidationDeriveSummary> {
    let _activity = activity.begin().await;
    projection_apply::derive_once_with_heartbeat(pool, loop_heartbeat).await
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

    #[tokio::test]
    async fn invalidation_derive_wedge_is_not_masked_by_parent_heartbeat() -> Result<()> {
        let database = TestDatabase::create_migrated(
            TestDatabaseConfig::new("bigname_worker_invalidation_wedge_test"),
            &bigname_storage::MIGRATOR,
            "failed to migrate invalidation wedge test database",
        )
        .await?;
        let instance_id = "invalidation-wedge-test";
        bigname_storage::register_service_loop(
            database.pool(),
            bigname_storage::WORKER_SERVICE_NAME,
            instance_id,
        )
        .await?;
        sqlx::query(
            r#"
            UPDATE service_loop_heartbeats
            SET started_at = clock_timestamp() - INTERVAL '2 minutes',
                heartbeat_at = clock_timestamp() - INTERVAL '1 minute'
            WHERE service_name = 'worker'
              AND instance_id = $1
              AND scope_kind = 'process'
            "#,
        )
        .bind(instance_id)
        .execute(database.pool())
        .await?;

        let (first_progress_change_id, later_unit_blocker) =
            crate::projection_apply::heartbeat_tests::seed_blocked_later_progress_unit(&database)
                .await?;
        let activity = RequiredSubtaskActivity::default();
        let parent_activity = activity.clone();
        let derive_pool = database.pool().clone();
        let derive = tokio::spawn(async move {
            let mut heartbeat = LoopHeartbeat::new(instance_id.to_owned(), Duration::ZERO);
            run_invalidation_derivation_iteration(&derive_pool, &mut heartbeat, &activity).await
        });
        timeout(
            Duration::from_secs(10),
            crate::projection_apply::heartbeat_tests::wait_for_derive_cursor(
                &database,
                first_progress_change_id,
            ),
        )
        .await
        .context("spawned invalidation derive did not commit a bounded progress unit")??;
        timeout(
            Duration::from_secs(10),
            crate::projection_apply::heartbeat_tests::wait_for_fresh_worker_heartbeat(
                &database,
                instance_id,
            ),
        )
        .await
        .context("spawned invalidation derive did not record progress")??;
        assert!(
            !derive.is_finished(),
            "the later invalidation derive unit must remain blocked"
        );
        sqlx::query(
            r#"
            UPDATE service_loop_heartbeats
            SET started_at = clock_timestamp() - INTERVAL '2 minutes',
                heartbeat_at = clock_timestamp() - INTERVAL '1 minute'
            WHERE service_name = 'worker'
              AND instance_id = $1
              AND scope_kind = 'process'
            "#,
        )
        .bind(instance_id)
        .execute(database.pool())
        .await?;
        let parent_pool = database.pool().clone();
        let parent = tokio::spawn(async move {
            let _required_subtask_exclusion = parent_activity.exclude_required_subtask().await;
            let mut parent_heartbeat = crate::primary_name::rebuild_heartbeat::LoopHeartbeat::new(
                instance_id.to_owned(),
                Duration::ZERO,
            );
            parent_heartbeat.record_if_due(&parent_pool).await;
        });
        sleep(Duration::from_millis(100)).await;
        assert!(
            !parent.is_finished(),
            "the parent heartbeat must wait while derive owns required-operation liveness"
        );
        let heartbeat_age = bigname_storage::load_service_loop_heartbeat(
            database.pool(),
            bigname_storage::WORKER_SERVICE_NAME,
            instance_id,
        )
        .await?
        .context("worker heartbeat must remain registered")?
        .age_seconds;
        later_unit_blocker.commit().await?;
        let summary = timeout(Duration::from_secs(10), derive)
            .await
            .context("spawned invalidation derive did not finish after release")?
            .context("spawned invalidation derive task failed")??;
        assert_eq!(
            summary.scanned_event_count,
            crate::projection_apply::NORMALIZED_EVENT_DERIVE_PROGRESS_LIMIT + 1
        );
        timeout(Duration::from_secs(10), parent)
            .await
            .context("parent heartbeat did not resume after derive completed")?
            .context("parent heartbeat task panicked")?;
        database.cleanup().await?;

        assert!(
            heartbeat_age >= 30,
            "the parent loop must not refresh liveness while required invalidation derivation is wedged"
        );
        Ok(())
    }

    #[tokio::test]
    async fn invalidation_derive_progress_does_not_mask_a_parent_wedge() -> Result<()> {
        let database = TestDatabase::create_migrated(
            TestDatabaseConfig::new("bigname_worker_parent_wedge_test"),
            &bigname_storage::MIGRATOR,
            "failed to migrate worker parent-wedge test database",
        )
        .await?;
        let instance_id = "worker-parent-wedge-test";
        bigname_storage::register_service_loop(
            database.pool(),
            bigname_storage::WORKER_SERVICE_NAME,
            instance_id,
        )
        .await?;
        sqlx::query(
            r#"
            UPDATE service_loop_heartbeats
            SET started_at = clock_timestamp() - INTERVAL '2 minutes',
                heartbeat_at = clock_timestamp() - INTERVAL '1 minute'
            WHERE service_name = 'worker'
              AND instance_id = $1
              AND scope_kind = 'process'
            "#,
        )
        .bind(instance_id)
        .execute(database.pool())
        .await?;

        let activity = RequiredSubtaskActivity::default();
        let parent_operation = activity.exclude_required_subtask().await;
        let child_activity = activity.clone();
        let child_pool = database.pool().clone();
        let child = tokio::spawn(async move {
            let _active_derive = child_activity.begin().await;
            let mut child_heartbeat = LoopHeartbeat::new(instance_id.to_owned(), Duration::ZERO);
            child_heartbeat.record_if_due(&child_pool).await;
        });
        sleep(Duration::from_millis(100)).await;
        assert!(
            !child.is_finished(),
            "required derive must wait while the parent apply loop owns liveness"
        );
        let heartbeat_age = bigname_storage::load_service_loop_heartbeat(
            database.pool(),
            bigname_storage::WORKER_SERVICE_NAME,
            instance_id,
        )
        .await?
        .context("worker heartbeat must remain registered")?
        .age_seconds;
        assert!(
            heartbeat_age >= 30,
            "required derive progress must not refresh liveness while the parent apply loop is wedged"
        );
        drop(parent_operation);
        timeout(Duration::from_secs(2), child)
            .await
            .context("derive heartbeat did not resume after the parent released ownership")?
            .context("derive heartbeat task panicked")?;
        database.cleanup().await?;
        Ok(())
    }
}
