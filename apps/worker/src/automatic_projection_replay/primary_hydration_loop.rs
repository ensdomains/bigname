use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

use sqlx::PgPool;
use tokio::sync::Mutex;
use tokio::time::{Duration, sleep};
use tracing::{info, warn};

use super::{SharedLoopHeartbeat, primary_hydration, subtask_supervision::SubtaskSpawner};
use crate::{
    primary_name::{self, rebuild_heartbeat::RequiredSubtaskActivity},
    projection_apply,
};

pub(super) fn background_primary_hydration_config(
    config: &Option<primary_name::PrimaryNameLegacyReverseHydrationConfig>,
    primary_hydration_started: bool,
) -> Option<primary_name::PrimaryNameLegacyReverseHydrationConfig> {
    (!primary_hydration_started)
        .then(|| config.clone())
        .flatten()
}

#[expect(clippy::too_many_arguments)]
pub(super) fn spawn(
    subtasks: &SubtaskSpawner,
    pool: PgPool,
    loop_heartbeat: SharedLoopHeartbeat,
    poll_interval_secs: u64,
    config: primary_name::PrimaryNameLegacyReverseHydrationConfig,
    projection_apply_generation: Arc<AtomicU64>,
    projection_apply_hydration_lock: Arc<Mutex<()>>,
    required_subtask_activity: RequiredSubtaskActivity,
) -> anyhow::Result<()> {
    subtasks.spawn("primary_name_legacy_reverse_hydration", async move {
        run(
            pool,
            loop_heartbeat,
            poll_interval_secs,
            config,
            projection_apply_generation,
            projection_apply_hydration_lock,
            required_subtask_activity,
        )
        .await;
        Ok(())
    })
}

async fn run(
    pool: PgPool,
    loop_heartbeat: SharedLoopHeartbeat,
    poll_interval_secs: u64,
    config: primary_name::PrimaryNameLegacyReverseHydrationConfig,
    projection_apply_generation: Arc<AtomicU64>,
    projection_apply_hydration_lock: Arc<Mutex<()>>,
    required_subtask_activity: RequiredSubtaskActivity,
) {
    let poll_interval = Duration::from_secs(poll_interval_secs.max(1));
    let mut bootstrap_completed = false;
    let mut last_trigger = primary_hydration::LegacyReverseHydrationTriggerState::default();
    let mut hydrated_projection_generation = projection_apply_generation.load(Ordering::Acquire);

    info!(
        service = "worker",
        projection = "primary_names_current",
        "primary_names_current legacy reverse-resolver hydration loop started"
    );

    loop {
        let mut progressed = false;
        let required_subtask_exclusion = required_subtask_activity.exclude_required_subtask().await;
        let apply_hydration_guard = projection_apply_hydration_lock.lock().await;
        match projection_apply::has_primary_hydration_blocking_work(&pool).await {
            Ok(true) => {
                drop(apply_hydration_guard);
                drop(required_subtask_exclusion);
                sleep(poll_interval).await;
                continue;
            }
            Ok(false) => {}
            Err(error) => {
                warn!(
                    service = "worker",
                    projection = "primary_names_current",
                    error = %format!("{error:#}"),
                    "failed to inspect projection apply work before primary-name hydration"
                );
                drop(apply_hydration_guard);
                drop(required_subtask_exclusion);
                sleep(poll_interval).await;
                continue;
            }
        }

        if !bootstrap_completed {
            let hydration_generation = projection_apply_generation.load(Ordering::Acquire);
            let hydration_result = {
                let mut loop_heartbeat = loop_heartbeat.lock().await;
                primary_hydration::hydrate_after_bootstrap(
                    &pool,
                    Some(&config),
                    &mut last_trigger,
                    &mut loop_heartbeat,
                )
                .await
            };
            match hydration_result {
                Ok(summary) => {
                    bootstrap_completed = summary.failed_lookup_count == 0;
                    progressed |= primary_hydration::bootstrap_hydration_made_progress(&summary);
                    if bootstrap_completed {
                        hydrated_projection_generation = hydration_generation;
                    }
                }
                Err(error) => {
                    warn!(
                        service = "worker",
                        projection = "primary_names_current",
                        error = %format!("{error:#}"),
                        "automatic primary_names_current legacy reverse-resolver bootstrap hydration failed"
                    );
                }
            }
        } else {
            let current_generation = projection_apply_generation.load(Ordering::Acquire);
            let mut projection_apply_changed = current_generation != hydrated_projection_generation;
            let hydration_result = {
                let mut loop_heartbeat = loop_heartbeat.lock().await;
                primary_hydration::hydrate_if_projection_changed_or_triggered(
                    &pool,
                    Some(&config),
                    &mut last_trigger,
                    &mut projection_apply_changed,
                    &mut loop_heartbeat,
                )
                .await
            };
            match hydration_result {
                Ok(summary) => {
                    if !projection_apply_changed {
                        hydrated_projection_generation = current_generation;
                    }
                    progressed |= summary.upserted_row_count > 0 || summary.deleted_row_count > 0;
                }
                Err(error) => {
                    warn!(
                        service = "worker",
                        projection = "primary_names_current",
                        error = %format!("{error:#}"),
                        "automatic primary_names_current legacy reverse-resolver hydration failed"
                    );
                }
            }
        }
        drop(apply_hydration_guard);
        drop(required_subtask_exclusion);

        if !progressed {
            sleep(poll_interval).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use anyhow::{Context, Result};
    use bigname_test_support::{TestDatabase, TestDatabaseConfig};
    use tokio::sync::Notify;

    use super::*;

    async fn test_database() -> Result<TestDatabase> {
        TestDatabase::create_migrated(
            TestDatabaseConfig::new("bigname_worker_primary_hydration_loop_test"),
            &bigname_storage::MIGRATOR,
            "failed to apply migrations for primary hydration loop tests",
        )
        .await
    }

    #[tokio::test]
    async fn idle_primary_hydration_does_not_mask_a_stalled_main_loop() -> Result<()> {
        let database = test_database().await?;
        let instance_id = "idle-primary-hydration-heartbeat-test";
        bigname_storage::register_service_loop(
            database.pool(),
            bigname_storage::WORKER_SERVICE_NAME,
            instance_id,
        )
        .await?;

        let mut config = primary_name::PrimaryNameLegacyReverseHydrationConfig::new(
            bigname_execution::ChainRpcUrls::default(),
        );
        config.resolver_addresses.clear();
        let hydration_task = tokio::spawn(run(
            database.pool().clone(),
            Arc::new(Mutex::new(
                crate::primary_name::rebuild_heartbeat::LoopHeartbeat::new(
                    instance_id.to_owned(),
                    Duration::from_secs(1),
                ),
            )),
            1,
            config,
            Arc::new(AtomicU64::new(0)),
            Arc::new(Mutex::new(())),
            RequiredSubtaskActivity::default(),
        ));

        sleep(Duration::from_millis(250)).await;
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
        sleep(Duration::from_millis(1_250)).await;

        let heartbeat_age = bigname_storage::load_service_loop_heartbeat(
            database.pool(),
            bigname_storage::WORKER_SERVICE_NAME,
            instance_id,
        )
        .await?
        .context("worker heartbeat must remain registered")?
        .age_seconds;
        hydration_task.abort();
        let _ = hydration_task.await;
        database.cleanup().await?;

        assert!(
            heartbeat_age >= 30,
            "an idle detached hydration loop refreshed the worker heartbeat and masked a stalled main loop"
        );
        Ok(())
    }

    #[tokio::test]
    async fn primary_hydration_preserves_an_active_shared_heartbeat_phase() -> Result<()> {
        let database = test_database().await?;
        let instance_id = "primary-hydration-phase-owner-test";
        bigname_storage::register_service_loop(
            database.pool(),
            bigname_storage::WORKER_SERVICE_NAME,
            instance_id,
        )
        .await?;

        let loop_heartbeat = Arc::new(Mutex::new(
            crate::primary_name::rebuild_heartbeat::LoopHeartbeat::new(
                instance_id.to_owned(),
                Duration::ZERO,
            ),
        ));
        let phase_started = Arc::new(Notify::new());
        let phase_release = Arc::new(Notify::new());
        let phase_task = tokio::spawn({
            let pool = database.pool().clone();
            let loop_heartbeat = Arc::clone(&loop_heartbeat);
            let phase_started = Arc::clone(&phase_started);
            let phase_release = Arc::clone(&phase_release);
            async move {
                let mut loop_heartbeat = loop_heartbeat.lock().await;
                loop_heartbeat
                    .run_phase(&pool, "test_primary_hydration_serialization", async move {
                        phase_started.notify_one();
                        phase_release.notified().await;
                        Ok(())
                    })
                    .await
            }
        });
        phase_started.notified().await;

        let mut config = primary_name::PrimaryNameLegacyReverseHydrationConfig::new(
            bigname_execution::ChainRpcUrls::default(),
        );
        config.resolver_addresses.clear();
        let projection_apply_hydration_lock = Arc::new(Mutex::new(()));
        let hydration_task = tokio::spawn(run(
            database.pool().clone(),
            Arc::clone(&loop_heartbeat),
            1,
            config,
            Arc::new(AtomicU64::new(0)),
            Arc::clone(&projection_apply_hydration_lock),
            RequiredSubtaskActivity::default(),
        ));
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if projection_apply_hydration_lock.try_lock().is_err() {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .context("primary hydration did not enter its serialized work section")?;

        let active_phase = bigname_storage::load_service_loop_heartbeat(
            database.pool(),
            bigname_storage::WORKER_SERVICE_NAME,
            instance_id,
        )
        .await?
        .context("worker heartbeat must remain registered")?
        .active_phase
        .map(|phase| phase.phase);

        hydration_task.abort();
        let _ = hydration_task.await;
        phase_release.notify_one();
        phase_task
            .await
            .context("shared heartbeat phase task failed to join")??;
        database.cleanup().await?;

        assert_eq!(
            active_phase.as_deref(),
            Some("test_primary_hydration_serialization"),
            "primary hydration must not retire a phase owned by another shared-heartbeat operation"
        );
        Ok(())
    }
}
