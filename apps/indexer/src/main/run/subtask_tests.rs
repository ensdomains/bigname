use std::{path::PathBuf, str::FromStr, sync::Arc};

use anyhow::{Context, Result};
use bigname_storage::DatabaseConfig;
use bigname_test_support::{TestDatabase, TestDatabaseConfig};
use sqlx::{ConnectOptions, PgPool, postgres::PgConnectOptions};
use tokio::{
    sync::{Notify, oneshot},
    time::{Duration, sleep, timeout},
};

use super::*;
use crate::{cli::HealthcheckArgs, healthcheck};

fn healthcheck_args(database: &TestDatabase, instance_id: &str) -> Result<HealthcheckArgs> {
    let database_url = PgConnectOptions::from_str(&bigname_test_support::database_url_from_env())
        .context("failed to parse indexer liveness test database URL")?
        .database(database.database_name())
        .to_url_lossy()
        .to_string();
    Ok(HealthcheckArgs {
        database: DatabaseConfig {
            database_url: Some(database_url),
            max_connections: 2,
        },
        manifests_root: PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../manifests/mainnet"),
        heartbeat_instance_id: Some(instance_id.to_owned()),
        heartbeat_max_age_secs: 1,
    })
}

#[test]
fn indexer_run_rejects_a_pool_without_a_progress_heartbeat_connection() {
    let error = ensure_indexer_run_pool_capacity(&DatabaseConfig {
        database_url: None,
        max_connections: 2,
    })
    .expect_err("the runtime guard, nested work guards, and progress heartbeat need four slots");
    assert!(
        error
            .to_string()
            .contains("at least 4 database connections"),
        "unexpected pool-capacity error: {error:#}"
    );

    let error = ensure_indexer_run_pool_capacity(&DatabaseConfig {
        database_url: None,
        max_connections: 3,
    })
    .expect_err("three slots still starve a heartbeat behind two nested work guards");
    assert!(
        error
            .to_string()
            .contains("at least 4 database connections"),
        "unexpected pool-capacity error: {error:#}"
    );

    ensure_indexer_run_pool_capacity(&DatabaseConfig {
        database_url: None,
        max_connections: 4,
    })
    .expect("four connections cover the runtime guard, nested work guards, and heartbeat");
}

async fn record_parent_heartbeat(pool: PgPool, instance_id: String) -> Result<()> {
    let chains = vec!["ethereum-mainnet".to_owned()];
    loop {
        bigname_storage::record_service_loop_heartbeat(
            &pool,
            bigname_storage::INDEXER_SERVICE_NAME,
            &instance_id,
            &chains,
        )
        .await?;
        sleep(Duration::from_millis(25)).await;
    }
}

async fn panic_when_released(release: Arc<Notify>) -> Result<()> {
    release.notified().await;
    panic!("injected normalized replay catch-up panic");
}

#[tokio::test]
async fn normalized_replay_catchup_panic_stops_indexer_liveness() -> Result<()> {
    let database = TestDatabase::create_migrated(
        TestDatabaseConfig::new("bigname_indexer_catchup_supervision_test"),
        &bigname_storage::MIGRATOR,
        "failed to migrate catch-up supervision test database",
    )
    .await?;
    let instance_id = "catchup-supervision-test";
    bigname_storage::register_service_loop(
        database.pool(),
        bigname_storage::INDEXER_SERVICE_NAME,
        instance_id,
    )
    .await?;

    let (subtasks, monitor) = subtask_supervision::channel("indexer");
    let release = Arc::new(Notify::new());
    spawn_normalized_replay_catchup(&subtasks, panic_when_released(Arc::clone(&release)))?;
    let parent = tokio::spawn(monitor.run(record_parent_heartbeat(
        database.pool().clone(),
        instance_id.to_owned(),
    )));

    sleep(Duration::from_millis(100)).await;
    healthcheck::healthcheck(healthcheck_args(&database, instance_id)?).await?;
    release.notify_one();
    let parent_result = timeout(Duration::from_secs(2), parent)
        .await
        .context("indexer did not detect the panicked catch-up subtask")?
        .context("indexer supervision task panicked")?;
    let error = parent_result.expect_err("indexer parent loop must fail after subtask panic");
    assert!(
        error
            .to_string()
            .contains(NORMALIZED_REPLAY_CATCHUP_SUBTASK)
            && error.to_string().contains("panicked"),
        "unexpected supervision error: {error:#}"
    );

    sqlx::query(
        r#"
        UPDATE service_loop_heartbeats
        SET started_at = clock_timestamp() - INTERVAL '3 seconds',
            heartbeat_at = clock_timestamp() - INTERVAL '2 seconds'
        WHERE service_name = 'indexer'
          AND instance_id = $1
          AND scope_kind = 'process'
        "#,
    )
    .bind(instance_id)
    .execute(database.pool())
    .await?;
    let health_error = healthcheck::healthcheck(healthcheck_args(&database, instance_id)?)
        .await
        .expect_err("stopped indexer heartbeat must become unhealthy");
    assert!(
        health_error.to_string().contains("stopped or wedged"),
        "unexpected indexer health error: {health_error:#}"
    );

    database.cleanup().await
}

#[tokio::test]
async fn normalized_replay_catchup_wedge_is_not_masked_by_parent_heartbeat() -> Result<()> {
    let database = TestDatabase::create_migrated(
        TestDatabaseConfig::new("bigname_indexer_catchup_wedge_test"),
        &bigname_storage::MIGRATOR,
        "failed to migrate catch-up wedge test database",
    )
    .await?;
    let instance_id = "catchup-wedge-test";
    bigname_storage::register_service_loop(
        database.pool(),
        bigname_storage::INDEXER_SERVICE_NAME,
        instance_id,
    )
    .await?;
    sqlx::query(
        r#"
        UPDATE service_loop_heartbeats
        SET started_at = clock_timestamp() - INTERVAL '2 minutes',
            heartbeat_at = clock_timestamp() - INTERVAL '1 minute'
        WHERE service_name = 'indexer'
          AND instance_id = $1
          AND scope_kind = 'process'
        "#,
    )
    .bind(instance_id)
    .execute(database.pool())
    .await?;

    let activity = RequiredSubtaskActivity::default();
    let wedged_catchup = activity.begin().await;
    let parent_activity = activity.clone();
    let parent_pool = database.pool().clone();
    let parent = tokio::spawn(async move {
        let _required_subtask_exclusion = parent_activity.exclude_required_subtask().await;
        let mut parent_heartbeat = StartupHeartbeat::new(instance_id.to_owned(), Duration::ZERO);
        parent_heartbeat
            .record(&parent_pool, &["ethereum-mainnet".to_owned()])
            .await
    });
    sleep(Duration::from_millis(100)).await;
    assert!(
        !parent.is_finished(),
        "the parent heartbeat must wait behind active required catch-up work"
    );
    let heartbeat_age = bigname_storage::load_service_loop_heartbeat(
        database.pool(),
        bigname_storage::INDEXER_SERVICE_NAME,
        instance_id,
    )
    .await?
    .context("indexer heartbeat must remain registered")?
    .age_seconds;
    assert!(
        heartbeat_age >= 30,
        "the parent poll must not refresh liveness while required catch-up work is wedged"
    );
    drop(wedged_catchup);
    timeout(Duration::from_secs(2), parent)
        .await
        .context("parent heartbeat did not resume after catch-up released ownership")?
        .context("parent heartbeat task panicked")??;
    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn indexer_shutdown_does_not_wait_for_required_subtask_activity() -> Result<()> {
    let activity = RequiredSubtaskActivity::default();
    let wedged_catchup = activity.begin().await;
    let waiting_activity = activity.clone();
    let (shutdown, shutdown_received) = oneshot::channel::<()>();
    let parent = tokio::spawn(async move {
        waiting_activity
            .exclude_required_subtask_or_shutdown(async {
                let _ = shutdown_received.await;
            })
            .await
    });

    sleep(Duration::from_millis(100)).await;
    assert!(
        !parent.is_finished(),
        "the parent must wait while required catch-up work owns the activity gate"
    );
    assert!(
        shutdown.send(()).is_ok(),
        "shutdown receiver must remain live"
    );
    let exclusion = timeout(Duration::from_secs(2), parent)
        .await
        .context("indexer parent did not observe shutdown while catch-up held the activity gate")?
        .context("indexer parent shutdown test task panicked")?;
    assert!(
        exclusion.is_none(),
        "shutdown must win without acquiring the parent activity exclusion"
    );

    drop(wedged_catchup);
    Ok(())
}

#[tokio::test]
async fn normalized_replay_progress_does_not_mask_a_parent_wedge() -> Result<()> {
    let database = TestDatabase::create_migrated(
        TestDatabaseConfig::new("bigname_indexer_parent_wedge_test"),
        &bigname_storage::MIGRATOR,
        "failed to migrate parent-wedge test database",
    )
    .await?;
    let instance_id = "parent-wedge-test";
    bigname_storage::register_service_loop(
        database.pool(),
        bigname_storage::INDEXER_SERVICE_NAME,
        instance_id,
    )
    .await?;
    sqlx::query(
        r#"
        UPDATE service_loop_heartbeats
        SET started_at = clock_timestamp() - INTERVAL '2 minutes',
            heartbeat_at = clock_timestamp() - INTERVAL '1 minute'
        WHERE service_name = 'indexer'
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
        let _active_catchup = child_activity.begin().await;
        let mut child_heartbeat = startup_heartbeat::NormalizedReplayHeartbeat::new(
            instance_id.to_owned(),
            Duration::ZERO,
            vec!["ethereum-mainnet".to_owned()],
        );
        bigname_adapters::StartupAdapterProgress::record(&mut child_heartbeat, &child_pool).await
    });
    sleep(Duration::from_millis(100)).await;
    assert!(
        !child.is_finished(),
        "required catch-up must wait while the parent poll owns liveness"
    );

    let heartbeat_age = bigname_storage::load_service_loop_heartbeat(
        database.pool(),
        bigname_storage::INDEXER_SERVICE_NAME,
        instance_id,
    )
    .await?
    .context("indexer heartbeat must remain registered")?
    .age_seconds;
    assert!(
        heartbeat_age >= 30,
        "required catch-up progress must not refresh liveness while the parent poll is wedged"
    );
    drop(parent_operation);
    timeout(Duration::from_secs(2), child)
        .await
        .context("catch-up heartbeat did not resume after the parent released ownership")?
        .context("catch-up heartbeat task panicked")??;
    database.cleanup().await?;
    Ok(())
}
