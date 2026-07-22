use std::{path::PathBuf, str::FromStr, sync::Arc};

use anyhow::{Context, Result};
use bigname_storage::DatabaseConfig;
use bigname_test_support::{TestDatabase, TestDatabaseConfig};
use sqlx::{ConnectOptions, PgPool, postgres::PgConnectOptions};
use tokio::{
    sync::Notify,
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
