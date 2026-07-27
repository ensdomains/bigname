use anyhow::Result;
use bigname_test_support::{TestDatabase, TestDatabaseConfig};

use super::*;

const PROFILE: &str = "startup-test";
const CHAIN: &str = "startup-chain";
const ADAPTER: &str = "test-startup-adapter";

async fn database(name: &str) -> Result<TestDatabase> {
    let database = TestDatabase::create_migrated(
        TestDatabaseConfig::new(name),
        &crate::MIGRATOR,
        "failed to migrate startup adapter checkpoint test database",
    )
    .await?;
    sqlx::query(
        "INSERT INTO raw_log_staging_input_revisions (
             chain_id,
             revision,
             retention_generation,
             retained_history_complete,
             incomplete_since
         ) VALUES ($1, 7, 3, FALSE, now())",
    )
    .bind(CHAIN)
    .execute(database.pool())
    .await?;
    sqlx::query("INSERT INTO discovery_admission_epochs (chain_id, epoch) VALUES ($1, 11)")
        .bind(CHAIN)
        .execute(database.pool())
        .await?;
    Ok(database)
}

async fn complete(database: &TestDatabase, version: i64) -> Result<StartupAdapterSyncKey> {
    let StartupAdapterSyncDecision::RunFullSync { started_key } =
        prepare_startup_adapter_sync(database.pool(), PROFILE, CHAIN, ADAPTER, version).await?
    else {
        panic!("fresh startup adapter checkpoint must run");
    };
    let started_key = started_key.expect("fixture key must be fully known");
    assert_eq!(
        complete_startup_adapter_sync(
            database.pool(),
            PROFILE,
            CHAIN,
            ADAPTER,
            version,
            Some(started_key),
        )
        .await?,
        StartupAdapterSyncCompletion::Completed
    );
    Ok(started_key)
}

#[tokio::test]
async fn completed_startup_adapter_checkpoint_reuses_only_an_exact_key() -> Result<()> {
    let database = database("startup_adapter_exact_key").await?;
    let original = complete(&database, 1).await?;

    assert_eq!(
        prepare_startup_adapter_sync(database.pool(), PROFILE, CHAIN, ADAPTER, 1).await?,
        StartupAdapterSyncDecision::ReuseCompleted,
        "an unchanged second boot must take the cheap completed-row verification"
    );

    sqlx::query(
        "UPDATE raw_log_staging_input_revisions SET revision = revision + 1 WHERE chain_id = $1",
    )
    .bind(CHAIN)
    .execute(database.pool())
    .await?;
    assert!(matches!(
        prepare_startup_adapter_sync(database.pool(), PROFILE, CHAIN, ADAPTER, 1).await?,
        StartupAdapterSyncDecision::RunFullSync {
            started_key: Some(key)
        } if key.raw_log_input_version.revision == original.raw_log_input_version.revision + 1
    ));

    sqlx::query(
        "UPDATE raw_log_staging_input_revisions
         SET retention_generation = retention_generation + 1
         WHERE chain_id = $1",
    )
    .bind(CHAIN)
    .execute(database.pool())
    .await?;
    assert!(matches!(
        prepare_startup_adapter_sync(database.pool(), PROFILE, CHAIN, ADAPTER, 1).await?,
        StartupAdapterSyncDecision::RunFullSync {
            started_key: Some(key)
        } if key.raw_log_input_version.retention_generation
            == original.raw_log_input_version.retention_generation + 1
    ));

    assert!(matches!(
        prepare_startup_adapter_sync(database.pool(), PROFILE, CHAIN, ADAPTER, 2).await?,
        StartupAdapterSyncDecision::RunFullSync {
            started_key: Some(key)
        } if key.adapter_semantic_version == 2
    ));

    database.cleanup().await
}

#[tokio::test]
async fn startup_adapter_checkpoint_fails_closed_on_missing_partial_and_skewed_state() -> Result<()>
{
    let database = database("startup_adapter_fail_closed").await?;
    complete(&database, 1).await?;

    sqlx::query(
        "UPDATE normalized_replay_adapter_checkpoints
         SET status = 'running', completed_at = NULL
         WHERE deployment_profile = $1 AND chain_id = $2 AND adapter = $3",
    )
    .bind(PROFILE)
    .bind(CHAIN)
    .bind(ADAPTER)
    .execute(database.pool())
    .await?;
    assert!(matches!(
        prepare_startup_adapter_sync(database.pool(), PROFILE, CHAIN, ADAPTER, 1).await?,
        StartupAdapterSyncDecision::RunFullSync { .. }
    ));

    complete(&database, 1).await?;
    sqlx::query(
        "UPDATE normalized_replay_adapter_checkpoints
         SET schema_migration_count = schema_migration_count - 1
         WHERE deployment_profile = $1 AND chain_id = $2 AND adapter = $3",
    )
    .bind(PROFILE)
    .bind(CHAIN)
    .bind(ADAPTER)
    .execute(database.pool())
    .await?;
    assert!(matches!(
        prepare_startup_adapter_sync(database.pool(), PROFILE, CHAIN, ADAPTER, 1).await?,
        StartupAdapterSyncDecision::RunFullSync { .. }
    ));

    complete(&database, 1).await?;
    sqlx::query(
        "UPDATE normalized_replay_adapter_checkpoints
         SET adapter_semantic_version = NULL
         WHERE deployment_profile = $1 AND chain_id = $2 AND adapter = $3",
    )
    .bind(PROFILE)
    .bind(CHAIN)
    .bind(ADAPTER)
    .execute(database.pool())
    .await?;
    assert!(matches!(
        prepare_startup_adapter_sync(database.pool(), PROFILE, CHAIN, ADAPTER, 1).await?,
        StartupAdapterSyncDecision::RunFullSync { .. }
    ));

    complete(&database, 1).await?;
    sqlx::query(
        "UPDATE normalized_replay_adapter_checkpoints
         SET state_payload = state_payload - $4
         WHERE deployment_profile = $1 AND chain_id = $2 AND adapter = $3",
    )
    .bind(PROFILE)
    .bind(CHAIN)
    .bind(ADAPTER)
    .bind(STARTUP_DISCOVERY_ADMISSION_EPOCH_FIELD)
    .execute(database.pool())
    .await?;
    assert!(matches!(
        prepare_startup_adapter_sync(database.pool(), PROFILE, CHAIN, ADAPTER, 1).await?,
        StartupAdapterSyncDecision::RunFullSync { .. }
    ));

    sqlx::query(
        "DELETE FROM normalized_replay_adapter_checkpoints
         WHERE deployment_profile = $1 AND chain_id = $2 AND adapter = $3",
    )
    .bind(PROFILE)
    .bind(CHAIN)
    .bind(ADAPTER)
    .execute(database.pool())
    .await?;
    assert!(matches!(
        prepare_startup_adapter_sync(database.pool(), PROFILE, CHAIN, ADAPTER, 1).await?,
        StartupAdapterSyncDecision::RunFullSync { .. }
    ));

    sqlx::query("DELETE FROM raw_log_staging_input_revisions WHERE chain_id = $1")
        .bind(CHAIN)
        .execute(database.pool())
        .await?;
    let StartupAdapterSyncDecision::RunFullSync { started_key } =
        prepare_startup_adapter_sync(database.pool(), PROFILE, CHAIN, ADAPTER, 1).await?
    else {
        panic!("an unknown raw-log input must never reuse completion");
    };
    assert_eq!(started_key, None);
    assert_eq!(
        complete_startup_adapter_sync(database.pool(), PROFILE, CHAIN, ADAPTER, 1, started_key,)
            .await?,
        StartupAdapterSyncCompletion::KeyUnknown
    );

    sqlx::query(
        "INSERT INTO raw_log_staging_input_revisions (
             chain_id,
             revision,
             retention_generation,
             retained_history_complete,
             incomplete_since
         ) VALUES ($1, 7, 3, FALSE, now())",
    )
    .bind(CHAIN)
    .execute(database.pool())
    .await?;
    sqlx::query("DROP TABLE _sqlx_migrations")
        .execute(database.pool())
        .await?;
    let StartupAdapterSyncDecision::RunFullSync { started_key } =
        prepare_startup_adapter_sync(database.pool(), PROFILE, CHAIN, ADAPTER, 1).await?
    else {
        panic!("unknown migration state must never reuse completion");
    };
    assert_eq!(started_key, None);

    database.cleanup().await
}

#[tokio::test]
async fn startup_adapter_checkpoint_rechecks_the_key_before_completion() -> Result<()> {
    let database = database("startup_adapter_completion_fence").await?;
    let StartupAdapterSyncDecision::RunFullSync { started_key } =
        prepare_startup_adapter_sync(database.pool(), PROFILE, CHAIN, ADAPTER, 1).await?
    else {
        panic!("fresh startup adapter checkpoint must run");
    };

    sqlx::query("UPDATE discovery_admission_epochs SET epoch = epoch + 1 WHERE chain_id = $1")
        .bind(CHAIN)
        .execute(database.pool())
        .await?;
    assert_eq!(
        complete_startup_adapter_sync(database.pool(), PROFILE, CHAIN, ADAPTER, 1, started_key,)
            .await?,
        StartupAdapterSyncCompletion::InputChanged
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*)::BIGINT
             FROM normalized_replay_adapter_checkpoints
             WHERE deployment_profile = $1 AND chain_id = $2 AND adapter = $3",
        )
        .bind(PROFILE)
        .bind(CHAIN)
        .bind(ADAPTER)
        .fetch_one(database.pool())
        .await?,
        0,
        "a drifted scan must not publish reusable completion"
    );

    database.cleanup().await
}
