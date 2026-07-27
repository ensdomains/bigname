use anyhow::Result;
use bigname_test_support::{TestDatabase, TestDatabaseConfig};
use sqlx::{Acquire, migrate::Migrate};

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
            Some(started_key.clone()),
        )
        .await?,
        StartupAdapterSyncCompletion::Completed
    );
    Ok(started_key)
}

async fn insert_canonical_head(
    database: &TestDatabase,
    block_number: i64,
    block_hash: &str,
) -> Result<()> {
    sqlx::query(
        r#"
        INSERT INTO chain_lineage (
            chain_id,
            block_hash,
            parent_hash,
            block_number,
            block_timestamp,
            canonicality_state
        )
        VALUES ($1, $2, NULL, $3, TO_TIMESTAMP($3), 'canonical')
        "#,
    )
    .bind(CHAIN)
    .bind(block_hash)
    .bind(block_number)
    .execute(database.pool())
    .await?;
    Ok(())
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
async fn canonical_lineage_head_is_part_of_the_exact_reuse_key() -> Result<()> {
    let database = database("startup_adapter_lineage_key").await?;
    let original = complete(&database, 1).await?;
    assert_eq!(original.canonical_lineage_head, None);

    insert_canonical_head(&database, 8, "0xempty-a").await?;
    let StartupAdapterSyncDecision::RunFullSync { started_key } =
        prepare_startup_adapter_sync(database.pool(), PROFILE, CHAIN, ADAPTER, 1).await?
    else {
        panic!("an empty-block lineage advance must invalidate startup reuse");
    };
    let advanced = started_key.expect("lineage advance must retain a known key");
    assert_eq!(
        advanced.canonical_lineage_head,
        Some(StartupCanonicalLineageHead {
            block_number: 8,
            block_hash: "0xempty-a".to_owned(),
        })
    );
    assert_eq!(
        advanced.raw_log_input_version, original.raw_log_input_version,
        "lineage movement must be detected without relying on a raw-log revision bump"
    );
    assert_eq!(
        complete_startup_adapter_sync(database.pool(), PROFILE, CHAIN, ADAPTER, 1, Some(advanced),)
            .await?,
        StartupAdapterSyncCompletion::Completed
    );

    sqlx::query(
        "UPDATE chain_lineage
         SET canonicality_state = 'orphaned'
         WHERE chain_id = $1 AND block_hash = '0xempty-a'",
    )
    .bind(CHAIN)
    .execute(database.pool())
    .await?;
    insert_canonical_head(&database, 8, "0xempty-b").await?;
    assert!(matches!(
        prepare_startup_adapter_sync(database.pool(), PROFILE, CHAIN, ADAPTER, 1).await?,
        StartupAdapterSyncDecision::RunFullSync {
            started_key: Some(StartupAdapterSyncKey {
                canonical_lineage_head: Some(StartupCanonicalLineageHead {
                    block_number: 8,
                    ref block_hash,
                }),
                ..
            })
        } if block_hash == "0xempty-b"
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

    complete(&database, 1).await?;
    sqlx::query(
        "UPDATE normalized_replay_adapter_checkpoints
         SET state_payload = state_payload - $4
         WHERE deployment_profile = $1 AND chain_id = $2 AND adapter = $3",
    )
    .bind(PROFILE)
    .bind(CHAIN)
    .bind(ADAPTER)
    .bind(STARTUP_CANONICAL_LINEAGE_HEAD_FIELD)
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

#[tokio::test]
async fn startup_adapter_completion_rechecks_canonical_lineage() -> Result<()> {
    let database = database("startup_adapter_lineage_completion_fence").await?;
    insert_canonical_head(&database, 8, "0xempty-a").await?;
    let StartupAdapterSyncDecision::RunFullSync { started_key } =
        prepare_startup_adapter_sync(database.pool(), PROFILE, CHAIN, ADAPTER, 1).await?
    else {
        panic!("fresh startup adapter checkpoint must run");
    };

    insert_canonical_head(&database, 9, "0xempty-b").await?;
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
        0
    );

    database.cleanup().await
}

#[tokio::test]
async fn startup_waits_on_the_migrator_lock_before_the_ledger_or_checkpoint_table() -> Result<()> {
    let database = database("startup_adapter_migration_lock_order").await?;
    let mut migration_connection = database.pool().acquire().await?;
    Migrate::lock(&mut *migration_connection).await?;
    let mut migration = migration_connection.begin().await?;
    sqlx::query("LOCK TABLE normalized_replay_adapter_checkpoints IN ACCESS EXCLUSIVE MODE")
        .execute(&mut *migration)
        .await?;

    let startup_pool = database.pool().clone();
    let startup = tokio::spawn(async move {
        prepare_startup_adapter_sync(&startup_pool, PROFILE, CHAIN, ADAPTER, 1).await
    });
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    tokio::time::timeout(
        std::time::Duration::from_secs(1),
        sqlx::query(
            "UPDATE _sqlx_migrations
             SET execution_time = execution_time
             WHERE version = (SELECT MAX(version) FROM _sqlx_migrations)",
        )
        .execute(&mut *migration),
    )
    .await
    .expect("migration ledger write must not wait behind startup")?;
    migration.commit().await?;
    Migrate::unlock(&mut *migration_connection).await?;
    drop(migration_connection);

    assert!(matches!(
        tokio::time::timeout(std::time::Duration::from_secs(2), startup)
            .await
            .expect("startup must proceed after the migration fence releases")??,
        StartupAdapterSyncDecision::RunFullSync { .. }
    ));

    database.cleanup().await
}

#[tokio::test]
async fn cancelled_startup_does_not_return_a_migrator_locked_connection_to_the_pool() -> Result<()>
{
    let database = database("startup_adapter_cancelled_migration_lock").await?;
    let mut blocker_connection = database.pool().acquire().await?;
    let mut blocker = blocker_connection.begin().await?;
    sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))")
        .bind(format!("raw_log_staging:{CHAIN}"))
        .execute(&mut *blocker)
        .await?;

    let startup_pool = database.pool().clone();
    let startup = tokio::spawn(async move {
        prepare_startup_adapter_sync(&startup_pool, PROFILE, CHAIN, ADAPTER, 1).await
    });
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        loop {
            let held_advisory_locks = sqlx::query_scalar::<_, i64>(
                "SELECT COUNT(*)::BIGINT
                 FROM pg_locks
                 WHERE locktype = 'advisory'
                   AND database = (
                       SELECT oid
                       FROM pg_database
                       WHERE datname = current_database()
                   )
                   AND granted",
            )
            .fetch_one(database.pool())
            .await
            .expect("advisory-lock inspection must succeed");
            if held_advisory_locks >= 2 {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("startup must acquire the migrator lock before waiting on the raw-log fence");

    startup.abort();
    assert!(
        startup
            .await
            .expect_err("aborted startup task must be cancelled")
            .is_cancelled()
    );
    blocker.rollback().await?;

    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        loop {
            let held_advisory_locks = sqlx::query_scalar::<_, i64>(
                "SELECT COUNT(*)::BIGINT
                 FROM pg_locks
                 WHERE locktype = 'advisory'
                   AND database = (
                       SELECT oid
                       FROM pg_database
                       WHERE datname = current_database()
                   )
                   AND granted",
            )
            .fetch_one(database.pool())
            .await
            .expect("advisory-lock inspection must succeed");
            if held_advisory_locks == 0 {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("cancelling startup must close the migrator-locked session");

    database.cleanup().await
}

#[tokio::test]
async fn raw_log_range_check_fails_closed_when_revision_evidence_is_missing() -> Result<()> {
    let database = database("startup_adapter_missing_block_revision").await?;
    sqlx::query(
        "UPDATE raw_log_staging_input_revisions
         SET revision = revision + 1
         WHERE chain_id = $1",
    )
    .bind(CHAIN)
    .execute(database.pool())
    .await?;

    assert!(
        crate::raw_log_staging_block_range_changed_since(database.pool(), CHAIN, 7, 0, 100).await?,
        "an advanced revision without per-block proof must reset a partial checkpoint"
    );

    sqlx::query(
        "INSERT INTO raw_log_staging_block_revisions (
             chain_id,
             block_hash,
             block_number,
             revision
         ) VALUES ($1, '0xoutside-consumed-boundary', 101, 9)",
    )
    .bind(CHAIN)
    .execute(database.pool())
    .await?;
    sqlx::query(
        "UPDATE raw_log_staging_input_revisions
         SET revision = 10
         WHERE chain_id = $1",
    )
    .bind(CHAIN)
    .execute(database.pool())
    .await?;
    assert!(
        crate::raw_log_staging_block_range_changed_since(database.pool(), CHAIN, 7, 0, 100).await?,
        "evidence for an earlier revision must not prove that the latest revision missed the range"
    );

    database.cleanup().await
}
