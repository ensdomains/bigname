use super::*;

use anyhow::{Context, Result};
use bigname_storage::DatabaseConfig;
use sqlx::{ConnectOptions, postgres::PgConnectOptions};
use std::str::FromStr;
use uuid::Uuid;

fn database_config(database: &bigname_test_support::TestDatabase) -> Result<DatabaseConfig> {
    let base_url = bigname_test_support::database_url_from_env();
    let database_url = PgConnectOptions::from_str(&base_url)
        .context("failed to parse test database URL")?
        .database(database.database_name())
        .to_url_lossy()
        .to_string();
    Ok(DatabaseConfig {
        database_url: Some(database_url),
        max_connections: 5,
    })
}

#[tokio::test]
async fn one_shot_rebuild_clears_projection_replay_marker() -> Result<()> {
    let database = bigname_test_support::TestDatabase::create_migrated(
        bigname_test_support::TestDatabaseConfig::new("bigname_worker_command_marker_hygiene_test"),
        &bigname_storage::MIGRATOR,
        "failed to apply migrations for worker command marker hygiene test",
    )
    .await?;

    sqlx::query(
        r#"
        INSERT INTO current_projection_replay_status (
            projection,
            replay_version,
            completed_normalized_target_block,
            requested_key_count,
            upserted_row_count,
            deleted_row_count
        )
        VALUES ('permissions_current', 4, 100, 1, 1, 0)
        "#,
    )
    .execute(database.pool())
    .await
    .context("failed to seed permissions_current replay marker")?;

    rebuild_permissions_current(PermissionsCurrentRebuildArgs {
        database: database_config(&database)?,
        resource_id: Some(Uuid::new_v4().to_string()),
    })
    .await?;

    let marker_count = sqlx::query_scalar::<_, i64>(
        r#"
        SELECT COUNT(*)::BIGINT
        FROM current_projection_replay_status
        WHERE projection = 'permissions_current'
        "#,
    )
    .fetch_one(database.pool())
    .await
    .context("failed to count permissions_current replay markers")?;
    assert_eq!(marker_count, 0);

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn standalone_full_rebuild_preserves_targeted_stage_when_replay_lock_is_held() -> Result<()> {
    let database = bigname_test_support::TestDatabase::create_migrated(
        bigname_test_support::TestDatabaseConfig::new("bigname_worker_command_replay_lock_test"),
        &bigname_storage::MIGRATOR,
        "failed to apply migrations for worker command replay lock test",
    )
    .await?;
    sqlx::query(
        r#"
        INSERT INTO current_projection_replay_attempt (
            singleton,
            replay_version,
            normalized_target_block,
            full_replay_input_revision,
            apply_baseline_change_id
        )
        VALUES (true, $1, 20, 0, 7)
        "#,
    )
    .bind(replay::CURRENT_PROJECTION_REPLAY_VERSION)
    .execute(database.pool())
    .await?;
    let checkpoint = replay::staging::ProjectionStagingCheckpoint::load_or_start(
        database.pool(),
        "name_current",
        Some(20),
    )
    .await?;
    let stage_table = checkpoint.stage_table(0)?.to_owned();
    let mut replay_lock = replay::replay_lock::try_acquire_replay_lock(database.pool())
        .await?
        .context("the test must acquire the automatic replay lock")?;

    let error = rebuild_name_current(NameCurrentRebuildArgs {
        database: database_config(&database)?,
        logical_name_id: None,
    })
    .await
    .expect_err("standalone full rebuild must fail while automatic replay owns the lock");
    assert!(
        format!("{error:#}").contains(
            "standalone name_current rebuild cannot start because another process owns the cross-process replay lock"
        ),
        "unexpected standalone rebuild lock error: {error:#}"
    );
    assert_eq!(
        sqlx::query_scalar::<_, Option<i64>>(
            "SELECT normalized_target_block FROM current_projection_replay_attempt WHERE singleton"
        )
        .fetch_one(database.pool())
        .await?,
        Some(20),
        "standalone rebuild must retain the automatic replay attempt"
    );
    assert_eq!(
        sqlx::query_scalar::<_, Option<i64>>(
            "SELECT completed_normalized_target_block FROM current_projection_staging_checkpoints WHERE projection = 'name_current'"
        )
        .fetch_one(database.pool())
        .await?,
        Some(20),
        "standalone rebuild must retain the automatic replay checkpoint target"
    );
    assert!(
        sqlx::query_scalar::<_, bool>("SELECT to_regclass($1) IS NOT NULL")
            .bind(stage_table)
            .fetch_one(database.pool())
            .await?,
        "standalone rebuild must retain the automatic replay logged stage table"
    );

    replay::replay_lock::release_replay_lock(&mut replay_lock).await?;
    database.cleanup().await
}
