use anyhow::Result;
use bigname_test_support::{TestDatabase, TestDatabaseConfig};

#[tokio::test]
async fn pointer_frontiers_match_original_for_small_broad_and_late_resources() -> Result<()> {
    let database = TestDatabase::create(TestDatabaseConfig::new("inventory_frontier")).await?;
    let mut tx = database.pool().begin().await?;
    sqlx::raw_sql(include_str!("mirror_fixture.sql"))
        .execute(&mut *tx)
        .await?;
    sqlx::raw_sql(
        "CREATE TEMP TABLE project_inventory_seen_resources(resource_id uuid PRIMARY KEY);
        CREATE TEMP TABLE project_inventory_frontier_resources(resource_id uuid PRIMARY KEY)",
    )
    .execute(&mut *tx)
    .await?;
    for broad in [false, true] {
        sqlx::raw_sql("TRUNCATE project_scope_names, project_scope_resources, project_inventory_seen_resources")
            .execute(&mut *tx).await?;
        sqlx::query("INSERT INTO project_scope_resources VALUES(md5('mirror-1')::uuid)")
            .execute(&mut *tx)
            .await?;
        if broad {
            sqlx::raw_sql("INSERT INTO project_scope_resources SELECT md5('mirror-'||i)::uuid FROM generate_series(2,400)i")
                .execute(&mut *tx).await?;
        }
        for pass in 0..3 {
            if pass == 1 {
                sqlx::query("INSERT INTO project_scope_resources SELECT resource_id FROM normalized_events WHERE resource_id IS NOT NULL ON CONFLICT DO NOTHING")
                    .execute(&mut *tx).await?;
            }
            sqlx::query("SAVEPOINT inventory_reference")
                .execute(&mut *tx)
                .await?;
            sqlx::query(include_str!("inventory_pointer_previous.sql"))
                .bind("bench")
                .bind(10_i64)
                .execute(&mut *tx)
                .await?;
            let expected: Vec<String> =
                sqlx::query_scalar("SELECT logical_name_id FROM project_scope_names ORDER BY 1")
                    .fetch_all(&mut *tx)
                    .await?;
            sqlx::query("ROLLBACK TO SAVEPOINT inventory_reference")
                .execute(&mut *tx)
                .await?;
            sqlx::query("RELEASE SAVEPOINT inventory_reference")
                .execute(&mut *tx)
                .await?;
            super::include_pointer_names(&mut tx, "bench", 10).await?;
            let actual: Vec<String> =
                sqlx::query_scalar("SELECT logical_name_id FROM project_scope_names ORDER BY 1")
                    .fetch_all(&mut *tx)
                    .await?;
            assert_eq!(actual, expected, "broad={broad} pass={pass}");
        }
    }
    tx.rollback().await?;
    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn profiled_inventory_names_preserve_history_and_execute_once() -> Result<()> {
    let database = TestDatabase::create(TestDatabaseConfig::new("inventory_profile")).await?;
    let mut tx = database.pool().begin().await?;
    sqlx::raw_sql(include_str!("mirror_fixture.sql"))
        .execute(&mut *tx)
        .await?;
    sqlx::raw_sql("CREATE TEMP TABLE project_inventory_seen_resources(resource_id uuid PRIMARY KEY);
        CREATE TEMP TABLE project_inventory_frontier_resources(resource_id uuid PRIMARY KEY);
        INSERT INTO project_scope_resources SELECT resource_id FROM normalized_events WHERE resource_id IS NOT NULL ON CONFLICT DO NOTHING")
        .execute(&mut *tx).await?;
    let session = crate::profile::Session::create(&std::env::temp_dir())?;
    for _ in 0..2 {
        sqlx::query("SAVEPOINT profile_reference")
            .execute(&mut *tx)
            .await?;
        super::include_pointer_names(&mut tx, "bench", 10).await?;
        let expected: Vec<String> =
            sqlx::query_scalar("SELECT logical_name_id FROM project_scope_names ORDER BY 1")
                .fetch_all(&mut *tx)
                .await?;
        sqlx::query("ROLLBACK TO SAVEPOINT profile_reference")
            .execute(&mut *tx)
            .await?;
        sqlx::query("RELEASE SAVEPOINT profile_reference")
            .execute(&mut *tx)
            .await?;
        session
            .scope(super::include_pointer_names(&mut tx, "bench", 10))
            .await?;
        let actual: Vec<String> =
            sqlx::query_scalar("SELECT logical_name_id FROM project_scope_names ORDER BY 1")
                .fetch_all(&mut *tx)
                .await?;
        assert_eq!(actual, expected);
    }
    assert_eq!(std::fs::read_dir(session.directory())?.count(), 2);
    std::fs::remove_dir_all(session.directory())?;
    tx.rollback().await?;
    database.cleanup().await?;
    Ok(())
}
