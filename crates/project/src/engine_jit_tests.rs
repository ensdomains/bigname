use anyhow::Result;
use bigname_test_support::{TestDatabase, TestDatabaseConfig};
use sqlx::{Acquire, postgres::PgPoolOptions};

fn request(chain: &str, mode: crate::RunMode, resume: bool) -> crate::BatchRequest {
    crate::BatchRequest {
        chain_id: chain.into(),
        mode,
        target_block: 10,
        affected_from_block: 10,
        affected_to_block: 10,
        resume_current: resume.then(|| crate::Marker {
            number: 10,
            hash: "canonical".into(),
        }),
    }
}

#[tokio::test]
async fn jit_policy_is_only_incremental_sepolia_and_reference_keeps_incoming_policy() -> Result<()>
{
    let database = TestDatabase::create(TestDatabaseConfig::new("project_jit_policy")).await?;
    let mut tx = database.pool().begin().await?;
    sqlx::query("SET LOCAL jit=on").execute(&mut *tx).await?;
    for (chain, mode, resume, expected) in [
        ("ethereum-sepolia", crate::RunMode::Normal, true, true),
        ("ethereum-sepolia", crate::RunMode::Redo, true, true),
        ("ethereum-sepolia", crate::RunMode::Redo, false, true),
        ("ethereum-sepolia", crate::RunMode::Normal, false, false),
        ("ethereum-mainnet", crate::RunMode::Normal, true, false),
        ("base-mainnet", crate::RunMode::Redo, true, false),
    ] {
        assert_eq!(
            super::applies(&mut tx, &request(chain, mode, resume)).await?,
            expected
        );
    }
    sqlx::query("SET LOCAL bigname.benchmark_reference='on'")
        .execute(&mut *tx)
        .await?;
    assert!(
        !super::applies(
            &mut tx,
            &request("ethereum-sepolia", crate::RunMode::Normal, true)
        )
        .await?
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>("SHOW jit")
            .fetch_one(&mut *tx)
            .await?,
        "on"
    );
    tx.rollback().await?;
    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn jit_savepoint_restores_success_error_drop_and_reused_connection() -> Result<()> {
    let database = TestDatabase::create(TestDatabaseConfig::new("project_jit_restore")).await?;
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect_with(database.pool().connect_options().as_ref().clone())
        .await?;
    sqlx::query("CREATE TABLE jit_probe(id integer PRIMARY KEY)")
        .execute(&pool)
        .await?;
    let original: String = sqlx::query_scalar("SHOW jit").fetch_one(&pool).await?;
    let pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&pool)
        .await?;
    for prior in ["on", "off"] {
        for outcome in ["success", "sql_error", "drop"] {
            sqlx::query("TRUNCATE jit_probe").execute(&pool).await?;
            let mut connection = pool.acquire().await?;
            let mut parent = connection.begin().await?;
            sqlx::query("SELECT set_config('jit',$1,true)")
                .bind(prior)
                .execute(&mut *parent)
                .await?;
            sqlx::query("INSERT INTO jit_probe VALUES(1)")
                .execute(&mut *parent)
                .await?;
            let mut local = super::Scope::begin(&mut parent).await?;
            assert_eq!(
                sqlx::query_scalar::<_, String>("SHOW jit")
                    .fetch_one(&mut **local.transaction())
                    .await?,
                "off"
            );
            sqlx::query("INSERT INTO jit_probe VALUES(2)")
                .execute(&mut **local.transaction())
                .await?;
            match outcome {
                "success" => assert_eq!(local.finish(Ok(17)).await?, 17),
                "sql_error" => {
                    let error = sqlx::query("INSERT INTO jit_probe VALUES(1)")
                        .execute(&mut **local.transaction())
                        .await
                        .unwrap_err();
                    let original_error = crate::ProjectError::database("original failure", error);
                    let expected = original_error.to_string();
                    let returned = local.finish(Err(original_error)).await.unwrap_err();
                    assert_eq!(returned.kind(), crate::ErrorKind::DataIntegrity);
                    assert_eq!(returned.to_string(), expected);
                }
                "drop" => drop(local),
                _ => unreachable!(),
            }
            // A caller can continue on the same connection after success or SQL error; partial
            // failed/cancelled work is gone and preexisting caller work remains.
            assert_eq!(
                sqlx::query_scalar::<_, String>("SHOW jit")
                    .fetch_one(&mut *parent)
                    .await?,
                prior
            );
            assert_eq!(
                sqlx::query_scalar::<_, i64>("SELECT count(*) FROM jit_probe")
                    .fetch_one(&mut *parent)
                    .await?,
                if outcome == "success" { 2 } else { 1 }
            );
            sqlx::query("INSERT INTO jit_probe VALUES(3)")
                .execute(&mut *parent)
                .await?;
            parent.commit().await?;
            drop(connection);
            let mut reused = pool.acquire().await?;
            assert_eq!(
                sqlx::query_scalar::<_, i32>("SELECT pg_backend_pid()")
                    .fetch_one(&mut *reused)
                    .await?,
                pid
            );
            assert_eq!(
                sqlx::query_scalar::<_, String>("SHOW jit")
                    .fetch_one(&mut *reused)
                    .await?,
                original
            );
        }
    }
    pool.close().await;
    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn derive_sql_error_restores_jit_and_partial_staging_before_returning() -> Result<()> {
    let database =
        TestDatabase::create(TestDatabaseConfig::new("project_jit_derive_error")).await?;
    let mut tx = database.pool().begin().await?;
    sqlx::query("SET LOCAL jit=on").execute(&mut *tx).await?;
    // Prepare can create the first stage, then fails on the deliberately missing children table.
    sqlx::query("CREATE TEMP TABLE name_current(id integer)")
        .execute(&mut *tx)
        .await?;
    let target = crate::Marker {
        number: 10,
        hash: "canonical".into(),
    };
    let error = super::super::derive(
        &mut tx,
        &request("ethereum-sepolia", crate::RunMode::Normal, true),
        &target,
    )
    .await
    .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("failed to create children_current stage")
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>("SHOW jit")
            .fetch_one(&mut *tx)
            .await?,
        "on"
    );
    assert!(
        sqlx::query_scalar::<_, bool>(
            "SELECT to_regclass('pg_temp.project_stage_name_current') IS NULL"
        )
        .fetch_one(&mut *tx)
        .await?
    );
    sqlx::query("INSERT INTO name_current VALUES(1)")
        .execute(&mut *tx)
        .await?;
    tx.rollback().await?;
    database.cleanup().await?;
    Ok(())
}
