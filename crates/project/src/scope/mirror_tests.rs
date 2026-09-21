use anyhow::Result;
use bigname_test_support::{TestDatabase, TestDatabaseConfig};
use sqlx::{Postgres, Transaction};

type Scope = (Vec<String>, Vec<String>);

async fn scope(tx: &mut Transaction<'_, Postgres>) -> Result<Scope> {
    let names = sqlx::query_scalar("SELECT logical_name_id FROM project_scope_names ORDER BY 1")
        .fetch_all(&mut **tx)
        .await?;
    let resources =
        sqlx::query_scalar("SELECT resource_id::text FROM project_scope_resources ORDER BY 1")
            .fetch_all(&mut **tx)
            .await?;
    Ok((names, resources))
}

async fn restore(tx: &mut Transaction<'_, Postgres>, value: &Scope) -> Result<()> {
    sqlx::query("TRUNCATE project_scope_names, project_scope_resources")
        .execute(&mut **tx)
        .await?;
    sqlx::query("INSERT INTO project_scope_names SELECT unnest($1::text[])")
        .bind(&value.0)
        .execute(&mut **tx)
        .await?;
    sqlx::query("INSERT INTO project_scope_resources SELECT unnest($1::text[])::uuid")
        .bind(&value.1)
        .execute(&mut **tx)
        .await?;
    Ok(())
}

#[tokio::test]
async fn mirror_pairs_preserve_each_closure_step_and_history_guards() -> Result<()> {
    let database =
        TestDatabase::create(TestDatabaseConfig::new("mirror_scope_differential")).await?;
    let mut tx = database.pool().begin().await?;
    sqlx::raw_sql(include_str!("mirror_fixture.sql"))
        .execute(&mut *tx)
        .await?;
    super::stage(&mut tx, "bench", 10).await?;
    // Try every name and both resource sides independently, then an empty initial scope.
    for seed in 0..97 {
        sqlx::query("TRUNCATE project_scope_names, project_scope_resources")
            .execute(&mut *tx)
            .await?;
        if seed < 32 {
            sqlx::query("INSERT INTO project_scope_names VALUES($1)")
                .bind(format!("name-{}", seed + 1))
                .execute(&mut *tx)
                .await?;
        } else if seed < 96 {
            let key = if seed < 64 {
                format!("v1-{}", seed - 31)
            } else {
                format!("mirror-{}", seed - 63)
            };
            sqlx::query("INSERT INTO project_scope_resources VALUES(md5($1)::uuid)")
                .bind(key)
                .execute(&mut *tx)
                .await?;
        }
        let mut converged = false;
        for pass in 0..12 {
            let before = scope(&mut tx).await?;
            sqlx::query(include_str!("mirror_previous.sql"))
                .bind("bench")
                .bind(10_i64)
                .execute(&mut *tx)
                .await?;
            let expected = scope(&mut tx).await?;
            restore(&mut tx, &before).await?;
            super::include(&mut tx).await?;
            let actual = scope(&mut tx).await?;
            assert_eq!(actual, expected, "seed {seed}, pass {pass}");
            if actual == before {
                converged = true;
                break;
            }
        }
        assert!(converged, "seed {seed}");
    }
    tx.rollback().await?;
    database.cleanup().await?;
    Ok(())
}
