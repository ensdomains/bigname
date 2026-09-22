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
async fn deployed_targeted_and_bulk_paths_match_independent_semantic_oracle() -> Result<()> {
    let database =
        TestDatabase::create(TestDatabaseConfig::new("mirror_scope_differential")).await?;
    let mut tx = database.pool().begin().await?;
    sqlx::raw_sql(include_str!("mirror_fixture.sql"))
        .execute(&mut *tx)
        .await?;
    // Try every name and both resource sides independently, then an empty initial scope.
    for seed in 0..194 {
        let force_bulk = seed >= 97;
        let seed = seed % 97;
        let mut strategy = super::stage(&mut tx, "bench", 10).await?;
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
        if force_bulk {
            super::stage_bulk(&mut tx, "bench", 10).await?;
            strategy.bulk = true;
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
            super::include(&mut tx, "bench", 10, &mut strategy).await?;
            let actual = scope(&mut tx).await?;
            assert_eq!(actual, expected, "seed {seed}, pass {pass}");
            if actual == before {
                converged = true;
                break;
            }
        }
        assert!(converged, "seed {seed}");
        super::finish(&mut tx, strategy).await?;
        sqlx::raw_sql("DROP TABLE project_mirror_seen_resources, project_mirror_seen_names, project_mirror_changed_nodes")
            .execute(&mut *tx).await?;
    }
    tx.rollback().await?;
    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn deployed_reference_switches_after_targeted_pass_and_reuses_bulk_for_late_keys()
-> Result<()> {
    let database = TestDatabase::create(TestDatabaseConfig::new("mirror_reference_switch")).await?;
    let mut tx = database.pool().begin().await?;
    sqlx::raw_sql(include_str!("mirror_fixture.sql"))
        .execute(&mut *tx)
        .await?;
    sqlx::query("TRUNCATE project_changed_events")
        .execute(&mut *tx)
        .await?;
    let mut strategy = super::stage(&mut tx, "bench", 10).await?;
    // Empty work takes the real targeted path, then another closure operator grows scope.
    super::include(&mut tx, "bench", 10, &mut strategy).await?;
    assert!(!strategy.bulk);
    for phase in 0..2 {
        if phase == 0 {
            sqlx::raw_sql("INSERT INTO project_scope_names SELECT 'unrelated-'||i FROM generate_series(1,257)i;
                INSERT INTO project_scope_resources VALUES(md5('v1-26')::uuid)")
                .execute(&mut *tx).await?;
        } else {
            sqlx::query("INSERT INTO project_scope_names VALUES('name-27') ON CONFLICT DO NOTHING")
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
            super::include(&mut tx, "bench", 10, &mut strategy).await?;
            assert!(strategy.bulk);
            let actual = scope(&mut tx).await?;
            assert_eq!(actual, expected, "phase {phase}, pass {pass}");
            if actual == before {
                converged = true;
                break;
            }
        }
        assert!(converged);
    }
    super::finish(&mut tx, strategy).await?;
    tx.rollback().await?;
    database.cleanup().await?;
    Ok(())
}
