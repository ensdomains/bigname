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
async fn mirror_tiny_delta_visits_only_affected_dependencies() -> Result<()> {
    use anyhow::ensure;
    use serde_json::Value;
    let database = TestDatabase::create(TestDatabaseConfig::new("mirror_tiny_delta")).await?;
    let mut tx = database.pool().begin().await?;
    sqlx::raw_sql(include_str!("mirror_fixture.sql"))
        .execute(&mut *tx)
        .await?;
    sqlx::raw_sql("SET LOCAL statement_timeout = '30s';
        TRUNCATE project_changed_events;
        INSERT INTO name_surfaces
        SELECT 'unrelated-'||i, 'ens', 'unrelated-'||i, 'bench', 10, 'block', 'canonical',
               ARRAY['unrelated-'||i, 'eth'] FROM generate_series(1,20000) i;
        INSERT INTO normalized_events
        SELECT 10000+i,'unrelated-v1-'||i,'bench','ens',md5(('unrelated-v1-'||i)::text)::uuid,
          NULL,'ResolverChanged','ens_v1_registry_l1',1,1,10,'block',0,0,'canonical','activated',
          jsonb_build_object('node','unrelated-'||i,'resolver','0xshared'),'{}','{}'
          FROM generate_series(1,20000)i;
        INSERT INTO normalized_events
        SELECT 100000+i,'unrelated-mirror-'||i,'bench','ens',md5(('unrelated-mirror-'||i)::text)::uuid,
          'unrelated-'||i,'ResolverChanged','ens_v2_registry_l1',1,1,10,'block',0,0,'canonical','activated',
          jsonb_build_object('node','unrelated-'||i,'resolver','0xmirror'),'{}','{}'
          FROM generate_series(1,20000)i;
        INSERT INTO project_scope_names VALUES('name-26');
        ANALYZE normalized_events; ANALYZE name_surfaces; ANALYZE project_changed_events;
        ANALYZE project_scope_names; ANALYZE project_scope_resources;")
        .execute(&mut *tx).await?;
    let initial = scope(&mut tx).await?;
    sqlx::query(include_str!("mirror_previous.sql"))
        .bind("bench")
        .bind(10_i64)
        .execute(&mut *tx)
        .await?;
    let expected = scope(&mut tx).await?;
    restore(&mut tx, &initial).await?;
    let start = std::time::Instant::now();
    super::stage(&mut tx, "bench", 10).await?;
    let broad: bool = sqlx::query_scalar(include_str!("mirror_broad.sql"))
        .bind("bench")
        .bind(10_i64)
        .fetch_one(&mut *tx)
        .await?;
    assert!(
        !broad,
        "isolated small update must retain targeted traversal"
    );
    sqlx::raw_sql("ANALYZE project_scope_names; ANALYZE project_scope_resources;")
        .execute(&mut *tx)
        .await?;
    let plan: Value = sqlx::query_scalar(&format!(
        "EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) {}",
        include_str!("mirror_include.sql")
    ))
    .bind("bench")
    .bind(10_i64)
    .fetch_one(&mut *tx)
    .await?;
    let elapsed = start.elapsed();
    assert_eq!(scope(&mut tx).await?, expected);
    fn examined(node: &Value) -> f64 {
        let own = if matches!(
            node["Relation Name"].as_str(),
            Some("normalized_events" | "name_surfaces")
        ) {
            node["Actual Loops"].as_f64().unwrap_or_default()
                * (node["Actual Rows"].as_f64().unwrap_or_default()
                    + node["Rows Removed by Filter"].as_f64().unwrap_or_default())
        } else {
            0.0
        };
        own + node["Plans"]
            .as_array()
            .into_iter()
            .flatten()
            .map(examined)
            .sum::<f64>()
    }
    eprintln!(
        "mirror plan cost={}, JIT={}",
        plan[0]["Plan"]["Total Cost"], plan[0]["JIT"]
    );
    let rows = examined(&plan[0]["Plan"]);
    eprintln!("mirror tiny delta: {elapsed:?} including setup; {rows} history/surface rows");
    ensure!(
        rows < 1000.0,
        "tiny mirror delta scanned unrelated history: {rows}"
    );
    tx.rollback().await?;
    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn mirror_bulk_handles_full_scope_shared_ancestor_and_late_growth() -> Result<()> {
    let database = TestDatabase::create(TestDatabaseConfig::new("mirror_bulk_scale")).await?;
    let mut tx = database.pool().begin().await?;
    sqlx::raw_sql(include_str!("mirror_fixture.sql"))
        .execute(&mut *tx)
        .await?;
    sqlx::raw_sql("SET LOCAL statement_timeout = '30s';
        TRUNCATE project_changed_events;
        INSERT INTO name_surfaces
        SELECT 'scale-'||i,'ens','scale-'||i,'bench',10,'block','canonical',
               ARRAY['scale-'||i,'eth'] FROM generate_series(1,2000)i;
        INSERT INTO normalized_events
        SELECT 10000+i,'scale-v1-'||i,'bench','ens',md5(('scale-v1-'||i)::text)::uuid,
          NULL,'ResolverChanged','ens_v1_registry_l1',1,1,10,'block',0,0,'canonical','activated',
          jsonb_build_object('node','scale-'||i,'resolver','0xshared'),'{}','{}'
          FROM generate_series(1,2000)i;
        INSERT INTO normalized_events
        SELECT 100000+i,'scale-mirror-'||i,'bench','ens',md5(('scale-mirror-'||i)::text)::uuid,
          'scale-'||i,'ResolverChanged','ens_v2_registry_l1',1,1,10,'block',0,0,'canonical','activated',
          jsonb_build_object('node','scale-'||i,'resolver','0xmirror'),'{}','{}'
          FROM generate_series(1,2000)i;
        ANALYZE normalized_events; ANALYZE name_surfaces;")
        .execute(&mut *tx).await?;
    for case in ["full", "ancestor", "late_growth", "changed_ancestor"] {
        sqlx::raw_sql(
            "TRUNCATE project_scope_names, project_scope_resources, project_changed_events",
        )
        .execute(&mut *tx)
        .await?;
        if case == "full" {
            sqlx::query(
                "INSERT INTO project_scope_names SELECT logical_name_id FROM name_surfaces",
            )
            .execute(&mut *tx)
            .await?;
        } else if case == "changed_ancestor" {
            sqlx::query("INSERT INTO project_changed_events SELECT * FROM normalized_events WHERE normalized_event_id=200")
                .execute(&mut *tx).await?;
        } else {
            sqlx::query("INSERT INTO project_scope_names VALUES($1)")
                .bind(if case == "ancestor" { "eth" } else { "name-26" })
                .execute(&mut *tx)
                .await?;
        }
        let mut strategy = super::stage(&mut tx, "bench", 10).await?;
        if case == "late_growth" {
            // Consume changed-node seeds and visited entries, then grow as another
            // closure arm can do. Switching must reconstruct from original facts.
            super::include(&mut tx, "bench", 10, &mut strategy).await?;
            assert!(!strategy.bulk);
            sqlx::query("INSERT INTO project_scope_names SELECT logical_name_id FROM name_surfaces ON CONFLICT DO NOTHING")
                .execute(&mut *tx).await?;
        }
        let mut converged = false;
        let started = std::time::Instant::now();
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
            assert!(
                strategy.bulk,
                "{case} must choose bulk before expensive traversal"
            );
            let actual = scope(&mut tx).await?;
            assert_eq!(actual, expected, "{case}, pass {pass}");
            if actual == before {
                converged = true;
                break;
            }
        }
        assert!(converged, "{case}");
        eprintln!(
            "mirror {case}: {:?} including reference comparisons and all closure steps",
            started.elapsed()
        );
        super::finish(&mut tx, strategy).await?;
        sqlx::raw_sql("DROP TABLE project_mirror_seen_resources, project_mirror_seen_names, project_mirror_changed_nodes")
            .execute(&mut *tx).await?;
    }
    tx.rollback().await?;
    database.cleanup().await?;
    Ok(())
}
