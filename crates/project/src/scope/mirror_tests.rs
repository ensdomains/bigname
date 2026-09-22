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
    for seed in 0..97 {
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
    let mut strategy = super::stage(&mut tx, "bench", 10).await?;
    super::include(&mut tx, "bench", 10, &mut strategy).await?;
    let elapsed = start.elapsed();
    assert_eq!(scope(&mut tx).await?, expected);
    let pointers: i64 = sqlx::query_scalar("SELECT count(*) FROM project_mirror_seen_pointers")
        .fetch_one(&mut *tx)
        .await?;
    ensure!(
        pointers < 32,
        "isolated update built unrelated mirror graph: {pointers}"
    );
    eprintln!("mirror tiny delta: {elapsed:?}; {pointers} affected historical pointers");
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

#[tokio::test]
async fn ancestor_discovery_reuses_history_for_late_descendants_without_losing_edges() -> Result<()>
{
    let database = TestDatabase::create(TestDatabaseConfig::new("mirror_ancestor_reuse")).await?;
    let mut tx = database.pool().begin().await?;
    sqlx::raw_sql(include_str!("mirror_fixture.sql"))
        .execute(&mut *tx)
        .await?;
    sqlx::raw_sql("TRUNCATE project_changed_events;
        INSERT INTO name_surfaces VALUES
          ('repeated','ens','repeated','bench',10,'block','canonical',ARRAY['eth','eth']),
          ('foreign','foreign','foreign','bench',10,'block','canonical',ARRAY['label-1','eth']),
          ('empty','ens','empty','bench',10,'block','canonical',ARRAY[]::text[]);
        INSERT INTO normalized_events VALUES
          (9001,'repeated-v1','bench','ens',md5('repeated-v1')::uuid,NULL,'ResolverChanged','ens_v1_registry_l1',1,1,10,'block',0,0,'canonical','activated','{\"node\":\"repeated\",\"resolver\":\"0xshared\"}','{}','{}'),
          (9002,'repeated-mirror','bench','ens',md5('repeated-mirror')::uuid,'repeated','ResolverChanged','ens_v2_registry_l1',1,1,10,'block',0,0,'canonical','activated','{\"resolver\":\"0xmirror\"}','{}','{}'),
          (9003,'foreign-v1','bench','foreign',md5('foreign-v1')::uuid,NULL,'ResolverChanged','ens_v1_registry_l1',1,1,10,'block',0,0,'canonical','activated','{\"node\":\"foreign\",\"resolver\":\"0xshared\"}','{}','{}'),
          (9004,'foreign-mirror','bench','foreign',md5('foreign-mirror')::uuid,'foreign','ResolverChanged','ens_v2_registry_l1',1,1,10,'block',0,0,'canonical','activated','{\"resolver\":\"0xmirror\"}','{}','{}');")
        .execute(&mut *tx).await?;
    for initial in ["ancestor", "ancestor_and_descendant", "empty_root"] {
        sqlx::raw_sql("TRUNCATE project_scope_names, project_scope_resources")
            .execute(&mut *tx)
            .await?;
        let mut strategy = super::stage(&mut tx, "bench", 10).await?;
        sqlx::query("INSERT INTO project_scope_names VALUES($1)")
            .bind(if initial == "empty_root" {
                "empty"
            } else {
                "eth"
            })
            .execute(&mut *tx)
            .await?;
        if initial == "ancestor_and_descendant" {
            sqlx::query("INSERT INTO project_scope_names VALUES('repeated')")
                .execute(&mut *tx)
                .await?;
        }
        let mut converged = false;
        for pass in 0..12 {
            if pass == 1 {
                sqlx::raw_sql("INSERT INTO project_scope_names VALUES('repeated'),('foreign'),('name-26') ON CONFLICT DO NOTHING;
                    INSERT INTO project_scope_resources VALUES(md5('mirror-27')::uuid), (md5('foreign-mirror')::uuid) ON CONFLICT DO NOTHING")
                    .execute(&mut *tx).await?;
            }
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
            assert_eq!(actual, expected, "{initial}, pass {pass}");
            if pass > 1 && actual == before {
                converged = true;
                break;
            }
        }
        assert!(converged, "{initial}");
        super::finish(&mut tx, strategy).await?;
        sqlx::raw_sql("DROP TABLE project_mirror_seen_resources, project_mirror_seen_names, project_mirror_changed_nodes")
            .execute(&mut *tx).await?;
    }
    tx.rollback().await?;
    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn historical_ancestor_resources_remain_factored_at_each_step() -> Result<()> {
    let database = TestDatabase::create(TestDatabaseConfig::new("mirror_history_fanout")).await?;
    let mut tx = database.pool().begin().await?;
    sqlx::raw_sql(include_str!("mirror_fixture.sql"))
        .execute(&mut *tx)
        .await?;
    sqlx::raw_sql("SET LOCAL statement_timeout='60s';
        INSERT INTO normalized_events
        SELECT 50000000+i,'historical-ancestor-'||i,'bench','ens',md5('historical-ancestor-'||i)::uuid,
          NULL,'ResolverChanged','ens_v1_registry_l1',1,1,10,'block',0,0,'canonical','activated',
          jsonb_build_object('node','ETH','resolver','0xshared'),'{}','{}'
        FROM generate_series(1,2913)i;
        ANALYZE normalized_events;")
        .execute(&mut *tx).await?;
    for case in ["mirror", "resource", "changed"] {
        sqlx::raw_sql(
            "TRUNCATE project_scope_names,project_scope_resources,project_changed_events",
        )
        .execute(&mut *tx)
        .await?;
        match case {
            "mirror" => {
                sqlx::query("INSERT INTO project_scope_names VALUES('name-1')")
                    .execute(&mut *tx)
                    .await?;
            }
            "resource" => {
                sqlx::query("INSERT INTO project_scope_resources VALUES(md5('historical-ancestor-1')::uuid)").execute(&mut *tx).await?;
            }
            _ => {
                sqlx::query("INSERT INTO project_changed_events SELECT * FROM normalized_events WHERE normalized_event_id=50000001").execute(&mut *tx).await?;
            }
        }
        let mut strategy = super::stage(&mut tx, "bench", 10).await?;
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
            assert_eq!(actual, expected, "{case} pass {pass}");
            let links: i64 = sqlx::query_scalar("SELECT count(*) FROM project_mirror_links")
                .fetch_one(&mut *tx)
                .await?;
            assert!(links < 200, "history multiplied mirror links: {links}");
            if actual == before {
                converged = true;
                break;
            }
        }
        assert!(converged, "{case}");
        super::finish(&mut tx, strategy).await?;
        sqlx::raw_sql("DROP TABLE project_mirror_seen_resources,project_mirror_seen_names,project_mirror_changed_nodes")
            .execute(&mut *tx).await?;
    }
    tx.rollback().await?;
    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn profiled_mirror_substages_preserve_each_nonempty_closure_step() -> Result<()> {
    let database = TestDatabase::create(TestDatabaseConfig::new("mirror_profile")).await?;
    let mut tx = database.pool().begin().await?;
    sqlx::raw_sql(include_str!("mirror_fixture.sql"))
        .execute(&mut *tx)
        .await?;
    let session = crate::profile::Session::create(&std::env::temp_dir())?;
    let mut expected = Vec::new();
    for profile in [false, true] {
        sqlx::query("SAVEPOINT profile_run")
            .execute(&mut *tx)
            .await?;
        sqlx::query("INSERT INTO project_scope_names VALUES('eth')")
            .execute(&mut *tx)
            .await?;
        let mut strategy = super::stage(&mut tx, "bench", 10).await?;
        let mut converged = false;
        for pass in 0..12 {
            let before = scope(&mut tx).await?;
            if profile {
                session
                    .scope(super::include(&mut tx, "bench", 10, &mut strategy))
                    .await?;
                assert_eq!(scope(&mut tx).await?, expected[pass], "pass {pass}");
            } else {
                super::include(&mut tx, "bench", 10, &mut strategy).await?;
                expected.push(scope(&mut tx).await?);
            }
            if scope(&mut tx).await? == before {
                converged = true;
                break;
            }
        }
        assert!(converged);
        super::finish(&mut tx, strategy).await?;
        sqlx::query("ROLLBACK TO SAVEPOINT profile_run")
            .execute(&mut *tx)
            .await?;
        sqlx::query("RELEASE SAVEPOINT profile_run")
            .execute(&mut *tx)
            .await?;
    }
    assert_eq!(
        std::fs::read_dir(session.directory())?.count(),
        12 * expected.len()
    );
    std::fs::remove_dir_all(session.directory())?;
    tx.rollback().await?;
    database.cleanup().await?;
    Ok(())
}

#[path = "mirror_dependency_repro.rs"]
mod dependency_repro;
