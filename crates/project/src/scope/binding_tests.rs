use anyhow::Result;
use bigname_test_support::{TestDatabase, TestDatabaseConfig};
use sqlx::{Postgres, Transaction};

async fn scope(tx: &mut Transaction<'_, Postgres>) -> Result<(Vec<String>, Vec<String>)> {
    Ok((
        sqlx::query_scalar("SELECT logical_name_id FROM project_scope_names ORDER BY 1")
            .fetch_all(&mut **tx)
            .await?,
        sqlx::query_scalar("SELECT resource_id::text FROM project_scope_resources ORDER BY 1")
            .fetch_all(&mut **tx)
            .await?,
    ))
}

#[tokio::test]
async fn each_binding_operator_preserves_full_scope_closure_and_late_keys() -> Result<()> {
    let database = TestDatabase::create(TestDatabaseConfig::new("binding_frontiers")).await?;
    let mut tx = database.pool().begin().await?;
    sqlx::raw_sql(include_str!("binding_fixture.sql"))
        .execute(&mut *tx)
        .await?;
    let target = crate::Marker {
        number: 10,
        hash: "canonical".into(),
    };
    for seed in ["name", "lease", "wrapper", "empty", "broad"] {
        sqlx::raw_sql(
            "TRUNCATE project_scope_names, project_scope_resources,
            project_binding_seen_names, project_binding_seen_resources",
        )
        .execute(&mut *tx)
        .await?;
        match seed {
            "broad" => {
                sqlx::raw_sql("INSERT INTO project_scope_names SELECT logical_name_id FROM name_surfaces ON CONFLICT DO NOTHING;
                    INSERT INTO project_scope_names SELECT 'unrelated-'||i FROM generate_series(1,300)i;
                    INSERT INTO project_scope_resources SELECT md5(kind||i)::uuid FROM generate_series(1,20)i CROSS JOIN unnest(ARRAY['lease-','registry-','wrapper-']) kind;
                    INSERT INTO project_scope_resources SELECT md5('unrelated-'||i)::uuid FROM generate_series(1,300)i")
                    .execute(&mut *tx).await?;
            }
            "name" => {
                sqlx::query("INSERT INTO project_scope_names VALUES ('ens:node-1')")
                    .execute(&mut *tx)
                    .await?;
            }
            "lease" | "wrapper" => {
                sqlx::query("INSERT INTO project_scope_resources VALUES(md5($1)::uuid)")
                    .bind(format!("{seed}-1"))
                    .execute(&mut *tx)
                    .await?;
            }
            _ => {}
        }
        let mut converged = false;
        for pass in 0..50 {
            // A different operator can introduce names/resources after an empty frontier.
            if pass == 1 {
                sqlx::raw_sql("INSERT INTO project_scope_names VALUES('ens:node-10') ON CONFLICT DO NOTHING;
                    INSERT INTO project_scope_resources VALUES(md5('lease-18')::uuid) ON CONFLICT DO NOTHING")
                    .execute(&mut *tx).await?;
            }
            let before = scope(&mut tx).await?;
            sqlx::query("SAVEPOINT binding_reference")
                .execute(&mut *tx)
                .await?;
            // Execute the actual pre-optimization SQL, including its original joins and guards.
            for statement in include_str!("binding_previous.sql")
                .split(';')
                .filter(|sql| !sql.trim().is_empty())
            {
                let query = sqlx::query(statement).bind("bench").bind(10_i64);
                let query = if statement.contains("$3") {
                    query.bind("canonical")
                } else {
                    query
                };
                query.execute(&mut *tx).await?;
            }
            let expected = scope(&mut tx).await?;
            sqlx::query("ROLLBACK TO SAVEPOINT binding_reference")
                .execute(&mut *tx)
                .await?;
            sqlx::query("RELEASE SAVEPOINT binding_reference")
                .execute(&mut *tx)
                .await?;
            super::close_binding_scope(&mut tx, "bench", &target).await?;
            let actual = scope(&mut tx).await?;
            assert_eq!(actual, expected, "{seed} pass {pass}");
            let excluded: i64 = sqlx::query_scalar(
                "SELECT count(*) FROM project_scope_resources
                WHERE resource_id IN (SELECT md5('excluded-'||i)::uuid FROM generate_series(1,4)i)",
            )
            .fetch_one(&mut *tx)
            .await?;
            assert_eq!(excluded, 0, "excluded history entered binding scope");
            if pass > 1 && before == actual {
                converged = true;
                break;
            }
        }
        assert!(converged, "{seed} did not converge");
    }
    tx.rollback().await?;
    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn profiled_binding_frontiers_preserve_each_nonempty_closure_step() -> Result<()> {
    let database = TestDatabase::create(TestDatabaseConfig::new("binding_profile")).await?;
    let mut tx = database.pool().begin().await?;
    sqlx::raw_sql(include_str!("binding_fixture.sql"))
        .execute(&mut *tx)
        .await?;
    sqlx::raw_sql("INSERT INTO project_scope_names SELECT logical_name_id FROM name_surfaces;
        INSERT INTO project_scope_resources SELECT md5(kind||i)::uuid FROM generate_series(1,20)i CROSS JOIN unnest(ARRAY['lease-','registry-','wrapper-'])kind;
        INSERT INTO project_scope_resources SELECT md5('unrelated-'||i)::uuid FROM generate_series(1,300)i")
        .execute(&mut *tx).await?;
    let session = crate::profile::Session::create(&std::env::temp_dir())?;
    let target = crate::Marker {
        number: 10,
        hash: "canonical".into(),
    };
    for pass in 0..3 {
        sqlx::query("SAVEPOINT profile_reference")
            .execute(&mut *tx)
            .await?;
        super::close_binding_scope(&mut tx, "bench", &target).await?;
        let expected = scope(&mut tx).await?;
        sqlx::query("ROLLBACK TO SAVEPOINT profile_reference")
            .execute(&mut *tx)
            .await?;
        sqlx::query("RELEASE SAVEPOINT profile_reference")
            .execute(&mut *tx)
            .await?;
        session
            .scope(super::close_binding_scope(&mut tx, "bench", &target))
            .await?;
        assert_eq!(scope(&mut tx).await?, expected, "pass {pass}");
    }
    assert_eq!(std::fs::read_dir(session.directory())?.count(), 6);
    std::fs::remove_dir_all(session.directory())?;
    tx.rollback().await?;
    database.cleanup().await?;
    Ok(())
}

async fn changed_operator(tx: &mut Transaction<'_, Postgres>, operator: usize) -> Result<()> {
    match operator {
        2 => {
            crate::scope::wrapper_registrar::include_names_for_scoped_registrars(tx, "bench", 10)
                .await?
        }
        3 => {
            crate::scope::registrar_bindings::include_names_for_scoped_unnamed_lease_rows(
                tx, "bench", 10,
            )
            .await?
        }
        _ => unreachable!(),
    }
    Ok(())
}

async fn old_operator(tx: &mut Transaction<'_, Postgres>, operator: usize) -> Result<()> {
    let statement = include_str!("binding_previous.sql")
        .split(';')
        .filter(|s| !s.trim().is_empty())
        .nth(operator)
        .unwrap();
    sqlx::query(statement)
        .bind("bench")
        .bind(10_i64)
        .execute(&mut **tx)
        .await?;
    Ok(())
}

#[tokio::test]
async fn binding_history_probes_preserve_null_visibility_namespace_and_closed_history() -> Result<()>
{
    let database =
        TestDatabase::create(TestDatabaseConfig::new("binding_history_variants")).await?;
    let mut tx = database.pool().begin().await?;
    sqlx::raw_sql(include_str!("binding_fixture.sql"))
        .execute(&mut *tx)
        .await?;
    sqlx::query("INSERT INTO project_scope_resources VALUES(md5('lease-1')::uuid)")
        .execute(&mut *tx)
        .await?;
    for mutation in [
        "SELECT 1",
        "UPDATE normalized_events SET namespace=NULL WHERE normalized_event_id=1",
        "UPDATE normalized_events SET namespace='basenames' WHERE normalized_event_id=1",
        "UPDATE normalized_events SET after_state='{}' WHERE normalized_event_id=1",
        "UPDATE normalized_events SET after_state='{\"namehash\":\"   \"}' WHERE normalized_event_id=1",
        "UPDATE normalized_events SET after_state='{\"namehash\":\"node-1\"}' WHERE normalized_event_id=1",
        "UPDATE normalized_events SET consumer_visibility='latent' WHERE normalized_event_id=1",
        "UPDATE normalized_events SET block_hash='orphan',block_number=9 WHERE normalized_event_id=1",
        "UPDATE normalized_events SET logical_name_id='ens:node-1' WHERE normalized_event_id=1",
        "UPDATE normalized_events SET canonicality_state='orphaned' WHERE normalized_event_id=1",
        "UPDATE normalized_events SET block_number=11 WHERE normalized_event_id=1",
        "UPDATE normalized_events SET chain_id='other' WHERE normalized_event_id=1",
        "UPDATE normalized_events SET event_kind='Other' WHERE normalized_event_id=1",
        "UPDATE normalized_events SET after_state=jsonb_build_object('wrapped_registrar_resource_id',upper(md5('lease-1')::uuid::text)) WHERE normalized_event_id=101",
        "UPDATE normalized_events SET consumer_visibility='latent' WHERE normalized_event_id=101",
        "UPDATE normalized_events SET block_hash='orphan',block_number=9 WHERE normalized_event_id=101",
        "UPDATE normalized_events SET chain_id='other' WHERE normalized_event_id=101",
        "UPDATE surface_bindings SET block_hash='orphan',block_number=9 WHERE resource_id=md5('lease-1')::uuid",
        "UPDATE surface_bindings SET authority_arm='ens_v2' WHERE resource_id=md5('lease-1')::uuid",
        "UPDATE name_surfaces SET namehash=NULL WHERE logical_name_id='ens:node-1'",
    ] {
        sqlx::query("SAVEPOINT variant").execute(&mut *tx).await?;
        sqlx::raw_sql(mutation).execute(&mut *tx).await?;
        for operator in [2, 3] {
            sqlx::query("SAVEPOINT old").execute(&mut *tx).await?;
            old_operator(&mut tx, operator).await?;
            let expected = scope(&mut tx).await?;
            sqlx::query("ROLLBACK TO SAVEPOINT old")
                .execute(&mut *tx)
                .await?;
            changed_operator(&mut tx, operator).await?;
            assert_eq!(
                scope(&mut tx).await?,
                expected,
                "operator={operator}, mutation={mutation}"
            );
            sqlx::query("ROLLBACK TO SAVEPOINT old")
                .execute(&mut *tx)
                .await?;
        }
        sqlx::query("ROLLBACK TO SAVEPOINT variant")
            .execute(&mut *tx)
            .await?;
    }
    tx.rollback().await?;
    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn broad_binding_history_uses_bounded_probes_and_matches_literal_old_sql() -> Result<()> {
    let database = TestDatabase::create(TestDatabaseConfig::new("binding_history_broad")).await?;
    let mut tx = database.pool().begin().await?;
    sqlx::raw_sql(include_str!("binding_fixture.sql"))
        .execute(&mut *tx)
        .await?;
    sqlx::raw_sql("CREATE INDEX ON chain_lineage(chain_id,block_number,block_hash);
        INSERT INTO chain_lineage SELECT 'bench','history-'||i,i,'canonical','2026-09-01'::timestamptz FROM generate_series(20,200000)i;
        INSERT INTO normalized_events SELECT 100000+i*20+e.normalized_event_id,e.resource_id,e.logical_name_id,e.namespace,e.chain_id,e.block_hash,e.block_number,e.source_family,e.event_kind,e.consumer_visibility,e.canonicality_state,e.after_state FROM normalized_events e CROSS JOIN generate_series(1,200)i WHERE e.normalized_event_id BETWEEN 1 AND 20;
        INSERT INTO normalized_events SELECT 1000000+i,md5('unrelated-event-'||i)::uuid,NULL,'ens','bench','canonical',10,'ens_v1_registrar_l1','RegistrationGranted','activated','canonical','{\"namehash\":\"unrelated\"}' FROM generate_series(1,100000)i;
        INSERT INTO project_scope_resources SELECT md5('lease-'||i)::uuid FROM generate_series(1,19)i;
        INSERT INTO project_scope_resources SELECT md5('broad-frontier-'||i)::uuid FROM generate_series(1,16000)i;
        ANALYZE chain_lineage; ANALYZE normalized_events;")
        .execute(&mut *tx).await?;
    let session = crate::profile::Session::create(&std::env::temp_dir())?;
    for pass in 0..3 {
        if pass == 1 {
            sqlx::query("INSERT INTO project_scope_resources VALUES(md5('lease-20')::uuid)")
                .execute(&mut *tx)
                .await?;
        }
        for operator in [2, 3] {
            sqlx::query("SAVEPOINT old").execute(&mut *tx).await?;
            old_operator(&mut tx, operator).await?;
            let expected = scope(&mut tx).await?;
            sqlx::query("ROLLBACK TO SAVEPOINT old")
                .execute(&mut *tx)
                .await?;
            session.scope(changed_operator(&mut tx, operator)).await?;
            assert_eq!(
                scope(&mut tx).await?,
                expected,
                "operator={operator}, pass={pass}"
            );
        }
    }
    fn assert_keyed(node: &serde_json::Value) {
        if node["Actual Loops"].as_u64().unwrap_or(0) > 0
            && matches!(
                node["Relation Name"].as_str(),
                Some("normalized_events" | "chain_lineage")
            )
        {
            assert_ne!(
                node["Node Type"].as_str(),
                Some("Seq Scan"),
                "whole history scan: {node}"
            );
            assert!(
                node["Actual Rows"].as_u64().unwrap_or(0) < 10000,
                "unbounded history input"
            );
        }
        if let Some(children) = node["Plans"].as_array() {
            for child in children {
                assert_keyed(child);
            }
        }
    }
    for entry in std::fs::read_dir(session.directory())? {
        let plan: serde_json::Value = serde_json::from_slice(&std::fs::read(entry?.path())?)?;
        assert_keyed(&plan[0]["Plan"]);
    }
    std::fs::remove_dir_all(session.directory())?;
    tx.rollback().await?;
    database.cleanup().await?;
    Ok(())
}
