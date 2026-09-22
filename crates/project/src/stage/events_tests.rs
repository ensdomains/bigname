use anyhow::{Result, ensure};
use bigname_test_support::{TestDatabase, TestDatabaseConfig};
use serde_json::Value;
use sqlx::{Postgres, Transaction};

async fn fixture(tx: &mut Transaction<'_, Postgres>) -> Result<()> {
    sqlx::raw_sql(include_str!("linked_records_fixture.sql"))
        .execute(&mut **tx)
        .await?;
    sqlx::raw_sql(
        r#"DROP TABLE project_events, project_staged_event_ids;
        ALTER TABLE normalized_events ADD COLUMN logical_name_id text;
        ALTER TABLE normalized_events ADD COLUMN resource_id uuid;
        INSERT INTO normalized_events(normalized_event_id,chain_id,event_kind,block_number,block_hash,canonicality_state,consumer_visibility,after_state,raw_fact_ref)
        VALUES(21,'bench','SourceManifestUpdated',NULL,NULL,'canonical','activated','{}','{"manifest":"null-block"}'),
              (22,'bench','ResolverRecordLinked',10,NULL,'canonical','activated','{"resolver":"0xshared"}','{}'),
              (23,'bench','ResolverRecordLinked',NULL,'current','canonical','activated','{"resolver":"0xshared"}','{}');
        CREATE TEMP TABLE project_event_ids(normalized_event_id bigint PRIMARY KEY);
        INSERT INTO project_event_ids SELECT normalized_event_id FROM normalized_events WHERE normalized_event_id NOT IN (2,3);
        ANALYZE normalized_events; ANALYZE project_event_ids; ANALYZE chain_lineage"#,
    )
    .execute(&mut **tx)
    .await?;
    Ok(())
}

async fn rows(tx: &mut Transaction<'_, Postgres>) -> Result<Vec<Value>> {
    Ok(sqlx::query_scalar(
        "SELECT to_jsonb(event) FROM project_events event ORDER BY normalized_event_id",
    )
    .fetch_all(&mut **tx)
    .await?)
}

#[tokio::test]
async fn staged_events_preserve_full_rows_admission_and_linked_repeats() -> Result<()> {
    let database = TestDatabase::create(TestDatabaseConfig::new("staged_event_rows")).await?;
    let mut tx = database.pool().begin().await?;
    fixture(&mut tx).await?;
    for full in [false, true] {
        for (chain, target) in [
            ("bench", 9_i64),
            ("bench", 10),
            ("bench", 11),
            ("other", 10),
        ] {
            sqlx::query("SAVEPOINT events_reference")
                .execute(&mut *tx)
                .await?;
            let source = if full {
                "normalized_events event"
            } else {
                "normalized_events event JOIN project_event_ids scope USING(normalized_event_id)"
            };
            let old = include_str!("events_previous.sql").replace("{event_source}", source);
            sqlx::query(&old)
                .bind(chain)
                .bind(target)
                .execute(&mut *tx)
                .await?;
            let expected = rows(&mut tx).await?;
            ensure!(!expected.is_empty());
            sqlx::query("ROLLBACK TO SAVEPOINT events_reference")
                .execute(&mut *tx)
                .await?;
            super::create(&mut tx, chain, target, full).await?;
            assert_eq!(
                rows(&mut tx).await?,
                expected,
                "full={full} {chain} {target}"
            );
            if !full {
                let selected: Vec<i64> = sqlx::query_scalar(
                    "SELECT normalized_event_id FROM project_staged_event_ids ORDER BY 1",
                )
                .fetch_all(&mut *tx)
                .await?;
                let staged: Vec<i64> =
                    sqlx::query_scalar("SELECT normalized_event_id FROM project_events ORDER BY 1")
                        .fetch_all(&mut *tx)
                        .await?;
                assert_eq!(selected, staged);
                // Later calls can widen their target, change chain, or admit a formerly
                // ineligible event. Only IDs actually staged may suppress those rows.
                sqlx::query("UPDATE normalized_events SET consumer_visibility='activated' WHERE normalized_event_id=8")
                    .execute(&mut *tx).await?;
                for (linked_chain, linked_target) in [
                    (chain, target),
                    (chain, target),
                    ("bench", 11),
                    ("other", 10),
                ] {
                    sqlx::query("SAVEPOINT linked_reference")
                        .execute(&mut *tx)
                        .await?;
                    sqlx::query(include_str!("linked_records_previous.sql"))
                        .bind(linked_chain)
                        .bind(linked_target)
                        .execute(&mut *tx)
                        .await?;
                    let expected = rows(&mut tx).await?;
                    sqlx::query("ROLLBACK TO SAVEPOINT linked_reference")
                        .execute(&mut *tx)
                        .await?;
                    super::super::linked_records::include(
                        &mut tx,
                        linked_chain,
                        linked_target,
                        false,
                    )
                    .await?;
                    assert_eq!(rows(&mut tx).await?, expected);
                    let mismatches: i64 = sqlx::query_scalar("SELECT count(*) FROM (
                        (SELECT normalized_event_id FROM project_events EXCEPT SELECT normalized_event_id FROM project_staged_event_ids)
                        UNION ALL
                        (SELECT normalized_event_id FROM project_staged_event_ids EXCEPT SELECT normalized_event_id FROM project_events)) mismatch")
                        .fetch_one(&mut *tx).await?;
                    assert_eq!(mismatches, 0);
                    sqlx::query("RELEASE SAVEPOINT linked_reference")
                        .execute(&mut *tx)
                        .await?;
                }
            }
            sqlx::query("ROLLBACK TO SAVEPOINT events_reference")
                .execute(&mut *tx)
                .await?;
            sqlx::query("RELEASE SAVEPOINT events_reference")
                .execute(&mut *tx)
                .await?;
        }
    }
    tx.rollback().await?;
    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn scoped_lineage_plan_probes_only_selected_event_blocks() -> Result<()> {
    let database = TestDatabase::create(TestDatabaseConfig::new("staged_lineage_plan")).await?;
    let mut tx = database.pool().begin().await?;
    fixture(&mut tx).await?;
    sqlx::raw_sql("INSERT INTO chain_lineage SELECT 'bench',100+i,'unrelated-'||i,'canonical' FROM generate_series(1,100000)i;
        ANALYZE chain_lineage")
        .execute(&mut *tx).await?;
    let session = crate::profile::Session::create(&std::env::temp_dir())?;
    session
        .scope(super::create(&mut tx, "bench", 10, false))
        .await?;
    let entry = std::fs::read_dir(session.directory())?.next().unwrap()?;
    let plan: Value = serde_json::from_slice(&std::fs::read(entry.path())?)?;
    fn visited(node: &Value) -> f64 {
        let own = if node["Relation Name"] == "chain_lineage" {
            node["Actual Loops"].as_f64().unwrap_or(0.0)
                * (1.0
                    + node["Actual Rows"].as_f64().unwrap_or(0.0)
                    + node["Rows Removed by Filter"].as_f64().unwrap_or(0.0))
        } else {
            0.0
        };
        own + node["Plans"]
            .as_array()
            .into_iter()
            .flatten()
            .map(visited)
            .sum::<f64>()
    }
    let examined = visited(&plan[0]["Plan"]);
    ensure!(
        examined > 0.0 && examined < 100.0,
        "read unrelated chain history: {examined}: {plan}"
    );
    ensure!(rows(&mut tx).await?.len() > 5);
    eprintln!(
        "scoped event admission: {} ms; {examined} lineage row/probe upper bound",
        plan[0]["Execution Time"]
    );
    std::fs::remove_dir_all(session.directory())?;
    tx.rollback().await?;
    database.cleanup().await?;
    Ok(())
}
