use anyhow::Result;
use bigname_test_support::{TestDatabase, TestDatabaseConfig};
use serde_json::Value;
use sqlx::{Postgres, Transaction};

const PREVIOUS: &str = include_str!("linked_records_previous.sql");

async fn snapshot(tx: &mut Transaction<'_, Postgres>) -> Result<Vec<Value>> {
    Ok(sqlx::query_scalar(
        "SELECT to_jsonb(event) FROM project_events event ORDER BY normalized_event_id",
    )
    .fetch_all(&mut **tx)
    .await?)
}

#[tokio::test]
async fn profiled_linked_inputs_match_literal_history_and_execute_once() -> Result<()> {
    let database = TestDatabase::create(TestDatabaseConfig::new("linked_input_profile")).await?;
    let mut tx = database.pool().begin().await?;
    sqlx::raw_sql(include_str!("linked_records_fixture.sql"))
        .execute(&mut *tx)
        .await?;
    let session = crate::profile::Session::create(&std::env::temp_dir())?;
    for (step, (chain, target)) in [
        ("bench", 9_i64),
        ("bench", 10),
        ("bench", 10),
        ("other", 10),
    ]
    .into_iter()
    .enumerate()
    {
        sqlx::query("SAVEPOINT linked_reference")
            .execute(&mut *tx)
            .await?;
        sqlx::query(PREVIOUS)
            .bind(chain)
            .bind(target)
            .execute(&mut *tx)
            .await?;
        let expected = snapshot(&mut tx).await?;
        assert!(!expected.is_empty());
        sqlx::query("ROLLBACK TO SAVEPOINT linked_reference")
            .execute(&mut *tx)
            .await?;
        super::include(&mut tx, chain, target, false).await?;
        assert_eq!(snapshot(&mut tx).await?, expected, "ordinary step {step}");
        assert_eq!(std::fs::read_dir(session.directory())?.count(), step);
        sqlx::query("ROLLBACK TO SAVEPOINT linked_reference")
            .execute(&mut *tx)
            .await?;
        session
            .scope(super::include(&mut tx, chain, target, false))
            .await?;
        assert_eq!(snapshot(&mut tx).await?, expected, "profiled step {step}");
        let calls: i64 = sqlx::query_scalar("SELECT count(*) FROM linked_input_calls")
            .fetch_one(&mut *tx)
            .await?;
        assert_eq!(calls, (step + 1) as i64, "INSERT executed more than once");
        assert_eq!(std::fs::read_dir(session.directory())?.count(), step + 1);
        sqlx::query("RELEASE SAVEPOINT linked_reference")
            .execute(&mut *tx)
            .await?;
    }
    let ids: Vec<i64> =
        sqlx::query_scalar("SELECT normalized_event_id FROM project_events ORDER BY 1")
            .fetch_all(&mut *tx)
            .await?;
    assert_eq!(
        ids,
        [1, 1, 2, 2, 3, 3, 6, 6, 12, 12, 17, 17, 18, 18, 19, 19, 20]
    );
    session
        .scope(super::include(&mut tx, "bench", 11, true))
        .await?;
    assert_eq!(std::fs::read_dir(session.directory())?.count(), 4);
    assert_eq!(snapshot(&mut tx).await?.len(), ids.len());
    for entry in std::fs::read_dir(session.directory())? {
        let entry = entry?;
        assert!(
            entry
                .file_name()
                .to_string_lossy()
                .contains("-linked_records-")
        );
        let plan: Value = serde_json::from_slice(&std::fs::read(entry.path())?)?;
        assert_eq!(plan[0]["Plan"]["Operation"], "Insert");
    }
    std::fs::remove_dir_all(session.directory())?;
    tx.rollback().await?;
    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn profiled_linked_inputs_preserve_database_error_classification() -> Result<()> {
    let database = TestDatabase::create(TestDatabaseConfig::new("linked_input_error")).await?;
    let mut tx = database.pool().begin().await?;
    sqlx::raw_sql(include_str!("linked_records_fixture.sql"))
        .execute(&mut *tx)
        .await?;
    // Case-equivalent scopes create duplicate IDs within one INSERT; preserve that error.
    sqlx::query("ALTER TABLE project_events ADD PRIMARY KEY(normalized_event_id)")
        .execute(&mut *tx)
        .await?;
    let session = crate::profile::Session::create(&std::env::temp_dir())?;
    sqlx::query("SAVEPOINT linked_error")
        .execute(&mut *tx)
        .await?;
    let ordinary = super::include(&mut tx, "bench", 10, false)
        .await
        .unwrap_err();
    sqlx::query("ROLLBACK TO SAVEPOINT linked_error")
        .execute(&mut *tx)
        .await?;
    let profiled = session
        .scope(super::include(&mut tx, "bench", 10, false))
        .await
        .unwrap_err();
    assert_eq!(ordinary.kind(), crate::ErrorKind::DataIntegrity);
    assert_eq!(profiled.kind(), ordinary.kind());
    assert_eq!(std::fs::read_dir(session.directory())?.count(), 0);
    std::fs::remove_dir_all(session.directory())?;
    tx.rollback().await?;
    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn linked_partial_index_and_narrow_ids_avoid_unrelated_history() -> Result<()> {
    let database = TestDatabase::create(TestDatabaseConfig::new("linked_input_index")).await?;
    let mut tx = database.pool().begin().await?;
    sqlx::raw_sql(include_str!("linked_records_fixture.sql"))
        .execute(&mut *tx)
        .await?;
    sqlx::raw_sql("INSERT INTO normalized_events
        SELECT 1000+i,'bench','RecordChanged',10,'current','canonical','activated',
               jsonb_build_object('resolver','0xShared','storage_model','node','value',repeat('wide-row',128)),
               jsonb_build_object('transaction_hash','unrelated-'||i)
        FROM generate_series(1,30000)i;
        INSERT INTO project_events SELECT * FROM normalized_events WHERE normalized_event_id > 1000;
        INSERT INTO project_staged_event_ids SELECT normalized_event_id FROM normalized_events WHERE normalized_event_id > 1000;
        ANALYZE normalized_events; ANALYZE project_events; ANALYZE project_staged_event_ids; ANALYZE project_scope_resolvers")
        .execute(&mut *tx).await?;
    sqlx::query("SAVEPOINT linked_index_reference")
        .execute(&mut *tx)
        .await?;
    sqlx::query(PREVIOUS)
        .bind("bench")
        .bind(10_i64)
        .execute(&mut *tx)
        .await?;
    let expected = snapshot(&mut tx).await?;
    sqlx::query("ROLLBACK TO SAVEPOINT linked_index_reference")
        .execute(&mut *tx)
        .await?;
    // Fixture UPDATEs ran in this transaction. Repack their HOT chains before
    // creating an expression index; otherwise indcheckxmin prevents its use in
    // this same snapshot even though the index is valid.
    sqlx::query("CLUSTER normalized_events USING normalized_events_pkey")
        .execute(&mut *tx)
        .await?;
    sqlx::raw_sql(include_str!("linked_records_index_candidate.sql"))
        .execute(&mut *tx)
        .await?;
    sqlx::query("ANALYZE normalized_events")
        .execute(&mut *tx)
        .await?;
    let plan: Value = sqlx::query_scalar(&format!(
        "EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON, TIMING OFF) {}",
        super::INCLUDE_SQL
    ))
    .bind("bench")
    .bind(10_i64)
    .fetch_one(&mut *tx)
    .await?;
    assert_eq!(snapshot(&mut tx).await?, expected);
    fn inspect(node: &Value, visited: &mut f64, indexed: &mut bool) {
        if node["Relation Name"] == "project_events" {
            assert_eq!(node["Operation"], "Insert", "anti-join reread wide stage");
        }
        if node["Relation Name"] == "normalized_events" {
            *visited += node["Actual Loops"].as_f64().unwrap_or(0.0)
                * (node["Actual Rows"].as_f64().unwrap_or(0.0)
                    + node["Rows Removed by Filter"].as_f64().unwrap_or(0.0));
        }
        *indexed |= node["Index Name"] == "normalized_events_linked_resolver_history_idx";
        for child in node["Plans"].as_array().into_iter().flatten() {
            inspect(child, visited, indexed);
        }
    }
    let (mut visited, mut indexed) = (0.0, false);
    inspect(&plan[0]["Plan"], &mut visited, &mut indexed);
    anyhow::ensure!(
        indexed && visited < 100.0,
        "linked history visited {visited} rows: {plan}"
    );
    eprintln!(
        "linked indexed input: {} ms; {visited} normalized rows",
        plan[0]["Execution Time"]
    );
    tx.rollback().await?;
    database.cleanup().await?;
    Ok(())
}
