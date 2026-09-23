use anyhow::{Result, ensure};
use bigname_test_support::{TestDatabase, TestDatabaseConfig};
use serde_json::Value;
use sqlx::raw_sql;

const SEED: &str = include_str!("../../../tests/event_selection/seed.sql");
const VARIANTS: &str = include_str!("../../../tests/event_selection/variants.sql");
const PREVIOUS: &str = include_str!("../../../tests/event_selection/previous.sql");

#[tokio::test]
async fn event_selection_preserves_history_sets_and_replay_predicates() -> Result<()> {
    let database = TestDatabase::create(TestDatabaseConfig::new("event_selection_sets")).await?;
    let mut tx = database.pool().begin().await?;
    raw_sql(
        &SEED
            .replace("__NAMES__", "30")
            .replace("__RECORDS__", "1000"),
    )
    .execute(&mut *tx)
    .await?;
    raw_sql(VARIANTS).execute(&mut *tx).await?;
    for (chain, target) in [
        ("bench", 9_i64),
        ("bench", 10),
        ("bench", 11),
        ("other", 10),
    ] {
        raw_sql("CREATE TEMP TABLE project_event_ids(normalized_event_id bigint PRIMARY KEY)")
            .execute(&mut *tx)
            .await?;
        sqlx::query(PREVIOUS)
            .bind(chain)
            .bind(target)
            .execute(&mut *tx)
            .await?;
        let old: Vec<i64> =
            sqlx::query_scalar("SELECT normalized_event_id FROM project_event_ids ORDER BY 1")
                .fetch_all(&mut *tx)
                .await?;
        raw_sql("DROP TABLE project_event_ids")
            .execute(&mut *tx)
            .await?;
        super::create(&mut tx, chain, target).await?;
        let new: Vec<i64> =
            sqlx::query_scalar("SELECT normalized_event_id FROM project_event_ids ORDER BY 1")
                .fetch_all(&mut *tx)
                .await?;
        assert_eq!(old, new, "event set changed for {chain} through {target}");
        ensure!(!new.is_empty(), "differential fixture selected nothing");
        // Repeating every arm must not change the selected set.
        sqlx::query(PREVIOUS)
            .bind(chain)
            .bind(target)
            .execute(&mut *tx)
            .await?;
        let repeated: Vec<i64> =
            sqlx::query_scalar("SELECT normalized_event_id FROM project_event_ids ORDER BY 1")
                .fetch_all(&mut *tx)
                .await?;
        assert_eq!(new, repeated);
        raw_sql("DROP TABLE project_event_ids")
            .execute(&mut *tx)
            .await?;
    }
    raw_sql(
        "TRUNCATE project_scope_names, project_scope_children, project_scope_resources,
        project_scope_primary, project_scope_ancestors, project_scope_resolvers,
        project_scope_resolver_candidate_events, project_scope_account_permissions, project_changed_events",
    )
    .execute(&mut *tx)
    .await?;
    super::create(&mut tx, "bench", 10).await?;
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM project_event_ids")
        .fetch_one(&mut *tx)
        .await?;
    assert_eq!(count, 1); // The null-block manifest event is intentionally unscoped.
    tx.rollback().await?;
    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn node_history_probes_remain_keyed_for_thousands_of_v2_names() -> Result<()> {
    use super::super::node_record_events::{self, SCOPED_NODE_RECORD_EVENT_IDS_SQL};
    let database = TestDatabase::create(TestDatabaseConfig::new("node_history_scale")).await?;
    let mut tx = database.pool().begin().await?;
    raw_sql(
        &SEED
            .replace("__NAMES__", "3000")
            .replace("__RECORDS__", "30000"),
    )
    .execute(&mut *tx)
    .await?;
    node_record_events::prepare(&mut tx, "bench", 10).await?;
    let plan: Value = sqlx::query_scalar(&format!(
        "EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) {SCOPED_NODE_RECORD_EVENT_IDS_SQL}"
    ))
    .bind("bench")
    .bind(10_i64)
    .fetch_one(&mut *tx)
    .await?;
    let mut examined = 0.0;
    let mut indexed = false;
    inspect(&plan[0]["Plan"], &mut examined, &mut indexed)?;
    ensure!(indexed, "node history never used its lookup index: {plan}");
    ensure!(
        examined < 30000.0,
        "node lookup rescanned unrelated history: {examined} rows"
    );
    assert_eq!(plan[0]["Plan"]["Actual Rows"], 3000);
    eprintln!(
        "node history query: {} ms; examined {examined} rows",
        plan[0]["Execution Time"]
    );
    tx.rollback().await?;
    database.cleanup().await?;
    Ok(())
}

fn inspect(node: &Value, examined: &mut f64, indexed: &mut bool) -> Result<()> {
    if node["Relation Name"] == "project_node_record_history" {
        *examined += node["Actual Loops"].as_f64().unwrap_or_default()
            * (node["Actual Rows"].as_f64().unwrap_or_default()
                + node["Rows Removed by Filter"].as_f64().unwrap_or_default()
                + node["Rows Removed by Index Recheck"]
                    .as_f64()
                    .unwrap_or_default());
    }
    *indexed |= node["Index Name"] == "project_node_record_history_lookup";
    for child in node["Plans"].as_array().into_iter().flatten() {
        inspect(child, examined, indexed)?;
    }
    Ok(())
}

#[tokio::test]
async fn project_batch_future_is_send_for_the_runner() -> Result<()> {
    fn require_send(_: impl Send) {}
    let pool = sqlx::postgres::PgPoolOptions::new()
        .connect_lazy("postgres://unused:unused@127.0.0.1/unused")?;
    let engine = crate::Engine::new(pool);
    require_send(engine.run_batch(crate::BatchRequest {
        chain_id: "ethereum-sepolia".into(),
        target_block: 10,
        affected_from_block: 0,
        affected_to_block: 10,
        resume_current: None,
        mode: crate::RunMode::Redo,
    }));
    Ok(())
}

#[tokio::test]
async fn tiny_node_scope_does_not_materialize_unrelated_history() -> Result<()> {
    use super::super::node_record_events::STAGE_HISTORY_SQL;
    let database = TestDatabase::create(TestDatabaseConfig::new("tiny_node_scope")).await?;
    let mut tx = database.pool().begin().await?;
    raw_sql(
        &SEED
            .replace("__NAMES__", "3000")
            .replace("__RECORDS__", "100000"),
    )
    .execute(&mut *tx)
    .await?;
    raw_sql(
        "TRUNCATE project_scope_names, project_scope_children;
        INSERT INTO project_scope_names VALUES ('ens:node-1');
        INSERT INTO project_scope_children VALUES ('ens:node-2');
        ANALYZE project_scope_names; ANALYZE project_scope_children;",
    )
    .execute(&mut *tx)
    .await?;
    let plan: Value = sqlx::query_scalar(&format!(
        "EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) {STAGE_HISTORY_SQL}"
    ))
    .bind("bench")
    .bind(10_i64)
    .fetch_one(&mut *tx)
    .await?;
    let ids: Vec<i64> = sqlx::query_scalar(
        "SELECT normalized_event_id FROM project_node_record_history ORDER BY 1",
    )
    .fetch_all(&mut *tx)
    .await?;
    assert_eq!(ids, vec![100001, 100002]);
    fn visited(node: &Value) -> f64 {
        let own = if node["Relation Name"] == "normalized_events" {
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
            .map(visited)
            .sum::<f64>()
    }
    let rows = visited(&plan[0]["Plan"]);
    ensure!(
        rows < 100.0,
        "tiny scope visited {rows} historical rows: {plan}"
    );
    eprintln!(
        "tiny scope materialization: {} ms; {rows} history rows",
        plan[0]["Execution Time"]
    );
    raw_sql("DROP TABLE project_node_record_history; TRUNCATE project_scope_names, project_scope_children;")
        .execute(&mut *tx).await?;
    super::super::node_record_events::prepare(&mut tx, "bench", 10).await?;
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM project_node_record_history")
        .fetch_one(&mut *tx)
        .await?;
    assert_eq!(count, 0);
    tx.rollback().await?;
    database.cleanup().await?;
    Ok(())
}
