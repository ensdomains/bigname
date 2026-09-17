use std::collections::BTreeSet;

use anyhow::{Result, ensure};
use bigname_test_support::{TestDatabase, TestDatabaseConfig};
use serde_json::{Value, json};
use sqlx::{Acquire, Postgres, Transaction, raw_sql};

use super::{ANALYZE_HISTORY_SCOPES_SQL, SCOPED_NAME_HISTORY_SQL, SCOPED_PRIMARY_HISTORY_SQL};

const CHAIN: &str = "project-history-test";
const BASELINE: &[&str] = &[
    include_str!("../../../../../schema-v2/baseline/01_chain.sql"),
    include_str!("../../../../../schema-v2/baseline/02_raw_facts.sql"),
    include_str!("../../../../../schema-v2/baseline/03_identity.sql"),
    include_str!("../../../../../schema-v2/baseline/04_manifests.sql"),
    include_str!("../../../../../schema-v2/baseline/05_normalized_events.sql"),
];
const FIXTURE: &str = include_str!("../../../tests/scoped_history/fixture.sql");
const UNRELATED: &str = include_str!("../../../tests/scoped_history/unrelated.sql");
const PREVIOUS_NAMES: &str = include_str!("../../../tests/scoped_history/previous_names.sql");
const PREVIOUS_PRIMARY: &str = include_str!("../../../tests/scoped_history/previous_primary.sql");
const MIGRATION: &str =
    include_str!("../../../../../migrations/20260917131000_project_scoped_history_indexes.sql");
const VALIDITY_CHECK: &str = include_str!(
    "../../../../../migrations/20260917161000_project_scoped_history_index_validity_check.sql"
);
const INDEX_SUFFIXES: &[&str] = &[
    "name_node",
    "name_child",
    "name_after_target",
    "name_before_target",
    "primary_after",
    "primary_before",
    "primary_after_source",
    "primary_before_source",
];

#[tokio::test]
async fn scoped_history_preserves_before_after_visibility_and_replay_evidence() -> Result<()> {
    let database =
        TestDatabase::create(TestDatabaseConfig::new("project_history_equality")).await?;
    let mut transaction = database.pool().begin().await?;
    initialize(&mut transaction).await?;
    verify_index_migration(&mut transaction).await?;
    for (previous, current, expected) in queries() {
        for (chain, target) in [
            (CHAIN, 9),
            (CHAIN, 10),
            (CHAIN, 11),
            ("project-history-other", 10),
        ] {
            let old = identities(&mut transaction, previous, chain, target).await?;
            let new = identities(&mut transaction, current, chain, target).await?;
            assert_eq!(
                new, old,
                "history multiset changed for {expected}, {chain}, {target}"
            );
            if chain == CHAIN && target == 10 {
                let expected = sqlx::query_scalar::<_, String>(&format!(
                    "SELECT event_identity FROM history_fixture WHERE {expected} ORDER BY 1"
                ))
                .fetch_all(&mut *transaction)
                .await?;
                assert_eq!(
                    new.into_iter().collect::<BTreeSet<_>>(),
                    expected.into_iter().collect()
                );
            }
        }
    }
    // A repeated event key and a name in both scope tables still yield one staged ID.
    let combined = format!("{SCOPED_NAME_HISTORY_SQL} UNION {SCOPED_PRIMARY_HISTORY_SQL}");
    let selected = identities(&mut transaction, &combined, CHAIN, 10).await?;
    assert_eq!(
        selected.len(),
        selected.iter().collect::<BTreeSet<_>>().len()
    );
    raw_sql("TRUNCATE project_scope_names, project_scope_children, project_scope_primary")
        .execute(&mut *transaction)
        .await?;
    for (_, current, _) in queries() {
        assert!(
            identities(&mut transaction, current, CHAIN, 10)
                .await?
                .is_empty()
        );
    }
    transaction.rollback().await?;
    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn scoped_history_work_stays_keyed_when_unrelated_history_quadruples() -> Result<()> {
    let database = TestDatabase::create(TestDatabaseConfig::new("project_history_plan")).await?;
    let mut transaction = database.pool().begin().await?;
    initialize(&mut transaction).await?;
    let mut evidence = Vec::new();
    for (from, to) in [(1_i64, 25_000_i64), (25_001, 100_000)] {
        sqlx::query(UNRELATED)
            .bind(from)
            .bind(to)
            .execute(&mut *transaction)
            .await?;
        raw_sql("ANALYZE normalized_events")
            .execute(&mut *transaction)
            .await?;
        raw_sql(ANALYZE_HISTORY_SCOPES_SQL)
            .execute(&mut *transaction)
            .await?;
        let mut used_indexes = BTreeSet::new();
        for (previous, current, label) in queries() {
            let old = identities(&mut transaction, previous, CHAIN, 10).await?;
            let new = identities(&mut transaction, current, CHAIN, 10).await?;
            assert_eq!(new, old, "unrelated history changed {label} selection");
            let plan = sqlx::query_scalar::<_, Value>(&format!(
                "EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) {current}"
            ))
            .bind(CHAIN)
            .bind(10_i64)
            .fetch_one(&mut *transaction)
            .await?;
            let visited = inspect_plan(&plan[0]["Plan"], &mut used_indexes)?;
            ensure!(
                visited <= 1_024.0,
                "{label} examined unrelated event rows: {visited}; {plan}"
            );
            evidence.push(json!({"unrelated_rows":to * 2,"query":label,
                "selected_occurrences":new.len(),"event_rows_examined":visited,"plan":plan}));
        }
        for suffix in INDEX_SUFFIXES {
            let index = format!("normalized_events_project_{suffix}_idx");
            ensure!(
                used_indexes.contains(&index),
                "missing correlated access through {index}: {used_indexes:?}"
            );
        }
    }
    if let Some(directory) = std::env::var_os("BIGNAME_PROJECT_HISTORY_EVIDENCE_DIR") {
        let directory = std::path::PathBuf::from(directory);
        std::fs::create_dir_all(&directory)?;
        std::fs::write(
            directory.join("project-history-plans.json"),
            serde_json::to_vec_pretty(&evidence)?,
        )?;
    }
    transaction.rollback().await?;
    database.cleanup().await?;
    Ok(())
}

fn queries() -> [(&'static str, &'static str, &'static str); 2] {
    [
        (PREVIOUS_NAMES, SCOPED_NAME_HISTORY_SQL, "expected_name"),
        (
            PREVIOUS_PRIMARY,
            SCOPED_PRIMARY_HISTORY_SQL,
            "expected_primary",
        ),
    ]
}

async fn initialize(transaction: &mut Transaction<'_, Postgres>) -> Result<()> {
    raw_sql("CREATE SCHEMA bigname_phase; SET LOCAL search_path TO bigname_phase, public")
        .execute(&mut **transaction)
        .await?;
    for script in BASELINE {
        raw_sql(script).execute(&mut **transaction).await?;
    }
    raw_sql(FIXTURE).execute(&mut **transaction).await?;
    Ok(())
}

async fn identities(
    transaction: &mut Transaction<'_, Postgres>,
    query: &str,
    chain: &str,
    target: i64,
) -> Result<Vec<String>> {
    Ok(sqlx::query_scalar(&format!(
        "WITH selected AS ({query}) SELECT event_identity FROM selected
         JOIN normalized_events USING (normalized_event_id) ORDER BY event_identity"
    ))
    .bind(chain)
    .bind(target)
    .fetch_all(&mut **transaction)
    .await?)
}

async fn verify_index_migration(transaction: &mut Transaction<'_, Postgres>) -> Result<()> {
    let definitions = |names: Vec<String>| {
        sqlx::query_as::<_, (String, String)>(
            "SELECT indexname, indexdef FROM pg_indexes
             WHERE schemaname = 'bigname_phase' AND indexname = ANY($1) ORDER BY indexname",
        )
        .bind(names)
    };
    let names = INDEX_SUFFIXES
        .iter()
        .map(|suffix| format!("normalized_events_project_{suffix}_idx"))
        .collect::<Vec<_>>();
    let baseline = definitions(names.clone())
        .fetch_all(&mut **transaction)
        .await?;
    ensure!(
        baseline.len() == INDEX_SUFFIXES.len(),
        "baseline omitted a history index"
    );
    for name in &names {
        sqlx::query(&format!("DROP INDEX {name}"))
            .execute(&mut **transaction)
            .await?;
    }
    // The initialized-database migration must recreate the fresh baseline and be repeatable.
    for _ in 0..2 {
        raw_sql(MIGRATION).execute(&mut **transaction).await?;
    }
    assert_eq!(
        baseline,
        definitions(names.clone())
            .fetch_all(&mut **transaction)
            .await?
    );
    raw_sql(VALIDITY_CHECK).execute(&mut **transaction).await?;
    // The check reads definitions under its own search_path and must put this one back.
    let search_path: String = sqlx::query_scalar("SELECT current_setting('search_path')")
        .fetch_one(&mut **transaction)
        .await?;
    ensure!(
        search_path == "bigname_phase, public",
        "validity check left search_path as {search_path}"
    );
    // IF NOT EXISTS adopts a relation by name alone. An interrupted concurrent build leaves an
    // invalid index, and a wrong manual build leaves other keys; the later check must refuse both.
    for (name, reviewed) in &baseline {
        // A valid, ready index whose JSON key literals start with the schema name indexes other
        // values, yet prints like the reviewed one once the schema name is stripped from the text.
        let schema_in_literal = reviewed.replace("->> '", "->> 'bigname_phase.");
        ensure!(&schema_in_literal != reviewed, "{name} has no JSON key");
        for (broken_shape, refusal) in [
            (
                format!(
                    "UPDATE pg_index SET indisvalid = false
                     WHERE indexrelid = 'bigname_phase.{name}'::regclass"
                ),
                "exists but is not a valid and ready index",
            ),
            (
                format!(
                    "DROP INDEX bigname_phase.{name};
                     CREATE INDEX {name} ON bigname_phase.normalized_events (block_number, chain_id)"
                ),
                "exists but does not have the reviewed definition",
            ),
            (
                format!("DROP INDEX bigname_phase.{name}; {schema_in_literal}"),
                "exists but does not have the reviewed definition",
            ),
            (
                format!("DROP INDEX bigname_phase.{name}"),
                "does not exist although bigname_phase.normalized_events does",
            ),
        ] {
            let mut savepoint = transaction.begin().await?;
            raw_sql(&broken_shape).execute(&mut *savepoint).await?;
            if !refusal.starts_with("does not exist") {
                // The index-building migration alone records success over the unusable index.
                raw_sql(MIGRATION).execute(&mut *savepoint).await?;
            }
            let error = raw_sql(VALIDITY_CHECK)
                .execute(&mut *savepoint)
                .await
                .expect_err("validity check accepted an unusable history index");
            ensure!(
                error.to_string().contains(&format!("{name} {refusal}")),
                "unexpected refusal for {name}: {error}"
            );
            savepoint.rollback().await?;
        }
    }
    assert_eq!(
        baseline,
        definitions(names).fetch_all(&mut **transaction).await?
    );
    Ok(())
}

fn inspect_plan(node: &Value, indexes: &mut BTreeSet<String>) -> Result<f64> {
    let mut examined = 0.0;
    if node["Relation Name"] == "normalized_events" {
        ensure!(
            matches!(
                node["Node Type"].as_str(),
                Some("Index Scan" | "Index Only Scan" | "Bitmap Heap Scan")
            ),
            "history access is not indexed: {node}"
        );
        let loops = node["Actual Loops"].as_f64().unwrap_or_default();
        examined = loops
            * (node["Actual Rows"].as_f64().unwrap_or_default()
                + node["Rows Removed by Filter"].as_f64().unwrap_or_default()
                + node["Rows Removed by Index Recheck"]
                    .as_f64()
                    .unwrap_or_default());
    }
    if let Some(index) = node["Index Name"]
        .as_str()
        .filter(|name| name.starts_with("normalized_events_"))
    {
        ensure!(
            index.starts_with("normalized_events_project_"),
            "history used a broad index: {node}"
        );
        let condition = node["Index Cond"].as_str().unwrap_or_default();
        ensure!(
            condition.contains("scope.") || condition.contains("project_scope_"),
            "history probe is not correlated: {node}"
        );
        indexes.insert(index.to_owned());
    }
    for child in node["Plans"].as_array().into_iter().flatten() {
        examined += inspect_plan(child, indexes)?;
    }
    Ok(examined)
}
