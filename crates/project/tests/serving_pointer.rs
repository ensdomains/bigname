//! Result and access-path regression checks for the serving-pointer staging statement.
use anyhow::{Context, Result, ensure};
use bigname_test_support::{TestDatabase, TestDatabaseConfig};
use serde_json::{Value, json};
use sqlx::{Postgres, Transaction, raw_sql};

const SOURCE: &str = include_str!("../src/builders/name_authority/stage.rs");
const PREFIX: &str = "CREATE TEMP TABLE project_name_serving ON COMMIT DROP AS";
const PREVIOUS: &str = include_str!("serving_pointer/previous.sql");
const FIXTURE: &str = include_str!("serving_pointer/fixture.sql");
const UNRELATED: &str = include_str!("serving_pointer/unrelated.sql");

// Exercise the actual statement without moving production SQL out of the shared staging file.
fn current() -> &'static str {
    let statement = SOURCE.split_once(PREFIX).expect("serving statement").1;
    statement.split_once("\",").expect("statement terminator").0
}
fn previous() -> &'static str {
    PREVIOUS
        .strip_prefix(PREFIX)
        .expect("previous statement")
        .trim()
        .trim_end_matches(';')
}
async fn initialize(tx: &mut Transaction<'_, Postgres>) -> Result<()> {
    raw_sql("SET LOCAL statement_timeout='30s'; SET LOCAL work_mem='16MB'; SET LOCAL jit=off;")
        .execute(&mut **tx)
        .await?;
    raw_sql(FIXTURE).execute(&mut **tx).await?;
    Ok(())
}
async fn rows(tx: &mut Transaction<'_, Postgres>, sql: &str) -> Result<Vec<Value>> {
    Ok(sqlx::query_scalar(&format!(
        "SELECT to_jsonb(result) FROM ({sql}) result ORDER BY logical_name_id"
    ))
    .fetch_all(&mut **tx)
    .await?)
}

#[tokio::test]
async fn serving_pointer_rewrite_preserves_the_complete_result_multiset() -> Result<()> {
    let db = TestDatabase::create(TestDatabaseConfig::new("serving_pointer_equality")).await?;
    let mut tx = db.pool().begin().await?;
    initialize(&mut tx).await?;
    let old = rows(&mut tx, previous()).await?;
    let new = rows(&mut tx, current()).await?;
    assert_eq!(
        new, old,
        "all serving columns and duplicate multiplicity must match"
    );
    let expected: Vec<(String, String)> =
        sqlx::query_as("SELECT logical_name_id,event_identity FROM expected ORDER BY 1")
            .fetch_all(&mut *tx)
            .await?;
    let actual = new
        .iter()
        .map(|v| {
            Ok((
                v["logical_name_id"].as_str().context("name")?.to_owned(),
                v["pointer_event_identity"]
                    .as_str()
                    .context("pointer")?
                    .to_owned(),
            ))
        })
        .collect::<Result<Vec<_>>>()?;
    assert_eq!(
        actual, expected,
        "clear/release/eligibility cases must not resurrect older pointers"
    );
    raw_sql("TRUNCATE project_name_authority")
        .execute(&mut *tx)
        .await?;
    assert!(rows(&mut tx, current()).await?.is_empty());
    assert!(rows(&mut tx, previous()).await?.is_empty());
    tx.rollback().await?;
    db.cleanup().await?;
    Ok(())
}

async fn explain(tx: &mut Transaction<'_, Postgres>, sql: &str) -> Result<Value> {
    Ok(sqlx::query_scalar(&format!(
        "EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON, TIMING OFF) {sql}"
    ))
    .fetch_one(&mut **tx)
    .await?)
}
fn inspected_rows(node: &Value) -> f64 {
    let own = if node["Relation Name"] == "project_events" {
        (node["Actual Rows"].as_f64().unwrap_or(0.0)
            + node["Rows Removed by Filter"].as_f64().unwrap_or(0.0)
            + node["Rows Removed by Index Recheck"]
                .as_f64()
                .unwrap_or(0.0))
            * node["Actual Loops"].as_f64().unwrap_or(0.0)
    } else {
        0.0
    };
    own + node["Plans"]
        .as_array()
        .map_or(0.0, |plans| plans.iter().map(inspected_rows).sum())
}
fn require_keyed_pointer_scans(node: &Value, inside_pointer: bool) -> Result<usize> {
    let inside = inside_pointer
        || node["Alias"]
            .as_str()
            .is_some_and(|alias| alias.starts_with("pointer"));
    let mut count = 0;
    if inside && node["Relation Name"] == "project_events" {
        if node["Node Type"] == "Bitmap Heap Scan" {
            // A bitmap heap scan carries only a recheck; the keyed condition
            // lives on its Bitmap Index Scan descendants.
            ensure!(
                keyed_bitmap_index_scans(node)? > 0,
                "bitmap heap scan without a keyed index scan: {node}"
            );
        } else {
            require_keyed_condition(node, node["Index Cond"].as_str().unwrap_or(""))?;
        }
        count += 1;
    }
    if let Some(plans) = node["Plans"].as_array() {
        for child in plans {
            count += require_keyed_pointer_scans(child, inside)?;
        }
    }
    Ok(count)
}
fn keyed_bitmap_index_scans(node: &Value) -> Result<usize> {
    let mut count = 0;
    if let Some(plans) = node["Plans"].as_array() {
        for child in plans {
            if child["Node Type"] == "Bitmap Index Scan" {
                require_keyed_condition(child, child["Index Cond"].as_str().unwrap_or(""))?;
                count += 1;
            } else if child["Node Type"] == "BitmapAnd" || child["Node Type"] == "BitmapOr" {
                count += keyed_bitmap_index_scans(child)?;
            }
        }
    }
    Ok(count)
}
fn require_keyed_condition(node: &Value, condition: &str) -> Result<()> {
    ensure!(
        condition.contains("logical_name_id =") || condition.contains("resource_id ="),
        "pointer scanned unrelated history: {node}"
    );
    ensure!(
        !condition.contains("logical_name_id IS NULL"),
        "broad NULL-name scan: {node}"
    );
    Ok(())
}

#[test]
fn plan_validator_accepts_keyed_index_and_bitmap_scans_and_rejects_broad_scans() {
    let keyed_index = json!({
        "Node Type": "Index Scan", "Alias": "pointer", "Relation Name": "project_events",
        "Index Cond": "(logical_name_id = authority.logical_name_id)"
    });
    assert_eq!(require_keyed_pointer_scans(&keyed_index, false).unwrap(), 1);
    let keyed_bitmap = json!({
        "Node Type": "Bitmap Heap Scan", "Alias": "pointer", "Relation Name": "project_events",
        "Recheck Cond": "(resource_id = linked.resource_id)",
        "Plans": [{
            "Node Type": "Bitmap Index Scan", "Index Name": "project_events_resource_idx",
            "Index Cond": "(resource_id = linked.resource_id)"
        }]
    });
    assert_eq!(
        require_keyed_pointer_scans(&keyed_bitmap, false).unwrap(),
        1
    );
    let unkeyed_bitmap = json!({
        "Node Type": "Bitmap Heap Scan", "Alias": "pointer", "Relation Name": "project_events",
        "Recheck Cond": "(logical_name_id IS NULL)",
        "Plans": [{
            "Node Type": "Bitmap Index Scan", "Index Name": "project_events_name_idx",
            "Index Cond": "(logical_name_id IS NULL)"
        }]
    });
    assert!(require_keyed_pointer_scans(&unkeyed_bitmap, false).is_err());
    let nested_or = json!({
        "Node Type": "Bitmap Heap Scan", "Alias": "pointer", "Relation Name": "project_events",
        "Plans": [{
            "Node Type": "BitmapOr",
            "Plans": [
                { "Node Type": "Bitmap Index Scan", "Index Cond": "(resource_id = linked.resource_id)" },
                { "Node Type": "Bitmap Index Scan", "Index Cond": "(logical_name_id = authority.logical_name_id)" }
            ]
        }]
    });
    assert_eq!(require_keyed_pointer_scans(&nested_or, false).unwrap(), 1);
    let empty_bitmap = json!({
        "Node Type": "Bitmap Heap Scan", "Alias": "pointer", "Relation Name": "project_events"
    });
    assert!(require_keyed_pointer_scans(&empty_bitmap, false).is_err());
    let seq_scan = json!({
        "Node Type": "Seq Scan", "Alias": "pointer", "Relation Name": "project_events"
    });
    assert!(require_keyed_pointer_scans(&seq_scan, false).is_err());
    let outside = json!({
        "Node Type": "Seq Scan", "Alias": "authority", "Relation Name": "project_events"
    });
    assert_eq!(require_keyed_pointer_scans(&outside, false).unwrap(), 0);
}

#[tokio::test]
async fn serving_pointer_plan_stays_keyed_as_unrelated_nameless_history_grows() -> Result<()> {
    let db = TestDatabase::create(TestDatabaseConfig::new("serving_pointer_plan")).await?;
    let mut tx = db.pool().begin().await?;
    initialize(&mut tx).await?;
    let mut evidence = Vec::new();
    for (from, to) in [(1_i64, 25_000_i64), (25_001, 100_000)] {
        sqlx::query(UNRELATED)
            .bind(from)
            .bind(to)
            .execute(&mut *tx)
            .await?;
        raw_sql(
            "ANALYZE project_events; ANALYZE project_name_authority; ANALYZE project_resources;",
        )
        .execute(&mut *tx)
        .await?;
        assert_eq!(
            rows(&mut tx, current()).await?,
            rows(&mut tx, previous()).await?
        );
        let new = explain(&mut tx, current()).await?;
        ensure!(
            require_keyed_pointer_scans(&new[0]["Plan"], false)? >= 3,
            "expected direct-name, linked-name and linked-resource access: {new}"
        );
        let visited = inspected_rows(&new[0]["Plan"]);
        ensure!(
            visited <= 1024.0,
            "rewrite examined unrelated history: {visited}; {new}"
        );
        let old = explain(&mut tx, previous()).await?;
        let old_visited = inspected_rows(&old[0]["Plan"]);
        ensure!(
            old_visited > visited + 1000.0,
            "fixture did not reproduce original broad scan: {old}"
        );
        evidence.push(json!({"unrelated_rows":to,"old_rows_examined":old_visited,
            "new_rows_examined":visited,"old_plan":old,"new_plan":new}));
    }
    if let Some(dir) = std::env::var_os("BIGNAME_SERVING_PLAN_EVIDENCE_DIR") {
        let dir = std::path::PathBuf::from(dir);
        std::fs::create_dir_all(&dir)?;
        std::fs::write(
            dir.join("serving-pointer-plans.json"),
            serde_json::to_vec_pretty(&evidence)?,
        )?;
    }
    tx.rollback().await?;
    db.cleanup().await?;
    Ok(())
}
