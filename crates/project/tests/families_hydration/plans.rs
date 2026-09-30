use anyhow::Result;
use serde_json::Value;

/// Count source rows visited, including filtered tuples, over every execution of each node.
pub fn visits(plan: &Value, relation: &str) -> f64 {
    fn count(node: &Value, relation: &str) -> f64 {
        let own = if node["Relation Name"] == relation {
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
            .into_iter()
            .flatten()
            .map(|child| count(child, relation))
            .sum::<f64>()
    }
    count(&plan[0]["Plan"], relation)
}

pub fn save(name: &str, plan: &Value) -> Result<()> {
    if let Ok(directory) = std::env::var("BIGNAME_HYDRATION_EVIDENCE_DIR") {
        std::fs::write(
            std::path::Path::new(&directory).join(format!("{name}.json")),
            serde_json::to_vec_pretty(plan)?,
        )?;
    }
    Ok(())
}

/// EXPLAIN an actual server PREPARE/EXECUTE with forced generic planning. Parameterized
/// EXPLAIN alone can produce a custom plan and miss the plan used after repeated sqlx calls.
pub async fn generic(pool: &sqlx::PgPool, statement: &str, arguments: &str) -> Result<Value> {
    let mut transaction = pool.begin().await?;
    sqlx::query("SET LOCAL plan_cache_mode = force_generic_plan")
        .execute(&mut *transaction)
        .await?;
    sqlx::raw_sql(&format!("PREPARE hydration_plan AS {statement}"))
        .execute(&mut *transaction)
        .await?;
    let plan = sqlx::query_scalar(&format!(
        "EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) EXECUTE hydration_plan({arguments})"
    ))
    .fetch_one(&mut *transaction)
    .await?;
    sqlx::raw_sql("DEALLOCATE hydration_plan")
        .execute(&mut *transaction)
        .await?;
    transaction.rollback().await?;
    Ok(plan)
}
