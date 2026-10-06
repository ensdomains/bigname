//! Manual executed/custom and planning-only/generic receipts for the retained count cases.

use std::{
    collections::BTreeMap,
    path::PathBuf,
    sync::{Arc, Mutex},
};

use anyhow::{Context, Result, ensure};
use serde_json::{Value, json};
use sqlx::{Connection, PgConnection, Postgres, QueryBuilder, Row};

use super::{AddressRead, catalogue_count, catalogue_direct_count, seams};
use crate::history::{
    ChainBlockRange, EventHistoryReadFilter, HistoryBlockWindow, HistoryScope, HistorySummaryMode,
};

#[tokio::test]
#[ignore = "manual isolated plans and scalar/working-set receipts on a retained fixture"]
async fn catalogue_count_retained_plans() -> Result<()> {
    let directory = PathBuf::from(std::env::var("BIGNAME_HISTORY_COUNT_PLAN_DIR")?);
    std::fs::create_dir_all(&directory)?;
    let mut connection =
        PgConnection::connect(&std::env::var("BIGNAME_HISTORY_COUNT_PLAN_URL")?).await?;
    sqlx::raw_sql("SET search_path=bigname_phase,public; SET jit=off; SET statement_timeout='10s'; BEGIN READ ONLY")
        .execute(&mut connection).await?;
    let (database, number, hash): (String,i64,String) = sqlx::query_as("SELECT current_database(),current_block_number,input_content_hash FROM project_family_marker WHERE chain_id='ethereum-mainnet'")
        .fetch_one(&mut connection).await?;
    ensure!(database.starts_with("bigname_api_test_tyr235_integrated"));
    ensure!(
        hash == bigname_content_hash::INTERPRETER_CONTENT_HASH,
        "fixture and compiled reader hashes differ"
    );
    ensure!(
        hash == std::env::var("BIGNAME_HISTORY_EXPECTED_HASH")?,
        "compiled hash differs from the pinned integration"
    );
    let published = BTreeMap::from([("ethereum-mainnet".to_owned(), number)]);
    let read = AddressRead {
        address: "0x000000000000000000000000000000000000a235",
        namespace: Some("ens"),
        relations: None,
        scope: HistoryScope::Both,
        canonical_only: true,
        published: Some(&published),
        catalogue: true,
    };
    let base = EventHistoryReadFilter {
        block_window: Some(HistoryBlockWindow {
            ranges: vec![ChainBlockRange {
                chain_id: "ethereum-mainnet".to_owned(),
                from_block: None,
                to_block: Some(number),
            }],
        }),
        ..Default::default()
    };
    let direct = EventHistoryReadFilter {
        event_kinds: vec!["RegistrationGranted".to_owned()],
        ..base.clone()
    };
    let mut plans = Vec::new();
    for (label, limit, scope) in [
        ("direct-capped", Some(10001), HistoryScope::Both),
        ("direct-exact", None, HistoryScope::Both),
        (
            "direct-resource-capped",
            Some(10001),
            HistoryScope::Resource,
        ),
    ] {
        let scoped = AddressRead { scope, ..read };
        let mut query = QueryBuilder::<Postgres>::new("EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) ");
        catalogue_direct_count::push_query(&mut query, &scoped, &direct, limit);
        plans.push(save_plan(&mut connection, &directory, label, query).await?);
    }
    let stats = Arc::new(Mutex::new(seams::AddressHistoryWorkingSet::default()));
    let result = seams::with_address_history_working_set(stats.clone(), async {
        let direct_count =
            catalogue_count::count(&mut connection, &read, &direct, HistorySummaryMode::Count)
                .await?;
        ensure!(direct_count == catalogue_count::CountOutcome::Exact(number as u64));
        Ok::<_, anyhow::Error>(json!({"direct_exact":format!("{direct_count:?}")}))
    })
    .await?;
    let stats = stats.lock().unwrap().clone();
    ensure!(stats.live.values().all(|value| *value == 0));
    let receipt = json!({"database":database,"names":number,"compiled_content_hash":hash,
        "plans":plans,"result":result,"working_set":format!("{stats:?}"),
        "generic_limit":"Generic EXPLAIN is planning only; custom plans are executed with ANALYZE and buffers."});
    std::fs::write(
        directory.join("receipt.json"),
        serde_json::to_vec_pretty(&receipt)?,
    )?;
    println!("{receipt}");
    sqlx::raw_sql("ROLLBACK").execute(&mut connection).await?;
    Ok(())
}

async fn save_plan(
    connection: &mut PgConnection,
    directory: &std::path::Path,
    label: &str,
    mut query: QueryBuilder<'_, Postgres>,
) -> Result<Value> {
    let sql = query.sql().to_owned();
    std::fs::write(directory.join(format!("{label}.sql")), &sql)?;
    let custom: Value = query
        .build_query_scalar()
        .persistent(false)
        .fetch_one(&mut *connection)
        .await
        .with_context(|| label.to_owned())?;
    std::fs::write(
        directory.join(format!("{label}-custom.json")),
        serde_json::to_vec_pretty(&custom)?,
    )?;
    let generic_sql = sql.replacen(
        "EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)",
        "EXPLAIN (GENERIC_PLAN, FORMAT JSON)",
        1,
    );
    let generic: Value = sqlx::raw_sql(&generic_sql)
        .fetch_one(&mut *connection)
        .await?
        .get(0);
    std::fs::write(
        directory.join(format!("{label}-generic.json")),
        serde_json::to_vec_pretty(&generic)?,
    )?;
    let custom_scans = scans(&custom[0]["Plan"]);
    let generic_scans = scans(&generic[0]["Plan"]);
    for scan in custom_scans.iter().chain(&generic_scans) {
        ensure!(
            scan["relation"] != "normalized_events" || scan["type"] != "Seq Scan",
            "unkeyed event scan in {label}: {scan}"
        );
    }
    Ok(
        json!({"label":label,"execution_ms":custom[0]["Execution Time"],"planning_ms":custom[0]["Planning Time"],
        "custom_scans":custom_scans,"generic_scans":generic_scans}),
    )
}

fn scans(node: &Value) -> Vec<Value> {
    let mut result = Vec::new();
    if node.get("Relation Name").is_some() {
        result.push(
            json!({"relation":node["Relation Name"],"type":node["Node Type"],"alias":node["Alias"],
            "index":node["Index Name"],"condition":node["Index Cond"],"rows":node["Actual Rows"],
            "removed":node["Rows Removed by Filter"],"loops":node["Actual Loops"],
            "buffers_hit":node["Shared Hit Blocks"],"buffers_read":node["Shared Read Blocks"]}),
        );
    }
    for child in node["Plans"].as_array().into_iter().flatten() {
        result.extend(scans(child));
    }
    result
}
