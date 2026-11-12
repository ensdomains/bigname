//! Explicit read-only plan capture of the production statements on the retained first gate.

use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
};

use anyhow::{Result, ensure};
use serde_json::{Value, json};
use sqlx::{Connection, PgConnection, Postgres, QueryBuilder, Row};

use super::{AddressRead, catalogue_count, catalogue_fence_tests::ROUTE_KINDS, catalogue_source};
use crate::history::{
    ChainBlockRange, EventHistoryReadFilter, HistoryBlockWindow, HistoryOrder, HistoryScope,
};

#[tokio::test]
#[ignore = "manual read-only plans of the Project-built retained first performance gate"]
async fn address_history_retained_catalogue_plans() -> Result<()> {
    let url = std::env::var("BIGNAME_HISTORY_RETAINED_DATABASE_URL")?;
    let directory = std::env::var("BIGNAME_ADDRESS_HISTORY_PLAN_DIR")?;
    let directory = Path::new(&directory);
    std::fs::create_dir_all(directory)?;
    let mut connection = PgConnection::connect(&url).await?;
    sqlx::raw_sql(
        "SET search_path=bigname_phase,public; SET jit=off; SET statement_timeout='120s'; BEGIN READ ONLY",
    )
    .execute(&mut connection)
    .await?;
    let published = BTreeMap::from([("ethereum-mainnet".to_owned(), 40_821)]);
    let relations = [crate::AddressNameRelation::TokenHolder];
    let read = AddressRead {
        address: "0x000000000000000000000000000000000000a235",
        namespace: Some("ens"),
        relations: Some(&relations),
        scope: HistoryScope::Both,
        canonical_only: true,
        published: Some(&published),
        catalogue: true,
    };
    let filter = EventHistoryReadFilter {
        order: HistoryOrder::Desc,
        // The route's default product kinds. Keep in step with product_history_event_kinds.
        event_kinds: [
            "AuthorityEpochChanged",
            "AuthorityTransferred",
            "EACRolesChanged",
            "ExpiryChanged",
            "LabelRegistered",
            "MigrationApplied",
            "PermissionChanged",
            "PermissionScopeChanged",
            "RecordChanged",
            "RecordVersionChanged",
            "RegistrationGranted",
            "RegistrationReleased",
            "RegistrationRenewed",
            "ResolverChanged",
            "ReverseChanged",
            "RolesChanged",
            "RootPermissionChanged",
            "SubregistryChanged",
            "TokenControlTransferred",
        ]
        .map(str::to_owned)
        .to_vec(),
        block_window: Some(HistoryBlockWindow {
            ranges: vec![ChainBlockRange {
                chain_id: "ethereum-mainnet".to_owned(),
                from_block: None,
                to_block: Some(40_821),
            }],
        }),
        ..Default::default()
    };
    let prefix = "EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) ";
    let mut query = QueryBuilder::<Postgres>::new(prefix);
    catalogue_source::push_seek_query(&mut query, &read, &filter, None);
    explain(&mut connection, query, directory, "initial-seek").await?;
    let mut query = QueryBuilder::<Postgres>::new("");
    catalogue_source::push_seek_query(&mut query, &read, &filter, None);
    let bucket: Option<i64> = query
        .build_query_scalar()
        .fetch_one(&mut connection)
        .await?;
    let bucket = bucket.expect("the retained first-gate owner has history");

    let mut query = QueryBuilder::<Postgres>::new(prefix);
    catalogue_source::push_candidate_query(&mut query, &read, &filter, None, bucket, None, 2);
    explain(&mut connection, query, directory, "page-one-prefix").await?;

    let mut query = QueryBuilder::<Postgres>::new(prefix);
    catalogue_count::push_names_query(&mut query, &read, &filter, None);
    explain(&mut connection, query, directory, "count-names-first").await?;
    let mut query = QueryBuilder::<Postgres>::new("");
    catalogue_count::push_names_query(&mut query, &read, &filter, None);
    let names: Vec<String> = query
        .build_query_scalar()
        .fetch_all(&mut connection)
        .await?;
    ensure!(
        names.len() == 256,
        "expected a fixed complete proof-name batch"
    );
    let mut query = QueryBuilder::<Postgres>::new(prefix);
    catalogue_count::push_proof_query(&mut query, &read, &filter, &names, 10_001);
    explain(&mut connection, query, directory, "count-proof-first").await?;
    sqlx::raw_sql("ROLLBACK").execute(&mut connection).await?;
    Ok(())
}

/// The fenced and the previous catalogue probes of one address at one bucket on a retained
/// database, for the before and after record: the candidate statement and the seek from that
/// bucket, each as a custom plan executed under `EXPLAIN (ANALYZE, BUFFERS)` and as a generic
/// plan. Read-only. The address, the bucket and the candidate limit come from the environment;
/// the published bound is the database's family marker.
#[tokio::test]
#[ignore = "manual read-only plans of the fenced and previous catalogue probes for one address"]
async fn address_history_catalogue_fence_plans() -> Result<()> {
    let url = std::env::var("BIGNAME_HISTORY_RETAINED_DATABASE_URL")?;
    let directory = PathBuf::from(std::env::var("BIGNAME_ADDRESS_HISTORY_PLAN_DIR")?);
    std::fs::create_dir_all(&directory)?;
    let address = std::env::var("BIGNAME_HISTORY_FENCE_ADDRESS")?.to_lowercase();
    let bucket: i64 = std::env::var("BIGNAME_HISTORY_FENCE_BUCKET")?.parse()?;
    let limit: i64 = std::env::var("BIGNAME_HISTORY_FENCE_LIMIT")
        .ok()
        .map(|limit| limit.parse())
        .transpose()?
        .unwrap_or(21);
    let chain = "ethereum-mainnet";
    let mut connection = PgConnection::connect(&url).await?;
    sqlx::raw_sql(
        "SET search_path=bigname_phase,public; SET jit=off; SET statement_timeout='280s'; BEGIN READ ONLY",
    )
    .execute(&mut connection)
    .await?;
    let block: i64 = sqlx::query_scalar(
        "SELECT current_block_number FROM project_family_marker WHERE chain_id = $1",
    )
    .bind(chain)
    .fetch_one(&mut connection)
    .await?;
    let published = BTreeMap::from([(chain.to_owned(), block)]);
    let read = AddressRead {
        address: &address,
        namespace: Some("ens"),
        relations: None,
        scope: HistoryScope::Both,
        canonical_only: true,
        published: Some(&published),
        catalogue: true,
    };
    let filter = EventHistoryReadFilter {
        order: HistoryOrder::Desc,
        event_kinds: ROUTE_KINDS.map(str::to_owned).to_vec(),
        block_window: Some(HistoryBlockWindow {
            ranges: vec![ChainBlockRange {
                chain_id: chain.to_owned(),
                from_block: None,
                to_block: Some(block),
            }],
        }),
        ..Default::default()
    };
    for (version, fence) in [("fenced", true), ("previous", false)] {
        for statement in ["candidate", "seek"] {
            let label = format!("{version}-{statement}-{bucket}");
            for generic in [false, true] {
                let mut query = QueryBuilder::<Postgres>::new(if generic {
                    "EXPLAIN (GENERIC_PLAN, FORMAT JSON) "
                } else {
                    "EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) "
                });
                if statement == "candidate" {
                    catalogue_source::push_candidate_statement(
                        &mut query, &read, &filter, None, bucket, None, limit, fence,
                    );
                } else {
                    catalogue_source::push_seek_statement(
                        &mut query,
                        &read,
                        &filter,
                        Some(bucket),
                        fence,
                    );
                }
                let mode = if generic { "generic" } else { "custom" };
                std::fs::write(directory.join(format!("{label}.sql")), query.sql())?;
                let plan: Value = if generic {
                    sqlx::raw_sql(query.sql())
                        .fetch_one(&mut connection)
                        .await?
                        .get(0)
                } else {
                    query
                        .build_query_scalar()
                        .persistent(false)
                        .fetch_one(&mut connection)
                        .await?
                };
                std::fs::write(
                    directory.join(format!("{label}-{mode}.json")),
                    serde_json::to_vec_pretty(&plan)?,
                )?;
                let mut scans = Vec::new();
                collect_scans(&plan[0]["Plan"], &mut scans);
                println!(
                    "{label}-{mode}: {}",
                    json!({"planning_ms":plan[0]["Planning Time"], "execution_ms":plan[0]["Execution Time"], "scans":scans})
                );
            }
        }
    }
    sqlx::raw_sql("ROLLBACK").execute(&mut connection).await?;
    Ok(())
}

async fn explain(
    connection: &mut PgConnection,
    mut query: QueryBuilder<'_, Postgres>,
    directory: &Path,
    label: &str,
) -> Result<()> {
    std::fs::write(directory.join(format!("{label}.sql")), query.sql())?;
    let plan: Value = query
        .build_query_scalar()
        .persistent(false)
        .fetch_one(connection)
        .await?;
    std::fs::write(
        directory.join(format!("{label}.json")),
        serde_json::to_vec_pretty(&plan)?,
    )?;
    let mut scans = Vec::new();
    collect_scans(&plan[0]["Plan"], &mut scans);
    println!(
        "{label}: {}",
        json!({"planning_ms":plan[0]["Planning Time"], "execution_ms":plan[0]["Execution Time"], "scans":scans})
    );
    ensure!(
        scans
            .iter()
            .all(|scan| scan["relation"] != "normalized_events"
                || scan["type"] != "Seq Scan"
                || scan["loops"].as_u64().unwrap_or(0) == 0),
        "{label} performed a whole normalized-event scan"
    );
    Ok(())
}

fn collect_scans(node: &Value, scans: &mut Vec<Value>) {
    if node["Relation Name"].is_string() {
        scans.push(
            json!({"relation":node["Relation Name"], "type":node["Node Type"],
            "index":node["Index Name"], "condition":node["Index Cond"], "rows":node["Actual Rows"],
            "removed":node["Rows Removed by Filter"], "loops":node["Actual Loops"],
            "shared_hit":node["Shared Hit Blocks"], "shared_read":node["Shared Read Blocks"]}),
        );
    }
    for child in node["Plans"].as_array().into_iter().flatten() {
        collect_scans(child, scans);
    }
}

/// The contract side of the `contract_address` walk reads the emitter's own index with the
/// emitter and the block bound in its index condition, and sorts nothing, in both orders and
/// for the null bucket, under a forced generic plan and after six runs of a prepared statement.
#[tokio::test]
async fn emitter_seek_keeps_its_index_under_a_generic_plan() -> Result<()> {
    const EMITTER: &str = "0x0000000000000000000000000000000000000021";
    let database = bigname_test_support::TestDatabase::create(
        bigname_test_support::TestDatabaseConfig::new("catalogue_emitter_seek_plan"),
    )
    .await?;
    let result = async {
        let mut connection = database.pool().acquire().await?;
        super::plan_tests::install(&mut connection, 32, 3_200).await?;
        // Ninety-seven emitters share the fixture's events, so the planner sees a selective key.
        sqlx::raw_sql(
            "UPDATE normalized_events SET raw_fact_ref = jsonb_build_object('emitting_address',
                 '0x' || lpad(to_hex(normalized_event_id % 97), 40, '0'));
             ANALYZE normalized_events",
        )
        .execute(&mut *connection)
        .await?;
        for order in [HistoryOrder::Desc, HistoryOrder::Asc] {
            let filter = EventHistoryReadFilter {
                order,
                ..Default::default()
            };
            for bucket in [0, 5, crate::history::catalogue_contract::NULL_BUCKET] {
                for (mode, runs) in [("force_generic_plan", 1), ("auto", 6)] {
                    let label = format!("{order:?} bucket {bucket} {mode}");
                    let plan = prepared_emitter_plan(
                        &mut connection,
                        EMITTER,
                        &filter,
                        bucket,
                        mode,
                        runs,
                    )
                    .await?;
                    let mut scans = Vec::new();
                    collect_scans(&plan[0]["Plan"], &mut scans);
                    collect_nodes(&plan[0]["Plan"], &mut scans);
                    ensure!(
                        scans.iter().all(|node| node["type"] != "Seq Scan"
                            && node["type"] != "Sort"
                            && node["type"] != "Bitmap Heap Scan"),
                        "{label}: {scans:?}"
                    );
                    let reads = scans
                        .iter()
                        .filter(|scan| scan["relation"] == "normalized_events")
                        .collect::<Vec<_>>();
                    ensure!(!reads.is_empty(), "{label}: {scans:?}");
                    for read in reads {
                        let condition = read["condition"].as_str().unwrap_or_default();
                        ensure!(
                            read["index"] == "normalized_events_emitter_history_idx"
                                && condition.contains("emitting_address")
                                && condition.contains("block_number"),
                            "{label}: {scans:?}"
                        );
                    }
                }
            }
        }
        Ok(())
    }
    .await;
    database.cleanup().await?;
    result
}

fn collect_nodes(node: &Value, nodes: &mut Vec<Value>) {
    nodes.push(json!({"type":node["Node Type"]}));
    for child in node["Plans"].as_array().into_iter().flatten() {
        collect_nodes(child, nodes);
    }
}

/// `EXPLAIN (ANALYZE)` of the emitter seek as a prepared statement in `mode`, after `runs - 1`
/// executions.
async fn prepared_emitter_plan(
    connection: &mut PgConnection,
    emitter: &str,
    filter: &EventHistoryReadFilter,
    bucket: i64,
    mode: &str,
    runs: usize,
) -> Result<Value> {
    use sqlx::{Arguments, Execute};

    let mut transaction = connection.begin().await?;
    sqlx::query(&format!("SET LOCAL plan_cache_mode = {mode}"))
        .execute(&mut *transaction)
        .await?;
    let mut query = QueryBuilder::<Postgres>::new("");
    super::catalogue::push_emitter_seek_query(&mut query, emitter, filter, bucket);
    sqlx::raw_sql(&format!("PREPARE emitter_seek AS {}", query.sql()))
        .execute(&mut *transaction)
        .await?;
    let mut built = query.build();
    let arguments = built
        .take_arguments()
        .map_err(anyhow::Error::from_boxed)?
        .unwrap_or_default();
    // EXECUTE takes no protocol parameters, so PostgreSQL renders each bound value as a quoted
    // literal for it.
    let placeholders = (1..=arguments.len())
        .map(|index| format!("quote_nullable(${index})"))
        .collect::<Vec<_>>()
        .join(", ");
    let literals: Vec<String> =
        sqlx::query_scalar_with(&format!("SELECT ARRAY[{placeholders}]"), arguments)
            .persistent(false)
            .fetch_one(&mut *transaction)
            .await?;
    let execute = format!("EXECUTE emitter_seek({})", literals.join(", "));
    for _ in 1..runs {
        sqlx::raw_sql(&execute).execute(&mut *transaction).await?;
    }
    let plan: Value = sqlx::query_scalar(&format!("EXPLAIN (ANALYZE, FORMAT JSON) {execute}"))
        .persistent(false)
        .fetch_one(&mut *transaction)
        .await?;
    transaction.rollback().await?;
    Ok(plan)
}
